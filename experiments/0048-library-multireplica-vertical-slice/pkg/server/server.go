// Package server wires the substrate's generic Options into the
// experiment 0048 CAPSTONE: the 0046-generated widgets.aggexp.io
// scheme + the stitched metadata-CR store with host-RV authority
// (0042) + the embedded lock (0043) + the per-watcher identity-aware
// watch (0044) + the read-path reconcile (0045), over the shared body
// CRD backend.
package server

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/spf13/pflag"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/dynamic"
	clientrest "k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	widgetsv1 "github.com/cheeseandcereal/aggexp/experiments/0048-library-multireplica-vertical-slice/pkg/apis/widgets/v1"
	"github.com/cheeseandcereal/aggexp/experiments/0048-library-multireplica-vertical-slice/pkg/apiserver"
	"github.com/cheeseandcereal/aggexp/experiments/0048-library-multireplica-vertical-slice/pkg/backend"
	"github.com/cheeseandcereal/aggexp/experiments/0048-library-multireplica-vertical-slice/pkg/locking"
	"github.com/cheeseandcereal/aggexp/experiments/0048-library-multireplica-vertical-slice/pkg/metastore"
	"github.com/cheeseandcereal/aggexp/experiments/0048-library-multireplica-vertical-slice/pkg/metrics"
	"github.com/cheeseandcereal/aggexp/experiments/0048-library-multireplica-vertical-slice/pkg/sweep"
	perwatch "github.com/cheeseandcereal/aggexp/experiments/0048-library-multireplica-vertical-slice/pkg/watch"
	"github.com/cheeseandcereal/aggexp/experiments/0048-library-multireplica-vertical-slice/pkg/widgetrest"
	"github.com/cheeseandcereal/aggexp/runtime/group"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"
)

// Options bundles the substrate options with experiment knobs.
type Options struct {
	*runtimeserver.Options

	ResyncPeriod   time.Duration
	WatchMode      string
	SharedPoll     bool
	PollInterval   time.Duration
	UpstreamBudget int
	BufferSize     int

	LeaseDuration time.Duration

	AdoptEnabled bool
	GCEnabled    bool
	MinAge       time.Duration
	NegCache     bool
	SweepInterval time.Duration
	DebugAddr    string

	MetricsInterval time.Duration
}

// NewOptions returns Options with defaults.
func NewOptions() *Options {
	return &Options{
		Options:         runtimeserver.NewOptions(),
		ResyncPeriod:    30 * time.Second,
		WatchMode:       "push",
		SharedPoll:      false,
		PollInterval:    5 * time.Second,
		UpstreamBudget:  0,
		BufferSize:      100,
		LeaseDuration:   15 * time.Second,
		AdoptEnabled:    true,
		GCEnabled:       true,
		MinAge:          30 * time.Second,
		NegCache:        false,
		SweepInterval:   2 * time.Minute,
		DebugAddr:       ":8444",
		MetricsInterval: 10 * time.Second,
	}
}

// AddFlags registers CLI flags.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	o.Options.PolicyGroup = widgetsv1.GroupName
	o.Options.Title = "aggexp-widgets"
	fs.DurationVar(&o.ResyncPeriod, "informer-resync", o.ResyncPeriod, "resync period for the metadata-CRD + body-CRD shared informers")
	fs.StringVar(&o.WatchMode, "watch-mode", o.WatchMode, "per-watcher live source: push | poll")
	fs.BoolVar(&o.SharedPoll, "shared-poll", o.SharedPoll, "run ONE system-identity poll loop for all watchers (no per-user watch authz)")
	fs.DurationVar(&o.PollInterval, "poll-interval", o.PollInterval, "per-watcher / shared poll cadence")
	fs.IntVar(&o.UpstreamBudget, "upstream-budget", o.UpstreamBudget, "cap on concurrent backend push subscriptions; 0 = unlimited")
	fs.IntVar(&o.BufferSize, "watch-buffer", o.BufferSize, "per-watcher channel buffer size")
	fs.DurationVar(&o.LeaseDuration, "lease-duration", o.LeaseDuration, "embedded-lock lease duration")
	fs.BoolVar(&o.AdoptEnabled, "adopt", o.AdoptEnabled, "read-path reconcile: adopt unknown backend objects")
	fs.BoolVar(&o.GCEnabled, "gc", o.GCEnabled, "read-path reconcile: collect orphan metadata records")
	fs.DurationVar(&o.MinAge, "collect-min-age", o.MinAge, "grace window before an orphan record is collected")
	fs.BoolVar(&o.NegCache, "neg-cache", o.NegCache, "enable the backend negative-existence cache")
	fs.DurationVar(&o.SweepInterval, "sweep-interval", o.SweepInterval, "periodic read-path reconcile sweep interval")
	fs.StringVar(&o.DebugAddr, "debug-addr", o.DebugAddr, "debug HTTP server address (counters + reconcile toggles)")
	fs.DurationVar(&o.MetricsInterval, "metrics-interval", o.MetricsInterval, "interval to log backend + hub instrumentation counters")
}

// Validate composes the substrate validator.
func (o *Options) Validate() error {
	var errs []error
	if err := o.Options.Validate(); err != nil {
		errs = append(errs, err)
	}
	if o.WatchMode != "push" && o.WatchMode != "poll" {
		errs = append(errs, fmt.Errorf("invalid --watch-mode %q (want push|poll)", o.WatchMode))
	}
	return utilerrors.NewAggregate(errs)
}

// Run wires everything and runs the apiserver.
func (o *Options) Run(ctx context.Context) error {
	o.Options.PolicyGroup = widgetsv1.GroupName

	cfg, err := o.Options.Config(runtimeserver.Input{
		Scheme:             apiserver.Scheme,
		Codecs:             apiserver.Codecs,
		OpenAPIDefinitions: widgetsv1.GetOpenAPIDefinitions,
	})
	if err != nil {
		return err
	}

	var restCfg *clientrest.Config
	switch {
	case cfg.ClientConfig != nil:
		restCfg = clientrest.CopyConfig(cfg.ClientConfig)
	case cfg.LoopbackClientConfig != nil:
		restCfg = clientrest.CopyConfig(cfg.LoopbackClientConfig)
	default:
		return fmt.Errorf("no client config available (CoreAPI / loopback); cannot talk to host cluster")
	}
	// FINDINGS/0042-0047: the dynamic client's default QPS of 5 throttles
	// multi-replica throughput (each served write is 2 CR writes plus a
	// body write, and read-path reconcile adds a host read per Get).
	// Raise it well above the default.
	restCfg.QPS = 200
	restCfg.Burst = 400
	dyn, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("building dynamic client: %w", err)
	}

	replicaID := os.Getenv("HOSTNAME")
	if replicaID == "" {
		replicaID = "<unknown>"
	}
	if v := os.Getenv("WIDGET_BACKEND_DELAY_SECONDS"); v != "" {
		if n, perr := strconv.Atoi(v); perr == nil {
			backend.DelaySeconds = n
			klog.InfoS("backend-delay-set", "seconds", n)
		}
	}

	counters := &metrics.Counters{}

	bodies := backend.New(backend.Options{
		Dynamic:         dyn,
		FieldManager:    "aggexp-widgets",
		ReplicaID:       replicaID,
		ResyncPeriod:    o.ResyncPeriod,
		UpstreamBudget:  o.UpstreamBudget,
		Metrics:         counters,
		NegCacheEnabled: o.NegCache,
	})
	store := metastore.New(metastore.Options{
		Dynamic:      dyn,
		FieldManager: "aggexp-widgets",
		Group:        widgetsv1.GroupName,
		Resource:     "widgets",
		ReplicaID:    replicaID,
		ResyncPeriod: o.ResyncPeriod,
	})
	locker := locking.New(locking.Options{
		Store:         store,
		GroupResource: schema.GroupResource{Group: widgetsv1.GroupName, Resource: "widgets"},
		Identity:      replicaID,
		LeaseDuration: o.LeaseDuration,
		RenewEnabled:  true,
	})

	mode := perwatch.ModePush
	if o.WatchMode == "poll" {
		mode = perwatch.ModePoll
	}
	widgets := widgetrest.New(widgetrest.Config{
		Store:        store,
		Bodies:       bodies,
		Locker:       locker,
		Counters:     counters,
		ReplicaID:    replicaID,
		Mode:         mode,
		SharedPoll:   o.SharedPoll,
		PollInterval: o.PollInterval,
		BufferSize:   o.BufferSize,
		MinAge:       o.MinAge,
		AdoptEnabled: o.AdoptEnabled,
		GCEnabled:    o.GCEnabled,
	})
	// Per-watcher Hub consumes raw metadata-CR informer events (with the
	// decoded Record so its re-homed emission filter can suppress lock
	// churn).
	store.SetRawSink(widgets)

	sweeper := sweep.New(sweep.Options{
		REST:     widgets,
		Bodies:   bodies,
		Counters: counters,
		Interval: o.SweepInterval,
	})

	g := &group.Group{
		GroupVersion:   widgetsv1.SchemeGroupVersion,
		Scheme:         apiserver.Scheme,
		Codecs:         apiserver.Codecs,
		ParameterCodec: apiserver.ParameterCodec,
		Resources:      map[string]rest.Storage{"widgets": widgets},
	}

	completed := cfg.Complete()
	srv, err := completed.New("aggexp-widgets-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return fmt.Errorf("creating apiserver: %w", err)
	}
	if err := srv.AddPostStartHook("aggexp-0048-start", func(hookCtx genericapiserver.PostStartHookContext) error {
		if berr := bodies.Start(hookCtx.Context); berr != nil {
			return berr
		}
		if serr := store.Start(hookCtx.Context); serr != nil {
			return serr
		}
		if o.SharedPoll {
			go widgets.Hub().RunSharedPoll(hookCtx.Context)
			klog.InfoS("shared-poll-loop-started", "replica", replicaID, "interval", o.PollInterval)
		}
		go sweeper.RunPeriodic(hookCtx.Context)
		go func() {
			if derr := sweeper.ServeDebug(hookCtx.Context, o.DebugAddr); derr != nil {
				klog.Warningf("debug server: %v", derr)
			}
		}()
		go func() {
			t := time.NewTicker(o.MetricsInterval)
			defer t.Stop()
			for {
				select {
				case <-hookCtx.Context.Done():
					return
				case <-t.C:
					klog.InfoS("aggexp-0048-metrics",
						"replica", replicaID,
						"mode", o.WatchMode,
						"sharedPoll", o.SharedPoll,
						"backend", bodies.Counters.Snapshot(),
						"hub", widgets.Hub().Counters.Snapshot(),
						"readPath", counters.Snapshot(),
					)
				}
			}
		}()
		go func() {
			<-hookCtx.Context.Done()
			widgets.Shutdown()
		}()
		return nil
	}); err != nil {
		return err
	}
	if err := g.Install(srv); err != nil {
		return fmt.Errorf("installing group: %w", err)
	}
	prepared := srv.PrepareRun()
	return prepared.RunWithContext(ctx)
}
