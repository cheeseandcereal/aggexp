// Package server wires the substrate's generic Options into
// experiment 0047 (composed 0043+0044) scheme + stitched metadata-CR store with host-RV
// authority and a per-watcher, identity-aware Widget body backend.
package server

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/pflag"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/dynamic"
	clientrest "k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	aggexpv1 "github.com/cheeseandcereal/aggexp/experiments/0047-host-etcd-write-ceiling/pkg/apis/aggexp/v1"
	"github.com/cheeseandcereal/aggexp/experiments/0047-host-etcd-write-ceiling/pkg/apiserver"
	"github.com/cheeseandcereal/aggexp/experiments/0047-host-etcd-write-ceiling/pkg/backend"
	generatedopenapi "github.com/cheeseandcereal/aggexp/experiments/0047-host-etcd-write-ceiling/pkg/generated/openapi"
	"github.com/cheeseandcereal/aggexp/experiments/0047-host-etcd-write-ceiling/pkg/locking"
	"github.com/cheeseandcereal/aggexp/experiments/0047-host-etcd-write-ceiling/pkg/metastore"
	perwatch "github.com/cheeseandcereal/aggexp/experiments/0047-host-etcd-write-ceiling/pkg/watch"
	"github.com/cheeseandcereal/aggexp/experiments/0047-host-etcd-write-ceiling/pkg/widgetrest"
	"github.com/cheeseandcereal/aggexp/runtime/group"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"
)

// Options bundles the substrate options with experiment knobs.
type Options struct {
	*runtimeserver.Options

	// ResyncPeriod for the metadata-CRD and body-CRD shared informers.
	ResyncPeriod time.Duration

	// WatchMode selects the per-watcher live source: "push" or "poll".
	WatchMode string

	// SharedPoll runs one system-identity poll loop for all watchers
	// (no per-user authz) instead of per-watcher backend access.
	SharedPoll bool

	// PollInterval is the per-watcher / shared poll cadence.
	PollInterval time.Duration

	// UpstreamBudget caps concurrent backend push subscriptions
	// (internal-multiplex pressure knob). 0 = unlimited.
	UpstreamBudget int

	// BufferSize is the per-watcher and per-subscriber channel buffer.
	BufferSize int

	// MetricsInterval logs backend + hub counters at this cadence.
	MetricsInterval time.Duration

	// LeaseDuration is the embedded-lock lease (0043). Renewal fires
	// every LeaseDuration/3 during a slow op.
	LeaseDuration time.Duration

	// BackendWriteDelay forces renewal heartbeats by making each
	// backend body Put take this long (0047 scenario 2 slow-backend).
	BackendWriteDelay time.Duration

	// ClientQPS / ClientBurst raise the dynamic client's rate limit so
	// the host kube-apiserver / etcd (not client-go's default QPS=5)
	// is the throughput gate when measuring the write ceiling.
	ClientQPS   int
	ClientBurst int
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
		MetricsInterval: 10 * time.Second,
		LeaseDuration:   locking.DefaultLeaseDuration,
		ClientQPS:       500,
		ClientBurst:     1000,
	}
}

// AddFlags registers CLI flags.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	o.Options.PolicyGroup = aggexpv1.GroupName
	o.Options.Title = "aggexp-widgets"
	fs.DurationVar(&o.ResyncPeriod, "informer-resync", o.ResyncPeriod,
		"resync period for the metadata-CRD shared informer")
	fs.StringVar(&o.WatchMode, "watch-mode", o.WatchMode,
		"per-watcher live source: push | poll")
	fs.BoolVar(&o.SharedPoll, "shared-poll", o.SharedPoll,
		"run ONE system-identity poll loop for all watchers (cheaper; does NOT enforce per-user authz)")
	fs.DurationVar(&o.PollInterval, "poll-interval", o.PollInterval,
		"per-watcher / shared poll cadence")
	fs.IntVar(&o.UpstreamBudget, "upstream-budget", o.UpstreamBudget,
		"cap on concurrent backend push subscriptions (internal-multiplex pressure knob); 0 = unlimited")
	fs.IntVar(&o.BufferSize, "watch-buffer", o.BufferSize,
		"per-watcher channel buffer size")
	fs.DurationVar(&o.MetricsInterval, "metrics-interval", o.MetricsInterval,
		"interval to log backend + hub instrumentation counters")
	fs.DurationVar(&o.LeaseDuration, "lease-duration", o.LeaseDuration,
		"embedded-lock lease duration (renewal fires every lease/3 during a slow op)")
	fs.DurationVar(&o.BackendWriteDelay, "backend-write-delay", o.BackendWriteDelay,
		"artificial backend body-write delay; forces lock-renewal heartbeats (0047 scenario 2)")
	fs.IntVar(&o.ClientQPS, "client-qps", o.ClientQPS,
		"dynamic-client QPS to the host kube-apiserver (raised so host etcd, not client-go throttling, gates throughput)")
	fs.IntVar(&o.ClientBurst, "client-burst", o.ClientBurst,
		"dynamic-client burst to the host kube-apiserver")
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
	o.Options.PolicyGroup = aggexpv1.GroupName

	cfg, err := o.Options.Config(runtimeserver.Input{
		Scheme:             apiserver.Scheme,
		Codecs:             apiserver.Codecs,
		OpenAPIDefinitions: generatedopenapi.GetOpenAPIDefinitions,
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
	// 0047: the dynamic client to the host kube-apiserver is in the hot
	// path of EVERY served write (acquire CAS + body Put + commit CAS).
	// client-go's default QPS=5/burst=10 would make THAT the throughput
	// gate, not host etcd — defeating the ceiling measurement. Raise it
	// so the bottleneck is the host kube-apiserver / etcd, which is the
	// thing under test. (Arbitrary headroom; recorded in the README.)
	restCfg.QPS = float32(o.ClientQPS)
	restCfg.Burst = o.ClientBurst
	dyn, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("building dynamic client: %w", err)
	}

	replicaID := os.Getenv("HOSTNAME")
	if replicaID == "" {
		replicaID = "<unknown>"
	}

	bodies := backend.New(backend.Options{
		Dynamic:        dyn,
		FieldManager:   "aggexp-widgets",
		ReplicaID:      replicaID,
		ResyncPeriod:   o.ResyncPeriod,
		UpstreamBudget: o.UpstreamBudget,
		WriteDelay:     o.BackendWriteDelay,
	})
	store := metastore.New(metastore.Options{
		Dynamic:      dyn,
		FieldManager: "aggexp-widgets",
		Group:        aggexpv1.GroupName,
		Resource:     "widgets",
		ReplicaID:    replicaID,
		ResyncPeriod: o.ResyncPeriod,
	})
	locker := locking.New(locking.Options{
		Store:         store,
		GroupResource: schema.GroupResource{Group: aggexpv1.GroupName, Resource: "widgets"},
		Identity:      replicaID,
		LeaseDuration: o.LeaseDuration,
		RenewEnabled:  true,
	})

	mode := perwatch.ModePush
	if o.WatchMode == "poll" {
		mode = perwatch.ModePoll
	}
	widgets := widgetrest.New(store, bodies, locker, replicaID, mode, o.SharedPoll, o.PollInterval, o.BufferSize)
	// Per-watcher Hub consumes raw metadata-CR informer events.
	store.SetRawSink(widgets)

	g := &group.Group{
		GroupVersion:   aggexpv1.SchemeGroupVersion,
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
	if err := srv.AddPostStartHook("aggexp-0047-start", func(hookCtx genericapiserver.PostStartHookContext) error {
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
		// Instrumentation: periodically log backend + hub counters so
		// the 0047 scenarios can read backend-call volume, watcher
		// count, and Get-cache hit/miss from the logs.
		go func() {
			t := time.NewTicker(o.MetricsInterval)
			defer t.Stop()
			for {
				select {
				case <-hookCtx.Context.Done():
					return
				case <-t.C:
					klog.InfoS("aggexp-0047-metrics",
						"replica", replicaID,
						"mode", o.WatchMode,
						"sharedPoll", o.SharedPoll,
						"upstreamBudget", o.UpstreamBudget,
						"leaseDuration", o.LeaseDuration.String(),
						"backendWriteDelay", o.BackendWriteDelay.String(),
						"backend", bodies.Counters.Snapshot(),
						"hub", widgets.Hub().Counters.Snapshot(),
						"metaWrites", store.WriteCounters.Snapshot(),
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
