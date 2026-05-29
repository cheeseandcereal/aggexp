// Package server wires the substrate's generic Options into
// experiment 0045's scheme + stitched metadata-CR store with host-RV
// authority, a shared-CRD Widget body backend that is the source of
// truth for existence, read-path reconcile (adopt/collect inline on
// Get/List), a periodic reconcile sweep, and a debug HTTP endpoint
// for counters/toggles.
package server

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/pflag"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/dynamic"
	clientrest "k8s.io/client-go/rest"

	aggexpv1 "github.com/cheeseandcereal/aggexp/experiments/0045-read-path-reconcile-amplification/pkg/apis/aggexp/v1"
	"github.com/cheeseandcereal/aggexp/experiments/0045-read-path-reconcile-amplification/pkg/apiserver"
	"github.com/cheeseandcereal/aggexp/experiments/0045-read-path-reconcile-amplification/pkg/backend"
	generatedopenapi "github.com/cheeseandcereal/aggexp/experiments/0045-read-path-reconcile-amplification/pkg/generated/openapi"
	"github.com/cheeseandcereal/aggexp/experiments/0045-read-path-reconcile-amplification/pkg/metastore"
	"github.com/cheeseandcereal/aggexp/experiments/0045-read-path-reconcile-amplification/pkg/metrics"
	"github.com/cheeseandcereal/aggexp/experiments/0045-read-path-reconcile-amplification/pkg/sweep"
	"github.com/cheeseandcereal/aggexp/experiments/0045-read-path-reconcile-amplification/pkg/widgetrest"
	"github.com/cheeseandcereal/aggexp/runtime/group"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"
)

// Options bundles the substrate options with experiment knobs.
type Options struct {
	*runtimeserver.Options

	// ResyncPeriod for the metadata-CRD and body-CRD shared informers.
	ResyncPeriod time.Duration

	// Reconcile policy.
	MinAge       time.Duration
	AdoptEnabled bool
	GCEnabled    bool
	SweepEvery   time.Duration

	// Negative-existence cache (default off; measure un-cached first).
	NegCacheEnabled bool
	NegCacheTTL     time.Duration

	// Debug HTTP endpoint address.
	DebugAddr string
}

// NewOptions returns Options with defaults.
func NewOptions() *Options {
	return &Options{
		Options:         runtimeserver.NewOptions(),
		ResyncPeriod:    30 * time.Second,
		MinAge:          30 * time.Second,
		AdoptEnabled:    true,
		GCEnabled:       true,
		SweepEvery:      2 * time.Minute,
		NegCacheEnabled: false,
		NegCacheTTL:     2 * time.Second,
		DebugAddr:       ":8444",
	}
}

// AddFlags registers CLI flags.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	o.Options.PolicyGroup = aggexpv1.GroupName
	o.Options.Title = "aggexp-widgets"
	fs.DurationVar(&o.ResyncPeriod, "informer-resync", o.ResyncPeriod,
		"resync period for the shared informers")
	fs.DurationVar(&o.MinAge, "min-age", o.MinAge,
		"grace window before an orphan metadata record is collected")
	fs.BoolVar(&o.AdoptEnabled, "adopt", o.AdoptEnabled,
		"adopt backend objects with no metadata record (inline + sweep)")
	fs.BoolVar(&o.GCEnabled, "gc", o.GCEnabled,
		"collect metadata records whose backend object is gone (inline + sweep)")
	fs.DurationVar(&o.SweepEvery, "sweep-every", o.SweepEvery,
		"periodic reconcile sweep interval")
	fs.BoolVar(&o.NegCacheEnabled, "neg-cache", o.NegCacheEnabled,
		"enable the backend negative-existence cache (default off)")
	fs.DurationVar(&o.NegCacheTTL, "neg-cache-ttl", o.NegCacheTTL,
		"TTL for negative-existence cache entries")
	fs.StringVar(&o.DebugAddr, "debug-addr", o.DebugAddr,
		"address for the debug/metrics HTTP endpoint")
}

// Validate composes the substrate validator.
func (o *Options) Validate() error {
	var errs []error
	if err := o.Options.Validate(); err != nil {
		errs = append(errs, err)
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
	dyn, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("building dynamic client: %w", err)
	}

	replicaID := os.Getenv("HOSTNAME")
	if replicaID == "" {
		replicaID = "<unknown>"
	}

	counters := &metrics.Counters{}

	bodies := backend.New(backend.Options{
		Dynamic:         dyn,
		FieldManager:    "aggexp-widgets",
		ReplicaID:       replicaID,
		ResyncPeriod:    o.ResyncPeriod,
		Counters:        counters,
		NegCacheEnabled: o.NegCacheEnabled,
		NegCacheTTL:     o.NegCacheTTL,
	})
	store := metastore.New(metastore.Options{
		Dynamic:      dyn,
		FieldManager: "aggexp-widgets",
		Group:        aggexpv1.GroupName,
		Resource:     "widgets",
		ReplicaID:    replicaID,
		ResyncPeriod: o.ResyncPeriod,
	})
	widgets := widgetrest.New(store, bodies, replicaID, 100, widgetrest.Config{
		Counters:     counters,
		MinAge:       o.MinAge,
		AdoptEnabled: o.AdoptEnabled,
		GCEnabled:    o.GCEnabled,
	})
	store.SetSink(widgets)
	store.SetStitcher(widgets)

	sweeper := sweep.New(sweep.Options{
		REST:     widgets,
		Bodies:   bodies,
		Counters: counters,
		Interval: o.SweepEvery,
	})

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
	if err := srv.AddPostStartHook("metastore-informer-start", func(hookCtx genericapiserver.PostStartHookContext) error {
		if berr := bodies.Start(hookCtx.Context); berr != nil {
			return berr
		}
		if serr := store.Start(hookCtx.Context); serr != nil {
			return serr
		}
		go sweeper.RunPeriodic(hookCtx.Context)
		go func() {
			if derr := sweeper.ServeDebug(hookCtx.Context, o.DebugAddr); derr != nil {
				// Non-fatal: the debug endpoint is a lab instrument.
				fmt.Fprintf(os.Stderr, "debug endpoint exited: %v\n", derr)
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
