// Package server wires the substrate's generic Options into
// experiment 0043's scheme + stitched metadata-CR store with host-RV
// authority, an embedded per-object lock, emission filtering, and a
// shared-CRD Widget body backend.
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

	aggexpv1 "github.com/cheeseandcereal/aggexp/experiments/0043-embedded-lock-emission-filtering/pkg/apis/aggexp/v1"
	"github.com/cheeseandcereal/aggexp/experiments/0043-embedded-lock-emission-filtering/pkg/apiserver"
	"github.com/cheeseandcereal/aggexp/experiments/0043-embedded-lock-emission-filtering/pkg/backend"
	generatedopenapi "github.com/cheeseandcereal/aggexp/experiments/0043-embedded-lock-emission-filtering/pkg/generated/openapi"
	"github.com/cheeseandcereal/aggexp/experiments/0043-embedded-lock-emission-filtering/pkg/locking"
	"github.com/cheeseandcereal/aggexp/experiments/0043-embedded-lock-emission-filtering/pkg/metastore"
	"github.com/cheeseandcereal/aggexp/experiments/0043-embedded-lock-emission-filtering/pkg/widgetrest"
	"github.com/cheeseandcereal/aggexp/runtime/group"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"
)

// Options bundles the substrate options with experiment knobs.
type Options struct {
	*runtimeserver.Options

	// ResyncPeriod for the metadata-CRD shared informer.
	ResyncPeriod time.Duration

	// LeaseDuration is the embedded lock's lease duration. Default 15s.
	LeaseDuration time.Duration

	// DisableRenewal turns off the renewal heartbeat (the experiment
	// flag for scenario coverage / sub-lease-latency backends).
	DisableRenewal bool
}

// NewOptions returns Options with defaults.
func NewOptions() *Options {
	return &Options{
		Options:       runtimeserver.NewOptions(),
		ResyncPeriod:  30 * time.Second,
		LeaseDuration: 15 * time.Second,
	}
}

// AddFlags registers CLI flags.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	o.Options.PolicyGroup = aggexpv1.GroupName
	o.Options.Title = "aggexp-widgets"
	fs.DurationVar(&o.ResyncPeriod, "informer-resync", o.ResyncPeriod,
		"resync period for the metadata-CRD shared informer")
	fs.DurationVar(&o.LeaseDuration, "lock-lease-duration", o.LeaseDuration,
		"embedded-lock lease duration (renewal interval is 1/3 of this)")
	fs.BoolVar(&o.DisableRenewal, "disable-lock-renewal", o.DisableRenewal,
		"disable the embedded-lock renewal heartbeat")
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

	// Scenario-3 debug knob: a backend write delay forcing a Put to
	// outlast the lease so the renewal heartbeat is exercised.
	if v := os.Getenv("WIDGET_BACKEND_DELAY_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			backend.DelaySeconds = n
		}
	}

	bodies := backend.New(backend.Options{
		Dynamic:      dyn,
		FieldManager: "aggexp-widgets",
		ReplicaID:    replicaID,
		ResyncPeriod: o.ResyncPeriod,
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
		RenewEnabled:  !o.DisableRenewal,
	})
	widgets := widgetrest.New(store, bodies, locker, replicaID, 100)
	store.SetSink(widgets)
	store.SetStitcher(widgets)

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
		// Start the body informer first so the metastore's informer
		// events (which drive watch fan-out and call StitchForRef →
		// body lookup) find a synced body cache.
		if berr := bodies.Start(hookCtx.Context); berr != nil {
			return berr
		}
		if serr := store.Start(hookCtx.Context); serr != nil {
			return serr
		}
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
