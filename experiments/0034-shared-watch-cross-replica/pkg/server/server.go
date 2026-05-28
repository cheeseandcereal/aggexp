// Package server wires the substrate's generic Options into this
// experiment's scheme + CRD-backed shared-informer backend with
// host-CRD RV authority.
package server

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/pflag"

	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/dynamic"
	clientrest "k8s.io/client-go/rest"

	aggexpv1 "github.com/cheeseandcereal/aggexp/experiments/0034-shared-watch-cross-replica/pkg/apis/aggexp/v1"
	"github.com/cheeseandcereal/aggexp/experiments/0034-shared-watch-cross-replica/pkg/apiserver"
	"github.com/cheeseandcereal/aggexp/experiments/0034-shared-watch-cross-replica/pkg/crdbackend"
	generatedopenapi "github.com/cheeseandcereal/aggexp/experiments/0034-shared-watch-cross-replica/pkg/generated/openapi"
	"github.com/cheeseandcereal/aggexp/experiments/0034-shared-watch-cross-replica/pkg/shared"
	"github.com/cheeseandcereal/aggexp/runtime/group"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"
)

// Options bundles the substrate options with experiment-specific knobs.
type Options struct {
	*runtimeserver.Options

	// WidgetNamespace is the namespace on the host cluster where
	// the WidgetStorage CRs (and exposed Widget resources) live.
	WidgetNamespace string
}

// NewOptions returns Options with defaults.
func NewOptions() *Options {
	return &Options{
		Options:         runtimeserver.NewOptions(),
		WidgetNamespace: "aggexp-widgets",
	}
}

// AddFlags registers CLI flags.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	o.Options.PolicyGroup = aggexpv1.GroupName
	o.Options.Title = "aggexp-widgets"
	fs.StringVar(&o.WidgetNamespace, "widget-namespace", o.WidgetNamespace,
		"namespace on the host cluster where WidgetStorage CRs live")
}

// Validate composes the substrate validator.
func (o *Options) Validate() error {
	var errs []error
	if err := o.Options.Validate(); err != nil {
		errs = append(errs, err)
	}
	if o.WidgetNamespace == "" {
		errs = append(errs, fmt.Errorf("--widget-namespace is required"))
	}
	return utilerrors.NewAggregate(errs)
}

// Run wires the CRD-backed backend into the substrate's Run.
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
	if cfg.ClientConfig != nil {
		restCfg = clientrest.CopyConfig(cfg.ClientConfig)
	} else if cfg.LoopbackClientConfig != nil {
		restCfg = clientrest.CopyConfig(cfg.LoopbackClientConfig)
	} else {
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

	backend := crdbackend.New(crdbackend.Options{
		Dynamic:      dyn,
		Namespace:    o.WidgetNamespace,
		ReplicaID:    replicaID,
		ResyncPeriod: 30 * time.Second,
	})
	widgets := shared.New(shared.Options{
		Backend:       backend,
		GroupResource: schema.GroupResource{Group: aggexpv1.GroupName, Resource: "widgets"},
	})
	backend.SetSink(widgets)

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
	if err := srv.AddPostStartHook("informer-start", func(hookCtx genericapiserver.PostStartHookContext) error {
		if serr := backend.Start(hookCtx.Context); serr != nil {
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
