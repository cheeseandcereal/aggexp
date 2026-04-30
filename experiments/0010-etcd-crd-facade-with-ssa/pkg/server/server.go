// Package server wires the substrate's generic Options into this
// experiment's scheme + CRD-backed backend.
package server

import (
	"context"
	"fmt"

	"github.com/spf13/pflag"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/dynamic"
	clientrest "k8s.io/client-go/rest"

	aggexpv1 "github.com/cheeseandcereal/aggexp/experiments/0010-etcd-crd-facade-with-ssa/pkg/apis/aggexp/v1"
	"github.com/cheeseandcereal/aggexp/experiments/0010-etcd-crd-facade-with-ssa/pkg/apiserver"
	"github.com/cheeseandcereal/aggexp/experiments/0010-etcd-crd-facade-with-ssa/pkg/crdbackend"
	generatedopenapi "github.com/cheeseandcereal/aggexp/experiments/0010-etcd-crd-facade-with-ssa/pkg/generated/openapi"
	"github.com/cheeseandcereal/aggexp/runtime/group"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"
	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// Options bundles the substrate options with experiment-specific knobs.
type Options struct {
	*runtimeserver.Options
}

// NewOptions returns Options with defaults.
func NewOptions() *Options {
	return &Options{
		Options: runtimeserver.NewOptions(),
	}
}

// AddFlags registers CLI flags.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	o.Options.PolicyGroup = aggexpv1.GroupName
	o.Options.Title = "aggexp-widgets"
}

// Validate composes the substrate validator.
func (o *Options) Validate() error {
	var errs []error
	if err := o.Options.Validate(); err != nil {
		errs = append(errs, err)
	}
	return utilerrors.NewAggregate(errs)
}

// Run wires the CRD-backed backend into the substrate's Run.
func (o *Options) Run(ctx context.Context) error {
	o.Options.PolicyGroup = aggexpv1.GroupName

	// Build the substrate config. We reuse o.Options.Config so the
	// loopback client, auth config, OpenAPI etc. are all set up.
	cfg, err := o.Options.Config(runtimeserver.Input{
		Scheme:             apiserver.Scheme,
		Codecs:             apiserver.Codecs,
		OpenAPIDefinitions: generatedopenapi.GetOpenAPIDefinitions,
	})
	if err != nil {
		return err
	}

	// The AA talks to the host kube-apiserver via the CoreAPI-configured
	// client. CoreAPI.ApplyTo (invoked inside Config) populates
	// cfg.ClientConfig when --kubeconfig is provided or we are
	// running in-cluster. We build a dynamic client off it.
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

	backend := crdbackend.New(crdbackend.Options{Dynamic: dyn})
	widgets := runtimestorage.New(runtimestorage.Options{
		Backend:       backend,
		GroupResource: schema.GroupResource{Group: aggexpv1.GroupName, Resource: "widgets"},
	})
	backend.SetPublisher(widgets)

	g := &group.Group{
		GroupVersion:   aggexpv1.SchemeGroupVersion,
		Scheme:         apiserver.Scheme,
		Codecs:         apiserver.Codecs,
		ParameterCodec: apiserver.ParameterCodec,
		Resources:      map[string]rest.Storage{"widgets": widgets},
	}

	// Complete & build server. We drive the full pipeline
	// ourselves because we already materialized cfg to obtain the
	// ClientConfig above.
	completed := cfg.Complete()
	srv, err := completed.New("aggexp-widgets-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return fmt.Errorf("creating apiserver: %w", err)
	}
	if err := srv.AddPostStartHook("crd-watch", func(hookCtx genericapiserver.PostStartHookContext) error {
		backend.Start(hookCtx.Context)
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
