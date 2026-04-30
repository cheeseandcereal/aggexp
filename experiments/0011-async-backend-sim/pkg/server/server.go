// Package server wires the substrate's generic Options into this
// experiment's scheme + async-mock-backed backend.
package server

import (
	"context"
	"time"

	"github.com/spf13/pflag"

	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"

	aggexpv1 "github.com/cheeseandcereal/aggexp/experiments/0011-async-backend-sim/pkg/apis/aggexp/v1"
	"github.com/cheeseandcereal/aggexp/experiments/0011-async-backend-sim/pkg/apiserver"
	"github.com/cheeseandcereal/aggexp/experiments/0011-async-backend-sim/pkg/asyncbackend"
	generatedopenapi "github.com/cheeseandcereal/aggexp/experiments/0011-async-backend-sim/pkg/generated/openapi"
	"github.com/cheeseandcereal/aggexp/runtime/group"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"
	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

type Options struct {
	*runtimeserver.Options

	MockURL      string
	PollInterval time.Duration
}

func NewOptions() *Options {
	return &Options{
		Options:      runtimeserver.NewOptions(),
		PollInterval: 5 * time.Second,
	}
}

func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	o.Options.PolicyGroup = aggexpv1.GroupName
	o.Options.Title = "aggexp-widgets"
	fs.StringVar(&o.MockURL, "async-mock-url", o.MockURL,
		"Base URL of the async mock (e.g. http://async-mock.aggexp-system.svc).")
	fs.DurationVar(&o.PollInterval, "poll-interval", o.PollInterval,
		"How often to poll the mock for watch-event synthesis.")
}

func (o *Options) Validate() error {
	var errs []error
	if err := o.Options.Validate(); err != nil {
		errs = append(errs, err)
	}
	return utilerrors.NewAggregate(errs)
}

func (o *Options) Run(ctx context.Context) error {
	o.Options.PolicyGroup = aggexpv1.GroupName

	backend, err := asyncbackend.New(asyncbackend.Options{
		MockURL:      o.MockURL,
		PollInterval: o.PollInterval,
	})
	if err != nil {
		return err
	}

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

	return o.Options.Run(
		ctx,
		"aggexp-widgets-apiserver",
		runtimeserver.Input{
			Scheme:             apiserver.Scheme,
			Codecs:             apiserver.Codecs,
			OpenAPIDefinitions: generatedopenapi.GetOpenAPIDefinitions,
		},
		[]runtimeserver.GroupInstaller{g},
		map[string]runtimeserver.PostStartFunc{
			"async-poller": func(hookCtx context.Context) error {
				backend.Start(hookCtx)
				go func() {
					<-hookCtx.Done()
					widgets.Shutdown()
				}()
				return nil
			},
		},
	)
}

var _ *genericapiserver.GenericAPIServer // keep import
