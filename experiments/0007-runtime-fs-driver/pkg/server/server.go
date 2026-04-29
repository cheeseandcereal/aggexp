// Package server wires the substrate's generic Options into this
// experiment's scheme + backend. The substrate does the heavy
// lifting; this file is deliberately short (the whole point of
// extracting runtime/).
package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/pflag"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"

	aggexpv1 "github.com/cheeseandcereal/aggexp/experiments/0007-runtime-fs-driver/pkg/apis/aggexp/v1"
	"github.com/cheeseandcereal/aggexp/experiments/0007-runtime-fs-driver/pkg/apiserver"
	"github.com/cheeseandcereal/aggexp/experiments/0007-runtime-fs-driver/pkg/fsbackend"
	generatedopenapi "github.com/cheeseandcereal/aggexp/experiments/0007-runtime-fs-driver/pkg/generated/openapi"
	"github.com/cheeseandcereal/aggexp/runtime/group"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"
	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// Options is the experiment's option set: the substrate options
// embedded verbatim, plus per-experiment knobs.
type Options struct {
	*runtimeserver.Options

	FSRoot         string
	FSPollInterval time.Duration
}

// NewOptions constructs defaults.
func NewOptions() *Options {
	return &Options{
		Options:        runtimeserver.NewOptions(),
		FSPollInterval: 5 * time.Second,
	}
}

// AddFlags registers flags, including the substrate flags.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	o.Options.PolicyGroup = aggexpv1.GroupName
	o.Options.Title = "aggexp-files"
	fs.StringVar(&o.FSRoot, "fs-root", o.FSRoot, "Absolute path whose contents are projected as File resources.")
	fs.DurationVar(&o.FSPollInterval, "fs-poll-interval", o.FSPollInterval, "How often to re-scan fs-root.")
}

// Validate composes the substrate validator with our own.
func (o *Options) Validate() error {
	var errs []error
	if err := o.Options.Validate(); err != nil {
		errs = append(errs, err)
	}
	if strings.TrimSpace(o.FSRoot) == "" {
		errs = append(errs, fmt.Errorf("--fs-root is required"))
	}
	return utilerrors.NewAggregate(errs)
}

// Run wires the fs backend into the substrate's Run.
func (o *Options) Run(ctx context.Context) error {
	o.Options.PolicyGroup = aggexpv1.GroupName

	backend := fsbackend.New(fsbackend.Options{
		Root:         o.FSRoot,
		PollInterval: o.FSPollInterval,
	})
	files := runtimestorage.New(runtimestorage.Options{
		Backend:       backend,
		GroupResource: schema.GroupResource{Group: aggexpv1.GroupName, Resource: "files"},
	})
	backend.SetPublisher(files)

	g := &group.Group{
		GroupVersion:   aggexpv1.SchemeGroupVersion,
		Scheme:         apiserver.Scheme,
		Codecs:         apiserver.Codecs,
		ParameterCodec: apiserver.ParameterCodec,
		Resources:      map[string]rest.Storage{"files": files},
	}

	return o.Options.Run(
		ctx,
		"aggexp-files-apiserver",
		runtimeserver.Input{
			Scheme:             apiserver.Scheme,
			Codecs:             apiserver.Codecs,
			OpenAPIDefinitions: generatedopenapi.GetOpenAPIDefinitions,
		},
		[]runtimeserver.GroupInstaller{g},
		map[string]runtimeserver.PostStartFunc{
			"fs-scanner": func(hookCtx context.Context) error {
				backend.Start(hookCtx)
				go func() {
					<-hookCtx.Done()
					files.Shutdown()
				}()
				return nil
			},
		},
	)
}

// Compile-time assertion that Group can be installed into a server.
var _ = func() *runtimeserver.Options { return (*runtimeserver.Options)(nil) }
var _ *genericapiserver.GenericAPIServer // keep import
