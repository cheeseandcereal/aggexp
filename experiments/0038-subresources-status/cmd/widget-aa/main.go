// Command widget-aa is experiment 0038: a library-mode aggregated
// apiserver with a /status subresource for spec/status split.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"

	"github.com/cheeseandcereal/aggexp/experiments/0038-subresources-status/pkg/backend"
	genopenapi "github.com/cheeseandcereal/aggexp/experiments/0038-subresources-status/pkg/openapi"
	"github.com/cheeseandcereal/aggexp/experiments/0038-subresources-status/pkg/specrest"
	"github.com/cheeseandcereal/aggexp/experiments/0038-subresources-status/pkg/statusrest"
	"github.com/cheeseandcereal/aggexp/experiments/0038-subresources-status/pkg/types"
	"github.com/cheeseandcereal/aggexp/runtime/group"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"
	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

var (
	scheme         = runtime.NewScheme()
	codecs         = serializer.NewCodecFactory(scheme)
	parameterCodec = runtime.NewParameterCodec(scheme)
)

func init() {
	gv := schema.GroupVersion{Group: types.GroupName, Version: "v1"}
	internalGV := schema.GroupVersion{Group: types.GroupName, Version: runtime.APIVersionInternal}

	// Register external version
	scheme.AddKnownTypes(gv, &types.Widget{}, &types.WidgetList{})
	metav1.AddToGroupVersion(scheme, gv)

	// Register internal version (required by the library's patch machinery)
	scheme.AddKnownTypes(internalGV, &types.Widget{}, &types.WidgetList{})

	// Identity conversion (internal == external for this experiment)
	utilruntime.Must(scheme.AddConversionFunc((*types.Widget)(nil), (*types.Widget)(nil),
		func(a, b interface{}, _ conversion.Scope) error {
			*b.(*types.Widget) = *a.(*types.Widget)
			return nil
		}))
	utilruntime.Must(scheme.AddConversionFunc((*types.WidgetList)(nil), (*types.WidgetList)(nil),
		func(a, b interface{}, _ conversion.Scope) error {
			*b.(*types.WidgetList) = *a.(*types.WidgetList)
			return nil
		}))

	// Register unversioned types required by the apiserver machinery
	metav1.AddToGroupVersion(scheme, schema.GroupVersion{Version: "v1"})
	unversioned := schema.GroupVersion{Group: "", Version: "v1"}
	utilruntime.Must(scheme.SetVersionPriority(unversioned))
	scheme.AddUnversionedTypes(unversioned,
		&metav1.Status{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	)
}

type options struct {
	*runtimeserver.Options
}

func newOptions() *options {
	return &options{
		Options: runtimeserver.NewOptions(),
	}
}

func (o *options) addFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	o.Options.PolicyGroup = types.GroupName
	o.Options.Title = "aggexp-0038-widget-aa"
}

func (o *options) run(ctx context.Context) error {
	gr := schema.GroupResource{Group: types.GroupName, Resource: "widgets"}
	mem := backend.New()

	store := runtimestorage.New(runtimestorage.Options{
		Backend:       mem,
		GroupResource: gr,
	})

	// The main resource wraps the store to preserve status on Update
	// (spec-only endpoint: Update never changes status).
	mainStore := specrest.New(store)

	// The status subresource shares the same backing store but enforces
	// status-only Update semantics (preserves spec from existing object).
	statusStore := statusrest.New(store)

	g := &group.Group{
		GroupVersion:   schema.GroupVersion{Group: types.GroupName, Version: "v1"},
		Scheme:         scheme,
		Codecs:         codecs,
		ParameterCodec: parameterCodec,
		Resources: map[string]rest.Storage{
			"widgets":        mainStore,
			"widgets/status": statusStore,
		},
	}

	return o.Options.Run(
		ctx,
		"aggexp-0038-widget-aa",
		runtimeserver.Input{
			Scheme:             scheme,
			Codecs:             codecs,
			OpenAPIDefinitions: genopenapi.GetOpenAPIDefinitions,
		},
		[]runtimeserver.GroupInstaller{g},
		map[string]runtimeserver.PostStartFunc{
			"shutdown": func(hookCtx context.Context) error {
				go func() {
					<-hookCtx.Done()
					store.Shutdown()
				}()
				return nil
			},
		},
	)
}

func main() {
	opts := newOptions()
	cmd := &cobra.Command{
		Use:   "widget-aa",
		Short: "0038: library-mode AA with /status subresource",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := opts.Options.Validate(); err != nil {
				return err
			}
			ctx := genericapiserver.SetupSignalContext()
			return opts.run(ctx)
		},
	}
	opts.addFlags(cmd.Flags())
	if code := cli.Run(cmd); code != 0 {
		fmt.Fprintln(os.Stderr, "widget-aa exited with error")
		os.Exit(code)
	}
}
