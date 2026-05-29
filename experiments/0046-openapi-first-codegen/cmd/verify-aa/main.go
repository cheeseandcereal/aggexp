// Command verify-aa is a tiny library-mode aggregated apiserver built
// entirely on the oapigen-generated Widget API package (experiment
// 0046). It exists only to verify wire parity — explain, server-side
// apply, and field selectors — on the generated types. There is no
// hand-written scheme/types/openapi-gen boilerplate here: everything
// type-level comes from pkg/apis/widgets/v1.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/component-base/cli"

	v1 "github.com/cheeseandcereal/aggexp/experiments/0046-openapi-first-codegen/pkg/apis/widgets/v1"
	"github.com/cheeseandcereal/aggexp/experiments/0046-openapi-first-codegen/pkg/backend"
	"github.com/cheeseandcereal/aggexp/runtime/group"
	"github.com/cheeseandcereal/aggexp/runtime/library"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"
)

var (
	scheme         = runtime.NewScheme()
	codecs         = serializer.NewCodecFactory(scheme)
	parameterCodec = runtime.NewParameterCodec(scheme)
)

func init() {
	// The generated AddToScheme registers the external + internal GVs,
	// the identity conversions, and the field-label conversion func.
	utilruntime.Must(v1.AddToScheme(scheme))

	// Unversioned meta types required by the apiserver machinery.
	metav1.AddToGroupVersion(scheme, v1.SchemeGroupVersion)
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
	return &options{Options: runtimeserver.NewOptions()}
}

func (o *options) addFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	o.Options.PolicyGroup = v1.GroupName
	o.Options.Title = "aggexp-0046-verify-aa"
}

func (o *options) run(ctx context.Context) error {
	gr := schema.GroupResource{Group: v1.GroupName, Resource: "widgets"}
	mem := backend.New()

	store := library.New(library.Options{
		Backend:               mem,
		GroupResource:         gr,
		OptimisticConcurrency: true,
		FieldSelectors: &library.FieldSelectorOptions{
			SelectableFields: v1.SelectableFields,
			Accessor:         v1.FieldAccessor,
		},
	})

	g := &group.Group{
		GroupVersion:   v1.SchemeGroupVersion,
		Scheme:         scheme,
		Codecs:         codecs,
		ParameterCodec: parameterCodec,
		Resources:      map[string]rest.Storage{"widgets": store},
	}

	return o.Options.Run(
		ctx,
		"aggexp-0046-verify-aa",
		runtimeserver.Input{
			Scheme:             scheme,
			Codecs:             codecs,
			OpenAPIDefinitions: v1.GetOpenAPIDefinitions,
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
		Use:   "verify-aa",
		Short: "0046: library-mode AA on oapigen-generated types",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := opts.Options.Validate(); err != nil {
				return err
			}
			ctx := genericapiserver.SetupSignalContext()
			return opts.run(ctx)
		},
	}
	opts.addFlags(cmd.Flags())
	if code := cli.Run(cmd); code != 0 {
		fmt.Fprintln(os.Stderr, "verify-aa exited with error")
		os.Exit(code)
	}
}
