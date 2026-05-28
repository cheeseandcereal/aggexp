// Command widget-aa is experiment 0032: a multi-replica library-mode
// aggregated apiserver with Lease-based per-object/per-resource locking.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/conversion"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/component-base/cli"

	"github.com/cheeseandcereal/aggexp/experiments/0032-lease-based-object-locking/pkg/backend"
	"github.com/cheeseandcereal/aggexp/experiments/0032-lease-based-object-locking/pkg/locker"
	"github.com/cheeseandcereal/aggexp/experiments/0032-lease-based-object-locking/pkg/types"
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
}

type options struct {
	*runtimeserver.Options
	lockMode string
}

func newOptions() *options {
	return &options{
		Options:  runtimeserver.NewOptions(),
		lockMode: "per-object",
	}
}

func (o *options) addFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	o.Options.PolicyGroup = types.GroupName
	o.Options.Title = "aggexp-0032-widget-aa"
	fs.StringVar(&o.lockMode, "lock-mode", o.lockMode, "Lock granularity: per-object or per-resource")
}

func (o *options) run(ctx context.Context) error {
	// Build in-cluster client for Lease operations
	cfg, err := restclient.InClusterConfig()
	if err != nil {
		return fmt.Errorf("in-cluster config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("clientset: %w", err)
	}

	// Ensure lock namespace exists
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: locker.LockNamespace}}
	_, _ = clientset.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})

	gr := schema.GroupResource{Group: types.GroupName, Resource: "widgets"}
	mem := backend.New()

	var wb runtimestorage.WritableBackend
	mode := locker.Mode(o.lockMode)
	wb = locker.New(mem, clientset, mode, gr)

	store := runtimestorage.New(runtimestorage.Options{
		Backend:       wb,
		GroupResource: gr,
	})

	g := &group.Group{
		GroupVersion:   schema.GroupVersion{Group: types.GroupName, Version: "v1"},
		Scheme:         scheme,
		Codecs:         codecs,
		ParameterCodec: parameterCodec,
		Resources:      map[string]rest.Storage{"widgets": store},
	}

	return o.Options.Run(
		ctx,
		"aggexp-0032-widget-aa",
		runtimeserver.Input{
			Scheme: scheme,
			Codecs: codecs,
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
		Short: "0032: multi-replica AA with Lease-based locking",
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
