// Command aggexp-metadata-crd-component is the 0024 middleware. It
// registers a dynamic API at startup by asking a gRPC backend for
// its schema (Track B — plain JSON Schema, middleware lifts to full
// Kubernetes OpenAPI), stitches KRM metadata from a shared
// ResourceMetadata CRD on every request, and serves buckets.aggexp.io/v1.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/dynamic"
	clientgorest "k8s.io/client-go/rest"
	"k8s.io/component-base/cli"
	"k8s.io/component-base/logs"
	"k8s.io/klog/v2"

	componentopenapi "github.com/cheeseandcereal/aggexp/runtime/component/openapi"
	componentpb "github.com/cheeseandcereal/aggexp/runtime/component/proto"
	componentscheme "github.com/cheeseandcereal/aggexp/runtime/component/scheme"
	"github.com/cheeseandcereal/aggexp/runtime/group"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"

	"github.com/cheeseandcereal/aggexp/experiments/0024-metadata-crd-store/component/metastore"
	"github.com/cheeseandcereal/aggexp/experiments/0024-metadata-crd-store/component/stitchedrest"
	"github.com/cheeseandcereal/aggexp/experiments/0024-metadata-crd-store/component/synthesis"
)

type options struct {
	*runtimeserver.Options
	BackendAddr    string
	BackendTimeout time.Duration
	// FieldManager for host-cluster writes to ResourceMetadata CRs.
	MetaFieldManager string
}

func newOptions() *options {
	return &options{
		Options:          runtimeserver.NewOptions(),
		BackendAddr:      "localhost:9090",
		BackendTimeout:   20 * time.Second,
		MetaFieldManager: "aggexp-middleware",
	}
}

func (o *options) addFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	fs.StringVar(&o.BackendAddr, "backend-addr", o.BackendAddr, "gRPC address of the resource backend.")
	fs.DurationVar(&o.BackendTimeout, "backend-timeout", o.BackendTimeout, "Timeout for startup backend RPCs.")
	fs.StringVar(&o.MetaFieldManager, "meta-field-manager", o.MetaFieldManager, "Field manager name used when writing ResourceMetadata CRs.")
}

func (o *options) run(ctx context.Context) error {
	// --- dial backend ---
	dialCtx, cancel := context.WithTimeout(ctx, o.BackendTimeout)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, o.BackendAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("dial backend %s: %w", o.BackendAddr, err)
	}
	client := componentpb.NewBackendClient(conn)

	schemaCtx, cancel2 := context.WithTimeout(ctx, o.BackendTimeout)
	defer cancel2()
	resp, err := client.GetSchema(schemaCtx, &componentpb.GetSchemaRequest{})
	if err != nil {
		return fmt.Errorf("GetSchema: %w", err)
	}
	klog.Infof("middleware: backend identifies resource: group=%s version=%s resource=%s kind=%s",
		resp.GetGroup(), resp.GetVersion(), resp.GetResource(), resp.GetKind())

	gv := schema.GroupVersion{Group: resp.GetGroup(), Version: resp.GetVersion()}
	gvk := gv.WithKind(resp.GetKind())
	listGVK := gv.WithKind(resp.GetKind() + "List")

	// --- Track B lift ---
	liftedOpenAPI, err := synthesis.LiftJSONSchemaToOpenAPI(gvk, resp.GetOpenapiV3())
	if err != nil {
		return fmt.Errorf("synthesis.LiftJSONSchemaToOpenAPI: %w", err)
	}
	klog.Infof("middleware: lifted %d-byte JSON schema to %d-byte Kubernetes OpenAPI",
		len(resp.GetOpenapiV3()), len(liftedOpenAPI))

	// --- Scheme wrapper (typed for SSA per 0017) ---
	desc := componentscheme.ResourceDescriptor{
		GroupVersion:    gv,
		Resource:        resp.GetResource(),
		Kind:            resp.GetKind(),
		Singular:        resp.GetSingular(),
		Namespaced:      resp.GetNamespaced(),
		UseTypedWrapper: true,
	}
	bundle := componentscheme.Build(desc)

	itemSchema, parseErr := componentopenapi.ParseBackendSchema(liftedOpenAPI, gvk)
	if parseErr != nil {
		return fmt.Errorf("parse lifted OpenAPI: %w", parseErr)
	}
	// Use our local WrapAsListV2Refs so list/item refs are
	// "#/definitions/..." and survive /openapi/v2 aggregation for
	// strict consumers like ArgoCD. See synthesis/synthesis.go
	// package doc for the rationale.
	listSchema := synthesis.WrapAsListV2Refs(listGVK, componentopenapi.FriendlyRef(bundle.ItemCanonicalName))

	cols := make([]metav1.TableColumnDefinition, 0, len(resp.GetColumns()))
	for _, c := range resp.GetColumns() {
		cols = append(cols, metav1.TableColumnDefinition{
			Name:        c.GetName(),
			Type:        c.GetType(),
			Format:      c.GetFormat(),
			Description: c.GetDescription(),
			Priority:    c.GetPriority(),
		})
	}

	// --- metastore client ---
	kubecfg, err := clientgorest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("build in-cluster kube client config (needed for ResourceMetadata CRD access): %w", err)
	}
	dyn, err := dynamic.NewForConfig(kubecfg)
	if err != nil {
		return fmt.Errorf("dynamic client: %w", err)
	}
	store := metastore.New(dyn, o.MetaFieldManager)
	klog.Infof("middleware: metastore ready (fieldManager=%s, CRD=%s)", o.MetaFieldManager, metastore.GVR)

	storage := stitchedrest.New(stitchedrest.Descriptor{
		GroupVersion:  gv,
		Resource:      resp.GetResource(),
		Kind:          resp.GetKind(),
		Singular:      resp.GetSingular(),
		Namespaced:    resp.GetNamespaced(),
		Writable:      resp.GetWritable(),
		Columns:       cols,
		RowFields:     resp.GetRowFields(),
		GroupResource: schema.GroupResource{Group: gv.Group, Resource: resp.GetResource()},
	}, client, store)

	g := &group.Group{
		GroupVersion:   bundle.Descriptor.GroupVersion,
		Scheme:         bundle.Scheme,
		Codecs:         bundle.Codecs,
		ParameterCodec: bundle.ParameterCodec,
		Resources:      map[string]rest.Storage{resp.GetResource(): storage},
	}

	in := runtimeserver.Input{
		Scheme:             bundle.Scheme,
		Codecs:             bundle.Codecs,
		OpenAPIDefinitions: componentopenapi.Compose(itemSchema, listSchema, bundle.ItemCanonicalName, bundle.ListCanonicalName),
	}

	if o.Options.PolicyGroup == "" {
		o.Options.PolicyGroup = gv.Group
	}

	return o.Options.Run(
		ctx,
		"aggexp-component-0024",
		in,
		[]runtimeserver.GroupInstaller{g},
		map[string]runtimeserver.PostStartFunc{
			"stitched-upstream-watches": func(hookCtx context.Context) error {
				storage.StartUpstreamWatches(hookCtx)
				go func() {
					<-hookCtx.Done()
					storage.Shutdown()
					_ = conn.Close()
				}()
				return nil
			},
		},
	)
}

func main() {
	opts := newOptions()
	cmd := &cobra.Command{
		Use:   "aggexp-metadata-crd-component",
		Short: "0024 middleware: stitches KRM metadata from a shared CRD onto a backend's business data.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := opts.Options.Validate(); err != nil {
				return err
			}
			return opts.run(genericapiserver.SetupSignalContext())
		},
	}
	opts.addFlags(cmd.Flags())
	logs.AddFlags(cmd.Flags())
	if code := cli.Run(cmd); code != 0 {
		fmt.Fprintln(os.Stderr, "aggexp-metadata-crd-component exited with error")
		os.Exit(code)
	}
}
