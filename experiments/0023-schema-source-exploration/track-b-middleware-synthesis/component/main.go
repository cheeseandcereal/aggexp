// Command note-aa-0023b is the Track B component server for
// experiment 0023. The backend ships a plain JSON Schema (no GVK
// extension, no ObjectMeta, no apiVersion/kind fields); this
// component lifts it into full Kubernetes OpenAPI v3 at startup via
// the synthesis package, then hands off to the substrate.
//
// This binary replicates the bulk of runtime/component.Run() rather
// than importing it, because the substrate's Run() dials and calls
// GetSchema internally — the synthesis hook has to run in between.
// A future refactor of the substrate (see thesis for the v2
// promotion in 0030) would allow either a hook or a caller-supplied
// OpenAPI override; that is out of scope here.
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
	"k8s.io/component-base/cli"
	"k8s.io/component-base/logs"
	"k8s.io/klog/v2"

	"github.com/cheeseandcereal/aggexp/experiments/0023-schema-source-exploration/track-b-middleware-synthesis/synthesis"
	"github.com/cheeseandcereal/aggexp/runtime/component/grpcbackend"
	componentopenapi "github.com/cheeseandcereal/aggexp/runtime/component/openapi"
	componentpb "github.com/cheeseandcereal/aggexp/runtime/component/proto"
	componentscheme "github.com/cheeseandcereal/aggexp/runtime/component/scheme"
	"github.com/cheeseandcereal/aggexp/runtime/group"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"
)

type trackBOptions struct {
	*runtimeserver.Options
	BackendAddr    string
	BackendTimeout time.Duration
}

func newOptions() *trackBOptions {
	return &trackBOptions{
		Options:        runtimeserver.NewOptions(),
		BackendAddr:    "localhost:9090",
		BackendTimeout: 20 * time.Second,
	}
}

func (o *trackBOptions) addFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	fs.StringVar(&o.BackendAddr, "backend-addr", o.BackendAddr, "gRPC address of the resource backend.")
	fs.DurationVar(&o.BackendTimeout, "backend-timeout", o.BackendTimeout, "Timeout for startup backend RPCs.")
}

func (o *trackBOptions) run(ctx context.Context) error {
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
	klog.Infof("component(track-B): backend identifies resource: group=%s version=%s resource=%s kind=%s",
		resp.GetGroup(), resp.GetVersion(), resp.GetResource(), resp.GetKind())

	gv := schema.GroupVersion{Group: resp.GetGroup(), Version: resp.GetVersion()}
	gvk := gv.WithKind(resp.GetKind())
	listGVK := gv.WithKind(resp.GetKind() + "List")

	// --- Track B distinguishing step ----------------------------
	// The backend shipped a plain JSON Schema of just spec+status.
	// Lift it into full Kubernetes OpenAPI v3 here.
	liftedOpenAPI, err := synthesis.LiftJSONSchemaToOpenAPI(gvk, resp.GetOpenapiV3())
	if err != nil {
		return fmt.Errorf("synthesis.LiftJSONSchemaToOpenAPI: %w", err)
	}
	klog.Infof("component(track-B): lifted %d-byte JSON schema to %d-byte Kubernetes OpenAPI v3",
		len(resp.GetOpenapiV3()), len(liftedOpenAPI))
	// ------------------------------------------------------------

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
	listSchema := componentopenapi.WrapAsList(listGVK, bundle.ItemCanonicalName)

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

	storage := grpcbackend.New(grpcbackend.Descriptor{
		GroupVersion:            gv,
		Resource:                resp.GetResource(),
		Kind:                    resp.GetKind(),
		Singular:                resp.GetSingular(),
		Namespaced:              resp.GetNamespaced(),
		Writable:                resp.GetWritable(),
		SupportsServerSideApply: resp.GetSupportsServerSideApply(),
		UseTypedWrapper:         true,
		Columns:                 cols,
		RowFields:               resp.GetRowFields(),
		GroupResource:           schema.GroupResource{Group: gv.Group, Resource: resp.GetResource()},
	}, client)

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
		"aggexp-component-0023b",
		in,
		[]runtimeserver.GroupInstaller{g},
		map[string]runtimeserver.PostStartFunc{
			"component-upstream-watch": func(hookCtx context.Context) error {
				storage.StartUpstreamWatch(hookCtx)
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
		Use:   "note-aa-0023b",
		Short: "Track B (middleware-synthesizes-OpenAPI) component server for 0023.",
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
		fmt.Fprintln(os.Stderr, "note-aa-0023b exited with error")
		os.Exit(code)
	}
}
