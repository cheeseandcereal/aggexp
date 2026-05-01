// Command note-aa-http is the 0026 component server. It is a
// near-copy of runtime/component.Run() with one structural change:
// a --backend-transport flag selects between gRPC (the existing
// path) and HTTP/JSON+SSE (this experiment's alternate).
//
// The HTTP transport is implemented as an adapter that satisfies
// runtime/component/proto.BackendClient, which lets us reuse the
// promoted runtime/component/{grpcbackend,scheme,openapi} packages
// verbatim. The wire differs; everything above the wire is shared.
//
// Schema source: 0023 Track B. The backend ships plain JSON Schema
// (no K8s-isms); synthesis lifts to full Kubernetes OpenAPI before
// handoff to openapi.ParseBackendSchema. The lift function is copied
// from experiments/0023-schema-source-exploration/.../synthesis
// because 0023's synthesis package is experiment-local and not yet
// promoted.
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

	"github.com/cheeseandcereal/aggexp/runtime/component/grpcbackend"
	componentopenapi "github.com/cheeseandcereal/aggexp/runtime/component/openapi"
	componentpb "github.com/cheeseandcereal/aggexp/runtime/component/proto"
	componentscheme "github.com/cheeseandcereal/aggexp/runtime/component/scheme"
	"github.com/cheeseandcereal/aggexp/runtime/group"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"
)

type options struct {
	*runtimeserver.Options
	BackendTransport string
	BackendAddr      string
	BackendTimeout   time.Duration
}

func newOptions() *options {
	return &options{
		Options:          runtimeserver.NewOptions(),
		BackendTransport: "grpc",
		BackendAddr:      "localhost:9090",
		BackendTimeout:   20 * time.Second,
	}
}

func (o *options) addFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	fs.StringVar(&o.BackendTransport, "backend-transport", o.BackendTransport,
		"Backend transport: grpc | http. grpc matches runtime/component.Run; "+
			"http speaks HTTP/JSON for CRUD and SSE for watch.")
	fs.StringVar(&o.BackendAddr, "backend-addr", o.BackendAddr,
		"Backend address. For grpc: host:port; for http: http(s)://host:port (or host:port, "+
			"http:// is assumed).")
	fs.DurationVar(&o.BackendTimeout, "backend-timeout", o.BackendTimeout,
		"Timeout for startup backend RPCs.")
}

func (o *options) validate() error {
	if err := o.Options.Validate(); err != nil {
		return err
	}
	switch o.BackendTransport {
	case "grpc", "http":
	default:
		return fmt.Errorf("invalid --backend-transport %q (want grpc|http)", o.BackendTransport)
	}
	if o.BackendAddr == "" {
		return fmt.Errorf("--backend-addr is required")
	}
	return nil
}

// newClient returns a componentpb.BackendClient over the selected
// transport. shutdown is a function the caller invokes at ctx
// cancellation; for grpc it closes the ClientConn, for http it's a
// no-op (net/http has no analogue).
func (o *options) newClient(ctx context.Context) (componentpb.BackendClient, func(), error) {
	switch o.BackendTransport {
	case "grpc":
		dialCtx, cancel := context.WithTimeout(ctx, o.BackendTimeout)
		defer cancel()
		conn, err := grpc.DialContext(dialCtx, o.BackendAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("dial backend %s: %w", o.BackendAddr, err)
		}
		return componentpb.NewBackendClient(conn), func() { _ = conn.Close() }, nil
	case "http":
		c := newHTTPClient(o.BackendAddr, o.BackendTimeout)
		return c, func() {}, nil
	default:
		return nil, nil, fmt.Errorf("unsupported transport %q", o.BackendTransport)
	}
}

func (o *options) run(ctx context.Context) error {
	client, shutdown, err := o.newClient(ctx)
	if err != nil {
		return err
	}

	schemaCtx, cancel := context.WithTimeout(ctx, o.BackendTimeout)
	defer cancel()
	resp, err := client.GetSchema(schemaCtx, &componentpb.GetSchemaRequest{})
	if err != nil {
		return fmt.Errorf("GetSchema (transport=%s): %w", o.BackendTransport, err)
	}
	klog.Infof("component(0026): backend describes %s/%s resource=%s kind=%s (transport=%s)",
		resp.GetGroup(), resp.GetVersion(), resp.GetResource(), resp.GetKind(),
		o.BackendTransport)

	gv := schema.GroupVersion{Group: resp.GetGroup(), Version: resp.GetVersion()}
	gvk := gv.WithKind(resp.GetKind())
	listGVK := gv.WithKind(resp.GetKind() + "List")

	// Track B: lift plain JSON Schema to full Kubernetes OpenAPI.
	lifted, err := LiftJSONSchemaToOpenAPI(gvk, resp.GetOpenapiV3())
	if err != nil {
		return fmt.Errorf("synthesis lift: %w", err)
	}
	klog.Infof("component(0026): lifted %d bytes JSON Schema -> %d bytes K8s OpenAPI",
		len(resp.GetOpenapiV3()), len(lifted))

	desc := componentscheme.ResourceDescriptor{
		GroupVersion:    gv,
		Resource:        resp.GetResource(),
		Kind:            resp.GetKind(),
		Singular:        resp.GetSingular(),
		Namespaced:      resp.GetNamespaced(),
		UseTypedWrapper: true,
	}
	bundle := componentscheme.Build(desc)

	itemSchema, parseErr := componentopenapi.ParseBackendSchema(lifted, gvk)
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
		"aggexp-component-0026",
		in,
		[]runtimeserver.GroupInstaller{g},
		map[string]runtimeserver.PostStartFunc{
			"component-upstream-watch": func(hookCtx context.Context) error {
				storage.StartUpstreamWatch(hookCtx)
				go func() {
					<-hookCtx.Done()
					storage.Shutdown()
					shutdown()
				}()
				return nil
			},
		},
	)
}

func main() {
	opts := newOptions()
	cmd := &cobra.Command{
		Use:   "note-aa-http",
		Short: "0026 component server; gRPC default, HTTP/JSON+SSE via --backend-transport=http.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := opts.validate(); err != nil {
				return err
			}
			return opts.run(genericapiserver.SetupSignalContext())
		},
	}
	opts.addFlags(cmd.Flags())
	logs.AddFlags(cmd.Flags())
	if code := cli.Run(cmd); code != 0 {
		fmt.Fprintln(os.Stderr, "note-aa-http exited with error")
		os.Exit(code)
	}
}
