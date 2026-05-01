// Command aggexp-metadata-store-gc-component is the 0028 middleware.
// It forks the 0024 metadata-CRD-backed middleware and adds a periodic
// garbage collector that reconciles ResourceMetadata records with the
// backend: records whose backend object is missing are deleted.
// See experiments/0028-metadata-store-gc/component/gc/gc.go.
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
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

	"github.com/cheeseandcereal/aggexp/experiments/0028-metadata-store-gc/component/gc"
	"github.com/cheeseandcereal/aggexp/experiments/0028-metadata-store-gc/component/metastore"
	"github.com/cheeseandcereal/aggexp/experiments/0028-metadata-store-gc/component/stitchedrest"
	"github.com/cheeseandcereal/aggexp/experiments/0028-metadata-store-gc/component/synthesis"
)

type options struct {
	*runtimeserver.Options
	BackendAddr    string
	BackendTimeout time.Duration
	// FieldManager for host-cluster writes to ResourceMetadata CRs.
	MetaFieldManager string

	// GC knobs.
	GCInterval time.Duration
	GCMinAge   time.Duration
	GCDebugAddr string
}

func newOptions() *options {
	return &options{
		Options:          runtimeserver.NewOptions(),
		BackendAddr:      "localhost:9090",
		BackendTimeout:   20 * time.Second,
		MetaFieldManager: "aggexp-middleware",
		// Arbitrary choice: 5 min periodic sweep. Recorded in README.
		GCInterval: 5 * time.Minute,
		GCMinAge:   30 * time.Second,
		// :8444 on all interfaces. The kind cluster's pod-network
		// can port-forward to this; no auth. Debug only.
		GCDebugAddr: "0.0.0.0:8444",
	}
}

func (o *options) addFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	fs.StringVar(&o.BackendAddr, "backend-addr", o.BackendAddr, "gRPC address of the resource backend.")
	fs.DurationVar(&o.BackendTimeout, "backend-timeout", o.BackendTimeout, "Timeout for startup backend RPCs.")
	fs.StringVar(&o.MetaFieldManager, "meta-field-manager", o.MetaFieldManager, "Field manager name used when writing ResourceMetadata CRs.")
	fs.DurationVar(&o.GCInterval, "gc-interval", o.GCInterval, "How often the metadata-store GC sweeps for orphans.")
	fs.DurationVar(&o.GCMinAge, "gc-min-age", o.GCMinAge, "Minimum age before a record is eligible for GC (grace window).")
	fs.StringVar(&o.GCDebugAddr, "gc-debug-addr", o.GCDebugAddr, "Address for the unauthenticated GC debug HTTP server (POST /gc/run, GET /gc/last).")
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

	// --- GC reconciler ---
	reconciler := gc.New(store, client, gc.Config{
		Group:    gv.Group,
		Resource: resp.GetResource(),
		Interval: o.GCInterval,
		MinAge:   o.GCMinAge,
	})

	return o.Options.Run(
		ctx,
		"aggexp-component-0028",
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
			"metadata-store-gc": func(hookCtx context.Context) error {
				reconciler.Start(hookCtx)
				// Serve an unauthenticated debug endpoint for the
				// demo: POST /gc/run triggers a sweep, GET /gc/last
				// returns the last result. Binding is configurable.
				mux := http.NewServeMux()
				mux.HandleFunc("/gc/run", reconciler.HandleRun)
				mux.HandleFunc("/gc/last", reconciler.HandleLast)
				mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte("ok"))
				})
				srv := &http.Server{
					Addr:              o.GCDebugAddr,
					Handler:           mux,
					ReadHeaderTimeout: 5 * time.Second,
				}
				ln, err := net.Listen("tcp", o.GCDebugAddr)
				if err != nil {
					return fmt.Errorf("gc-debug listen %s: %w", o.GCDebugAddr, err)
				}
				klog.Infof("gc-debug: serving /gc/run and /gc/last on %s", o.GCDebugAddr)
				go func() {
					if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
						klog.Warningf("gc-debug: server exited: %v", err)
					}
				}()
				go func() {
					<-hookCtx.Done()
					shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					_ = srv.Shutdown(shutdownCtx)
				}()
				return nil
			},
		},
	)
}

func main() {
	opts := newOptions()
	cmd := &cobra.Command{
		Use:   "aggexp-metadata-store-gc-component",
		Short: "0028 middleware: 0024 with a metadata-store garbage collector.",
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
		fmt.Fprintln(os.Stderr, "aggexp-metadata-store-gc-component exited with error")
		os.Exit(code)
	}
}
