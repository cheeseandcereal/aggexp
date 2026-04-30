package component

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/klog/v2"

	"github.com/cheeseandcereal/aggexp/runtime/component/grpcbackend"
	componentopenapi "github.com/cheeseandcereal/aggexp/runtime/component/openapi"
	componentpb "github.com/cheeseandcereal/aggexp/runtime/component/proto"
	componentscheme "github.com/cheeseandcereal/aggexp/runtime/component/scheme"
	"github.com/cheeseandcereal/aggexp/runtime/group"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"
)

// Options is the public configuration surface for a component
// server. It composes runtime/server.Options with the
// component-specific backend knobs.
type Options struct {
	*runtimeserver.Options

	// BackendAddr is the gRPC address of the backend. Required.
	BackendAddr string
	// BackendTimeout bounds startup calls (Dial, GetSchema).
	BackendTimeout time.Duration
	// UseTypedWrapper registers a typed Go wrapper
	// (runtime/component/scheme.Object) under the resource GVK
	// instead of *unstructured.Unstructured. Required for
	// Server-Side Apply; see runtime/component/scheme's package doc.
	UseTypedWrapper bool
	// ServerName is the name reported to the generic apiserver.
	// Defaults to "aggexp-component".
	ServerName string
}

// NewOptions returns defaults.
func NewOptions() *Options {
	opts := &Options{
		Options:        runtimeserver.NewOptions(),
		BackendAddr:    "localhost:9090",
		BackendTimeout: 10 * time.Second,
		// Typed wrapper on by default: the substrate assumes callers
		// want SSA working. Flip off with --use-typed-wrapper=false
		// for the unstructured-only path if that's the experiment.
		UseTypedWrapper: true,
		ServerName:      "aggexp-component",
	}
	return opts
}

// AddFlags registers flags onto fs.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	fs.StringVar(&o.BackendAddr, "backend-addr", o.BackendAddr,
		"gRPC address of the resource backend.")
	fs.DurationVar(&o.BackendTimeout, "backend-timeout", o.BackendTimeout,
		"Timeout for startup backend RPCs (Dial, GetSchema).")
	fs.BoolVar(&o.UseTypedWrapper, "use-typed-wrapper", o.UseTypedWrapper,
		"Register a typed Go wrapper under the resource GVK instead of *unstructured.Unstructured. "+
			"Required for Server-Side Apply. See runtime/component/scheme.")
	fs.StringVar(&o.ServerName, "server-name", o.ServerName,
		"Name reported to the generic apiserver.")
}

// Validate composes the substrate's checks with component-specific
// validation.
func (o *Options) Validate() error {
	var errs []error
	if err := o.Options.Validate(); err != nil {
		errs = append(errs, err)
	}
	if strings.TrimSpace(o.BackendAddr) == "" {
		errs = append(errs, fmt.Errorf("--backend-addr is required"))
	}
	return utilerrors.NewAggregate(errs)
}

// Run dials the backend, fetches its schema, builds the Scheme +
// OpenAPI defs + rest.Storage around it, and hands the whole thing
// to runtime/server.Options.Run. Blocks until ctx is cancelled.
//
// Backend connection is insecure-grpc today; callers that need
// in-cluster mTLS or SPIFFE should fork this function and swap the
// grpc dial options. The substrate deliberately does not grow a
// configuration surface for TLS until a second consumer demands it.
func Run(ctx context.Context, o *Options) error {
	dialCtx, cancel := context.WithTimeout(ctx, o.BackendTimeout)
	defer cancel()

	conn, err := grpc.DialContext(
		dialCtx, o.BackendAddr,
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
	klog.Infof("component: backend schema: group=%s version=%s resource=%s kind=%s namespaced=%v writable=%v ssa=%v",
		resp.GetGroup(), resp.GetVersion(), resp.GetResource(), resp.GetKind(),
		resp.GetNamespaced(), resp.GetWritable(), resp.GetSupportsServerSideApply())

	gv := schema.GroupVersion{Group: resp.GetGroup(), Version: resp.GetVersion()}
	gvk := gv.WithKind(resp.GetKind())
	listGVK := gv.WithKind(resp.GetKind() + "List")

	desc := componentscheme.ResourceDescriptor{
		GroupVersion:    gv,
		Resource:        resp.GetResource(),
		Kind:            resp.GetKind(),
		Singular:        resp.GetSingular(),
		Namespaced:      resp.GetNamespaced(),
		UseTypedWrapper: o.UseTypedWrapper,
	}
	bundle := componentscheme.Build(desc)

	itemSchema, parseErr := componentopenapi.ParseBackendSchema(resp.GetOpenapiV3(), gvk)
	if parseErr != nil {
		klog.Warningf("component: backend OpenAPI did not parse cleanly (%v); using preserve-unknown-fields fallback", parseErr)
		itemSchema = componentopenapi.Fallback(gvk, "")
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
		UseTypedWrapper:         o.UseTypedWrapper,
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

	// If the caller didn't set PolicyGroup, default to the served
	// group so the external authorizer (if any) scopes to this
	// resource.
	if o.Options.PolicyGroup == "" {
		o.Options.PolicyGroup = gv.Group
	}

	serverName := o.ServerName
	if serverName == "" {
		serverName = "aggexp-component"
	}

	return o.Options.Run(
		ctx,
		serverName,
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
