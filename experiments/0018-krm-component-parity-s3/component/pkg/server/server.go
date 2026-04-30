// Package server composes the runtime/server substrate with this
// experiment's dynamic schema + gRPC backend. At startup it dials the
// backend, asks it for its schema, builds a scheme around the
// returned GVR+Kind, and installs the resource into the generic
// apiserver.
package server

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
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"

	generatedopenapi "github.com/cheeseandcereal/aggexp/experiments/0018-krm-component-parity-s3/component/pkg/generated/openapi"

	"github.com/cheeseandcereal/aggexp/runtime/group"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"

	krmv1 "github.com/cheeseandcereal/aggexp/experiments/0013-krm-component-skeleton/gen/aggexp/krm/v1"

	"github.com/cheeseandcereal/aggexp/experiments/0018-krm-component-parity-s3/component/pkg/grpcbackend"
	schemebuild "github.com/cheeseandcereal/aggexp/experiments/0018-krm-component-parity-s3/component/pkg/scheme"
)

// Options composes the substrate Options with our grpc-specific
// knobs.
type Options struct {
	*runtimeserver.Options

	BackendAddr    string
	BackendTimeout time.Duration
}

// NewOptions returns defaults.
func NewOptions() *Options {
	return &Options{
		Options:        runtimeserver.NewOptions(),
		BackendAddr:    "localhost:9090",
		BackendTimeout: 10 * time.Second,
	}
}

// AddFlags registers CLI flags.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	o.Options.Title = "aggexp-krm-component"
	fs.StringVar(&o.BackendAddr, "backend-addr", o.BackendAddr, "gRPC address of the thin backend.")
	fs.DurationVar(&o.BackendTimeout, "backend-timeout", o.BackendTimeout, "Timeout for backend RPCs.")
}

// Validate composes substrate + our own checks.
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

// Run dials the backend, fetches the schema, builds an unstructured
// scheme around it, wires a grpcbackend.REST, and hands the whole
// thing to runtime/server.Options.Run.
func (o *Options) Run(ctx context.Context) error {
	// Dial backend.
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
	client := krmv1.NewBackendClient(conn)

	// Fetch schema.
	schemaCtx, cancel2 := context.WithTimeout(ctx, o.BackendTimeout)
	defer cancel2()
	resp, err := client.GetSchema(schemaCtx, &krmv1.GetSchemaRequest{})
	if err != nil {
		return fmt.Errorf("GetSchema: %w", err)
	}
	klog.Infof("backend reports: group=%s version=%s resource=%s kind=%s singular=%s namespaced=%v writable=%v",
		resp.GetGroup(), resp.GetVersion(), resp.GetResource(), resp.GetKind(),
		resp.GetSingular(), resp.GetNamespaced(), resp.GetWritable())

	gv := schema.GroupVersion{Group: resp.GetGroup(), Version: resp.GetVersion()}
	desc := schemebuild.ResourceDescriptor{
		GroupVersion: gv,
		Resource:     resp.GetResource(),
		Kind:         resp.GetKind(),
		Singular:     resp.GetSingular(),
		Namespaced:   resp.GetNamespaced(),
	}
	bundle := schemebuild.Build(desc)

	// Build the rest.Storage.
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
		GroupVersion:  gv,
		Resource:      resp.GetResource(),
		Kind:          resp.GetKind(),
		Singular:      resp.GetSingular(),
		Namespaced:    resp.GetNamespaced(),
		Writable:      resp.GetWritable(),
		Columns:       cols,
		RowFields:     resp.GetRowFields(),
		GroupResource: schema.GroupResource{Group: gv.Group, Resource: resp.GetResource()},
	}, client)

	g := &group.Group{
		GroupVersion:   bundle.Descriptor.GroupVersion,
		Scheme:         bundle.Scheme,
		Codecs:         bundle.Codecs,
		ParameterCodec: bundle.ParameterCodec,
		Resources:      map[string]rest.Storage{resp.GetResource(): storage},
	}

	// OpenAPI: the apiserver requires a non-nil OpenAPIV3Config to
	// install a group (SSA is GA and builds a TypeConverter from
	// the spec). We supply a minimal GetDefinitions that describes
	// the unstructured Note at the target GVK using a pass-through
	// object schema. It's enough for the library to construct a
	// spec; kubectl explain works in a very degraded mode.
	in := runtimeserver.Input{
		Scheme:             bundle.Scheme,
		Codecs:             bundle.Codecs,
		OpenAPIDefinitions: minimalOpenAPIDefinitions(gv.WithKind(resp.GetKind()), gv.WithKind(resp.GetKind()+"List")),
	}

	o.Options.PolicyGroup = gv.Group

	return o.Options.Run(
		ctx,
		"aggexp-krm-component",
		in,
		[]runtimeserver.GroupInstaller{g},
		map[string]runtimeserver.PostStartFunc{
			"upstream-watch": func(hookCtx context.Context) error {
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

// minimalOpenAPIDefinitions returns a GetOpenAPIDefinitions function
// that advertises a nearly-empty object schema for the Go types we
// registered with the Scheme (*unstructured.Unstructured and
// *unstructured.UnstructuredList), PLUS the full set of
// meta/v1 + runtime + version definitions the generic apiserver
// itself depends on (it uses k8s.io/apimachinery/pkg/version.Info
// for /version, etc.). The generic apiserver needs a schema keyed
// by the Go type name (via openapi.NewDefinitionNamer) to build a
// managedFields TypeConverter; it does not, however, need a faithful
// schema for our unstructured resources -- we are deliberately
// unstructured.
//
// Because all registered GVKs share the same underlying Go type,
// kubectl explain will show the same "catch-all" schema regardless
// of which resource the user asks about. That is a known limitation
// of the unstructured path and one of the reasons experiment 0017
// will revisit dynamic type registration.
func minimalOpenAPIDefinitions(_, _ schema.GroupVersionKind) common.GetOpenAPIDefinitions {
	obj := common.OpenAPIDefinition{
		Schema: spec.Schema{
			VendorExtensible: spec.VendorExtensible{
				Extensions: spec.Extensions{
					"x-kubernetes-preserve-unknown-fields": true,
				},
			},
			SchemaProps: spec.SchemaProps{
				Description: "Dynamic resource served by the 0013 KRM component skeleton.",
				Type:        spec.StringOrArray{"object"},
			},
		},
	}
	return func(ref common.ReferenceCallback) map[string]common.OpenAPIDefinition {
		// Start from the full generated set (meta/v1, runtime,
		// version.Info, etc.) because the library itself
		// dereferences those names while building the spec. Then
		// add our unstructured Note shim.
		defs := generatedopenapi.GetOpenAPIDefinitions(ref)
		// Keys are the Go-type-name form openapi.NewDefinitionNamer
		// uses: import-path + "." + TypeName.
		defs["k8s.io/apimachinery/pkg/apis/meta/v1/unstructured.Unstructured"] = obj
		defs["k8s.io/apimachinery/pkg/apis/meta/v1/unstructured.UnstructuredList"] = obj
		return defs
	}
}
