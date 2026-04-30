// Package server composes the runtime/server substrate with this
// experiment's dynamic schema + gRPC backend. At startup it dials the
// backend, asks it for its schema, builds a scheme around the
// returned GVR+Kind, and installs the resource into the generic
// apiserver.
//
// Compared to 0013 (the forked predecessor), this package:
//
//   - Parses the backend's OpenAPI v3 JSON at startup and composes
//     it into the GetOpenAPIDefinitions map keyed at the Go-type
//     canonical name for *unstructured.Unstructured. This is the
//     key that `openapi.NewDefinitionNamer(Scheme)` and the
//     kube-openapi builder agree on. Before this, the schema was a
//     preserve-unknown-fields stub and kubectl explain rendered
//     only the resource description.
//
//   - Ensures the resource schema carries the
//     `x-kubernetes-group-version-kind` extension (either from the
//     backend's own schema or synthesized here, for defense in
//     depth). `managedfields.NewTypeConverter` indexes parsed
//     schemas by GVK exclusively via this extension; without it,
//     SSA's typed converter has no entry for our kind.
//
//   - Registers a custom GetDefinitionName callback. It still
//     defers to openapi.NewDefinitionNamer for non-dynamic types
//     (meta/v1, runtime.Info, etc.), but for the unstructured key
//     it returns a friendly, deterministic uniqueName and stamps
//     the GVK extension.
//
//   - Propagates the client-supplied field-manager name through
//     the refined gRPC Create/Update/Apply envelopes.
package server

import (
	"context"
	"encoding/json"
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

	generatedopenapi "github.com/cheeseandcereal/aggexp/experiments/0017-krm-protocol-refinement/component/pkg/generated/openapi"

	"github.com/cheeseandcereal/aggexp/runtime/group"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"

	krmv1 "github.com/cheeseandcereal/aggexp/experiments/0017-krm-protocol-refinement/gen/aggexp/krm/v1"

	"github.com/cheeseandcereal/aggexp/experiments/0017-krm-protocol-refinement/component/pkg/grpcbackend"
	schemebuild "github.com/cheeseandcereal/aggexp/experiments/0017-krm-protocol-refinement/component/pkg/scheme"
)

// unstructuredCanonicalName is the canonical Go-type name used as
// the defs-map key when the scheme is registered with
// *unstructured.Unstructured. It must match what
// kube-openapi/pkg/util.GetCanonicalTypeName returns for the
// registered sample object.
const unstructuredCanonicalName = "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured.Unstructured"
const unstructuredListCanonicalName = "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured.UnstructuredList"

// dynCanonicalName is the analogous name when the scheme is
// registered with *dyn.Object. It is the import path + "." + type
// name.
const dynCanonicalName = "github.com/cheeseandcereal/aggexp/experiments/0017-krm-protocol-refinement/component/pkg/dyn.Object"
const dynListCanonicalName = "github.com/cheeseandcereal/aggexp/experiments/0017-krm-protocol-refinement/component/pkg/dyn.ObjectList"

// Options composes the substrate Options with our grpc-specific
// knobs.
type Options struct {
	*runtimeserver.Options

	BackendAddr    string
	BackendTimeout time.Duration
	// UseTypedWrapper switches the component server off of
	// *unstructured.Unstructured and onto the typed dyn.Object
	// wrapper. See pkg/dyn and FINDINGS/0017 for the motivation.
	// Default: false (keeps 0013's behavior; kubectl explain still
	// works; SSA still fails at the empty-object GVK step).
	UseTypedWrapper bool
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
	fs.BoolVar(&o.UseTypedWrapper, "use-typed-wrapper", o.UseTypedWrapper,
		"Register a typed Go wrapper (dyn.Object) under the resource GVK instead of *unstructured.Unstructured. "+
			"Experimental; see FINDINGS/0017 for the tradeoffs.")
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
	klog.Infof("backend reports: group=%s version=%s resource=%s kind=%s singular=%s namespaced=%v writable=%v ssa=%v",
		resp.GetGroup(), resp.GetVersion(), resp.GetResource(), resp.GetKind(),
		resp.GetSingular(), resp.GetNamespaced(), resp.GetWritable(),
		resp.GetSupportsServerSideApply())

	gv := schema.GroupVersion{Group: resp.GetGroup(), Version: resp.GetVersion()}
	gvk := gv.WithKind(resp.GetKind())
	listGVK := gv.WithKind(resp.GetKind() + "List")

	desc := schemebuild.ResourceDescriptor{
		GroupVersion:    gv,
		Resource:        resp.GetResource(),
		Kind:            resp.GetKind(),
		Singular:        resp.GetSingular(),
		Namespaced:      resp.GetNamespaced(),
		UseTypedWrapper: o.UseTypedWrapper,
	}
	bundle := schemebuild.Build(desc)

	// Parse the backend's OpenAPI. If it fails or is missing we
	// fall back to the 0013 preserve-unknown-fields shim.
	backendSchema, parseErr := parseBackendOpenAPI(resp.GetOpenapiV3(), gvk)
	if parseErr != nil {
		klog.Warningf("backend OpenAPI did not parse cleanly (%v); falling back to preserve-unknown-fields stub", parseErr)
		backendSchema = fallbackObjectSchema(gvk)
	}
	listSchema := wrapAsList(backendSchema, listGVK, func() string {
		if o.UseTypedWrapper {
			return dynCanonicalName
		}
		return unstructuredCanonicalName
	}())

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

	// Compose: library's generated defs (meta/v1, runtime, etc.)
	// + the backend-derived schema, keyed at the canonical name.
	itemKey := unstructuredCanonicalName
	listKey := unstructuredListCanonicalName
	if o.UseTypedWrapper {
		itemKey = dynCanonicalName
		listKey = dynListCanonicalName
	}
	in := runtimeserver.Input{
		Scheme:             bundle.Scheme,
		Codecs:             bundle.Codecs,
		OpenAPIDefinitions: composedDefinitions(backendSchema, listSchema, itemKey, listKey),
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

// parseBackendOpenAPI parses the JSON-encoded OpenAPI v3 schema
// object the backend shipped, and ensures it carries an
// x-kubernetes-group-version-kind extension matching gvk. Returns
// the parsed schema ready to drop into the defs map.
//
// Returns an error if the payload is empty, is not a JSON object,
// or isn't decodable into spec.Schema.
func parseBackendOpenAPI(raw []byte, gvk schema.GroupVersionKind) (spec.Schema, error) {
	if len(raw) == 0 {
		return spec.Schema{}, fmt.Errorf("no OpenAPI provided")
	}
	var s spec.Schema
	if err := json.Unmarshal(raw, &s); err != nil {
		return spec.Schema{}, fmt.Errorf("unmarshal: %w", err)
	}
	// Defense: stamp the GVK extension regardless of whether the
	// backend provided it. Both managedfields.NewTypeConverter
	// (for SSA) and kubectl's explain index by this extension.
	if s.Extensions == nil {
		s.Extensions = spec.Extensions{}
	}
	s.Extensions["x-kubernetes-group-version-kind"] = []interface{}{
		map[string]interface{}{
			"group":   gvk.Group,
			"version": gvk.Version,
			"kind":    gvk.Kind,
		},
	}
	// Defense: if the schema forgot `type: object` at the root,
	// set it. `spec.Schema.SchemaProps.Type` is a []string wrapper.
	if len(s.Type) == 0 {
		s.Type = spec.StringOrArray{"object"}
	}
	return s, nil
}

// fallbackObjectSchema produces a preserve-unknown-fields schema
// stamped with the GVK extension, for use when the backend ships
// no OpenAPI or ships one that fails to parse.
func fallbackObjectSchema(gvk schema.GroupVersionKind) spec.Schema {
	return spec.Schema{
		VendorExtensible: spec.VendorExtensible{
			Extensions: spec.Extensions{
				"x-kubernetes-preserve-unknown-fields": true,
				"x-kubernetes-group-version-kind": []interface{}{
					map[string]interface{}{
						"group":   gvk.Group,
						"version": gvk.Version,
						"kind":    gvk.Kind,
					},
				},
			},
		},
		SchemaProps: spec.SchemaProps{
			Description: "Dynamic resource served by the 0017 KRM component server. " +
				"Backend did not provide an OpenAPI schema; fields are not documented.",
			Type: spec.StringOrArray{"object"},
		},
	}
}

// wrapAsList returns a schema for the <Kind>List envelope.
// Structure is the standard items-array wrapper with a metadata
// field. The list GVK extension is included so kubectl explain
// on the list kind also works. itemKey is the defs-map key under
// which the item schema is registered (differs between the
// unstructured and typed-wrapper paths).
func wrapAsList(_ spec.Schema, listGVK schema.GroupVersionKind, itemKey string) spec.Schema {
	return spec.Schema{
		VendorExtensible: spec.VendorExtensible{
			Extensions: spec.Extensions{
				"x-kubernetes-group-version-kind": []interface{}{
					map[string]interface{}{
						"group":   listGVK.Group,
						"version": listGVK.Version,
						"kind":    listGVK.Kind,
					},
				},
			},
		},
		SchemaProps: spec.SchemaProps{
			Description: fmt.Sprintf("%s is a list of %s.", listGVK.Kind, strings.TrimSuffix(listGVK.Kind, "List")),
			Type:        spec.StringOrArray{"object"},
			Required:    []string{"items"},
			Properties: map[string]spec.Schema{
				"apiVersion": {SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"string"},
					Description: "APIVersion defines the versioned schema of this representation of an object."}},
				"kind": {SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"string"},
					Description: "Kind is a string value representing the REST resource this object represents."}},
				"metadata": {SchemaProps: spec.SchemaProps{
					Description: "Standard list metadata.",
					Ref:         specRef("k8s.io/apimachinery/pkg/apis/meta/v1.ListMeta"),
				}},
				"items": {SchemaProps: spec.SchemaProps{
					Description: "List of items.",
					Type:        spec.StringOrArray{"array"},
					Items: &spec.SchemaOrArray{
						Schema: &spec.Schema{SchemaProps: spec.SchemaProps{
							Ref: specRef(itemKey),
						}},
					},
				}},
			},
		},
	}
}

func specRef(name string) spec.Ref {
	// kube-openapi resolves refs via the configured ReferenceCallback.
	// The callback we install elsewhere rewrites the defs name into
	// a relative $ref. Using MustCreateRef directly here is fine
	// because the builder will re-write it when emitting.
	r, _ := spec.NewRef("#/components/schemas/" + friendlyRef(name))
	return r
}

// friendlyRef reverses the first path segment to match kube-openapi's
// friendlyName convention (io.k8s... instead of k8s.io...). This keeps
// our internal refs round-trippable with the builder's renderer.
func friendlyRef(name string) string {
	parts := strings.SplitN(name, "/", 2)
	if len(parts) == 0 {
		return name
	}
	first := parts[0]
	pieces := strings.Split(first, ".")
	for i, j := 0, len(pieces)-1; i < j; i, j = i+1, j-1 {
		pieces[i], pieces[j] = pieces[j], pieces[i]
	}
	first = strings.Join(pieces, ".")
	if len(parts) == 1 {
		return first
	}
	return first + "." + strings.ReplaceAll(parts[1], "/", ".")
}

// composedDefinitions returns a GetOpenAPIDefinitions function that
// stitches the library's generated defs (meta/v1, runtime, etc.)
// together with the backend-supplied schema keyed at the
// caller-specified canonical names.
func composedDefinitions(itemSchema, listSchema spec.Schema, itemKey, listKey string) common.GetOpenAPIDefinitions {
	return func(ref common.ReferenceCallback) map[string]common.OpenAPIDefinition {
		defs := generatedopenapi.GetOpenAPIDefinitions(ref)
		defs[itemKey] = common.OpenAPIDefinition{
			Schema:       itemSchema,
			Dependencies: dependenciesOf(itemSchema),
		}
		defs[listKey] = common.OpenAPIDefinition{
			Schema:       listSchema,
			Dependencies: dependenciesOf(listSchema),
		}
		return defs
	}
}

// dependenciesOf walks a schema and returns the set of referenced
// type names. The library calls buildDefinitionRecursively for each
// dependency; missing deps produce a klog.Fatal. For our item schema
// we reference metav1.ObjectMeta when the backend exposes a metadata
// field; we list ObjectMeta and ListMeta conservatively because the
// library's cost for an unused dep is zero.
func dependenciesOf(_ spec.Schema) []string {
	return []string{
		"k8s.io/apimachinery/pkg/apis/meta/v1.ObjectMeta",
		"k8s.io/apimachinery/pkg/apis/meta/v1.ListMeta",
	}
}
