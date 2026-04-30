package openapi

import (
	"encoding/json"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// GVKExtension is the x-kubernetes-group-version-kind extension key.
// Both managedfields.NewTypeConverter (for SSA) and kubectl explain
// index by this extension.
const GVKExtension = "x-kubernetes-group-version-kind"

// GeneratedDefinitions returns the baseline OpenAPI definitions the
// library's apiserver plumbing needs: meta/v1, runtime.Info, and
// unstructured types. The output is the committed openapi-gen
// result from this package's generated.go.
func GeneratedDefinitions(ref common.ReferenceCallback) map[string]common.OpenAPIDefinition {
	return GetOpenAPIDefinitions(ref)
}

// ParseBackendSchema parses the backend's OpenAPI v3 JSON blob and
// stamps the GVK extension defensively.
//
// Errors: empty payload, not-a-JSON-object, spec.Schema decode.
// On success the returned schema is safe to drop into the defs map.
func ParseBackendSchema(raw []byte, gvk schema.GroupVersionKind) (spec.Schema, error) {
	if len(raw) == 0 {
		return spec.Schema{}, fmt.Errorf("no OpenAPI provided")
	}
	var s spec.Schema
	if err := json.Unmarshal(raw, &s); err != nil {
		return spec.Schema{}, fmt.Errorf("unmarshal OpenAPI v3: %w", err)
	}
	if s.Extensions == nil {
		s.Extensions = spec.Extensions{}
	}
	s.Extensions[GVKExtension] = []interface{}{
		map[string]interface{}{
			"group":   gvk.Group,
			"version": gvk.Version,
			"kind":    gvk.Kind,
		},
	}
	if len(s.Type) == 0 {
		s.Type = spec.StringOrArray{"object"}
	}
	return s, nil
}

// Fallback is the preserve-unknown-fields schema used when the
// backend ships no OpenAPI or ships one that fails to parse.
// kubectl explain will render only the generic description.
func Fallback(gvk schema.GroupVersionKind, description string) spec.Schema {
	if description == "" {
		description = "Dynamic resource served by a component-server backend. " +
			"Backend did not provide an OpenAPI schema; fields are not documented."
	}
	return spec.Schema{
		VendorExtensible: spec.VendorExtensible{
			Extensions: spec.Extensions{
				"x-kubernetes-preserve-unknown-fields": true,
				GVKExtension: []interface{}{
					map[string]interface{}{
						"group":   gvk.Group,
						"version": gvk.Version,
						"kind":    gvk.Kind,
					},
				},
			},
		},
		SchemaProps: spec.SchemaProps{
			Description: description,
			Type:        spec.StringOrArray{"object"},
		},
	}
}

// WrapAsList builds the <Kind>List envelope schema. itemKey is the
// defs-map key under which the item schema is registered.
func WrapAsList(listGVK schema.GroupVersionKind, itemKey string) spec.Schema {
	return spec.Schema{
		VendorExtensible: spec.VendorExtensible{
			Extensions: spec.Extensions{
				GVKExtension: []interface{}{
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
					Ref:         mustSpecRef("k8s.io/apimachinery/pkg/apis/meta/v1.ListMeta"),
				}},
				"items": {SchemaProps: spec.SchemaProps{
					Description: "List of items.",
					Type:        spec.StringOrArray{"array"},
					Items: &spec.SchemaOrArray{
						Schema: &spec.Schema{SchemaProps: spec.SchemaProps{
							Ref: mustSpecRef(itemKey),
						}},
					},
				}},
			},
		},
	}
}

// Compose returns a GetOpenAPIDefinitions function that stitches
// GeneratedDefinitions together with the backend-supplied item /
// list schemas keyed at the caller-specified canonical names.
func Compose(itemSchema, listSchema spec.Schema, itemKey, listKey string) common.GetOpenAPIDefinitions {
	return func(ref common.ReferenceCallback) map[string]common.OpenAPIDefinition {
		defs := GetOpenAPIDefinitions(ref)
		defs[itemKey] = common.OpenAPIDefinition{
			Schema:       itemSchema,
			Dependencies: baselineDependencies(),
		}
		defs[listKey] = common.OpenAPIDefinition{
			Schema:       listSchema,
			Dependencies: baselineDependencies(),
		}
		return defs
	}
}

// baselineDependencies lists meta/v1 refs the library may need to
// resolve when building definitions for the resource. Listing extras
// is cheap; missing deps produce a klog.Fatal at apiserver startup.
func baselineDependencies() []string {
	return []string{
		"k8s.io/apimachinery/pkg/apis/meta/v1.ObjectMeta",
		"k8s.io/apimachinery/pkg/apis/meta/v1.ListMeta",
	}
}

func mustSpecRef(name string) spec.Ref {
	r, err := spec.NewRef("#/components/schemas/" + FriendlyRef(name))
	if err != nil {
		// spec.NewRef only fails on unparseable URIs; the input
		// above is a plain JSON-pointer fragment and cannot fail.
		panic(fmt.Sprintf("unreachable: %v", err))
	}
	return r
}

// FriendlyRef converts a Go canonical type name (e.g.
// "k8s.io/apimachinery/pkg/apis/meta/v1.ObjectMeta") into the
// reverse-DNS style kube-openapi's builder uses internally
// ("io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta").
//
// Exported for consumers that need to build their own refs against
// the composed defs map.
func FriendlyRef(name string) string {
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
