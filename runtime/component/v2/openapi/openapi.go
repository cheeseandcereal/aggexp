// Package openapi assembles the OpenAPI definition map the generic
// apiserver consumes for the v2 component substrate.
//
// v2 differences from runtime/component/openapi:
//
//   - Refs to meta/v1 types use "#/definitions/..." (v2-style), not
//     "#/components/schemas/..." (v3-style). The v3 form is silently
//     accepted by /openapi/v2 consumers like kubectl but rejected by
//     strict consumers (ArgoCD's gitops-engine cluster cache). The
//     fix is substrate-level per FINDINGS/0024. kubectl's /openapi/v3
//     client emits a warning with v2-style refs on apply; properly
//     serving per-endpoint refs would require library-level work and
//     is explicitly deferred.
//   - Synthesize lifts plain JSON Schema (Track B, 0023) into full
//     Kubernetes OpenAPI by stamping apiVersion/kind/metadata/GVK.
//     The 127-line transformation is mechanical and resource-neutral;
//     see FINDINGS/0023 for the ergonomics argument.
//   - Compose returns a GetOpenAPIDefinitions closure with a
//     per-invocation definitions map, so dynamic InstallAPIGroup
//     after PrepareRun picks up newly-registered groups (provided
//     the caller nil'd OpenAPIV3Config.Definitions on the recommended
//     config — see FINDINGS/0027 for the cache-defeat consequent).
//
// Baseline meta/v1 + unstructured definitions are re-exported from
// runtime/component/openapi (the openapi-gen output is amortized
// across both substrate versions; regenerating is mechanical and
// not worth duplicating 2,700 lines for).
package openapi

import (
	"encoding/json"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"

	v1openapi "github.com/cheeseandcereal/aggexp/runtime/component/openapi"
)

// GVKExtension is the x-kubernetes-group-version-kind extension key.
const GVKExtension = "x-kubernetes-group-version-kind"

// BaselineDefinitions returns the meta/v1, runtime, and unstructured
// type definitions required for a working apiserver. Re-exported
// from the v1 package's generated openapi-gen output.
func BaselineDefinitions(ref common.ReferenceCallback) map[string]common.OpenAPIDefinition {
	return v1openapi.GetOpenAPIDefinitions(ref)
}

// ParseBackendSchema parses a single OpenAPI v3 schema object
// (already in Kubernetes-dialect form; Track A) and defensively
// stamps the GVK extension.
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

// Fallback is the preserve-unknown-fields schema used when a backend
// supplies nothing or an unparseable schema.
func Fallback(gvk schema.GroupVersionKind, description string) spec.Schema {
	if description == "" {
		description = "Dynamic resource served by a component-server backend. " +
			"No OpenAPI was supplied; fields are undocumented."
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
// defs-map key under which the item schema is registered. Refs use
// "#/definitions/..." form for cross-consumer compatibility (0024).
func WrapAsList(listGVK schema.GroupVersionKind, itemKey string) spec.Schema {
	itemRef, _ := spec.NewRef("#/definitions/" + FriendlyRef(itemKey))
	listMetaRef, _ := spec.NewRef("#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ListMeta")
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
				"apiVersion": {SchemaProps: spec.SchemaProps{
					Type:        spec.StringOrArray{"string"},
					Description: "APIVersion defines the versioned schema of this representation of an object.",
				}},
				"kind": {SchemaProps: spec.SchemaProps{
					Type:        spec.StringOrArray{"string"},
					Description: "Kind is a string value representing the REST resource this object represents.",
				}},
				"metadata": {SchemaProps: spec.SchemaProps{
					Description: "Standard list metadata.",
					Ref:         listMetaRef,
				}},
				"items": {SchemaProps: spec.SchemaProps{
					Description: "List of items.",
					Type:        spec.StringOrArray{"array"},
					Items: &spec.SchemaOrArray{
						Schema: &spec.Schema{SchemaProps: spec.SchemaProps{Ref: itemRef}},
					},
				}},
			},
		},
	}
}

// Compose returns a GetOpenAPIDefinitions function that composes a
// *live* snapshot of definitions on every invocation. The caller
// passes a getter that yields the current per-group item/list
// schemas; the closure merges them with the baseline meta/v1
// definitions.
//
// This is the dynamic-install-friendly shape: as new groups are
// registered at reconcile time, their schemas appear in the next
// openapi-builder pass. See FINDINGS/0027 for the cache-defeat
// consequent the caller must honor
// (cfg.OpenAPIV3Config.Definitions = nil after Config()).
func Compose(get func() map[string]common.OpenAPIDefinition) common.GetOpenAPIDefinitions {
	return func(ref common.ReferenceCallback) map[string]common.OpenAPIDefinition {
		defs := BaselineDefinitions(ref)
		for k, v := range get() {
			defs[k] = v
		}
		return defs
	}
}

// ComposeStatic is the single-group convenience form: build a
// closure that always returns BaselineDefinitions plus one item /
// list pair. Useful for the single-AA consumer path.
func ComposeStatic(itemSchema, listSchema spec.Schema, itemKey, listKey string) common.GetOpenAPIDefinitions {
	return Compose(func() map[string]common.OpenAPIDefinition {
		return map[string]common.OpenAPIDefinition{
			itemKey: {Schema: itemSchema, Dependencies: baselineDependencies()},
			listKey: {Schema: listSchema, Dependencies: baselineDependencies()},
		}
	})
}

// Synthesize lifts a plain JSON Schema (describing only a resource's
// business shape, typically {spec, status}) into full Kubernetes
// OpenAPI (Track B from 0023). The lift adds apiVersion, kind,
// metadata ($ref to meta/v1 ObjectMeta), and the GVK extension.
// Existing root-level properties are preserved.
//
// Refs emitted use "#/definitions/..." form (0024 consequent).
func Synthesize(gvk schema.GroupVersionKind, jsonSchema []byte) ([]byte, error) {
	if len(jsonSchema) == 0 {
		return nil, fmt.Errorf("empty JSON schema")
	}
	var root map[string]any
	if err := json.Unmarshal(jsonSchema, &root); err != nil {
		return nil, fmt.Errorf("unmarshal JSON schema: %w", err)
	}
	if t, ok := root["type"]; !ok || t == nil {
		root["type"] = "object"
	}
	props, _ := root["properties"].(map[string]any)
	if props == nil {
		props = map[string]any{}
	}
	props["apiVersion"] = map[string]any{
		"type":        "string",
		"description": "APIVersion defines the versioned schema of this representation of an object.",
	}
	props["kind"] = map[string]any{
		"type":        "string",
		"description": "Kind is a string value representing the REST resource this object represents.",
	}
	props["metadata"] = map[string]any{
		"description": "Standard object metadata.",
		"$ref":        "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta",
	}
	if specAny, ok := props["spec"]; ok {
		if specMap, ok := specAny.(map[string]any); ok {
			if _, hasType := specMap["type"]; !hasType {
				specMap["type"] = "object"
			}
		}
	}
	if statusAny, ok := props["status"]; ok {
		if statusMap, ok := statusAny.(map[string]any); ok {
			if _, hasType := statusMap["type"]; !hasType {
				statusMap["type"] = "object"
			}
		}
	}
	root["properties"] = props

	root[GVKExtension] = []map[string]any{
		{"group": gvk.Group, "version": gvk.Version, "kind": gvk.Kind},
	}

	if _, ok := root["description"]; !ok {
		root["description"] = fmt.Sprintf(
			"%s is a Kubernetes resource served via runtime/component/v2 (Track B synthesis).",
			gvk.Kind)
	}

	out, err := json.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("marshal lifted schema: %w", err)
	}
	return out, nil
}

func baselineDependencies() []string {
	return []string{
		"k8s.io/apimachinery/pkg/apis/meta/v1.ObjectMeta",
		"k8s.io/apimachinery/pkg/apis/meta/v1.ListMeta",
	}
}

// FriendlyRef converts a Go canonical type name (e.g.
// "k8s.io/apimachinery/pkg/apis/meta/v1.ObjectMeta") into the
// reverse-DNS key kube-openapi uses internally
// ("io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta").
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
