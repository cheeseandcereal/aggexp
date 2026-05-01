// Package synthesis lifts a plain JSON Schema (describing just the
// business spec + status shape of a resource) into a full
// Kubernetes-flavored OpenAPI v3 schema suitable for feeding into
// runtime/component/openapi.Compose and thus into the apiserver's
// explain + SSA paths.
//
// The backend author writing the input JSON Schema does not need to
// know anything about Kubernetes' OpenAPI dialect. They write:
//
//	{
//	  "type": "object",
//	  "properties": {
//	    "spec": { "type": "object", "properties": { "title": ... } },
//	    "status": { "type": "object", "properties": { ... } }
//	  }
//	}
//
// Synthesis adds:
//   - the x-kubernetes-group-version-kind extension on the top-level
//     schema (required by kubectl explain's GVK index and by
//     managedfields.NewTypeConverter),
//   - apiVersion / kind string properties (wire contract),
//   - metadata property with $ref to meta/v1 ObjectMeta (required so
//     SSA / merges touching metadata have a schema to walk).
//
// 0024 addendum: refs use the "#/definitions/..." v2-style form
// rather than the "#/components/schemas/..." v3-style form. The
// substrate's runtime/component/openapi/compose.go and the earlier
// 0023 Track B implementation emit v3-style refs. kube-apiserver's
// /openapi/v2 aggregator does NOT rewrite them to v2 form, so
// strict OpenAPI consumers (ArgoCD's gitops-engine cluster cache)
// reject the result. "#/definitions/" refs are acceptable to both
// /openapi/v2 and /openapi/v3 endpoints for local references within
// the same document, so we use them unconditionally. See the 0024
// FINDINGS for the empirical evidence; this is a consequent of
// kube-openapi's current behavior, not a fundamental.
package synthesis

import (
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// LiftJSONSchemaToOpenAPI takes a plain JSON Schema describing a
// resource's spec+status shape and returns a full Kubernetes OpenAPI
// v3 object schema. The input is expected to be a JSON object; if
// it is not, an error is returned.
//
// The lift is non-destructive with respect to backend-supplied
// fields: any existing properties on the root schema are preserved.
// Only apiVersion, kind, metadata, and the GVK extension are added
// (or overwritten). The backend SHOULD not ship any of these itself
// — if it does, synthesis wins (this is the "middleware knows
// Kubernetes, backend doesn't" contract).
func LiftJSONSchemaToOpenAPI(gvk schema.GroupVersionKind, jsonSchema []byte) ([]byte, error) {
	if len(jsonSchema) == 0 {
		return nil, fmt.Errorf("empty JSON schema")
	}
	var root map[string]any
	if err := json.Unmarshal(jsonSchema, &root); err != nil {
		return nil, fmt.Errorf("unmarshal JSON schema: %w", err)
	}
	// Ensure object type at the root.
	if t, ok := root["type"]; !ok || t == nil {
		root["type"] = "object"
	}

	props, _ := root["properties"].(map[string]any)
	if props == nil {
		props = map[string]any{}
	}

	// apiVersion / kind — wire-level properties every Kubernetes
	// object has. kubectl will serialize them regardless of the
	// schema, but having them in the schema is what "explain"
	// displays and what structured-merge-diff walks.
	props["apiVersion"] = map[string]any{
		"type":        "string",
		"description": "APIVersion defines the versioned schema of this representation of an object.",
	}
	props["kind"] = map[string]any{
		"type":        "string",
		"description": "Kind is a string value representing the REST resource this object represents.",
	}
	// metadata — $ref to ObjectMeta. The component server's
	// composed defs map pre-loads meta/v1 definitions, so the
	// reverse-DNS key below resolves at openapi-builder time.
	//
	// Observed during 0024's ArgoCD probe: kube-apiserver's
	// /openapi/v2 aggregator does NOT rewrite "#/components/schemas/"
	// refs to the v2-style "#/definitions/" form. Stricter OpenAPI
	// consumers (ArgoCD's cluster cache) reject the v3-style ref
	// inside a v2 document. We emit "#/definitions/" here to stay
	// compatible with both endpoints: kube-apiserver's v3 output
	// also accepts "#/definitions/" refs when cross-referencing
	// local definitions.
	props["metadata"] = map[string]any{
		"description": "Standard object metadata.",
		"$ref":        "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta",
	}

	// If the backend provided a spec but did not mark it as type
	// object, default it. Same for status. Best-effort; if the
	// backend ships something unusual we leave it alone.
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

	// x-kubernetes-group-version-kind — this is the Kubernetes
	// wire extension kubectl explain indexes by and
	// managedfields.NewTypeConverter requires to key the type.
	// openapi.ParseBackendSchema also stamps this defensively,
	// but synthesis is the canonical source — we want the lifted
	// schema to be self-sufficient.
	root["x-kubernetes-group-version-kind"] = []map[string]any{
		{"group": gvk.Group, "version": gvk.Version, "kind": gvk.Kind},
	}

	// Description: preserve what the backend said, or default.
	if _, ok := root["description"]; !ok {
		root["description"] = fmt.Sprintf(
			"%s is a Kubernetes resource served via middleware schema synthesis (0023 Track B).",
			gvk.Kind)
	}

	out, err := json.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("marshal lifted schema: %w", err)
	}
	return out, nil
}

// WrapAsListV2Refs builds the <Kind>List envelope schema using
// "#/definitions/..." refs (v2-compatible). Matches
// runtime/component/openapi.WrapAsList in shape; differs only in
// ref format.
func WrapAsListV2Refs(listGVK schema.GroupVersionKind, itemKey string) spec.Schema {
	itemRef, _ := spec.NewRef("#/definitions/" + itemKey)
	listMetaRef, _ := spec.NewRef("#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ListMeta")
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
			Description: fmt.Sprintf("%s is a list.", listGVK.Kind),
			Type:        spec.StringOrArray{"object"},
			Required:    []string{"items"},
			Properties: map[string]spec.Schema{
				"apiVersion": {SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"string"}}},
				"kind":       {SchemaProps: spec.SchemaProps{Type: spec.StringOrArray{"string"}}},
				"metadata":   {SchemaProps: spec.SchemaProps{Ref: listMetaRef, Description: "Standard list metadata."}},
				"items": {SchemaProps: spec.SchemaProps{
					Type: spec.StringOrArray{"array"},
					Items: &spec.SchemaOrArray{
						Schema: &spec.Schema{SchemaProps: spec.SchemaProps{Ref: itemRef}},
					},
					Description: "List of items.",
				}},
			},
		},
	}
}
