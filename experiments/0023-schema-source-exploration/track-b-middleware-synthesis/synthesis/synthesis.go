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
// The returned []byte is valid OpenAPI v3 JSON the component server
// can pass to openapi.ParseBackendSchema verbatim.
package synthesis

import (
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
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
	props["metadata"] = map[string]any{
		"description": "Standard object metadata.",
		"$ref":        "#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta",
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
