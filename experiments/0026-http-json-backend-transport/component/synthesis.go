// Copy-fork of experiments/0023-schema-source-exploration/track-b-
// middleware-synthesis/synthesis/synthesis.go.
//
// 0023's synthesis package is experiment-local and has not been
// promoted into runtime/. Rather than introduce an unrelated
// cross-experiment import (and violate "don't refactor across
// experiment boundaries"), we copy the function verbatim. The
// 0030 substrate promotion is the right place to unify.

package main

import (
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// LiftJSONSchemaToOpenAPI lifts a plain JSON Schema describing a
// resource's spec+status shape into Kubernetes-flavored OpenAPI v3.
// See FINDINGS/0023-schema-source-exploration.md for the rationale
// and the list of Kubernetes concepts this function adds (apiVersion,
// kind, metadata $ref, GVK extension).
func LiftJSONSchemaToOpenAPI(gvk schema.GroupVersionKind, jsonSchema []byte) ([]byte, error) {
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
		"$ref":        "#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta",
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
	root["x-kubernetes-group-version-kind"] = []map[string]any{
		{"group": gvk.Group, "version": gvk.Version, "kind": gvk.Kind},
	}
	if _, ok := root["description"]; !ok {
		root["description"] = fmt.Sprintf(
			"%s is a Kubernetes resource served via middleware schema synthesis (0026 HTTP transport).",
			gvk.Kind)
	}
	out, err := json.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("marshal lifted schema: %w", err)
	}
	return out, nil
}
