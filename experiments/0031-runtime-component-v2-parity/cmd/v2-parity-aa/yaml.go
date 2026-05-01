package main

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

// decodeYAML turns a single-document YAML into an
// unstructured.Unstructured. Embedded CRDs from the v2 substrate
// ship as single-document YAMLs, so this is minimal on purpose.
func decodeYAML(raw []byte) (*unstructured.Unstructured, error) {
	var m map[string]interface{}
	if err := yaml.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("yaml unmarshal: %w", err)
	}
	return &unstructured.Unstructured{Object: m}, nil
}
