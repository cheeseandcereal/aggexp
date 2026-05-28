// Package openapi provides minimal OpenAPI definitions for experiment 0038.
package openapi

import (
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"

	// Reuse 0007's generated openapi for all the standard meta types.
	gen0007 "github.com/cheeseandcereal/aggexp/experiments/0007-runtime-fs-driver/pkg/generated/openapi"
)

// GetOpenAPIDefinitions reuses 0007's full meta-type definitions and
// adds Widget/WidgetList on top with spec+status.
func GetOpenAPIDefinitions(ref common.ReferenceCallback) map[string]common.OpenAPIDefinition {
	defs := gen0007.GetOpenAPIDefinitions(ref)

	// Use the ref callback to produce the correct $ref path for ObjectMeta.
	metaRef := ref("k8s.io/apimachinery/pkg/apis/meta/v1.ObjectMeta")

	defs["github.com/cheeseandcereal/aggexp/experiments/0038-subresources-status/pkg/types.Widget"] = common.OpenAPIDefinition{
		Schema: spec.Schema{
			SchemaProps: spec.SchemaProps{
				Description: "Widget is a resource with spec and status for the subresource experiment.",
				Type:        []string{"object"},
				Properties: map[string]spec.Schema{
					"apiVersion": {SchemaProps: spec.SchemaProps{Description: "APIVersion defines the versioned schema of this representation of an object.", Type: []string{"string"}}},
					"kind":       {SchemaProps: spec.SchemaProps{Description: "Kind is a string value representing the REST resource this object represents.", Type: []string{"string"}}},
					"metadata":   {SchemaProps: spec.SchemaProps{Ref: metaRef}},
					"spec": {SchemaProps: spec.SchemaProps{
						Description: "User-controlled specification.",
						Type:        []string{"object"},
						Properties: map[string]spec.Schema{
							"color": {SchemaProps: spec.SchemaProps{Description: "The widget color.", Type: []string{"string"}}},
							"size":  {SchemaProps: spec.SchemaProps{Description: "The widget size.", Type: []string{"string"}}},
						},
					}},
					"status": {SchemaProps: spec.SchemaProps{
						Description: "Controller-controlled status.",
						Type:        []string{"object"},
						Properties: map[string]spec.Schema{
							"phase":   {SchemaProps: spec.SchemaProps{Description: "Phase: Pending, Active, or Failed.", Type: []string{"string"}}},
							"message": {SchemaProps: spec.SchemaProps{Description: "Human-readable status message.", Type: []string{"string"}}},
						},
					}},
				},
			},
			VendorExtensible: spec.VendorExtensible{Extensions: map[string]interface{}{
				"x-kubernetes-group-version-kind": []interface{}{
					map[string]interface{}{"group": "widgets.aggexp.io", "version": "v1", "kind": "Widget"},
				},
			}},
		},
		Dependencies: []string{"k8s.io/apimachinery/pkg/apis/meta/v1.ObjectMeta"},
	}
	defs["github.com/cheeseandcereal/aggexp/experiments/0038-subresources-status/pkg/types.WidgetList"] = common.OpenAPIDefinition{
		Schema: spec.Schema{
			SchemaProps: spec.SchemaProps{
				Description: "WidgetList is a list of Widget objects.",
				Type:        []string{"object"},
			},
			VendorExtensible: spec.VendorExtensible{Extensions: map[string]interface{}{
				"x-kubernetes-group-version-kind": []interface{}{
					map[string]interface{}{"group": "widgets.aggexp.io", "version": "v1", "kind": "WidgetList"},
				},
			}},
		},
	}
	return defs
}
