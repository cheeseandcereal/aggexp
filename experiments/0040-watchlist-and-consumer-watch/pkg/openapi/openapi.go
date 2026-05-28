// Package openapi provides minimal OpenAPI definitions for experiment 0040.
package openapi

import (
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"

	gen0007 "github.com/cheeseandcereal/aggexp/experiments/0007-runtime-fs-driver/pkg/generated/openapi"
)

// GetOpenAPIDefinitions reuses 0007's full meta-type definitions and
// adds Widget/WidgetList and Gadget/GadgetList on top.
func GetOpenAPIDefinitions(ref common.ReferenceCallback) map[string]common.OpenAPIDefinition {
	defs := gen0007.GetOpenAPIDefinitions(ref)

	defs["github.com/cheeseandcereal/aggexp/experiments/0040-watchlist-and-consumer-watch/pkg/types.Widget"] = common.OpenAPIDefinition{
		Schema: spec.Schema{
			SchemaProps: spec.SchemaProps{
				Description: "Widget is a push-mode resource for experiment 0040.",
				Type:        []string{"object"},
				Properties: map[string]spec.Schema{
					"metadata": {SchemaProps: spec.SchemaProps{Ref: spec.MustCreateRef("#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta")}},
					"spec": {SchemaProps: spec.SchemaProps{
						Type: []string{"object"},
						Properties: map[string]spec.Schema{
							"color": {SchemaProps: spec.SchemaProps{Type: []string{"string"}}},
							"size":  {SchemaProps: spec.SchemaProps{Type: []string{"string"}}},
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
	}
	defs["github.com/cheeseandcereal/aggexp/experiments/0040-watchlist-and-consumer-watch/pkg/types.WidgetList"] = common.OpenAPIDefinition{
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

	defs["github.com/cheeseandcereal/aggexp/experiments/0040-watchlist-and-consumer-watch/pkg/types.Gadget"] = common.OpenAPIDefinition{
		Schema: spec.Schema{
			SchemaProps: spec.SchemaProps{
				Description: "Gadget is a poll-mode resource for experiment 0040.",
				Type:        []string{"object"},
				Properties: map[string]spec.Schema{
					"metadata": {SchemaProps: spec.SchemaProps{Ref: spec.MustCreateRef("#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta")}},
					"spec": {SchemaProps: spec.SchemaProps{
						Type: []string{"object"},
						Properties: map[string]spec.Schema{
							"model":    {SchemaProps: spec.SchemaProps{Type: []string{"string"}}},
							"firmware": {SchemaProps: spec.SchemaProps{Type: []string{"string"}}},
						},
					}},
				},
			},
			VendorExtensible: spec.VendorExtensible{Extensions: map[string]interface{}{
				"x-kubernetes-group-version-kind": []interface{}{
					map[string]interface{}{"group": "gadgets.aggexp.io", "version": "v1", "kind": "Gadget"},
				},
			}},
		},
	}
	defs["github.com/cheeseandcereal/aggexp/experiments/0040-watchlist-and-consumer-watch/pkg/types.GadgetList"] = common.OpenAPIDefinition{
		Schema: spec.Schema{
			SchemaProps: spec.SchemaProps{
				Description: "GadgetList is a list of Gadget objects.",
				Type:        []string{"object"},
			},
			VendorExtensible: spec.VendorExtensible{Extensions: map[string]interface{}{
				"x-kubernetes-group-version-kind": []interface{}{
					map[string]interface{}{"group": "gadgets.aggexp.io", "version": "v1", "kind": "GadgetList"},
				},
			}},
		},
	}

	return defs
}
