package openapi

import (
	"encoding/json"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

func TestParseBackendSchemaStampsGVKExtension(t *testing.T) {
	t.Parallel()
	raw, _ := json.Marshal(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"spec": map[string]any{"type": "object"},
		},
	})
	gvk := schema.GroupVersionKind{Group: "aggexp.io", Version: "v1", Kind: "Note"}
	s, err := ParseBackendSchema(raw, gvk)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ext, ok := s.Extensions[GVKExtension]
	if !ok {
		t.Fatalf("GVK extension missing from parsed schema: %+v", s.Extensions)
	}
	list, _ := ext.([]interface{})
	if len(list) != 1 {
		t.Fatalf("GVK extension not a one-entry list: %v", ext)
	}
	m, _ := list[0].(map[string]interface{})
	if m["group"] != "aggexp.io" || m["version"] != "v1" || m["kind"] != "Note" {
		t.Errorf("GVK extension contents wrong: %v", m)
	}
}

func TestParseBackendSchemaOverwritesBadExtension(t *testing.T) {
	t.Parallel()
	raw, _ := json.Marshal(map[string]any{
		"type": "object",
		"x-kubernetes-group-version-kind": []map[string]any{
			{"group": "wrong.io", "version": "v0", "kind": "Other"},
		},
	})
	gvk := schema.GroupVersionKind{Group: "aggexp.io", Version: "v1", Kind: "Note"}
	s, err := ParseBackendSchema(raw, gvk)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	list, _ := s.Extensions[GVKExtension].([]interface{})
	m, _ := list[0].(map[string]interface{})
	if m["group"] != "aggexp.io" {
		t.Errorf("backend's bad GVK extension should be overwritten; got %v", m)
	}
}

func TestParseBackendSchemaErrorsOnEmpty(t *testing.T) {
	t.Parallel()
	if _, err := ParseBackendSchema(nil, schema.GroupVersionKind{}); err == nil {
		t.Fatal("expected error for empty payload")
	}
}

func TestFallbackCarriesPreserveUnknownFields(t *testing.T) {
	t.Parallel()
	gvk := schema.GroupVersionKind{Group: "aggexp.io", Version: "v1", Kind: "Note"}
	s := Fallback(gvk, "custom desc")
	if s.Description != "custom desc" {
		t.Errorf("description not honored: %q", s.Description)
	}
	if got := s.Extensions["x-kubernetes-preserve-unknown-fields"]; got != true {
		t.Errorf("preserve-unknown-fields missing: %v", got)
	}
	if _, ok := s.Extensions[GVKExtension]; !ok {
		t.Errorf("GVK extension missing on fallback")
	}
}

func TestWrapAsListBuildsItemsRef(t *testing.T) {
	t.Parallel()
	listGVK := schema.GroupVersionKind{Group: "aggexp.io", Version: "v1", Kind: "NoteList"}
	ls := WrapAsList(listGVK, "example.io/pkg.Note")
	items, ok := ls.Properties["items"]
	if !ok {
		t.Fatal("items property missing")
	}
	if items.Items == nil || items.Items.Schema == nil {
		t.Fatal("items.Items.Schema missing")
	}
	refStr := items.Items.Schema.Ref.String()
	if refStr == "" {
		t.Error("items Ref empty")
	}
}

func TestComposeIncludesBaselineAndOverride(t *testing.T) {
	t.Parallel()
	itemSchema := Fallback(schema.GroupVersionKind{Group: "a", Version: "v1", Kind: "K"}, "item")
	listSchema := WrapAsList(schema.GroupVersionKind{Group: "a", Version: "v1", Kind: "KList"}, "item.canonical")
	fn := Compose(itemSchema, listSchema, "item.canonical", "list.canonical")
	defs := fn(func(path string) spec.Ref {
		r, _ := spec.NewRef("#/components/schemas/" + path)
		return r
	})
	if _, ok := defs["k8s.io/apimachinery/pkg/apis/meta/v1.ObjectMeta"]; !ok {
		t.Error("composed defs missing baseline ObjectMeta")
	}
	if _, ok := defs["item.canonical"]; !ok {
		t.Error("composed defs missing item override")
	}
	if _, ok := defs["list.canonical"]; !ok {
		t.Error("composed defs missing list override")
	}
}

func TestFriendlyRef(t *testing.T) {
	t.Parallel()
	got := FriendlyRef("k8s.io/apimachinery/pkg/apis/meta/v1.ObjectMeta")
	want := "io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"
	if got != want {
		t.Errorf("FriendlyRef = %q, want %q", got, want)
	}
	got = FriendlyRef("github.com/cheeseandcereal/aggexp/runtime/component/scheme.Object")
	want = "com.github.cheeseandcereal.aggexp.runtime.component.scheme.Object"
	if got != want {
		t.Errorf("FriendlyRef = %q, want %q", got, want)
	}
}
