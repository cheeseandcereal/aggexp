package openapi

import (
	"encoding/json"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

func TestSynthesize_AddsKubernetesKeys(t *testing.T) {
	in := []byte(`{
		"type":"object",
		"properties":{
			"spec":{"type":"object","properties":{"title":{"type":"string"}}}
		}
	}`)
	gvk := schema.GroupVersionKind{Group: "notes.aggexp.io", Version: "v1", Kind: "Note"}
	out, err := Synthesize(gvk, in)
	if err != nil {
		t.Fatalf("Synthesize: %v", err)
	}
	var m map[string]any
	_ = json.Unmarshal(out, &m)
	props, _ := m["properties"].(map[string]any)
	if _, ok := props["apiVersion"]; !ok {
		t.Errorf("apiVersion missing")
	}
	if _, ok := props["kind"]; !ok {
		t.Errorf("kind missing")
	}
	md, _ := props["metadata"].(map[string]any)
	ref, _ := md["$ref"].(string)
	if !strings.HasPrefix(ref, "#/definitions/") {
		t.Errorf("metadata ref not v2-style: %q", ref)
	}
	ext, ok := m[GVKExtension].([]any)
	if !ok || len(ext) == 0 {
		t.Errorf("GVK extension missing: %v", m[GVKExtension])
	}
}

func TestSynthesize_PreservesExistingDescription(t *testing.T) {
	in := []byte(`{"description":"keep me","type":"object","properties":{"spec":{}}}`)
	out, _ := Synthesize(schema.GroupVersionKind{Group: "g", Version: "v", Kind: "K"}, in)
	var m map[string]any
	_ = json.Unmarshal(out, &m)
	if m["description"] != "keep me" {
		t.Errorf("description overwritten: %v", m["description"])
	}
}

func TestParseBackendSchema_StampsGVK(t *testing.T) {
	gvk := schema.GroupVersionKind{Group: "g", Version: "v1", Kind: "K"}
	s, err := ParseBackendSchema([]byte(`{"type":"object"}`), gvk)
	if err != nil {
		t.Fatalf("ParseBackendSchema: %v", err)
	}
	ext, ok := s.Extensions[GVKExtension].([]interface{})
	if !ok || len(ext) == 0 {
		t.Fatalf("GVK extension missing: %+v", s.Extensions)
	}
	m := ext[0].(map[string]interface{})
	if m["group"] != "g" || m["version"] != "v1" || m["kind"] != "K" {
		t.Errorf("GVK wrong: %v", m)
	}
}

func TestWrapAsList_V2Refs(t *testing.T) {
	lg := schema.GroupVersionKind{Group: "g", Version: "v1", Kind: "KList"}
	s := WrapAsList(lg, ObjectItemCanonicalNameForTest())
	// items ref should use "#/definitions/".
	items := s.Properties["items"]
	got := items.Items.Schema.Ref.String()
	if !strings.HasPrefix(got, "#/definitions/") {
		t.Errorf("items ref %q not v2-style", got)
	}
	md := s.Properties["metadata"]
	if !strings.HasPrefix(md.Ref.String(), "#/definitions/") {
		t.Errorf("metadata ref %q not v2-style", md.Ref.String())
	}
}

// ObjectItemCanonicalNameForTest exists only so the test file can
// reference the typed wrapper name without importing the scheme
// package (which would create a dependency cycle in practice — not
// here, but conservative regardless).
func ObjectItemCanonicalNameForTest() string {
	return "github.com/cheeseandcereal/aggexp/runtime/component/v2/scheme.Object"
}

func TestCompose_Live(t *testing.T) {
	state := map[string]common.OpenAPIDefinition{}
	var get = func() map[string]common.OpenAPIDefinition { return state }
	fn := Compose(get)

	cb := common.ReferenceCallback(func(path string) spec.Ref { return spec.Ref{} })
	defs := fn(cb)
	if _, ok := defs["k8s.io/apimachinery/pkg/apis/meta/v1.ObjectMeta"]; !ok {
		t.Errorf("baseline ObjectMeta missing")
	}

	// Add a group and re-invoke; the new item should appear.
	state["my/item"] = common.OpenAPIDefinition{}
	defs2 := fn(cb)
	if _, ok := defs2["my/item"]; !ok {
		t.Errorf("live update not reflected")
	}
}
