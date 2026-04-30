package scheme

import (
	"encoding/json"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestObjectJSONRoundTrip(t *testing.T) {
	t.Parallel()

	raw := []byte(`{
  "apiVersion": "aggexp.io/v1",
  "kind": "Note",
  "metadata": {"name": "hello", "namespace": "default", "uid": "abc"},
  "spec": {"title": "hi", "body": "there"},
  "status": {"updatedAt": "2026-04-29T00:00:00Z"}
}`)

	var obj Object
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if obj.APIVersion != "aggexp.io/v1" || obj.Kind != "Note" {
		t.Errorf("TypeMeta lost: %+v", obj.TypeMeta)
	}
	if obj.Name != "hello" || obj.Namespace != "default" || obj.UID != "abc" {
		t.Errorf("ObjectMeta lost: %+v", obj.ObjectMeta)
	}
	spec, _ := obj.Content["spec"].(map[string]interface{})
	if spec["title"] != "hi" {
		t.Errorf("spec.title not preserved: %v", spec)
	}

	out, err := json.Marshal(&obj)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Re-decode and compare key fields.
	var again Object
	if err := json.Unmarshal(out, &again); err != nil {
		t.Fatalf("re-unmarshal: %v", err)
	}
	if again.APIVersion != obj.APIVersion || again.Name != obj.Name {
		t.Errorf("round trip mismatch: %s vs %s", string(raw), string(out))
	}
	if s, _ := again.Content["spec"].(map[string]interface{}); s["title"] != "hi" {
		t.Errorf("spec.title lost on roundtrip: %s", string(out))
	}
}

func TestObjectDeepCopy(t *testing.T) {
	t.Parallel()
	o := &Object{
		Content: map[string]interface{}{
			"spec": map[string]interface{}{"title": "x"},
		},
	}
	o.Name = "foo"
	cp := o.DeepCopyObject().(*Object)
	if cp == o {
		t.Fatal("DeepCopyObject returned the same pointer")
	}
	// Mutate the copy; original should be untouched.
	cp.Name = "bar"
	cp.Content["spec"].(map[string]interface{})["title"] = "y"
	if o.Name != "foo" {
		t.Errorf("original Name mutated")
	}
	if o.Content["spec"].(map[string]interface{})["title"] != "x" {
		t.Errorf("original Content mutated")
	}
}

func TestObjectAsMapIncludesAllFields(t *testing.T) {
	t.Parallel()
	o := &Object{
		Content: map[string]interface{}{
			"spec": map[string]interface{}{"title": "hi"},
		},
	}
	o.APIVersion = "aggexp.io/v1"
	o.Kind = "Note"
	o.Name = "n1"

	m := o.AsMap()
	if m["apiVersion"] != "aggexp.io/v1" {
		t.Errorf("AsMap missing apiVersion: %v", m)
	}
	if m["kind"] != "Note" {
		t.Errorf("AsMap missing kind: %v", m)
	}
	meta, ok := m["metadata"].(map[string]interface{})
	if !ok || meta["name"] != "n1" {
		t.Errorf("AsMap missing/bad metadata.name: %v", m["metadata"])
	}
	if m["spec"].(map[string]interface{})["title"] != "hi" {
		t.Errorf("AsMap missing spec.title")
	}
}

func TestBuildUnstructuredMode(t *testing.T) {
	t.Parallel()
	gv := schema.GroupVersion{Group: "aggexp.io", Version: "v1"}
	b := Build(ResourceDescriptor{
		GroupVersion: gv,
		Resource:     "notes",
		Kind:         "Note",
		Singular:     "note",
		Namespaced:   true,
	})
	if b.ItemCanonicalName != UnstructuredCanonicalName {
		t.Errorf("ItemCanonicalName = %q, want %q", b.ItemCanonicalName, UnstructuredCanonicalName)
	}
	gvk := gv.WithKind("Note")
	obj, err := b.Scheme.New(gvk)
	if err != nil {
		t.Fatalf("Scheme.New(%v): %v", gvk, err)
	}
	// For unstructured, the zero value has no GVK (that's the SSA
	// blocker documented in FINDINGS/0017); assert only that the
	// type is constructible.
	if obj == nil {
		t.Fatal("Scheme.New returned nil")
	}
}

func TestBuildTypedWrapperModeRegistersInternalVersion(t *testing.T) {
	t.Parallel()
	gv := schema.GroupVersion{Group: "aggexp.io", Version: "v1"}
	b := Build(ResourceDescriptor{
		GroupVersion:    gv,
		Resource:        "notes",
		Kind:            "Note",
		Singular:        "note",
		Namespaced:      true,
		UseTypedWrapper: true,
	})
	if b.ItemCanonicalName != DynObjectCanonicalName {
		t.Errorf("ItemCanonicalName = %q, want %q", b.ItemCanonicalName, DynObjectCanonicalName)
	}
	// External GVK.
	obj, err := b.Scheme.New(gv.WithKind("Note"))
	if err != nil {
		t.Fatalf("Scheme.New external: %v", err)
	}
	if _, ok := obj.(*Object); !ok {
		t.Errorf("external New returned %T, want *Object", obj)
	}
	// Internal GVK. The generic apiserver's SSA path calls
	// Scheme.New under the internal GroupVersion; without this
	// registration SSA fails with "no kind Note is registered for
	// the internal version".
	internal := schema.GroupVersion{Group: "aggexp.io", Version: "__internal"}
	if _, err := b.Scheme.New(internal.WithKind("Note")); err != nil {
		t.Errorf("Scheme.New internal: %v", err)
	}
	// ObjectKinds on a zero-value *Object should succeed for the
	// external GV (this is the key property the typed wrapper
	// exists to provide).
	kinds, _, err := b.Scheme.ObjectKinds(&Object{})
	if err != nil {
		t.Errorf("ObjectKinds on zero value: %v", err)
	}
	foundExternal := false
	for _, k := range kinds {
		if k.GroupVersion() == gv && k.Kind == "Note" {
			foundExternal = true
		}
	}
	if !foundExternal {
		t.Errorf("ObjectKinds did not include external GVK: %v", kinds)
	}
}
