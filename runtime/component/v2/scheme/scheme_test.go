package scheme

import (
	"encoding/json"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestBuild_TypedWrapper(t *testing.T) {
	gv := schema.GroupVersion{Group: "notes.aggexp.io", Version: "v1"}
	b := Build(ResourceDescriptor{
		GroupVersion: gv, Resource: "notes", Kind: "Note", Singular: "note",
		Namespaced: true, UseTypedWrapper: true,
	})
	if b.ItemCanonicalName != ObjectCanonicalName {
		t.Errorf("ItemCanonicalName=%q, want %q", b.ItemCanonicalName, ObjectCanonicalName)
	}
	obj, err := b.Scheme.New(gv.WithKind("Note"))
	if err != nil {
		t.Fatalf("scheme.New: %v", err)
	}
	if _, ok := obj.(*Object); !ok {
		t.Fatalf("scheme.New returned %T, want *Object", obj)
	}
	// The List kind too.
	list, err := b.Scheme.New(gv.WithKind("NoteList"))
	if err != nil {
		t.Fatalf("scheme.New List: %v", err)
	}
	if _, ok := list.(*ObjectList); !ok {
		t.Fatalf("NoteList is %T, want *ObjectList", list)
	}
}

func TestBuild_Unstructured(t *testing.T) {
	gv := schema.GroupVersion{Group: "notes.aggexp.io", Version: "v1"}
	b := Build(ResourceDescriptor{
		GroupVersion: gv, Resource: "notes", Kind: "Note", Singular: "note",
		UseTypedWrapper: false,
	})
	if b.ItemCanonicalName != UnstructuredCanonicalName {
		t.Errorf("ItemCanonicalName=%q", b.ItemCanonicalName)
	}
	obj, err := b.Scheme.New(gv.WithKind("Note"))
	if err != nil {
		t.Fatalf("scheme.New: %v", err)
	}
	if _, ok := obj.(*unstructured.Unstructured); !ok {
		t.Fatalf("scheme.New returned %T, want *Unstructured", obj)
	}
}

func TestObjectRoundTrip(t *testing.T) {
	raw := []byte(`{
		"apiVersion": "notes.aggexp.io/v1",
		"kind": "Note",
		"metadata": {"name": "hello", "namespace": "default",
			"annotations": {"aggexp.io/x": "y"}},
		"spec": {"title": "Hello", "body": "World"},
		"status": {"phase": "Ready"}
	}`)
	var o Object
	if err := o.UnmarshalJSON(raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if o.Name != "hello" || o.Namespace != "default" {
		t.Errorf("ObjectMeta wrong: %+v", o.ObjectMeta)
	}
	if o.APIVersion != "notes.aggexp.io/v1" || o.Kind != "Note" {
		t.Errorf("TypeMeta wrong: %+v", o.TypeMeta)
	}
	if v := o.Annotations["aggexp.io/x"]; v != "y" {
		t.Errorf("annotation not surfaced: got %q", v)
	}
	spec, ok := o.Content["spec"].(map[string]interface{})
	if !ok {
		t.Fatalf("spec not in Content: %+v", o.Content)
	}
	if spec["title"] != "Hello" {
		t.Errorf("spec.title=%v, want Hello", spec["title"])
	}

	out, err := o.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back map[string]any
	_ = json.Unmarshal(out, &back)
	meta, _ := back["metadata"].(map[string]any)
	if meta["name"] != "hello" {
		t.Errorf("roundtrip lost name: %v", back)
	}
	ann, _ := meta["annotations"].(map[string]any)
	if ann["aggexp.io/x"] != "y" {
		t.Errorf("roundtrip lost annotation: %v", meta)
	}
}

func TestObject_DeepCopy(t *testing.T) {
	o := &Object{
		TypeMeta: metav1.TypeMeta{APIVersion: "notes.aggexp.io/v1", Kind: "Note"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "a",
			Annotations: map[string]string{"k": "v"},
		},
		Content: map[string]interface{}{
			"spec": map[string]interface{}{"x": 1},
		},
	}
	cp := o.DeepCopyObject().(*Object)
	cp.Name = "b"
	cp.Content["spec"].(map[string]interface{})["x"] = 2
	if o.Name != "a" {
		t.Errorf("DeepCopy leaked Name: %s", o.Name)
	}
	if o.Content["spec"].(map[string]interface{})["x"] != 1 {
		t.Errorf("DeepCopy leaked Content: %v", o.Content)
	}
}

func TestAsMap(t *testing.T) {
	o := &Object{
		TypeMeta: metav1.TypeMeta{APIVersion: "notes.aggexp.io/v1", Kind: "Note"},
		ObjectMeta: metav1.ObjectMeta{Name: "x"},
		Content: map[string]interface{}{"spec": map[string]interface{}{"title": "t"}},
	}
	m := o.AsMap()
	if m["apiVersion"] != "notes.aggexp.io/v1" {
		t.Errorf("apiVersion: %v", m)
	}
	if spec, ok := m["spec"].(map[string]interface{}); !ok || spec["title"] != "t" {
		t.Errorf("spec: %v", m)
	}
	md, _ := m["metadata"].(map[string]interface{})
	if md["name"] != "x" {
		t.Errorf("metadata.name: %v", md)
	}
}
