package watch

import (
	"encoding/json"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestBookmarkObject_TypedWrapper(t *testing.T) {
	gvk := schema.GroupVersionKind{Group: "g", Version: "v", Kind: "K"}
	obj := BookmarkObject(gvk, "123", true)
	acc, err := meta.Accessor(obj)
	if err != nil {
		t.Fatalf("accessor: %v", err)
	}
	if acc.GetResourceVersion() != "123" {
		t.Errorf("RV: %s", acc.GetResourceVersion())
	}
	if acc.GetAnnotations()[InitialEventsEndAnnotation] != "true" {
		t.Errorf("annotation missing: %v", acc.GetAnnotations())
	}
	raw, _ := json.Marshal(obj)
	if !strings.Contains(string(raw), `"k8s.io/initial-events-end":"true"`) {
		t.Errorf("marshaled obj lost annotation: %s", raw)
	}
}

func TestBookmarkObject_Unstructured(t *testing.T) {
	gvk := schema.GroupVersionKind{Group: "g", Version: "v", Kind: "K"}
	obj := BookmarkObject(gvk, "5", false)
	acc, _ := meta.Accessor(obj)
	if acc.GetResourceVersion() != "5" {
		t.Errorf("RV: %s", acc.GetResourceVersion())
	}
}

func TestAuthority_Monotonic(t *testing.T) {
	a := New()
	if a.Current() != "1" {
		t.Errorf("initial=%s", a.Current())
	}
	r1 := a.Next()
	r2 := a.Next()
	if r1 == r2 {
		t.Errorf("Next didn't advance: %s %s", r1, r2)
	}
}

func TestAuthority_ObserveAdvances(t *testing.T) {
	a := New()
	a.Observe("100")
	if a.Current() != "100" {
		t.Errorf("Observe didn't bump: %s", a.Current())
	}
	// Observing a smaller value doesn't retrograde.
	a.Observe("50")
	if a.Current() != "100" {
		t.Errorf("Observe regressed: %s", a.Current())
	}
	// Non-numeric ignored.
	a.Observe("abc")
	if a.Current() != "100" {
		t.Errorf("Observe broke on bad input: %s", a.Current())
	}
}

func TestAuthority_Resolve(t *testing.T) {
	a := New()
	got := a.Resolve("42")
	if got != "42" {
		t.Errorf("Resolve record RV: %s", got)
	}
	if a.Current() != "42" {
		t.Errorf("Resolve didn't Observe: %s", a.Current())
	}
	got = a.Resolve("")
	if got != "42" {
		t.Errorf("Resolve fallback: %s", got)
	}
}
