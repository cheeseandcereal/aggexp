package metadatastore

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
)

// newFake builds a fake dynamic client with the ResourceMetadata
// list kind registered.
func newFake() *fake.FakeDynamicClient {
	return fake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			GVR: "ResourceMetadataList",
		},
	)
}

func TestRecordName(t *testing.T) {
	tests := []struct {
		ref      ResourceRef
		wantHash bool
		contains string
	}{
		{
			ref:      ResourceRef{Group: "notes.aggexp.io", Resource: "notes", Namespace: "default", Name: "hello"},
			contains: "notes-aggexp-io.notes.default.hello",
		},
		{
			ref:      ResourceRef{Group: "notes.aggexp.io", Resource: "notes", Name: "hello"},
			contains: "notes-aggexp-io.notes.cluster.hello",
		},
		{
			ref:      ResourceRef{Group: "notes.aggexp.io", Resource: "notes", Name: strings.Repeat("x", 400)},
			wantHash: true,
		},
		{
			ref:      ResourceRef{Group: "g", Resource: "r", Namespace: "ns", Name: "INVALID_UPPER"},
			wantHash: true,
		},
	}
	for _, tt := range tests {
		got := RecordName(tt.ref)
		if tt.wantHash {
			if !strings.HasPrefix(got, "rmeta-") {
				t.Errorf("RecordName(%+v)=%q, want rmeta-prefix", tt.ref, got)
			}
		} else if got != tt.contains {
			t.Errorf("RecordName(%+v)=%q, want %q", tt.ref, got, tt.contains)
		}
	}
}

func TestStore_PutGetDelete(t *testing.T) {
	s := New(newFake(), "test")
	ctx := context.Background()

	ref := ResourceRef{Group: "notes.aggexp.io", Resource: "notes", Namespace: "default", Name: "hello"}
	rec := &Record{
		Ref:               ref,
		UID:               "abc-123",
		CreationTimestamp: metav1.NewTime(time.Now().UTC().Truncate(time.Second)),
		Labels:            map[string]string{"app": "demo"},
		Annotations:       map[string]string{"owner": "alice"},
		Finalizers:        []string{"aggexp.io/finalizer"},
	}
	stored, err := s.Put(ctx, rec)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if stored.UID != "abc-123" {
		t.Errorf("UID round-trip: %s", stored.UID)
	}

	got, err := s.Get(ctx, ref)
	if err != nil || got == nil {
		t.Fatalf("Get: got=%v err=%v", got, err)
	}
	if got.Labels["app"] != "demo" {
		t.Errorf("labels lost: %v", got.Labels)
	}
	if len(got.Finalizers) != 1 || got.Finalizers[0] != "aggexp.io/finalizer" {
		t.Errorf("finalizers lost: %v", got.Finalizers)
	}

	if err := s.Delete(ctx, ref); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got2, err := s.Get(ctx, ref)
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if got2 != nil {
		t.Errorf("Record still present after Delete: %+v", got2)
	}
	// Delete is idempotent.
	if err := s.Delete(ctx, ref); err != nil {
		t.Errorf("second Delete: %v", err)
	}
}

func TestStore_List_FiltersByGR(t *testing.T) {
	s := New(newFake(), "test")
	ctx := context.Background()

	_, _ = s.Put(ctx, &Record{Ref: ResourceRef{Group: "a.io", Resource: "r1", Name: "x"}, UID: "1"})
	_, _ = s.Put(ctx, &Record{Ref: ResourceRef{Group: "a.io", Resource: "r1", Name: "y"}, UID: "2"})
	_, _ = s.Put(ctx, &Record{Ref: ResourceRef{Group: "b.io", Resource: "r2", Name: "z"}, UID: "3"})

	got, err := s.List(ctx, "a.io", "r1")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("want 2 records, got %d", len(got))
	}
}

func TestRefFromUnstructured(t *testing.T) {
	u := &unstructured.Unstructured{}
	raw := []byte(`{
		"apiVersion":"aggexpmeta.aggexp.io/v1",
		"kind":"ResourceMetadata",
		"metadata":{"name":"r"},
		"spec":{"resourceRef":{"group":"g","resource":"r","namespace":"ns","name":"n"}}
	}`)
	_ = json.Unmarshal(raw, &u.Object)
	ref := RefFromUnstructured(u)
	if ref.Group != "g" || ref.Resource != "r" || ref.Namespace != "ns" || ref.Name != "n" {
		t.Errorf("RefFromUnstructured: %+v", ref)
	}
}

func TestEmbeddedCRD(t *testing.T) {
	if !strings.Contains(string(CRDYAML), "resourcemetadatas.aggexpmeta.aggexp.io") {
		t.Errorf("embedded CRDYAML missing expected content")
	}
}
