package grpcbackend

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/dynamic/fake"

	"github.com/cheeseandcereal/aggexp/runtime/component/v2/metadatastore"
	componentscheme "github.com/cheeseandcereal/aggexp/runtime/component/v2/scheme"
)

// TestREST_MetadataStitching exercises the core v2 integration: a
// backend sees only spec+status, the metadata store holds uid +
// labels + annotations + finalizers, and the stitched Get returns
// both.
func TestREST_MetadataStitching(t *testing.T) {
	b := newFakeBackend()
	r := newREST(b)
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			metadatastore.GVR: "ResourceMetadataList",
		},
	)
	store := metadatastore.New(dyn, "test")
	r = r.WithMetadataStore(store)

	ctx := genericapirequest.WithNamespace(context.Background(), "default")

	o := &componentscheme.Object{}
	o.SetName("hello")
	o.SetNamespace("default")
	o.SetLabels(map[string]string{"app": "demo"})
	o.SetAnnotations(map[string]string{"owner": "alice"})
	o.SetFinalizers([]string{"aggexp.io/hold"})
	o.SetGroupVersionKind(schema.GroupVersionKind{Group: "notes.aggexp.io", Version: "v1", Kind: "Note"})
	o.Content = map[string]interface{}{"spec": map[string]interface{}{"title": "H"}}

	created, err := r.Create(ctx, o, nil, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	co := created.(*componentscheme.Object)
	if co.Labels["app"] != "demo" {
		t.Errorf("labels lost on Create response: %v", co.Labels)
	}
	if co.Annotations["owner"] != "alice" {
		t.Errorf("annotations lost: %v", co.Annotations)
	}
	if len(co.Finalizers) != 1 {
		t.Errorf("finalizers lost: %v", co.Finalizers)
	}

	// Metastore record exists.
	rec, err := store.Get(ctx, metadatastore.ResourceRef{
		Group: "notes.aggexp.io", Resource: "notes", Namespace: "default", Name: "hello",
	})
	if err != nil || rec == nil {
		t.Fatalf("Record missing after Create: err=%v rec=%v", err, rec)
	}
	if rec.UID == "" || rec.UID == "synthetic" {
		t.Errorf("Record UID not persisted cleanly: %q", rec.UID)
	}

	// Get returns stitched object.
	got, err := r.Get(ctx, "hello", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	gotObj := got.(*componentscheme.Object)
	if gotObj.Labels["app"] != "demo" {
		t.Errorf("Get: labels lost: %v", gotObj.Labels)
	}
	if gotObj.UID == "" {
		t.Errorf("Get: UID empty")
	}

	// Delete with finalizers still set → backend NOT called, record
	// gets deletionTimestamp.
	_, _, err = r.Delete(ctx, "hello", nil, &metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("Delete (finalizer blocked): %v", err)
	}
	// Backend should still have the item.
	b.mu.Lock()
	_, stillPresent := b.items["default/hello"]
	b.mu.Unlock()
	if !stillPresent {
		t.Errorf("finalizer-blocked delete should have preserved backend object")
	}
	// Record should have deletionTimestamp.
	rec2, _ := store.Get(ctx, metadatastore.ResourceRef{
		Group: "notes.aggexp.io", Resource: "notes", Namespace: "default", Name: "hello",
	})
	if rec2 == nil || rec2.DeletionTimestamp == nil {
		t.Errorf("Record missing deletionTimestamp after blocked delete: %+v", rec2)
	}
}
