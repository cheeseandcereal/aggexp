package gc

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"

	"github.com/cheeseandcereal/aggexp/runtime/component/v2/metadatastore"
	componentv2pb "github.com/cheeseandcereal/aggexp/runtime/component/v2/proto"
)

// fakeBackend is a minimal componentv2pb.BackendClient returning a
// fixed List. Only List is exercised by the GC sweep.
type fakeBackend struct {
	componentv2pb.BackendClient // interface promotion (all nil methods panic on call)
	items                       [][]byte
}

func (f *fakeBackend) List(_ context.Context, _ *componentv2pb.ListRequest, _ ...grpc.CallOption) (*componentv2pb.ListResponse, error) {
	return &componentv2pb.ListResponse{ItemsJson: f.items}, nil
}

func newStore(t *testing.T) *metadatastore.Store {
	t.Helper()
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			metadatastore.GVR: "ResourceMetadataList",
		},
	)
	return metadatastore.New(dyn, "test")
}

func TestGC_HappyPath_NoOrphans(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	_, _ = s.Put(ctx, &metadatastore.Record{
		Ref: metadatastore.ResourceRef{Group: "g", Resource: "r", Name: "a"},
		UID: "1", CreationTimestamp: metav1.NewTime(time.Now().UTC().Add(-1 * time.Hour)),
	})
	b := &fakeBackend{items: [][]byte{[]byte(`{"metadata":{"name":"a"}}`)}}
	r := New(s, b, Config{Group: "g", Resource: "r", MinAge: time.Millisecond})

	res := r.RunOnce(ctx)
	if res == nil {
		t.Fatal("RunOnce returned nil")
	}
	if res.Orphans != 0 || res.Deleted != 0 {
		t.Errorf("expected no orphans, got %+v", res)
	}
}

func TestGC_OrphanDeleted(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	old := metav1.NewTime(time.Now().UTC().Add(-1 * time.Hour))
	_, _ = s.Put(ctx, &metadatastore.Record{
		Ref: metadatastore.ResourceRef{Group: "g", Resource: "r", Name: "a"},
		UID: "1", CreationTimestamp: old,
	})
	_, _ = s.Put(ctx, &metadatastore.Record{
		Ref: metadatastore.ResourceRef{Group: "g", Resource: "r", Name: "orphan"},
		UID: "2", CreationTimestamp: old,
	})
	b := &fakeBackend{items: [][]byte{[]byte(`{"metadata":{"name":"a"}}`)}}
	r := New(s, b, Config{Group: "g", Resource: "r", MinAge: time.Millisecond})

	res := r.RunOnce(ctx)
	if res.Orphans != 1 || res.Deleted != 1 {
		t.Errorf("want 1 orphan deleted, got %+v", res)
	}

	// Record should be gone.
	got, _ := s.Get(ctx, metadatastore.ResourceRef{Group: "g", Resource: "r", Name: "orphan"})
	if got != nil {
		t.Errorf("orphan still present")
	}
}

func TestGC_GraceWindowSkipsFresh(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	fresh := metav1.NewTime(time.Now().UTC().Add(-2 * time.Second))
	_, _ = s.Put(ctx, &metadatastore.Record{
		Ref: metadatastore.ResourceRef{Group: "g", Resource: "r", Name: "fresh"},
		UID: "1", CreationTimestamp: fresh,
	})
	b := &fakeBackend{items: nil} // backend empty — fresh looks like an orphan
	r := New(s, b, Config{Group: "g", Resource: "r", MinAge: 30 * time.Second})

	res := r.RunOnce(ctx)
	if res.Deleted != 0 {
		t.Errorf("grace window should have skipped; got %+v", res)
	}
	if len(res.Skipped) != 1 {
		t.Errorf("want 1 skipped record, got %d", len(res.Skipped))
	}
}

func TestGC_FinalizerProtects(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	_, _ = s.Put(ctx, &metadatastore.Record{
		Ref:        metadatastore.ResourceRef{Group: "g", Resource: "r", Name: "sticky"},
		UID:        "1",
		Finalizers: []string{"aggexp.io/hold"},
		CreationTimestamp: metav1.NewTime(time.Now().UTC().Add(-1 * time.Hour)),
	})
	b := &fakeBackend{items: nil}
	r := New(s, b, Config{Group: "g", Resource: "r", MinAge: time.Millisecond})
	res := r.RunOnce(ctx)
	if res.Deleted != 0 {
		t.Errorf("finalizer should block delete; got %+v", res)
	}
}

func TestGC_OverlapReturnsConflict(t *testing.T) {
	s := newStore(t)
	b := &fakeBackend{}
	r := New(s, b, Config{Group: "g", Resource: "r"})
	r.mu.Lock()
	r.running = true
	r.mu.Unlock()
	if got := r.RunOnce(context.Background()); got != nil {
		t.Errorf("want nil (already running), got %+v", got)
	}
}
