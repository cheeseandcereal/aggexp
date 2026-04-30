package grpcbackend

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"

	componentpb "github.com/cheeseandcereal/aggexp/runtime/component/proto"
	componentscheme "github.com/cheeseandcereal/aggexp/runtime/component/scheme"
)

// fakeBackendClient is an in-memory implementation of
// componentpb.BackendClient for tests. Only methods we need are
// filled in; others error cleanly.
type fakeBackendClient struct {
	items        map[string][]byte // key: ns|name
	createErr    error
	getErr       error
	getNotFound  string // name
	listItems    [][]byte
	lastCreateFM string
	lastUpdateFM string
}

func (f *fakeBackendClient) key(ns, name string) string { return ns + "|" + name }

func (f *fakeBackendClient) GetSchema(ctx context.Context, in *componentpb.GetSchemaRequest, opts ...grpc.CallOption) (*componentpb.GetSchemaResponse, error) {
	return nil, fmt.Errorf("not used in these tests")
}
func (f *fakeBackendClient) Get(ctx context.Context, in *componentpb.GetRequest, opts ...grpc.CallOption) (*componentpb.GetResponse, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	if f.getNotFound != "" && in.Name == f.getNotFound {
		return nil, grpcstatus.Errorf(codes.NotFound, "not found")
	}
	raw, ok := f.items[f.key(in.Namespace, in.Name)]
	if !ok {
		return nil, grpcstatus.Errorf(codes.NotFound, "not found")
	}
	return &componentpb.GetResponse{ObjectJson: raw}, nil
}
func (f *fakeBackendClient) List(ctx context.Context, in *componentpb.ListRequest, opts ...grpc.CallOption) (*componentpb.ListResponse, error) {
	if f.listItems != nil {
		return &componentpb.ListResponse{ItemsJson: f.listItems}, nil
	}
	out := make([][]byte, 0, len(f.items))
	for _, v := range f.items {
		out = append(out, v)
	}
	return &componentpb.ListResponse{ItemsJson: out}, nil
}
func (f *fakeBackendClient) Create(ctx context.Context, in *componentpb.CreateRequest, opts ...grpc.CallOption) (*componentpb.CreateResponse, error) {
	if f.createErr != nil {
		return nil, f.createErr
	}
	f.lastCreateFM = in.FieldManager
	return &componentpb.CreateResponse{ObjectJson: in.ObjectJson}, nil
}
func (f *fakeBackendClient) Update(ctx context.Context, in *componentpb.UpdateRequest, opts ...grpc.CallOption) (*componentpb.UpdateResponse, error) {
	f.lastUpdateFM = in.FieldManager
	return &componentpb.UpdateResponse{ObjectJson: in.ObjectJson, Created: false}, nil
}
func (f *fakeBackendClient) Apply(ctx context.Context, in *componentpb.ApplyRequest, opts ...grpc.CallOption) (*componentpb.ApplyResponse, error) {
	return &componentpb.ApplyResponse{ObjectJson: in.ObjectJson}, nil
}
func (f *fakeBackendClient) Delete(ctx context.Context, in *componentpb.DeleteRequest, opts ...grpc.CallOption) (*componentpb.DeleteResponse, error) {
	raw, ok := f.items[f.key(in.Namespace, in.Name)]
	if !ok {
		return nil, grpcstatus.Errorf(codes.NotFound, "not found")
	}
	return &componentpb.DeleteResponse{ObjectJson: raw, Deleted: true}, nil
}
func (f *fakeBackendClient) Watch(ctx context.Context, in *componentpb.WatchRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[componentpb.WatchEvent], error) {
	return nil, fmt.Errorf("watch not exercised in unit tests")
}

func newTestREST(t *testing.T, useTyped, writable bool) (*REST, *fakeBackendClient) {
	t.Helper()
	fake := &fakeBackendClient{items: map[string][]byte{}}
	r := New(Descriptor{
		GroupVersion:    schema.GroupVersion{Group: "aggexp.io", Version: "v1"},
		Resource:        "notes",
		Kind:            "Note",
		Singular:        "note",
		Namespaced:      true,
		Writable:        writable,
		UseTypedWrapper: useTyped,
		Columns: []metav1.TableColumnDefinition{
			{Name: "Name", Type: "string"},
			{Name: "Title", Type: "string"},
		},
		RowFields: []string{".metadata.name", ".spec.title"},
	}, fake)
	t.Cleanup(r.Shutdown)
	return r, fake
}

func TestNewNewListUnstructuredMode(t *testing.T) {
	r, _ := newTestREST(t, false, true)
	obj := r.New()
	if _, ok := obj.(*unstructured.Unstructured); !ok {
		t.Errorf("New() = %T, want *unstructured.Unstructured", obj)
	}
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Kind != "Note" || gvk.Group != "aggexp.io" {
		t.Errorf("GVK stamped wrong: %+v", gvk)
	}
	list := r.NewList()
	if _, ok := list.(*unstructured.UnstructuredList); !ok {
		t.Errorf("NewList() = %T", list)
	}
}

func TestNewNewListTypedMode(t *testing.T) {
	r, _ := newTestREST(t, true, true)
	obj := r.New()
	if _, ok := obj.(*componentscheme.Object); !ok {
		t.Errorf("New() = %T, want *scheme.Object", obj)
	}
	list := r.NewList()
	if _, ok := list.(*componentscheme.ObjectList); !ok {
		t.Errorf("NewList() = %T", list)
	}
}

func TestGetRoundTrip(t *testing.T) {
	r, fake := newTestREST(t, false, true)
	raw, _ := json.Marshal(map[string]any{
		"apiVersion": "aggexp.io/v1",
		"kind":       "Note",
		"metadata":   map[string]any{"name": "hello", "namespace": "default"},
		"spec":       map[string]any{"title": "hi"},
	})
	fake.items[fake.key("default", "hello")] = raw

	ctx := withNamespace(context.Background(), "default")
	obj, err := r.Get(ctx, "hello", nil)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	u := obj.(*unstructured.Unstructured)
	if u.GetName() != "hello" {
		t.Errorf("Get returned wrong name: %s", u.GetName())
	}
	// Component must stamp an RV on GET (per the 0018 finding).
	if u.GetResourceVersion() == "" {
		t.Errorf("Get did not stamp a resourceVersion")
	}
}

func TestGetTranslatesNotFound(t *testing.T) {
	r, fake := newTestREST(t, false, true)
	fake.getNotFound = "missing"
	ctx := withNamespace(context.Background(), "default")
	_, err := r.Get(ctx, "missing", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	// apierrors.IsNotFound should recognize the translated error.
	if got := grpcstatus.Code(err); got == codes.NotFound {
		// That's the raw gRPC error — we want the translated
		// Kubernetes error instead.
		t.Errorf("translateErr did not run: got raw gRPC %v", got)
	}
}

func TestCreateWritesFieldManager(t *testing.T) {
	r, fake := newTestREST(t, false, true)
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "aggexp.io", Version: "v1", Kind: "Note"})
	obj.SetName("x")
	obj.SetNamespace("default")
	_, err := r.Create(context.Background(), obj, nil, &metav1.CreateOptions{FieldManager: "kubectl"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if fake.lastCreateFM != "kubectl" {
		t.Errorf("fieldManager not forwarded: got %q", fake.lastCreateFM)
	}
}

func TestCreateRejectedIfNotWritable(t *testing.T) {
	r, _ := newTestREST(t, false, false)
	obj := &unstructured.Unstructured{}
	obj.SetName("x")
	_, err := r.Create(context.Background(), obj, nil, nil)
	if err == nil {
		t.Fatal("expected MethodNotSupported")
	}
}

func TestLookupFieldPath(t *testing.T) {
	t.Parallel()
	m := map[string]any{
		"metadata": map[string]any{"name": "hello"},
		"spec":     map[string]any{"title": "hi"},
	}
	if got := LookupField(m, ".metadata.name"); got != "hello" {
		t.Errorf(".metadata.name = %v", got)
	}
	if got := LookupField(m, ".spec.title"); got != "hi" {
		t.Errorf(".spec.title = %v", got)
	}
	// Missing path renders as empty string for kubectl cells.
	if got := LookupField(m, ".missing.path"); got != "" {
		t.Errorf("missing path = %v", got)
	}
}

// withNamespace injects a namespace into ctx using the apiserver's
// request metadata; mirrors what the endpoints layer does.
func withNamespace(ctx context.Context, ns string) context.Context {
	return genericapirequest.WithNamespace(ctx, ns)
}
