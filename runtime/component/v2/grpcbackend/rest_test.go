package grpcbackend

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilwatch "k8s.io/apimachinery/pkg/watch"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"

	"github.com/cheeseandcereal/aggexp/runtime/component/v2/admission"
	componentv2pb "github.com/cheeseandcereal/aggexp/runtime/component/v2/proto"
	componentscheme "github.com/cheeseandcereal/aggexp/runtime/component/v2/scheme"
)

// --- fakeBackend ---

type fakeBackend struct {
	mu     sync.Mutex
	items  map[string][]byte // ns/name -> JSON
	schema []byte
	events chan *componentv2pb.WatchEvent

	// validateDeny: when set, Validate returns denied.
	validateDeny bool
	// mutateObject: when non-nil, Mutate returns it.
	mutateObject []byte
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		items:  map[string][]byte{},
		events: make(chan *componentv2pb.WatchEvent, 16),
	}
}

func (f *fakeBackend) GetSchema(_ context.Context, _ *componentv2pb.GetSchemaRequest, _ ...grpc.CallOption) (*componentv2pb.GetSchemaResponse, error) {
	return &componentv2pb.GetSchemaResponse{Schema: f.schema}, nil
}
func (f *fakeBackend) Get(_ context.Context, in *componentv2pb.GetRequest, _ ...grpc.CallOption) (*componentv2pb.GetResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := in.GetNamespace() + "/" + in.GetName()
	raw, ok := f.items[key]
	if !ok {
		return nil, grpcstatus.Errorf(codes.NotFound, "not found")
	}
	return &componentv2pb.GetResponse{ObjectJson: raw}, nil
}
func (f *fakeBackend) List(_ context.Context, in *componentv2pb.ListRequest, _ ...grpc.CallOption) (*componentv2pb.ListResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]byte, 0, len(f.items))
	for _, raw := range f.items {
		out = append(out, raw)
	}
	return &componentv2pb.ListResponse{ItemsJson: out}, nil
}
func (f *fakeBackend) Create(_ context.Context, in *componentv2pb.CreateRequest, _ ...grpc.CallOption) (*componentv2pb.CreateResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	nm, ns := nameNamespaceFromJSON(in.GetObjectJson())
	if ns == "" {
		ns = in.GetNamespace()
	}
	f.items[ns+"/"+nm] = in.GetObjectJson()
	return &componentv2pb.CreateResponse{ObjectJson: in.GetObjectJson()}, nil
}
func (f *fakeBackend) Update(_ context.Context, in *componentv2pb.UpdateRequest, _ ...grpc.CallOption) (*componentv2pb.UpdateResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := in.GetNamespace() + "/" + in.GetName()
	f.items[key] = in.GetObjectJson()
	return &componentv2pb.UpdateResponse{ObjectJson: in.GetObjectJson()}, nil
}
func (f *fakeBackend) Apply(_ context.Context, in *componentv2pb.ApplyRequest, _ ...grpc.CallOption) (*componentv2pb.ApplyResponse, error) {
	return &componentv2pb.ApplyResponse{ObjectJson: in.GetObjectJson()}, nil
}
func (f *fakeBackend) Delete(_ context.Context, in *componentv2pb.DeleteRequest, _ ...grpc.CallOption) (*componentv2pb.DeleteResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := in.GetNamespace() + "/" + in.GetName()
	raw, ok := f.items[key]
	if !ok {
		return nil, grpcstatus.Errorf(codes.NotFound, "not found")
	}
	delete(f.items, key)
	return &componentv2pb.DeleteResponse{ObjectJson: raw, Deleted: true}, nil
}
func (f *fakeBackend) Watch(ctx context.Context, _ *componentv2pb.WatchRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[componentv2pb.WatchEvent], error) {
	return &fakeStream{ctx: ctx, ch: f.events}, nil
}
func (f *fakeBackend) Validate(_ context.Context, in *componentv2pb.ValidateRequest, _ ...grpc.CallOption) (*componentv2pb.ValidateResponse, error) {
	if f.validateDeny {
		return &componentv2pb.ValidateResponse{
			Allowed: false,
			Causes:  []*componentv2pb.AdmissionCause{{Field: "spec.x", Message: "bad"}},
		}, nil
	}
	return &componentv2pb.ValidateResponse{Allowed: true}, nil
}
func (f *fakeBackend) Mutate(_ context.Context, in *componentv2pb.MutateRequest, _ ...grpc.CallOption) (*componentv2pb.MutateResponse, error) {
	if f.mutateObject != nil {
		return &componentv2pb.MutateResponse{MutatedObjectJson: f.mutateObject}, nil
	}
	return &componentv2pb.MutateResponse{}, nil
}

// fakeStream is a minimal grpc.ServerStreamingClient.
type fakeStream struct {
	ctx context.Context
	ch  chan *componentv2pb.WatchEvent
}

func (s *fakeStream) Recv() (*componentv2pb.WatchEvent, error) {
	select {
	case <-s.ctx.Done():
		return nil, io.EOF
	case ev, ok := <-s.ch:
		if !ok {
			return nil, io.EOF
		}
		return ev, nil
	}
}
func (s *fakeStream) Context() context.Context     { return s.ctx }
func (s *fakeStream) Header() (metadata.MD, error) { return nil, nil }
func (s *fakeStream) Trailer() metadata.MD         { return nil }
func (s *fakeStream) CloseSend() error             { return nil }
func (s *fakeStream) SendMsg(_ any) error          { return nil }
func (s *fakeStream) RecvMsg(m any) error {
	ev, err := s.Recv()
	if err != nil {
		return err
	}
	out, ok := m.(*componentv2pb.WatchEvent)
	if !ok {
		return nil
	}
	out.Type = ev.Type
	out.ObjectJson = ev.ObjectJson
	return nil
}

var _ grpc.ServerStreamingClient[componentv2pb.WatchEvent] = (*fakeStream)(nil)

// --- test helpers ---

func newREST(b *fakeBackend) *REST {
	return New(Descriptor{
		GroupVersion:  schema.GroupVersion{Group: "notes.aggexp.io", Version: "v1"},
		Resource:      "notes",
		Kind:          "Note",
		Singular:      "note",
		Namespaced:    true,
		Writable:      true,
		UseTypedWrapper: true,
		Columns:       []metav1.TableColumnDefinition{{Name: "Name", Type: "string"}},
		RowFields:     []string{".metadata.name"},
		GroupResource: schema.GroupResource{Group: "notes.aggexp.io", Resource: "notes"},
		WatchMode:     ModePoll,
	}, b)
}

// --- tests ---

func TestREST_NewAndNewList_Typed(t *testing.T) {
	r := newREST(newFakeBackend())
	if _, ok := r.New().(*componentscheme.Object); !ok {
		t.Errorf("New not typed Object")
	}
	if _, ok := r.NewList().(*componentscheme.ObjectList); !ok {
		t.Errorf("NewList not typed ObjectList")
	}
}

func TestREST_Get_NotFound(t *testing.T) {
	r := newREST(newFakeBackend())
	_, err := r.Get(context.Background(), "nope", &metav1.GetOptions{})
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("expected NotFound, got %v", err)
	}
}

func TestREST_CreateGetList(t *testing.T) {
	b := newFakeBackend()
	r := newREST(b)
	ctx := genericapirequest.WithNamespace(context.Background(), "default")

	o := &componentscheme.Object{}
	o.SetName("hello")
	o.SetNamespace("default")
	o.SetGroupVersionKind(schema.GroupVersionKind{Group: "notes.aggexp.io", Version: "v1", Kind: "Note"})
	o.Content = map[string]interface{}{"spec": map[string]interface{}{"title": "H"}}

	created, err := r.Create(ctx, o, nil, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, ok := created.(*componentscheme.Object); !ok {
		t.Errorf("Create returned %T", created)
	}

	got, err := r.Get(ctx, "hello", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.(*componentscheme.Object).Name != "hello" {
		t.Errorf("Get wrong: %v", got)
	}

	list, err := r.List(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	l := list.(*componentscheme.ObjectList)
	if len(l.Items) != 1 {
		t.Errorf("want 1 item, got %d", len(l.Items))
	}
}

func TestREST_Watch_EmitsBookmark(t *testing.T) {
	b := newFakeBackend()
	r := newREST(b)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w, err := r.Watch(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()

	// Empty store means prefix = [BOOKMARK] only.
	ev := <-w.ResultChan()
	if ev.Type != utilwatch.Bookmark {
		t.Errorf("first event type=%v, want BOOKMARK", ev.Type)
	}
	// The BOOKMARK should carry the initial-events-end annotation.
	raw, _ := json.Marshal(ev.Object)
	if !strings.Contains(string(raw), `"k8s.io/initial-events-end":"true"`) {
		t.Errorf("BOOKMARK missing annotation: %s", raw)
	}
}

func TestREST_Watch_AddedThenBookmark(t *testing.T) {
	b := newFakeBackend()
	r := newREST(b)
	ctx := genericapirequest.WithNamespace(context.Background(), "default")
	// Seed with one object.
	o := &componentscheme.Object{}
	o.SetName("h")
	o.SetNamespace("default")
	o.SetGroupVersionKind(schema.GroupVersionKind{Group: "notes.aggexp.io", Version: "v1", Kind: "Note"})
	o.Content = map[string]interface{}{}
	_, _ = r.Create(ctx, o, nil, &metav1.CreateOptions{})

	wctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	w, err := r.Watch(wctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()
	seenTypes := []utilwatch.EventType{}
	for i := 0; i < 2; i++ {
		select {
		case ev := <-w.ResultChan():
			seenTypes = append(seenTypes, ev.Type)
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for event %d; seen=%v", i, seenTypes)
		}
	}
	if len(seenTypes) < 2 || seenTypes[len(seenTypes)-1] != utilwatch.Bookmark {
		t.Errorf("bookmark must be last in prefix; got types=%v", seenTypes)
	}
}

func TestREST_Admission_MiddlewareDeny(t *testing.T) {
	b := newFakeBackend()
	r := newREST(b)
	eng, err := admission.New(admission.Config{
		Validations: []admission.Validation{
			{Expression: `has(object.spec.title) && size(object.spec.title) >= 3`, Message: "title too short", FieldPath: "spec.title"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	r = r.WithAdmission(eng)

	o := &componentscheme.Object{}
	o.SetName("h")
	o.SetNamespace("default")
	o.SetGroupVersionKind(schema.GroupVersionKind{Group: "notes.aggexp.io", Version: "v1", Kind: "Note"})
	o.Content = map[string]interface{}{"spec": map[string]interface{}{"title": "X"}}

	_, err = r.Create(context.Background(), o, nil, &metav1.CreateOptions{})
	if err == nil {
		t.Fatalf("expected admission denial")
	}
	if !apierrors.IsInvalid(err) {
		t.Errorf("want Invalid (422), got %v", err)
	}
}

func TestREST_Admission_BackendDeny(t *testing.T) {
	b := newFakeBackend()
	b.validateDeny = true
	r := newREST(b)
	r.desc.SupportsValidation = true

	o := &componentscheme.Object{}
	o.SetName("h")
	o.SetNamespace("default")
	o.SetGroupVersionKind(schema.GroupVersionKind{Group: "notes.aggexp.io", Version: "v1", Kind: "Note"})
	o.Content = map[string]interface{}{"spec": map[string]interface{}{}}

	_, err := r.Create(context.Background(), o, nil, &metav1.CreateOptions{})
	if err == nil || !apierrors.IsInvalid(err) {
		t.Errorf("expected Invalid from backend validate, got %v", err)
	}
}

func TestREST_RVMonotonic_OnPublish(t *testing.T) {
	b := newFakeBackend()
	r := newREST(b)
	ctx := genericapirequest.WithNamespace(context.Background(), "default")
	for i := 0; i < 3; i++ {
		o := &componentscheme.Object{}
		o.SetName("n")
		o.SetNamespace("default")
		o.SetGroupVersionKind(schema.GroupVersionKind{Group: "notes.aggexp.io", Version: "v1", Kind: "Note"})
		o.Content = map[string]interface{}{}
		_, _ = r.Create(ctx, o, nil, &metav1.CreateOptions{})
	}
	if rv := r.CurrentResourceVersion(); rv == "1" {
		t.Errorf("counter did not advance: %s", rv)
	}
}

func TestLookupField(t *testing.T) {
	m := map[string]any{"a": map[string]any{"b": "c"}}
	if got := LookupField(m, ".a.b"); got != "c" {
		t.Errorf("got %v", got)
	}
	if got := LookupField(m, ".missing"); got != "" {
		t.Errorf("got %v", got)
	}
}

func TestBusinessJSON_StripsMetadata(t *testing.T) {
	raw := []byte(`{
		"apiVersion":"notes.aggexp.io/v1","kind":"Note",
		"metadata":{"name":"x","namespace":"n","uid":"u","labels":{"a":"b"},"annotations":{"c":"d"}},
		"spec":{"title":"t"},"status":{"p":"R"}
	}`)
	out, err := businessJSONFromBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(out, &m)
	md, _ := m["metadata"].(map[string]any)
	if _, has := md["labels"]; has {
		t.Errorf("labels should be stripped: %v", md)
	}
	if _, has := md["uid"]; has {
		t.Errorf("uid should be stripped: %v", md)
	}
	if md["name"] != "x" || md["namespace"] != "n" {
		t.Errorf("name/namespace lost: %v", md)
	}
}
