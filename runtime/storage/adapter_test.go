package storage_test

import (
	"context"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"

	"github.com/cheeseandcereal/aggexp/runtime/storage"
)

// ----- a minimal typed resource used only in tests. -----

type Thing struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	Color string
}

func (t *Thing) DeepCopyObject() runtime.Object {
	if t == nil {
		return nil
	}
	out := &Thing{TypeMeta: t.TypeMeta, Color: t.Color}
	t.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	return out
}

type ThingList struct {
	metav1.TypeMeta
	metav1.ListMeta
	Items []Thing
}

func (l *ThingList) DeepCopyObject() runtime.Object {
	if l == nil {
		return nil
	}
	out := &ThingList{TypeMeta: l.TypeMeta}
	l.ListMeta.DeepCopyInto(&out.ListMeta)
	if l.Items != nil {
		out.Items = make([]Thing, len(l.Items))
		copy(out.Items, l.Items)
	}
	return out
}

// ----- the test backend. -----

type thingBackend struct {
	mu    sync.RWMutex
	items map[string]*Thing
}

func newThingBackend() *thingBackend {
	return &thingBackend{items: map[string]*Thing{}}
}

func (b *thingBackend) New() runtime.Object     { return &Thing{} }
func (b *thingBackend) NewList() runtime.Object { return &ThingList{} }
func (b *thingBackend) Kind() string            { return "Thing" }
func (b *thingBackend) SingularName() string    { return "thing" }
func (b *thingBackend) NamespaceScoped() bool   { return false }

func (b *thingBackend) Get(ctx context.Context, u user.Info, name string) (runtime.Object, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, ok := b.items[name]
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "things"}, name)
	}
	return t.DeepCopyObject(), nil
}

func (b *thingBackend) List(ctx context.Context, u user.Info, opts storage.ListOptions) (runtime.Object, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := &ThingList{}
	for _, t := range b.items {
		out.Items = append(out.Items, *t)
	}
	return out, nil
}

func (b *thingBackend) TableColumns() []metav1.TableColumnDefinition {
	return []metav1.TableColumnDefinition{
		{Name: "Name", Type: "string"},
		{Name: "Color", Type: "string"},
	}
}

func (b *thingBackend) RowsFor(obj runtime.Object) ([]metav1.TableRow, error) {
	row := func(t *Thing) metav1.TableRow {
		return metav1.TableRow{Cells: []interface{}{t.Name, t.Color}}
	}
	switch v := obj.(type) {
	case *Thing:
		return []metav1.TableRow{row(v)}, nil
	case *ThingList:
		rs := make([]metav1.TableRow, 0, len(v.Items))
		for i := range v.Items {
			rs = append(rs, row(&v.Items[i]))
		}
		return rs, nil
	}
	return nil, nil
}

// put is a test helper for preloading the backend.
func (b *thingBackend) put(name, color string, lbls map[string]string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.items[name] = &Thing{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: lbls},
		Color:      color,
	}
}

// A writable variant used for the write-path tests.
type writableThingBackend struct {
	*thingBackend
}

func (w *writableThingBackend) Create(ctx context.Context, u user.Info, obj runtime.Object) (runtime.Object, error) {
	t := obj.(*Thing)
	if t.Name == "" {
		return nil, apierrors.NewBadRequest("name required")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, exists := w.items[t.Name]; exists {
		return nil, apierrors.NewAlreadyExists(schema.GroupResource{Resource: "things"}, t.Name)
	}
	stored := t.DeepCopyObject().(*Thing)
	stored.UID = "fake-uid"
	stored.CreationTimestamp = metav1.NewTime(time.Unix(0, 0))
	w.items[t.Name] = stored
	return stored.DeepCopyObject(), nil
}

func (w *writableThingBackend) Update(ctx context.Context, u user.Info, name string, obj runtime.Object, forceAllowCreate bool) (runtime.Object, bool, error) {
	t := obj.(*Thing)
	t.Name = name
	w.mu.Lock()
	defer w.mu.Unlock()
	_, exists := w.items[name]
	if !exists && !forceAllowCreate {
		return nil, false, apierrors.NewNotFound(schema.GroupResource{Resource: "things"}, name)
	}
	stored := t.DeepCopyObject().(*Thing)
	w.items[name] = stored
	return stored.DeepCopyObject(), !exists, nil
}

func (w *writableThingBackend) Delete(ctx context.Context, u user.Info, name string) (runtime.Object, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	existing, ok := w.items[name]
	if !ok {
		return nil, false, apierrors.NewNotFound(schema.GroupResource{Resource: "things"}, name)
	}
	delete(w.items, name)
	return existing.DeepCopyObject(), true, nil
}

// ----- actual tests. -----

func TestGetNotFound(t *testing.T) {
	r := storage.New(storage.Options{Backend: newThingBackend()})
	t.Cleanup(r.Shutdown)
	_, err := r.Get(context.Background(), "missing", &metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestListAndLabelFilter(t *testing.T) {
	b := newThingBackend()
	b.put("red-1", "red", map[string]string{"tag": "warm"})
	b.put("blue-1", "blue", map[string]string{"tag": "cool"})
	b.put("blue-2", "blue", map[string]string{"tag": "cool"})
	r := storage.New(storage.Options{Backend: b})
	t.Cleanup(r.Shutdown)

	// No selector: all three.
	out, err := r.List(context.Background(), &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	lst := out.(*ThingList)
	if len(lst.Items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(lst.Items))
	}
	if lst.ListMeta.ResourceVersion == "" {
		t.Fatal("expected ResourceVersion set on list")
	}

	// With selector: only cool.
	sel, err := labels.Parse("tag=cool")
	if err != nil {
		t.Fatal(err)
	}
	out, err = r.List(context.Background(), &metainternalversion.ListOptions{LabelSelector: sel})
	if err != nil {
		t.Fatal(err)
	}
	lst = out.(*ThingList)
	if len(lst.Items) != 2 {
		t.Fatalf("expected 2 items after filter, got %d", len(lst.Items))
	}
}

func TestResourceVersionMonotonic(t *testing.T) {
	r := storage.New(storage.Options{Backend: newThingBackend()})
	t.Cleanup(r.Shutdown)
	first := r.CurrentResourceVersion()
	r.PublishAdded(&Thing{ObjectMeta: metav1.ObjectMeta{Name: "a"}})
	r.PublishModified(&Thing{ObjectMeta: metav1.ObjectMeta{Name: "a"}})
	r.PublishDeleted(&Thing{ObjectMeta: metav1.ObjectMeta{Name: "a"}})
	second := r.CurrentResourceVersion()
	if first == second {
		t.Fatalf("expected RV to bump; stayed at %s", first)
	}
	// Three bumps, one per Publish call.
	if got, want := second, "4"; got != want {
		t.Fatalf("expected RV=4 after 3 publishes, got %s", got)
	}
}

func TestWatchReceivesInitialAndLiveEvents(t *testing.T) {
	b := newThingBackend()
	b.put("pre", "green", nil)
	r := storage.New(storage.Options{Backend: b})
	t.Cleanup(r.Shutdown)

	// Simulate aggregation-layer user context.
	ctx := genericapirequest.WithUser(context.Background(), &user.DefaultInfo{Name: "tester"})

	w, err := r.Watch(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	// Expect one initial ADDED for "pre".
	select {
	case ev := <-w.ResultChan():
		th := ev.Object.(*Thing)
		if th.Name != "pre" {
			t.Fatalf("expected pre, got %s", th.Name)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial ADDED")
	}

	// Publish a live one.
	r.PublishAdded(&Thing{ObjectMeta: metav1.ObjectMeta{Name: "live"}, Color: "purple"})

	select {
	case ev := <-w.ResultChan():
		th := ev.Object.(*Thing)
		if th.Name != "live" {
			t.Fatalf("expected live, got %s", th.Name)
		}
		if th.ResourceVersion == "" {
			t.Fatal("expected RV stamped on published object")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for live event")
	}
}

func TestWatchResourceExpiredOnStaleRV(t *testing.T) {
	r := storage.New(storage.Options{Backend: newThingBackend()})
	t.Cleanup(r.Shutdown)
	r.NextResourceVersion() // bump to 2 so "1" is stale
	_, err := r.Watch(context.Background(), &metainternalversion.ListOptions{ResourceVersion: "1"})
	if !apierrors.IsResourceExpired(err) {
		t.Fatalf("expected ResourceExpired, got %v", err)
	}
}

func TestWatchFiltersByLabel(t *testing.T) {
	b := newThingBackend()
	b.put("warm-1", "red", map[string]string{"tag": "warm"})
	b.put("cool-1", "blue", map[string]string{"tag": "cool"})
	r := storage.New(storage.Options{Backend: b})
	t.Cleanup(r.Shutdown)

	sel, err := labels.Parse("tag=warm")
	if err != nil {
		t.Fatal(err)
	}
	w, err := r.Watch(context.Background(), &metainternalversion.ListOptions{LabelSelector: sel})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	got := []string{}
	timeout := time.After(500 * time.Millisecond)
outer:
	for {
		select {
		case ev, ok := <-w.ResultChan():
			if !ok {
				break outer
			}
			th := ev.Object.(*Thing)
			got = append(got, th.Name)
		case <-timeout:
			break outer
		}
	}
	if len(got) != 1 || got[0] != "warm-1" {
		t.Fatalf("expected only warm-1 in initial events, got %v", got)
	}
}

func TestCreateOnReadOnlyBackendRejected(t *testing.T) {
	r := storage.New(storage.Options{
		Backend:       newThingBackend(),
		GroupResource: schema.GroupResource{Resource: "things"},
	})
	t.Cleanup(r.Shutdown)
	_, err := r.Create(context.Background(), &Thing{ObjectMeta: metav1.ObjectMeta{Name: "x"}}, nil, &metav1.CreateOptions{})
	if err == nil {
		t.Fatal("expected error creating on read-only backend")
	}
	if !apierrors.IsMethodNotSupported(err) {
		t.Fatalf("expected MethodNotSupported, got %v", err)
	}
}

func TestWritablePathFanOut(t *testing.T) {
	wb := &writableThingBackend{thingBackend: newThingBackend()}
	r := storage.New(storage.Options{
		Backend:       wb,
		GroupResource: schema.GroupResource{Resource: "things"},
	})
	t.Cleanup(r.Shutdown)

	w, err := r.Watch(context.Background(), &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	_, err = r.Create(context.Background(), &Thing{ObjectMeta: metav1.ObjectMeta{Name: "x"}, Color: "blue"}, nil, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	select {
	case ev := <-w.ResultChan():
		if string(ev.Type) != "ADDED" {
			t.Fatalf("expected ADDED, got %s", ev.Type)
		}
		th := ev.Object.(*Thing)
		if th.Name != "x" {
			t.Fatalf("expected x, got %s", th.Name)
		}
		if th.ResourceVersion == "" {
			t.Fatal("expected RV on created watch event")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ADDED event from Create")
	}
}

func TestTableRows(t *testing.T) {
	b := newThingBackend()
	b.put("x", "red", nil)
	r := storage.New(storage.Options{Backend: b})
	t.Cleanup(r.Shutdown)

	obj, err := r.Get(context.Background(), "x", &metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tab, err := r.ConvertToTable(context.Background(), obj, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(tab.Rows) != 1 || len(tab.ColumnDefinitions) != 2 {
		t.Fatalf("unexpected table shape: rows=%d cols=%d", len(tab.Rows), len(tab.ColumnDefinitions))
	}
	if got := tab.Rows[0].Cells[0].(string); got != "x" {
		t.Fatalf("expected name=x in first cell, got %v", got)
	}
}
