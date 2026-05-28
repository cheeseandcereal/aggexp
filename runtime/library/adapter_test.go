package library

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"

	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// --- test fixtures ---

// testObj is a minimal runtime.Object for testing.
type testObj struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              testSpec   `json:"spec,omitempty"`
	Status            testStatus `json:"status,omitempty"`
}

type testSpec struct {
	Color string `json:"color,omitempty"`
	Size  string `json:"size,omitempty"`
}

type testStatus struct {
	Phase string `json:"phase,omitempty"`
}

func (t *testObj) DeepCopyObject() runtime.Object {
	c := *t
	return &c
}

func (t *testObj) GetObjectKind() schema.ObjectKind {
	return &t.TypeMeta
}

type testObjList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []testObj `json:"items"`
}

func (t *testObjList) DeepCopyObject() runtime.Object {
	c := *t
	c.Items = make([]testObj, len(t.Items))
	copy(c.Items, t.Items)
	return &c
}

func (t *testObjList) GetObjectKind() schema.ObjectKind {
	return &t.TypeMeta
}

// memBackend is a simple in-memory WritableBackend for testing.
type memBackend struct {
	mu    sync.RWMutex
	store map[string]*testObj
}

func newMemBackend() *memBackend {
	return &memBackend{store: make(map[string]*testObj)}
}

func (b *memBackend) seed(objs ...*testObj) {
	for _, o := range objs {
		b.store[o.Name] = o
	}
}

func (b *memBackend) New() runtime.Object     { return &testObj{} }
func (b *memBackend) NewList() runtime.Object { return &testObjList{} }
func (b *memBackend) Kind() string            { return "TestObj" }
func (b *memBackend) SingularName() string    { return "testobj" }
func (b *memBackend) NamespaceScoped() bool   { return true }

func (b *memBackend) Get(_ context.Context, _ user.Info, name string) (runtime.Object, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	obj, ok := b.store[name]
	if !ok {
		return nil, fmt.Errorf("not found: %s", name) // simplified
	}
	c := *obj
	return &c, nil
}

func (b *memBackend) List(_ context.Context, _ user.Info, _ runtimestorage.ListOptions) (runtime.Object, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	list := &testObjList{}
	for _, o := range b.store {
		c := *o
		list.Items = append(list.Items, c)
	}
	return list, nil
}

func (b *memBackend) TableColumns() []metav1.TableColumnDefinition {
	return []metav1.TableColumnDefinition{
		{Name: "Name", Type: "string"},
		{Name: "Color", Type: "string"},
	}
}

func (b *memBackend) RowsFor(obj runtime.Object) ([]metav1.TableRow, error) {
	switch v := obj.(type) {
	case *testObj:
		return []metav1.TableRow{{Cells: []interface{}{v.Name, v.Spec.Color}}}, nil
	case *testObjList:
		rows := make([]metav1.TableRow, len(v.Items))
		for i := range v.Items {
			rows[i] = metav1.TableRow{Cells: []interface{}{v.Items[i].Name, v.Items[i].Spec.Color}}
		}
		return rows, nil
	}
	return nil, fmt.Errorf("unexpected type %T", obj)
}

func (b *memBackend) Create(_ context.Context, _ user.Info, obj runtime.Object) (runtime.Object, error) {
	o := obj.(*testObj)
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.store[o.Name]; exists {
		return nil, fmt.Errorf("already exists: %s", o.Name)
	}
	stored := *o
	b.store[o.Name] = &stored
	c := stored
	return &c, nil
}

func (b *memBackend) Update(_ context.Context, _ user.Info, name string, obj runtime.Object, forceAllowCreate bool) (runtime.Object, bool, error) {
	o := obj.(*testObj)
	b.mu.Lock()
	defer b.mu.Unlock()
	_, exists := b.store[name]
	if !exists && !forceAllowCreate {
		return nil, false, fmt.Errorf("not found: %s", name)
	}
	stored := *o
	b.store[name] = &stored
	c := stored
	return &c, !exists, nil
}

func (b *memBackend) Delete(_ context.Context, _ user.Info, name string) (runtime.Object, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	obj, ok := b.store[name]
	if !ok {
		return nil, false, fmt.Errorf("not found: %s", name)
	}
	delete(b.store, name)
	c := *obj
	return &c, true, nil
}

var _ runtimestorage.WritableBackend = (*memBackend)(nil)

var testGR = schema.GroupResource{Group: "test.io", Resource: "testobjs"}

func makeTestObj(name, color string) *testObj {
	return &testObj{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec:       testSpec{Color: color},
	}
}

// --- Tests ---

func TestDeterministicUID(t *testing.T) {
	gr := schema.GroupResource{Group: "example.io", Resource: "widgets"}
	uid1 := DeterministicUID(gr, "default", "my-widget")
	uid2 := DeterministicUID(gr, "default", "my-widget")
	uid3 := DeterministicUID(gr, "default", "other-widget")

	if uid1 != uid2 {
		t.Errorf("expected same UID, got %s vs %s", uid1, uid2)
	}
	if uid1 == uid3 {
		t.Errorf("expected different UIDs for different names")
	}

	// Verify format: 8-4-4-4-12
	if len(string(uid1)) != 36 {
		t.Errorf("expected 36 char UID, got %d: %s", len(string(uid1)), uid1)
	}
	if string(uid1)[8] != '-' || string(uid1)[13] != '-' || string(uid1)[18] != '-' || string(uid1)[23] != '-' {
		t.Errorf("UID format wrong: %s", uid1)
	}
}

func TestPagination(t *testing.T) {
	backend := newMemBackend()
	backend.seed(
		makeTestObj("alpha", "red"),
		makeTestObj("bravo", "blue"),
		makeTestObj("charlie", "green"),
		makeTestObj("delta", "yellow"),
		makeTestObj("echo", "purple"),
	)

	store := New(Options{
		Backend:       backend,
		GroupResource: testGR,
		Pagination:    true,
	})
	defer store.Shutdown()

	ctx := context.Background()

	// Page 1: limit=2
	opts := &metainternalversion.ListOptions{Limit: 2}
	result, err := store.List(ctx, opts)
	if err != nil {
		t.Fatalf("List page 1: %v", err)
	}
	list := result.(*testObjList)
	if len(list.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(list.Items))
	}
	if list.Items[0].Name != "alpha" || list.Items[1].Name != "bravo" {
		t.Errorf("unexpected items: %v, %v", list.Items[0].Name, list.Items[1].Name)
	}
	if list.Continue == "" {
		t.Fatal("expected continue token")
	}
	if list.GetRemainingItemCount() == nil || *list.GetRemainingItemCount() != 3 {
		t.Errorf("expected 3 remaining, got %v", list.GetRemainingItemCount())
	}

	// Page 2
	opts2 := &metainternalversion.ListOptions{Limit: 2, Continue: list.Continue}
	result2, err := store.List(ctx, opts2)
	if err != nil {
		t.Fatalf("List page 2: %v", err)
	}
	list2 := result2.(*testObjList)
	if len(list2.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(list2.Items))
	}
	if list2.Items[0].Name != "charlie" || list2.Items[1].Name != "delta" {
		t.Errorf("unexpected items: %v, %v", list2.Items[0].Name, list2.Items[1].Name)
	}

	// Page 3 (last)
	opts3 := &metainternalversion.ListOptions{Limit: 2, Continue: list2.Continue}
	result3, err := store.List(ctx, opts3)
	if err != nil {
		t.Fatalf("List page 3: %v", err)
	}
	list3 := result3.(*testObjList)
	if len(list3.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(list3.Items))
	}
	if list3.Continue != "" {
		t.Errorf("expected empty continue on last page, got %q", list3.Continue)
	}
}

func TestFieldSelector(t *testing.T) {
	backend := newMemBackend()
	backend.seed(
		makeTestObj("alpha", "red"),
		makeTestObj("bravo", "blue"),
		makeTestObj("charlie", "red"),
	)

	store := New(Options{
		Backend:       backend,
		GroupResource: testGR,
		FieldSelectors: &FieldSelectorOptions{
			SelectableFields: []string{"spec.color"},
			Accessor: func(obj runtime.Object, field string) (string, bool) {
				if o, ok := obj.(*testObj); ok && field == "spec.color" {
					return o.Spec.Color, true
				}
				return "", false
			},
		},
	})
	defer store.Shutdown()

	ctx := context.Background()

	// Filter by spec.color=red
	sel, _ := fields.ParseSelector("spec.color=red")
	opts := &metainternalversion.ListOptions{FieldSelector: sel}
	result, err := store.List(ctx, opts)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	list := result.(*testObjList)
	if len(list.Items) != 2 {
		t.Fatalf("expected 2 items with color=red, got %d", len(list.Items))
	}

	// Filter by metadata.name=bravo
	sel2, _ := fields.ParseSelector("metadata.name=bravo")
	opts2 := &metainternalversion.ListOptions{FieldSelector: sel2}
	result2, err := store.List(ctx, opts2)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	list2 := result2.(*testObjList)
	if len(list2.Items) != 1 || list2.Items[0].Name != "bravo" {
		t.Fatalf("expected bravo, got %v", list2.Items)
	}

	// Unknown field should fail
	sel3, _ := fields.ParseSelector("spec.unknown=x")
	opts3 := &metainternalversion.ListOptions{FieldSelector: sel3}
	_, err = store.List(ctx, opts3)
	if err == nil {
		t.Fatal("expected error for unknown field")
	}
}

func TestOptimisticConcurrency(t *testing.T) {
	backend := newMemBackend()
	backend.seed(makeTestObj("widget-1", "red"))

	store := New(Options{
		Backend:               backend,
		GroupResource:         testGR,
		OptimisticConcurrency: true,
	})
	defer store.Shutdown()

	ctx := context.Background()

	// Create to get initial RV tracked.
	createObj := makeTestObj("widget-2", "blue")
	createObj.Name = "widget-2"
	created, err := store.Create(ctx, createObj, nil, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	createdObj := created.(*testObj)
	rv := createdObj.ResourceVersion
	if rv == "" {
		t.Fatal("expected RV on created object")
	}

	// Get should return the tracked RV.
	got, err := store.Get(ctx, "widget-2", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	gotObj := got.(*testObj)
	if gotObj.ResourceVersion != rv {
		t.Errorf("expected RV %s, got %s", rv, gotObj.ResourceVersion)
	}

	// Update with correct RV should succeed.
	gotObj.Spec.Color = "green"
	_, _, err = store.Update(ctx, "widget-2",
		&simpleUpdateInfo{obj: gotObj},
		nil, nil, false, &metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Update with correct RV: %v", err)
	}

	// Update with stale RV should fail with 409.
	staleObj := &testObj{ObjectMeta: metav1.ObjectMeta{Name: "widget-2", Namespace: "default", ResourceVersion: rv}, Spec: testSpec{Color: "stale"}}
	_, _, err = store.Update(ctx, "widget-2",
		&simpleUpdateInfo{obj: staleObj},
		nil, nil, false, &metav1.UpdateOptions{})
	if err == nil {
		t.Fatal("expected conflict error")
	}

	// Update with empty RV should fail.
	noRVObj := &testObj{ObjectMeta: metav1.ObjectMeta{Name: "widget-2", Namespace: "default"}, Spec: testSpec{Color: "no-rv"}}
	_, _, err = store.Update(ctx, "widget-2",
		&simpleUpdateInfo{obj: noRVObj},
		nil, nil, false, &metav1.UpdateOptions{})
	if err == nil {
		t.Fatal("expected conflict error for empty RV")
	}
}

func TestBookmark(t *testing.T) {
	backend := newMemBackend()
	backend.seed(makeTestObj("alpha", "red"))

	store := New(Options{
		Backend:       backend,
		GroupResource: testGR,
		Bookmark:      true,
	})
	defer store.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Watch with allowWatchBookmarks=true
	opts := &metainternalversion.ListOptions{
		ResourceVersion:   "0",
		AllowWatchBookmarks: true,
	}
	w, err := store.Watch(ctx, opts)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()

	// Should get ADDED event + BOOKMARK.
	ev1 := <-w.ResultChan()
	if ev1.Type != watch.Added {
		t.Fatalf("expected ADDED, got %s", ev1.Type)
	}

	ev2 := <-w.ResultChan()
	if ev2.Type != watch.Bookmark {
		t.Fatalf("expected BOOKMARK, got %s", ev2.Type)
	}
	// Check annotation.
	bmObj := ev2.Object.(metav1.ObjectMetaAccessor)
	anns := bmObj.GetObjectMeta().GetAnnotations()
	if anns["k8s.io/initial-events-end"] != "true" {
		t.Errorf("expected initial-events-end annotation, got %v", anns)
	}
}

func TestBookmarkNotEmittedWhenDisallowed(t *testing.T) {
	backend := newMemBackend()
	backend.seed(makeTestObj("alpha", "red"))

	store := New(Options{
		Backend:       backend,
		GroupResource: testGR,
		Bookmark:      true,
	})
	defer store.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Watch with allowWatchBookmarks=false
	opts := &metainternalversion.ListOptions{
		ResourceVersion:     "0",
		AllowWatchBookmarks: false,
	}
	w, err := store.Watch(ctx, opts)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()

	ev1 := <-w.ResultChan()
	if ev1.Type != watch.Added {
		t.Fatalf("expected ADDED, got %s", ev1.Type)
	}

	// Next event should be timeout (no bookmark).
	select {
	case ev := <-w.ResultChan():
		if ev.Type == watch.Bookmark {
			t.Fatal("did not expect BOOKMARK when allowWatchBookmarks=false")
		}
	case <-time.After(200 * time.Millisecond):
		// Expected — no bookmark emitted.
	}
}

func TestPollWatcher(t *testing.T) {
	var mu sync.Mutex
	items := []runtime.Object{
		makeTestObj("alpha", "red"),
		makeTestObj("bravo", "blue"),
	}

	lister := PollListerFunc(func(_ context.Context) ([]runtime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		out := make([]runtime.Object, len(items))
		copy(out, items)
		return out, nil
	})

	var events []watch.EventType
	var eventMu sync.Mutex
	pub := &trackingPublisher{
		onEvent: func(et watch.EventType, _ runtime.Object) {
			eventMu.Lock()
			events = append(events, et)
			eventMu.Unlock()
		},
	}

	pw := NewPollWatcher(lister, pub, 50*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	go pw.Run(ctx)

	// Wait for first poll.
	time.Sleep(80 * time.Millisecond)

	eventMu.Lock()
	if len(events) != 2 {
		t.Fatalf("expected 2 ADDED events, got %d", len(events))
	}
	eventMu.Unlock()

	// Add a new item.
	mu.Lock()
	items = append(items, makeTestObj("charlie", "green"))
	mu.Unlock()

	time.Sleep(80 * time.Millisecond)

	eventMu.Lock()
	if len(events) != 3 || events[2] != watch.Added {
		t.Fatalf("expected 3 events with last ADDED, got %v", events)
	}
	eventMu.Unlock()

	// Remove an item.
	mu.Lock()
	items = items[1:] // remove alpha
	mu.Unlock()

	time.Sleep(80 * time.Millisecond)

	eventMu.Lock()
	if len(events) < 4 {
		t.Fatalf("expected at least 4 events, got %d", len(events))
	}
	// Last event should be Deleted.
	lastEvt := events[len(events)-1]
	if lastEvt != watch.Deleted {
		t.Errorf("expected last event DELETED, got %s", lastEvt)
	}
	eventMu.Unlock()

	cancel()
}

func TestDeterministicUIDOnCreate(t *testing.T) {
	backend := newMemBackend()
	store := New(Options{
		Backend:           backend,
		GroupResource:     testGR,
		DeterministicUIDs: true,
	})
	defer store.Shutdown()

	ctx := context.Background()
	obj := makeTestObj("widget-x", "red")
	result, err := store.Create(ctx, obj, nil, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	created := result.(*testObj)
	expectedUID := DeterministicUID(testGR, "default", "widget-x")
	if created.UID != expectedUID {
		t.Errorf("expected UID %s, got %s", expectedUID, created.UID)
	}
}

func TestLabelSelectorOnList(t *testing.T) {
	backend := newMemBackend()
	obj1 := makeTestObj("alpha", "red")
	obj1.Labels = map[string]string{"env": "prod"}
	obj2 := makeTestObj("bravo", "blue")
	obj2.Labels = map[string]string{"env": "dev"}
	backend.seed(obj1, obj2)

	store := New(Options{
		Backend:       backend,
		GroupResource: testGR,
	})
	defer store.Shutdown()

	ctx := context.Background()
	sel, _ := labels.Parse("env=prod")
	opts := &metainternalversion.ListOptions{LabelSelector: sel}
	result, err := store.List(ctx, opts)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	list := result.(*testObjList)
	if len(list.Items) != 1 || list.Items[0].Name != "alpha" {
		t.Errorf("expected [alpha], got %v", list.Items)
	}
}

func TestCRUD(t *testing.T) {
	backend := newMemBackend()
	store := New(Options{
		Backend:       backend,
		GroupResource: testGR,
	})
	defer store.Shutdown()

	ctx := context.Background()

	// Create.
	obj := makeTestObj("test-1", "red")
	created, err := store.Create(ctx, obj, nil, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.(*testObj).Name != "test-1" {
		t.Errorf("unexpected name: %s", created.(*testObj).Name)
	}

	// Get.
	got, err := store.Get(ctx, "test-1", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.(*testObj).Spec.Color != "red" {
		t.Errorf("unexpected color: %s", got.(*testObj).Spec.Color)
	}

	// Delete.
	_, deleted, err := store.Delete(ctx, "test-1", nil, &metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !deleted {
		t.Error("expected deleted=true")
	}
}

func TestPaginationWithFieldSelector(t *testing.T) {
	backend := newMemBackend()
	backend.seed(
		makeTestObj("alpha", "red"),
		makeTestObj("bravo", "red"),
		makeTestObj("charlie", "blue"),
		makeTestObj("delta", "red"),
	)

	store := New(Options{
		Backend:       backend,
		GroupResource: testGR,
		Pagination:    true,
		FieldSelectors: &FieldSelectorOptions{
			SelectableFields: []string{"spec.color"},
			Accessor: func(obj runtime.Object, field string) (string, bool) {
				if o, ok := obj.(*testObj); ok && field == "spec.color" {
					return o.Spec.Color, true
				}
				return "", false
			},
		},
	})
	defer store.Shutdown()

	ctx := context.Background()

	// Filter by color=red, then paginate with limit=2.
	sel, _ := fields.ParseSelector("spec.color=red")
	opts := &metainternalversion.ListOptions{
		FieldSelector: sel,
		Limit:         2,
	}
	result, err := store.List(ctx, opts)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	list := result.(*testObjList)
	if len(list.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(list.Items))
	}
	// Continue token should be set (3 red items total, showing 2).
	if list.Continue == "" {
		t.Fatal("expected continue token")
	}
}

// --- helper types ---

type simpleUpdateInfo struct {
	obj runtime.Object
}

func (s *simpleUpdateInfo) Preconditions() *metav1.Preconditions { return nil }
func (s *simpleUpdateInfo) UpdatedObject(_ context.Context, _ runtime.Object) (runtime.Object, error) {
	return s.obj, nil
}

type trackingPublisher struct {
	onEvent func(et watch.EventType, obj runtime.Object)
}

func (p *trackingPublisher) PublishAdded(obj runtime.Object)    { p.onEvent(watch.Added, obj) }
func (p *trackingPublisher) PublishModified(obj runtime.Object) { p.onEvent(watch.Modified, obj) }
func (p *trackingPublisher) PublishDeleted(obj runtime.Object)  { p.onEvent(watch.Deleted, obj) }
