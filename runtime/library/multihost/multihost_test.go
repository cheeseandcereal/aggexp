package multihost

import (
	"context"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	apiwatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
)

// ---- test fixtures: a minimal served Widget + Converter ----

type widget struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              widgetSpec `json:"spec,omitempty"`
}

type widgetSpec struct {
	Color string `json:"color,omitempty"`
	Size  int64  `json:"size,omitempty"`
}

func (w *widget) DeepCopyObject() runtime.Object {
	c := *w
	return &c
}
func (w *widget) GetObjectKind() schema.ObjectKind { return &w.TypeMeta }

type widgetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []widget `json:"items"`
}

func (l *widgetList) DeepCopyObject() runtime.Object {
	c := *l
	c.Items = make([]widget, len(l.Items))
	copy(c.Items, l.Items)
	return &c
}
func (l *widgetList) GetObjectKind() schema.ObjectKind { return &l.TypeMeta }

type widgetConverter struct{}

func (widgetConverter) New() runtime.Object     { return &widget{} }
func (widgetConverter) NewList() runtime.Object { return &widgetList{} }

func (widgetConverter) BodyFromObject(obj runtime.Object) Body {
	w := obj.(*widget)
	return Body{Fields: map[string]interface{}{
		"color": w.Spec.Color,
		"size":  w.Spec.Size,
	}}
}

func (widgetConverter) Stitch(ref ResourceRef, body Body, rec *Record) runtime.Object {
	w := &widget{}
	w.TypeMeta.Kind = "Widget"
	w.TypeMeta.APIVersion = "example.io/v1"
	w.Name = ref.Name
	w.Namespace = ref.Namespace
	if v, ok := body.Fields["color"].(string); ok {
		w.Spec.Color = v
	}
	switch v := body.Fields["size"].(type) {
	case int64:
		w.Spec.Size = v
	case float64:
		w.Spec.Size = int64(v)
	}
	return w
}

func (widgetConverter) RecordFromObject(obj runtime.Object, ref ResourceRef) *Record {
	w := obj.(*widget)
	return &Record{
		Ref:               ref,
		UID:               string(w.UID),
		CreationTimestamp: w.CreationTimestamp,
		Labels:            w.Labels,
		Annotations:       w.Annotations,
		Finalizers:        append([]string(nil), w.Finalizers...),
	}
}

var (
	testGR   = schema.GroupResource{Group: "example.io", Resource: "widgets"}
	metaGVR  = schema.GroupVersionResource{Group: "widgetmeta.example.io", Version: "v1", Resource: "resourcemetadatas"}
	bodyGVR  = schema.GroupVersionResource{Group: "widgetbody.example.io", Version: "v1", Resource: "widgetbodies"}
	metaKind = "ResourceMetadata"
	bodyKind = "WidgetBody"
)

// newFakeDynamic builds a fake dynamic client with the metadata + body
// list kinds registered so informers and List work.
func newFakeDynamic() *dynamicfake.FakeDynamicClient {
	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(metaGVR.GroupVersion().WithKind(metaKind), &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(metaGVR.GroupVersion().WithKind(metaKind+"List"), &unstructured.UnstructuredList{})
	scheme.AddKnownTypeWithName(bodyGVR.GroupVersion().WithKind(bodyKind), &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(bodyGVR.GroupVersion().WithKind(bodyKind+"List"), &unstructured.UnstructuredList{})
	gvrToListKind := map[schema.GroupVersionResource]string{
		metaGVR: metaKind + "List",
		bodyGVR: bodyKind + "List",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToListKind)
	installEtcdSim(dyn)
	return dyn
}

// installEtcdSim makes the fake dynamic client behave like etcd for the
// two CRDs the multi-host stores use: Create/Update stamp a fresh,
// monotonically-increasing resourceVersion and a UID, and Update
// enforces compare-and-swap (a write carrying a stale RV is rejected
// with 409 Conflict). The real arc ran against a kube-apiserver where
// etcd provides exactly this; the fake client does not, so we simulate
// it. This is what makes the host-RV authority and the lock CAS
// surfaces testable without a live cluster.
func installEtcdSim(dyn *dynamicfake.FakeDynamicClient) {
	var mu sync.Mutex
	var counter int64
	// current[gvr/name] -> stored RV (the CAS authority).
	current := map[string]string{}
	key := func(res, name string) string { return res + "/" + name }

	tracker := dyn.Tracker()

	stamp := func(res string, u *unstructured.Unstructured, isCreate bool) error {
		mu.Lock()
		defer mu.Unlock()
		k := key(res, u.GetName())
		if !isCreate {
			stored, ok := current[k]
			if ok && stored != "" {
				incoming := u.GetResourceVersion()
				if incoming != "" && incoming != stored {
					return apierrors.NewConflict(schema.GroupResource{Resource: res}, u.GetName(),
						errModified())
				}
			}
		}
		counter++
		rv := itoaTest(counter)
		u.SetResourceVersion(rv)
		if u.GetUID() == "" {
			u.SetUID(types.UID("uid-" + rv))
		}
		current[k] = rv
		return nil
	}

	for _, gvr := range []schema.GroupVersionResource{metaGVR, bodyGVR} {
		res := gvr.Resource
		dyn.PrependReactor("create", res, func(action clienttesting.Action) (bool, runtime.Object, error) {
			ca := action.(clienttesting.CreateAction)
			u := ca.GetObject().(*unstructured.Unstructured).DeepCopy()
			if err := stamp(res, u, true); err != nil {
				return true, nil, err
			}
			if err := tracker.Create(gvr, u, u.GetNamespace()); err != nil {
				return true, nil, err
			}
			return true, u, nil
		})
		dyn.PrependReactor("update", res, func(action clienttesting.Action) (bool, runtime.Object, error) {
			ua := action.(clienttesting.UpdateAction)
			u := ua.GetObject().(*unstructured.Unstructured).DeepCopy()
			if err := stamp(res, u, false); err != nil {
				return true, nil, err
			}
			if err := tracker.Update(gvr, u, u.GetNamespace()); err != nil {
				return true, nil, err
			}
			return true, u, nil
		})
		dyn.PrependReactor("delete", res, func(action clienttesting.Action) (bool, runtime.Object, error) {
			da := action.(clienttesting.DeleteAction)
			mu.Lock()
			delete(current, key(res, da.GetName()))
			mu.Unlock()
			if err := tracker.Delete(gvr, da.GetNamespace(), da.GetName()); err != nil {
				return true, nil, err
			}
			return true, nil, nil
		})
	}
}

func itoaTest(i int64) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

// newTestREST builds and starts a multi-host REST with the given option
// overrides applied. It returns the REST and the fake client.
func newTestREST(t *testing.T, mutate func(*Options)) (*REST, *dynamicfake.FakeDynamicClient) {
	t.Helper()
	dyn := newFakeDynamic()
	opts := Options{
		Dynamic:            dyn,
		Converter:          widgetConverter{},
		GroupResource:      testGR,
		MetaGVR:            metaGVR,
		MetaKind:           metaKind,
		BodyGVR:            bodyGVR,
		BodyKind:           bodyKind,
		ReplicaID:          "replica-test",
		Lock:               true,
		TransactionalWrite: true,
		Watch:              true,
		DisableLockRenew:   true, // keep tests deterministic
	}
	if mutate != nil {
		mutate(&opts)
	}
	r := New(opts)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return r, dyn
}

func ctxFor(name string, groups ...string) context.Context {
	ctx := genericapirequest.WithNamespace(context.Background(), "default")
	u := &user.DefaultInfo{Name: name, Groups: groups}
	return genericapirequest.WithUser(ctx, u)
}

func systemCtx() context.Context { return ctxFor("alice", "system:masters") }

func makeWidget(name, color string, size int64) *widget {
	w := &widget{}
	w.Name = name
	w.Namespace = "default"
	w.Spec.Color = color
	w.Spec.Size = size
	return w
}

// waitInformer gives the fake informer a moment to observe a write.
func waitInformer() { time.Sleep(60 * time.Millisecond) }

// ---- Capability: host-RV stamping (Get/List/Watch carry metadata-CR RV) ----

func TestHostRVStamping(t *testing.T) {
	r, dyn := newTestREST(t, nil)
	ctx := systemCtx()

	created, err := r.Create(ctx, makeWidget("w1", "red", 1), nil, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cw := created.(*widget)
	if cw.ResourceVersion == "" {
		t.Fatal("created object has no RV")
	}

	// The served RV must equal the metadata CR's host RV.
	metaName := RecordName(ResourceRef{Group: "example.io", Resource: "widgets", Namespace: "default", Name: "w1"})
	u, gerr := dyn.Resource(metaGVR).Get(ctx, metaName, metav1.GetOptions{})
	if gerr != nil {
		t.Fatalf("get metadata CR: %v", gerr)
	}
	if cw.ResourceVersion != u.GetResourceVersion() {
		t.Errorf("Create RV %s != metadata-CR RV %s", cw.ResourceVersion, u.GetResourceVersion())
	}

	waitInformer()

	// Get carries the same RV.
	got, err := r.Get(ctx, "w1", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.(*widget).ResourceVersion != u.GetResourceVersion() {
		t.Errorf("Get RV %s != metadata-CR RV %s", got.(*widget).ResourceVersion, u.GetResourceVersion())
	}

	// List carries the metadata-CR high-water RV on ListMeta.
	listed, err := r.List(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	wl := listed.(*widgetList)
	if wl.ResourceVersion == "" {
		t.Error("list has no RV")
	}
	if len(wl.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(wl.Items))
	}
	if wl.Items[0].ResourceVersion != u.GetResourceVersion() {
		t.Errorf("list item RV %s != metadata-CR RV %s", wl.Items[0].ResourceVersion, u.GetResourceVersion())
	}

	// Watch carries the metadata-CR RV on the replayed ADDED.
	wctx, cancel := context.WithCancel(systemCtx())
	defer cancel()
	w, err := r.Watch(wctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()
	ev := <-w.ResultChan()
	if ev.Type != apiwatch.Added {
		t.Fatalf("expected ADDED, got %s", ev.Type)
	}
	if ev.Object.(*widget).ResourceVersion != u.GetResourceVersion() {
		t.Errorf("watch ADDED RV %s != metadata-CR RV %s", ev.Object.(*widget).ResourceVersion, u.GetResourceVersion())
	}
}

// ---- Capability: emission filtering (lock-only suppressed, visible emitted) ----

func TestEmissionFiltering(t *testing.T) {
	r, _ := newTestREST(t, func(o *Options) {
		o.WatchModeSelect = WatchPoll
		o.PollInterval = time.Hour // never fires during the test
	})
	hub := r.Hub()

	ref := ResourceRef{Group: "example.io", Resource: "widgets", Namespace: "default", Name: "w1"}

	// Seed a body so StitchFor can find one (before the watcher, so the
	// initial replay does not race).
	bs := r.bodies
	body1 := Body{Owner: "alice", Fields: map[string]interface{}{"color": "red", "size": int64(1)}}
	if err := bs.Put(context.Background(), "default", "w1", body1); err != nil {
		t.Fatalf("seed body: %v", err)
	}
	waitInformer()

	// Register a watcher (system identity sees all). Drain its replay
	// (bookmark) and any immediate poll-loop ADDED for the seeded body.
	wctx, cancel := context.WithCancel(systemCtx())
	defer cancel()
	w := hub.NewWatch(wctx, &user.DefaultInfo{Name: "alice", Groups: []string{"system:masters"}}, "", nil, WatchPoll, nil)
	defer w.Stop()
	drainUntilBookmark(t, w)
	drainAll(w, 200*time.Millisecond)

	base := &Record{Ref: ref, RecordRV: "10", UID: "u1", BodyHash: HashBody(body1)}

	// First event (ADDED) with a visible signature: should emit. Seed
	// the per-watcher dedup with a low RV so this ADDED (rv 10) passes.
	hub.OnMetadataEvent(apiwatch.Added, ref, base, "10")
	if ev := recvTimeout(w, 500*time.Millisecond); ev == nil || ev.Type != apiwatch.Added {
		t.Fatalf("expected ADDED emission, got %v", ev)
	}

	// Lock-only transition: same visible signature, higher RV (lock
	// acquire churn). Must be SUPPRESSED.
	lockChurn := &Record{Ref: ref, RecordRV: "11", UID: "u1", BodyHash: base.BodyHash,
		Lock: &LockState{HolderIdentity: "replica-x"}}
	hub.OnMetadataEvent(apiwatch.Modified, ref, lockChurn, "11")
	if ev := recvTimeout(w, 200*time.Millisecond); ev != nil {
		t.Fatalf("lock-only transition should be suppressed, got %s", ev.Type)
	}

	// Real body change: new body hash -> visible signature differs ->
	// must EMIT.
	body2 := Body{Owner: "alice", Fields: map[string]interface{}{"color": "blue", "size": int64(2)}}
	if err := bs.Put(context.Background(), "default", "w1", body2); err != nil {
		t.Fatalf("update body: %v", err)
	}
	waitInformer()
	visible := &Record{Ref: ref, RecordRV: "12", UID: "u1", BodyHash: HashBody(body2)}
	hub.OnMetadataEvent(apiwatch.Modified, ref, visible, "12")
	if ev := recvTimeout(w, 500*time.Millisecond); ev == nil || ev.Type != apiwatch.Modified {
		t.Fatalf("expected MODIFIED emission for visible change, got %v", ev)
	}
}

// ---- Capability: transactional write path (0049) ----
//
// CAS conflict on body OR metadata -> retry -> 409 on budget exhaustion,
// NEVER a 500-equivalent (InternalError). Also proves no divergence.

func TestTransactionalWrite_BodyConflictRetriesThen409(t *testing.T) {
	r, dyn := newTestREST(t, func(o *Options) {
		o.TransactionAttempts = 3
	})
	ctx := systemCtx()

	// Seed an object so Update has something to update.
	if _, err := r.Create(ctx, makeWidget("w1", "red", 1), nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed Create: %v", err)
	}
	waitInformer()

	// Make EVERY body-CR update conflict, forever. The transaction must
	// retry up to the budget and then surface a clean 409, never a 500.
	dyn.PrependReactor("update", bodyGVR.Resource, func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(schema.GroupResource{Group: bodyGVR.Group, Resource: bodyGVR.Resource}, "w1",
			errModified())
	})

	w := makeWidget("w1", "blue", 2)
	_, _, err := r.Update(ctx, "w1", &fixedUpdate{obj: w}, nil, nil, false, &metav1.UpdateOptions{})
	if err == nil {
		t.Fatal("expected an error after exhausting retry budget")
	}
	if !apierrors.IsConflict(err) {
		t.Fatalf("expected 409 Conflict, got %T: %v (status=%v)", err, err, statusOf(err))
	}
	if apierrors.IsInternalError(err) {
		t.Fatalf("must never surface a 500 InternalError, got: %v", err)
	}
	if r.Counters().WriteRetry.Load() == 0 {
		t.Error("expected at least one retry to have fired")
	}
}

func TestTransactionalWrite_MetadataCommitConflictRetriesThen409(t *testing.T) {
	r, dyn := newTestREST(t, func(o *Options) {
		o.TransactionAttempts = 3
	})
	ctx := systemCtx()

	if _, err := r.Create(ctx, makeWidget("w1", "red", 1), nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed Create: %v", err)
	}
	waitInformer()

	// Make the metadata-CR commit conflict forever. The acquire write is
	// also an update on the metadata CR; to target only the commit we
	// fail every metadata update with a conflict. The acquire path
	// retries within its own budget and then 409s at the outer txn
	// level — still a 409, never a 500.
	dyn.PrependReactor("update", metaGVR.Resource, func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(schema.GroupResource{Group: metaGVR.Group, Resource: metaGVR.Resource}, "w1",
			errModified())
	})

	w := makeWidget("w1", "blue", 2)
	_, _, err := r.Update(ctx, "w1", &fixedUpdate{obj: w}, nil, nil, false, &metav1.UpdateOptions{})
	if err == nil {
		t.Fatal("expected an error")
	}
	if !apierrors.IsConflict(err) {
		t.Fatalf("expected 409 Conflict, got %T: %v", err, err)
	}
	if apierrors.IsInternalError(err) {
		t.Fatalf("must never surface 500, got: %v", err)
	}
}

func TestTransactionalWrite_NoDivergence(t *testing.T) {
	r, dyn := newTestREST(t, nil)
	ctx := systemCtx()

	if _, err := r.Create(ctx, makeWidget("w1", "red", 1), nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed Create: %v", err)
	}
	waitInformer()

	// Inject a SINGLE transient body conflict, then let it succeed. The
	// transaction must retry and converge: body, metadata commit, and
	// served object all agree, no orphaned lock.
	var once sync.Once
	dyn.PrependReactor("update", bodyGVR.Resource, func(clienttesting.Action) (bool, runtime.Object, error) {
		failed := false
		once.Do(func() { failed = true })
		if failed {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Group: bodyGVR.Group, Resource: bodyGVR.Resource}, "w1", errModified())
		}
		return false, nil, nil
	})

	w := makeWidget("w1", "blue", 7)
	updated, _, err := r.Update(ctx, "w1", &fixedUpdate{obj: w}, nil, nil, false, &metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Update should succeed after a transient conflict: %v", err)
	}
	uw := updated.(*widget)
	if uw.Spec.Color != "blue" || uw.Spec.Size != 7 {
		t.Errorf("served object diverged: %+v", uw.Spec)
	}
	if r.Counters().WriteRetry.Load() == 0 {
		t.Error("expected a retry to have fired")
	}
	if r.Counters().MaxWriteDepth.Load() == 0 {
		t.Error("expected max write depth >= 1 after a retry")
	}

	// The metadata CR must carry the new body hash and NO lingering lock.
	metaName := RecordName(ResourceRef{Group: "example.io", Resource: "widgets", Namespace: "default", Name: "w1"})
	u, gerr := dyn.Resource(metaGVR).Get(ctx, metaName, metav1.GetOptions{})
	if gerr != nil {
		t.Fatalf("get metadata CR: %v", gerr)
	}
	if _, found, _ := unstructured.NestedMap(u.Object, "spec", "lock"); found {
		t.Error("metadata CR has a lingering lock after commit")
	}
	bh, _, _ := unstructured.NestedString(u.Object, "spec", "observed", "bodyHash")
	wantHash := HashBody(Body{Owner: "alice", Fields: map[string]interface{}{"color": "blue", "size": int64(7)}})
	if bh != wantHash {
		t.Errorf("metadata bodyHash %s != recomputed %s (divergence)", bh, wantHash)
	}
	// The served RV equals the metadata CR RV (no split).
	if uw.ResourceVersion != u.GetResourceVersion() {
		t.Errorf("served RV %s != metadata RV %s", uw.ResourceVersion, u.GetResourceVersion())
	}
}

// The single-shot (non-transactional) path surfaces a commit conflict as
// a 500-equivalent — the regression baseline that 0049 fixes.
func TestNonTransactionalWrite_SurfacesInternalError(t *testing.T) {
	r, dyn := newTestREST(t, func(o *Options) {
		o.TransactionalWrite = false
	})
	ctx := systemCtx()

	if _, err := r.Create(ctx, makeWidget("w1", "red", 1), nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed Create: %v", err)
	}
	waitInformer()

	// Make the body update conflict; with the discipline OFF this is a
	// single shot and the conflict surfaces as a 500.
	dyn.PrependReactor("update", bodyGVR.Resource, func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(schema.GroupResource{Group: bodyGVR.Group, Resource: bodyGVR.Resource}, "w1", errModified())
	})

	_, _, err := r.Update(ctx, "w1", &fixedUpdate{obj: makeWidget("w1", "blue", 2)}, nil, nil, false, &metav1.UpdateOptions{})
	if err == nil {
		t.Fatal("expected an error")
	}
	if !apierrors.IsInternalError(err) {
		t.Fatalf("baseline should surface a 500 InternalError, got %T: %v", err, err)
	}
}

// ---- Capability: pre-acquire OCC ordering ----

func TestPreAcquireOCC(t *testing.T) {
	r, _ := newTestREST(t, nil)
	ctx := systemCtx()

	created, err := r.Create(ctx, makeWidget("w1", "red", 1), nil, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	waitInformer()
	goodRV := created.(*widget).ResourceVersion

	// Update carrying the CURRENT RV succeeds (lock churn bumps the CR
	// RV in between, but OCC compares against the pre-acquire RV).
	upd := makeWidget("w1", "green", 1)
	upd.ResourceVersion = goodRV
	out, _, err := r.Update(ctx, "w1", &fixedUpdate{obj: upd}, nil, nil, false, &metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Update with current RV should succeed: %v", err)
	}
	waitInformer()
	_ = out

	// Update reusing the now-stale RV must 409.
	stale := makeWidget("w1", "purple", 1)
	stale.ResourceVersion = goodRV
	_, _, err = r.Update(ctx, "w1", &fixedUpdate{obj: stale}, nil, nil, false, &metav1.UpdateOptions{})
	if err == nil {
		t.Fatal("expected 409 for stale RV")
	}
	if !apierrors.IsConflict(err) {
		t.Fatalf("expected 409 Conflict, got %T: %v", err, err)
	}
}

// ---- Capability: per-watcher dedup cache hit behavior ----

func TestPerWatcherDedupCache(t *testing.T) {
	r, _ := newTestREST(t, nil)
	hub := r.Hub()
	bs := r.bodies

	// Seed one body owned by alice.
	if err := bs.Put(context.Background(), "default", "w1", Body{Owner: "alice", Fields: map[string]interface{}{"color": "red", "size": int64(1)}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	waitInformer()

	alice := &user.DefaultInfo{Name: "alice"}
	ref := ResourceRef{Group: "example.io", Resource: "widgets", Namespace: "default", Name: "w1"}

	// Register N watchers sharing alice's identity.
	const n = 5
	var ws []*PerWatcher
	for i := 0; i < n; i++ {
		wctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		w := hub.NewWatch(wctx, alice, "", nil, WatchPush, nil)
		ws = append(ws, w)
		t.Cleanup(w.Stop)
	}
	waitInformer()

	hitBefore := hub.Counters.GetCacheHit.Load()
	missBefore := hub.Counters.GetCacheMiss.Load()

	// One metadata event fans out to all N watchers. With the dedup
	// cache keyed on (identity, ns, name) and all watchers sharing
	// alice, exactly ONE StitchFor (miss) should fire and N-1 hits.
	rec := &Record{Ref: ref, RecordRV: "5", UID: "u1", BodyHash: HashBody(Body{Owner: "alice", Fields: map[string]interface{}{"color": "red", "size": int64(1)}})}
	hub.OnMetadataEvent(apiwatch.Modified, ref, rec, "5")

	hits := hub.Counters.GetCacheHit.Load() - hitBefore
	misses := hub.Counters.GetCacheMiss.Load() - missBefore
	if misses != 1 {
		t.Errorf("expected exactly 1 cache miss (one StitchFor), got %d", misses)
	}
	if hits != n-1 {
		t.Errorf("expected %d cache hits, got %d", n-1, hits)
	}
}

// ---- Capability: read-path reconcile (adopt/collect) + off by default ----

func TestReadPathReconcile_OffByDefault(t *testing.T) {
	r, dyn := newTestREST(t, nil) // ReadPathReconcile defaults false
	if r.reconcile {
		t.Fatal("ReadPathReconcile must default to OFF")
	}
	ctx := systemCtx()

	// A backend body created OUT OF BAND, then a normal Get. With
	// reconcile OFF the read path uses the informer cache and makes NO
	// authoritative (direct) backend existence query — the
	// amplification-trading reconcile is not engaged.
	putBodyDirect(t, dyn, "default", "foreign", Body{Owner: "alice", Fields: map[string]interface{}{"color": "x", "size": int64(0)}})
	waitInformer()

	_, _ = r.Get(ctx, "foreign", &metav1.GetOptions{})
	if r.Counters().BackendGet.Load() != 0 {
		t.Errorf("reconcile off must not make authoritative backend Gets, got %d", r.Counters().BackendGet.Load())
	}
	if r.Counters().AdoptInline.Load() != 0 {
		t.Errorf("reconcile off must not adopt, got %d", r.Counters().AdoptInline.Load())
	}
}

func TestReadPathReconcile_AdoptAndCollect(t *testing.T) {
	r, dyn := newTestREST(t, func(o *Options) {
		o.ReadPathReconcile = true
		o.Adopt = true
		o.GCEnabled = true
		o.CollectMinAge = time.Nanosecond // collect immediately for the test
	})
	ctx := systemCtx()

	// Out-of-band backend body, no record. Get adopts it.
	putBodyDirect(t, dyn, "default", "adopted", Body{Owner: "alice", Fields: map[string]interface{}{"color": "y", "size": int64(3)}})
	waitInformer()

	got, err := r.Get(ctx, "adopted", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get should adopt: %v", err)
	}
	gw := got.(*widget)
	if gw.UID == "" || gw.UID[:9] == "synthetic" {
		t.Errorf("adopted object should have a persisted (non-synthetic) UID, got %q", gw.UID)
	}
	if r.Counters().AdoptInline.Load() != 1 {
		t.Errorf("expected 1 inline adopt, got %d", r.Counters().AdoptInline.Load())
	}
	if r.Counters().BackendGet.Load() == 0 {
		t.Error("reconcile on should make authoritative backend Gets (1:1 amplification)")
	}

	// Now delete the backend body out of band. Get must 404 AND collect
	// the orphan record.
	if err := dyn.Resource(bodyGVR).Delete(ctx, BodyName("default", "adopted"), metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete body: %v", err)
	}
	waitInformer()
	_, err = r.Get(ctx, "adopted", &metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected NotFound after backend delete, got %v", err)
	}
	if r.Counters().CollectInline.Load() != 1 {
		t.Errorf("expected 1 inline collect, got %d", r.Counters().CollectInline.Load())
	}
}

// ---- Capability: per-user identity gate on Get/List ----

func TestIdentityGate(t *testing.T) {
	r, _ := newTestREST(t, nil)

	// alice creates a widget.
	if _, err := r.Create(ctxFor("alice"), makeWidget("a1", "red", 1), nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("alice Create: %v", err)
	}
	waitInformer()

	// bob cannot Get alice's widget.
	_, err := r.Get(ctxFor("bob"), "a1", &metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("bob should not see alice's widget, got %v", err)
	}
	// bob's List is empty.
	bl, err := r.List(ctxFor("bob"), &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("bob List: %v", err)
	}
	if len(bl.(*widgetList).Items) != 0 {
		t.Errorf("bob should see 0 widgets, got %d", len(bl.(*widgetList).Items))
	}
	// alice's List sees it.
	al, err := r.List(ctxFor("alice"), &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("alice List: %v", err)
	}
	if len(al.(*widgetList).Items) != 1 {
		t.Errorf("alice should see 1 widget, got %d", len(al.(*widgetList).Items))
	}
}

// ---- CRUD round-trip happy path ----

func TestCRUDRoundTrip(t *testing.T) {
	r, _ := newTestREST(t, nil)
	ctx := systemCtx()

	created, err := r.Create(ctx, makeWidget("w1", "red", 1), nil, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.(*widget).UID == "" {
		t.Error("created widget has no UID")
	}
	waitInformer()

	got, err := r.Get(ctx, "w1", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.(*widget).Spec.Color != "red" {
		t.Errorf("unexpected color: %s", got.(*widget).Spec.Color)
	}

	_, _, err = r.Delete(ctx, "w1", nil, &metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	waitInformer()
	_, err = r.Get(ctx, "w1", &metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected NotFound after delete, got %v", err)
	}
}

// ---- helpers ----

type fixedUpdate struct{ obj runtime.Object }

func (f *fixedUpdate) Preconditions() *metav1.Preconditions { return nil }
func (f *fixedUpdate) UpdatedObject(_ context.Context, _ runtime.Object) (runtime.Object, error) {
	return f.obj, nil
}

func errModified() error {
	return apierrors.NewConflict(schema.GroupResource{}, "x", nil)
}

func statusOf(err error) int32 {
	if se, ok := err.(*apierrors.StatusError); ok {
		return se.ErrStatus.Code
	}
	return -1
}

func putBodyDirect(t *testing.T, dyn *dynamicfake.FakeDynamicClient, ns, name string, b Body) {
	t.Helper()
	u := encodeBody(ns, name, b, bodyGVR, bodyKind)
	u.SetName(BodyName(ns, name))
	if _, err := dyn.Resource(bodyGVR).Create(context.Background(), u, metav1.CreateOptions{}); err != nil {
		t.Fatalf("put body direct: %v", err)
	}
}

func drainUntilBookmark(t *testing.T, w *PerWatcher) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev := <-w.ResultChan():
			if ev.Type == apiwatch.Bookmark {
				return
			}
		case <-deadline:
			return
		}
	}
}

func recvTimeout(w *PerWatcher, d time.Duration) *apiwatch.Event {
	select {
	case ev := <-w.ResultChan():
		return &ev
	case <-time.After(d):
		return nil
	}
}

// drainAll consumes any pending events within d (used to discard the
// immediate poll-loop replay before driving manual hub events).
func drainAll(w *PerWatcher, d time.Duration) {
	deadline := time.After(d)
	for {
		select {
		case <-w.ResultChan():
		case <-deadline:
			return
		}
	}
}

var _ = types.UID("")
