// Package crdbackend implements a runtime/storage.WritableBackend that
// forwards every operation to a CRD on the host kube-apiserver, using
// a shared dynamic informer for Get/List/Watch. The aggregated
// apiserver therefore holds **no business state of its own**; the
// host-cluster CRD is the single source of truth.
//
// Multiple replicas of this AA all back the same CRD. Each replica
// has its own informer subscribed to the same CRD, so each replica
// observes the same wire-level event stream from kube-apiserver and
// re-broadcasts it to its own watch clients via the runtime/storage
// adapter's Publisher.
//
// RV-authority decision: this experiment ABANDONS the per-replica
// atomic.Uint64 RV that runtime/storage.adapter normally synthesizes.
// Instead, each event carries the host CRD's resourceVersion as the
// observed RV, propagated unchanged from the informer event. Reasons:
//
//   - The host CRD's RV is a single monotonic stream from etcd. All
//     replicas see the same numbers in the same order. A client
//     disconnecting from replica A and reconnecting to replica B can
//     pass the last RV it observed and replica B can interpret it
//     consistently.
//
//   - Per-replica RVs progress independently across replicas (each
//     replica's atomic.Uint64 starts at zero on pod start). A client
//     that resumed against a different replica with an unknown RV
//     would 410-Gone or get stale-skipped events.
//
//   - The substrate's runtime/storage.adapter.go normally calls
//     stampRV() with NextResourceVersion(); this experiment uses the
//     adapter's broadcaster directly via raw watch.Events to bypass
//     the stamping. See server.go's wiring.
package crdbackend

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0034-shared-watch-cross-replica/pkg/apis/aggexp"
	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// StorageGVR is the CRD served on the host cluster that backs the
// exposed widgets.aggexp.io/v1 API. We use a separate group from
// the AA's exposed group because an APIService claims an entire
// (group, version), so widgets.aggexp.io/v1 (AA) and a CRD in the
// same group/version cannot coexist.
var StorageGVR = schema.GroupVersionResource{
	Group:    "aggexpstorage.aggexp.io",
	Version:  "v1",
	Resource: "widgetstorages",
}

// StorageKind is the Kind used in unstructured objects written to the
// CRD.
const StorageKind = "WidgetStorage"

// StorageAPIVersion matches StorageGVR's Group/Version.
const StorageAPIVersion = "aggexpstorage.aggexp.io/v1"

// Options configures the backend.
type Options struct {
	// Dynamic is a dynamic client against the host kube-apiserver.
	// Required.
	Dynamic dynamic.Interface

	// Namespace constrains the informer + writes. Required because
	// this experiment uses a namespace-scoped CRD.
	Namespace string

	// ReplicaID is a short string identifying this replica in logs
	// (e.g. the pod name). Used to make cross-replica behavior
	// observable in `kubectl logs`.
	ReplicaID string

	// ResyncPeriod for the shared informer. 0 means no resync.
	// Per FINDINGS/0024: a resync forces every cached object back
	// through the event handler as MODIFIED, which is helpful for
	// cross-replica reconciliation but adds CPU. Setting this to
	// 0 makes the experiment depend purely on watch fidelity.
	ResyncPeriod time.Duration
}

// Backend implements runtime.storage.WritableBackend by talking to a
// CRD on the host cluster, with a shared informer feeding watch
// events.
type Backend struct {
	dyn       dynamic.Interface
	client    dynamic.ResourceInterface
	namespace string
	replicaID string

	factory dynamicinformer.DynamicSharedInformerFactory
	lister  cache.GenericLister

	mu        sync.RWMutex
	publisher EventSink
}

// EventSink is what the backend uses to fan out events. It is the
// runtime/storage adapter's Broadcaster, plumbed via a small adapter
// in this experiment. Unlike the standard runtimestorage.Publisher
// which stamps a per-replica RV, EventSink lets us pass through the
// host CRD's RV that arrives from the informer event stream.
type EventSink interface {
	// Action publishes a watch event with the given type and
	// object. The object's ResourceVersion is preserved (host CRD
	// authority).
	Action(et watch.EventType, obj runtime.Object)
	// CurrentResourceVersion returns the last observed RV; used by
	// List to stamp ListMeta.
	CurrentResourceVersion() string
	// SetCurrentResourceVersion records the observed RV so List
	// responses match what the watch will replay.
	SetCurrentResourceVersion(rv string)
}

// New constructs a Backend.
func New(opts Options) *Backend {
	if opts.Dynamic == nil {
		panic("crdbackend.New: Dynamic client is required")
	}
	if opts.Namespace == "" {
		panic("crdbackend.New: Namespace is required (storage CRD is namespace-scoped)")
	}
	b := &Backend{
		dyn:       opts.Dynamic,
		namespace: opts.Namespace,
		replicaID: opts.ReplicaID,
		client:    opts.Dynamic.Resource(StorageGVR).Namespace(opts.Namespace),
	}
	b.factory = dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		opts.Dynamic, opts.ResyncPeriod, opts.Namespace, nil,
	)
	return b
}

// SetSink wires the broadcaster the backend will fan events through.
// Must be called before Start.
func (b *Backend) SetSink(s EventSink) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.publisher = s
}

// Start spins up the shared informer and begins forwarding events.
// Returns once the informer's initial cache sync is complete.
func (b *Backend) Start(ctx context.Context) error {
	informer := b.factory.ForResource(StorageGVR)
	b.lister = informer.Lister()

	_, err := informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { b.handleEvent(watch.Added, obj) },
		UpdateFunc: func(_, obj interface{}) { b.handleEvent(watch.Modified, obj) },
		DeleteFunc: func(obj interface{}) { b.handleDelete(obj) },
	})
	if err != nil {
		return fmt.Errorf("add event handler: %w", err)
	}

	b.factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.Informer().HasSynced) {
		return fmt.Errorf("informer cache sync failed")
	}
	klog.InfoS("informer-synced", "replica", b.replicaID, "namespace", b.namespace)
	return nil
}

func (b *Backend) handleEvent(et watch.EventType, obj interface{}) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		klog.V(2).InfoS("informer-non-unstructured", "type", fmt.Sprintf("%T", obj))
		return
	}
	w := storageToWidget(u)
	rv := u.GetResourceVersion()
	klog.V(2).InfoS("informer-event", "replica", b.replicaID, "type", et, "name", u.GetName(), "rv", rv)

	b.mu.RLock()
	pub := b.publisher
	b.mu.RUnlock()
	if pub == nil {
		return
	}
	pub.SetCurrentResourceVersion(rv)
	pub.Action(et, w)
}

func (b *Backend) handleDelete(obj interface{}) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	b.handleEvent(watch.Deleted, obj)
}

// ---- runtime/storage.Backend identity ----

func (b *Backend) New() runtime.Object        { return &aggexp.Widget{} }
func (b *Backend) NewList() runtime.Object    { return &aggexp.WidgetList{} }
func (b *Backend) Kind() string               { return "Widget" }
func (b *Backend) SingularName() string       { return "widget" }
func (b *Backend) NamespaceScoped() bool      { return true }

// ---- Get ----

func (b *Backend) Get(ctx context.Context, _ user.Info, name string) (runtime.Object, error) {
	obj, err := b.lister.ByNamespace(b.namespace).Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, apierrors.NewNotFound(aggexp.Resource("widgets"), name)
		}
		return nil, apierrors.NewInternalError(fmt.Errorf("informer Get %s: %w", name, err))
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, apierrors.NewInternalError(fmt.Errorf("unexpected object %T", obj))
	}
	return storageToWidget(u), nil
}

// ---- List ----

func (b *Backend) List(ctx context.Context, _ user.Info, _ runtimestorage.ListOptions) (runtime.Object, error) {
	objs, err := b.lister.ByNamespace(b.namespace).List(everything())
	if err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("informer List: %w", err))
	}
	list := &aggexp.WidgetList{Items: make([]aggexp.Widget, 0, len(objs))}
	maxRV := ""
	for _, o := range objs {
		u, ok := o.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		w := storageToWidget(u)
		list.Items = append(list.Items, *w)
		if rv := u.GetResourceVersion(); rv > maxRV {
			maxRV = rv
		}
	}
	sort.Slice(list.Items, func(i, j int) bool {
		return list.Items[i].Name < list.Items[j].Name
	})
	// Use the publisher's recorded current RV (the high-water mark
	// of events the informer has dispatched). This matches what
	// the broadcaster's prefix replay will start from on watch.
	b.mu.RLock()
	pub := b.publisher
	b.mu.RUnlock()
	if pub != nil {
		list.ResourceVersion = pub.CurrentResourceVersion()
	} else {
		list.ResourceVersion = maxRV
	}
	return list, nil
}

// ---- Create ----

func (b *Backend) Create(ctx context.Context, u user.Info, obj runtime.Object) (runtime.Object, error) {
	w, ok := obj.(*aggexp.Widget)
	if !ok {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("expected Widget, got %T", obj))
	}
	if w.Name == "" {
		return nil, apierrors.NewBadRequest("metadata.name is required")
	}
	klog.V(2).InfoS("create", "replica", b.replicaID, "name", w.Name, "user", userName(u))
	storage := widgetToStorage(w, b.namespace)
	created, err := b.client.Create(ctx, storage, metav1.CreateOptions{FieldManager: "aggexp-widgets"})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil, apierrors.NewAlreadyExists(aggexp.Resource("widgets"), w.Name)
		}
		return nil, apierrors.NewInternalError(fmt.Errorf("dynamic Create %s: %w", w.Name, err))
	}
	return storageToWidget(created), nil
}

// ---- Update ----

func (b *Backend) Update(ctx context.Context, u user.Info, name string, obj runtime.Object, forceAllowCreate bool) (runtime.Object, bool, error) {
	w, ok := obj.(*aggexp.Widget)
	if !ok {
		return nil, false, apierrors.NewBadRequest(fmt.Sprintf("expected Widget, got %T", obj))
	}
	if w.Name == "" {
		w.Name = name
	}
	if w.Name != name {
		return nil, false, apierrors.NewBadRequest(fmt.Sprintf("body name %q != path name %q", w.Name, name))
	}
	klog.V(2).InfoS("update", "replica", b.replicaID, "name", name, "user", userName(u))

	existing, err := b.client.Get(ctx, name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, false, apierrors.NewInternalError(fmt.Errorf("dynamic Get %s: %w", name, err))
	}
	created := false
	storage := widgetToStorage(w, b.namespace)
	if err != nil { // not-found
		if !forceAllowCreate {
			return nil, false, apierrors.NewNotFound(aggexp.Resource("widgets"), name)
		}
		newObj, cerr := b.client.Create(ctx, storage, metav1.CreateOptions{FieldManager: "aggexp-widgets"})
		if cerr != nil {
			return nil, false, apierrors.NewInternalError(fmt.Errorf("dynamic Create %s: %w", name, cerr))
		}
		existing = newObj
		created = true
	} else {
		// Preserve the CRD's resourceVersion on the update — this
		// gives us etcd-level optimistic concurrency for free.
		// Concurrent writes from different AA replicas to the same
		// object will see one win and one get a 409, which is
		// EXACTLY the contract this experiment's hypothesis depends
		// on (no in-AA locking; CRD CAS is enough).
		storage.SetResourceVersion(existing.GetResourceVersion())
		newObj, uerr := b.client.Update(ctx, storage, metav1.UpdateOptions{FieldManager: "aggexp-widgets"})
		if uerr != nil {
			if apierrors.IsConflict(uerr) {
				return nil, false, uerr
			}
			return nil, false, apierrors.NewInternalError(fmt.Errorf("dynamic Update %s: %w", name, uerr))
		}
		existing = newObj
	}
	return storageToWidget(existing), created, nil
}

// ---- Delete ----

func (b *Backend) Delete(ctx context.Context, u user.Info, name string) (runtime.Object, bool, error) {
	klog.V(2).InfoS("delete", "replica", b.replicaID, "name", name, "user", userName(u))
	existing, err := b.client.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, apierrors.NewNotFound(aggexp.Resource("widgets"), name)
		}
		return nil, false, apierrors.NewInternalError(fmt.Errorf("dynamic Get %s: %w", name, err))
	}
	if derr := b.client.Delete(ctx, name, metav1.DeleteOptions{}); derr != nil {
		if apierrors.IsNotFound(derr) {
			return nil, false, apierrors.NewNotFound(aggexp.Resource("widgets"), name)
		}
		return nil, false, apierrors.NewInternalError(fmt.Errorf("dynamic Delete %s: %w", name, derr))
	}
	post, gerr := b.client.Get(ctx, name, metav1.GetOptions{})
	if gerr != nil && apierrors.IsNotFound(gerr) {
		return storageToWidget(existing), true, nil
	}
	if gerr != nil {
		return nil, false, apierrors.NewInternalError(fmt.Errorf("dynamic Get post-delete %s: %w", name, gerr))
	}
	return storageToWidget(post), false, nil
}

// ---- Table ----

func (b *Backend) TableColumns() []metav1.TableColumnDefinition {
	return []metav1.TableColumnDefinition{
		{Name: "Name", Type: "string", Format: "name"},
		{Name: "Color", Type: "string"},
		{Name: "Size", Type: "integer"},
		{Name: "Age", Type: "string"},
	}
}

func (b *Backend) RowsFor(obj runtime.Object) ([]metav1.TableRow, error) {
	row := func(w *aggexp.Widget) metav1.TableRow {
		age := ""
		if !w.CreationTimestamp.IsZero() {
			age = time.Since(w.CreationTimestamp.Time).Round(time.Second).String()
		}
		return metav1.TableRow{
			Cells: []interface{}{
				w.Name,
				w.Spec.Color,
				int64(w.Spec.Size),
				age,
			},
			Object: runtime.RawExtension{Object: w},
		}
	}
	switch v := obj.(type) {
	case *aggexp.Widget:
		return []metav1.TableRow{row(v)}, nil
	case *aggexp.WidgetList:
		rs := make([]metav1.TableRow, 0, len(v.Items))
		for i := range v.Items {
			rs = append(rs, row(&v.Items[i]))
		}
		return rs, nil
	}
	return nil, fmt.Errorf("unexpected object %T", obj)
}

// ---- transformations between Widget and CRD shape ----

// widgetToStorage produces an Unstructured suitable for writing to
// the backing CRD. ObjectMeta passes through, namespace is forced.
func widgetToStorage(w *aggexp.Widget, namespace string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion(StorageAPIVersion)
	u.SetKind(StorageKind)
	om := w.ObjectMeta.DeepCopy()
	om.Namespace = namespace
	// Rewrite ManagedFieldsEntry.APIVersion to the storage group/version,
	// as 0010 found necessary. SSA isn't the focus of 0034 but this
	// keeps things consistent if a client tries it.
	for i := range om.ManagedFields {
		if om.ManagedFields[i].APIVersion == "aggexp.io/v1" || om.ManagedFields[i].APIVersion == "" {
			om.ManagedFields[i].APIVersion = StorageAPIVersion
		}
	}
	metaMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(om)
	if err == nil {
		u.Object["metadata"] = metaMap
	}
	spec := map[string]interface{}{}
	if w.Spec.Color != "" {
		spec["color"] = w.Spec.Color
	}
	spec["size"] = int64(w.Spec.Size)
	u.Object["spec"] = spec
	u.Object["status"] = map[string]interface{}{
		"observedSize": int64(w.Status.ObservedSize),
	}
	return u
}

// storageToWidget converts an Unstructured WidgetStorage row to the
// internal Widget. ObjectMeta passes through; managedFields apiVersion
// is rewritten back to the exposed group on read.
func storageToWidget(u *unstructured.Unstructured) *aggexp.Widget {
	w := &aggexp.Widget{}
	w.TypeMeta.Kind = "Widget"
	w.TypeMeta.APIVersion = "aggexp.io/v1"

	if raw, ok := u.Object["metadata"].(map[string]interface{}); ok {
		_ = runtime.DefaultUnstructuredConverter.FromUnstructured(raw, &w.ObjectMeta)
	}
	for i := range w.ManagedFields {
		if w.ManagedFields[i].APIVersion == StorageAPIVersion {
			w.ManagedFields[i].APIVersion = "aggexp.io/v1"
		}
	}

	if spec, ok := u.Object["spec"].(map[string]interface{}); ok {
		if v, ok := spec["color"].(string); ok {
			w.Spec.Color = v
		}
		if v, ok := spec["size"]; ok {
			w.Spec.Size = int32(toInt64(v))
		}
	}
	if status, ok := u.Object["status"].(map[string]interface{}); ok {
		if v, ok := status["observedSize"]; ok {
			w.Status.ObservedSize = int32(toInt64(v))
		}
	}
	return w
}

// ---- small helpers ----

func userName(u user.Info) string {
	if u == nil {
		return "<nil>"
	}
	return u.GetName()
}

func toInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case int:
		return int64(x)
	case float64:
		return int64(x)
	}
	return 0
}

// Compile-time assertions.
var (
	_ runtimestorage.Backend         = (*Backend)(nil)
	_ runtimestorage.WritableBackend = (*Backend)(nil)
)
