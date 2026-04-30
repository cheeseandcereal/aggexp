// Package crdbackend implements a runtime/storage.WritableBackend that
// forwards every operation (Get, List, Watch, Create, Update, Delete)
// to a CustomResourceDefinition served by the host kube-apiserver. The
// aggregated apiserver therefore holds **no state of its own** — it is
// a facade — yet all library-layer features that assume persistence
// (SSA managedFields, finalizers, labels, annotations, ownerReferences)
// now survive because the CRD's etcd row carries them.
//
// The thesis: "if a stateless AA needs state, don't give it its own
// etcd; point it at a CRD on the host cluster." This is strictly
// weaker than running your own etcd — one more kube-apiserver hop per
// request — but far stronger than no persistence at all, and a
// natural fit in environments where a CRD is operationally cheap to
// install.
//
// The backend also demonstrates two facade transformations:
//
//  1. Field-rename. On the backing CRD the counter is stored as
//     `storedCounter`; on the exposed v1 Widget it is `counter`. The
//     backend renames on read and write. Proves the facade can
//     transform without touching the backing store.
//  2. Identity-aware filter. When the caller's user name starts with
//     "alice-", spec.tags on the response is filtered to only include
//     keys starting with "alice-". Arbitrary lab demonstration that
//     identity reaches the backend.
//
// Watch is implemented by opening a dynamic watch on the CRD and
// forwarding events through the runtime/storage.Publisher.
package crdbackend

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0010-etcd-crd-facade-with-ssa/pkg/apis/aggexp"
	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// StorageGVR is the CRD served on the host cluster that backs the
// exposed widgets.aggexp.io/v1 API.
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
}

// Backend implements runtime.storage.WritableBackend by talking to a
// CRD on the host cluster.
type Backend struct {
	client dynamic.NamespaceableResourceInterface

	mu        sync.RWMutex
	publisher runtimestorage.Publisher
}

// New constructs a Backend.
func New(opts Options) *Backend {
	if opts.Dynamic == nil {
		panic("crdbackend.New: Dynamic client is required")
	}
	return &Backend{
		client: opts.Dynamic.Resource(StorageGVR),
	}
}

// SetPublisher wires the adapter's publisher so the watch-forwarder
// can fan events out. Must be called before Start.
func (b *Backend) SetPublisher(p runtimestorage.Publisher) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.publisher = p
}

// Start opens a dynamic watch on the CRD and forwards events through
// the publisher until ctx is cancelled. Re-opens on watch errors with
// a minimal restart loop (no backoff; lab scope).
func (b *Backend) Start(ctx context.Context) {
	go b.watchLoop(ctx)
}

func (b *Backend) watchLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		w, err := b.client.Watch(ctx, metav1.ListOptions{})
		if err != nil {
			klog.V(2).InfoS("crd-watch-open-failed", "err", err)
			select {
			case <-ctx.Done():
				return
			// Arbitrary short backoff; not tuned.
			case <-timerAfter(2):
			}
			continue
		}
		b.forwardEvents(ctx, w)
	}
}

func (b *Backend) forwardEvents(ctx context.Context, w watch.Interface) {
	defer w.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-w.ResultChan():
			if !ok {
				klog.V(2).InfoS("crd-watch-channel-closed")
				return
			}
			b.handleEvent(ev)
		}
	}
}

func (b *Backend) handleEvent(ev watch.Event) {
	u, ok := ev.Object.(*unstructured.Unstructured)
	if !ok {
		klog.V(2).InfoS("crd-watch-non-unstructured", "type", fmt.Sprintf("%T", ev.Object))
		return
	}
	w := storageToWidget(u)
	b.mu.RLock()
	pub := b.publisher
	b.mu.RUnlock()
	if pub == nil {
		return
	}
	switch ev.Type {
	case watch.Added:
		pub.PublishAdded(w)
	case watch.Modified:
		pub.PublishModified(w)
	case watch.Deleted:
		pub.PublishDeleted(w)
	}
}

// ---- runtime/storage.Backend identity ----

// New returns a new empty Widget (hub/internal form).
func (b *Backend) New() runtime.Object { return &aggexp.Widget{} }

// NewList returns a new empty WidgetList (hub/internal form).
func (b *Backend) NewList() runtime.Object { return &aggexp.WidgetList{} }

// Kind returns the externally-exposed Kind.
func (b *Backend) Kind() string { return "Widget" }

// SingularName returns the kubectl-friendly singular name.
func (b *Backend) SingularName() string { return "widget" }

// NamespaceScoped is false for this experiment.
func (b *Backend) NamespaceScoped() bool { return false }

// ---- Get ----

// Get fetches the WidgetStorage CRD row for name from the host
// cluster, converts it to a Widget, and applies identity-aware
// transformations.
func (b *Backend) Get(ctx context.Context, u user.Info, name string) (runtime.Object, error) {
	logUser("get", u, "name", name)
	u2, err := b.client.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, apierrors.NewNotFound(aggexp.Resource("widgets"), name)
		}
		return nil, apierrors.NewInternalError(fmt.Errorf("dynamic Get %s: %w", name, err))
	}
	w := storageToWidget(u2)
	applyIdentityTransform(w, u)
	return w, nil
}

// ---- List ----

// List fetches all WidgetStorage rows and returns them as a
// WidgetList. LabelSelector is unused by the backend (the adapter
// filters post-hoc).
func (b *Backend) List(ctx context.Context, u user.Info, _ runtimestorage.ListOptions) (runtime.Object, error) {
	logUser("list", u)
	ul, err := b.client.List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("dynamic List: %w", err))
	}
	list := &aggexp.WidgetList{Items: make([]aggexp.Widget, 0, len(ul.Items))}
	for i := range ul.Items {
		w := storageToWidget(&ul.Items[i])
		applyIdentityTransform(w, u)
		list.Items = append(list.Items, *w)
	}
	sort.Slice(list.Items, func(i, j int) bool {
		return list.Items[i].Name < list.Items[j].Name
	})
	// Surface the CRD list's resourceVersion so kubectl's
	// list-view is consistent.
	list.ResourceVersion = ul.GetResourceVersion()
	return list, nil
}

// ---- Create ----

// Create writes a new WidgetStorage on the host cluster.
func (b *Backend) Create(ctx context.Context, u user.Info, obj runtime.Object) (runtime.Object, error) {
	w, ok := obj.(*aggexp.Widget)
	if !ok {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("expected Widget, got %T", obj))
	}
	if w.Name == "" {
		return nil, apierrors.NewBadRequest("metadata.name is required")
	}
	logUser("create", u, "name", w.Name, "managedFields", len(w.ManagedFields), "finalizers", len(w.Finalizers), "ownerRefs", len(w.OwnerReferences))
	storage := widgetToStorage(w)
	created, err := b.client.Create(ctx, storage, metav1.CreateOptions{FieldManager: "aggexp-widgets"})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil, apierrors.NewAlreadyExists(aggexp.Resource("widgets"), w.Name)
		}
		return nil, apierrors.NewInternalError(fmt.Errorf("dynamic Create %s: %w", w.Name, err))
	}
	out := storageToWidget(created)
	applyIdentityTransform(out, u)
	return out, nil
}

// ---- Update ----

// Update performs a full replacement on the CRD row. It preserves the
// CRD's resourceVersion for optimistic concurrency.
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
	logUser("update", u, "name", name, "managedFields", len(w.ManagedFields), "finalizers", len(w.Finalizers), "ownerRefs", len(w.OwnerReferences))

	existing, err := b.client.Get(ctx, name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, false, apierrors.NewInternalError(fmt.Errorf("dynamic Get %s: %w", name, err))
	}
	created := false
	storage := widgetToStorage(w)
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
		// Preserve the CRD's resourceVersion on the update.
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
	out := storageToWidget(existing)
	applyIdentityTransform(out, u)
	return out, created, nil
}

// ---- Delete ----

// Delete removes the CRD row.
func (b *Backend) Delete(ctx context.Context, u user.Info, name string) (runtime.Object, bool, error) {
	logUser("delete", u, "name", name)
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
	// Re-read to see if the host treated this as a pending delete
	// (finalizers present) or actually removed the row.
	post, gerr := b.client.Get(ctx, name, metav1.GetOptions{})
	if gerr != nil && apierrors.IsNotFound(gerr) {
		w := storageToWidget(existing)
		applyIdentityTransform(w, u)
		return w, true, nil
	}
	if gerr != nil {
		return nil, false, apierrors.NewInternalError(fmt.Errorf("dynamic Get post-delete %s: %w", name, gerr))
	}
	// Finalizers are preventing deletion. Return the in-progress
	// object; the library surfaces deletionTimestamp to clients.
	w := storageToWidget(post)
	applyIdentityTransform(w, u)
	return w, false, nil
}

// ---- Table ----

// TableColumns returns the kubectl columns for Widget.
func (b *Backend) TableColumns() []metav1.TableColumnDefinition {
	return []metav1.TableColumnDefinition{
		{Name: "Name", Type: "string", Format: "name", Description: "Widget name."},
		{Name: "Counter", Type: "integer", Description: "Spec counter value."},
		{Name: "Tags", Type: "integer", Description: "Number of tags (after identity filter)."},
		{Name: "Description", Type: "string", Description: "Free-text description."},
	}
}

// RowsFor emits rows for a single Widget or a WidgetList.
func (b *Backend) RowsFor(obj runtime.Object) ([]metav1.TableRow, error) {
	row := func(w *aggexp.Widget) metav1.TableRow {
		return metav1.TableRow{
			Cells: []interface{}{
				w.Name,
				w.Spec.Counter,
				int64(len(w.Spec.Tags)),
				w.Spec.Description,
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

// widgetToStorage converts an internal Widget into an Unstructured
// suitable for writing to the CRD. Key operations:
//
//   - spec.counter          -> spec.storedCounter (field rename demo)
//   - status.observedCounter passes through
//   - ObjectMeta is preserved **verbatim** so managedFields,
//     finalizers, ownerReferences, labels, annotations, uid,
//     resourceVersion all survive the round trip — the whole point
//     of the experiment.
//   - Each ManagedFieldsEntry.APIVersion is rewritten from
//     "aggexp.io/v1" to the backing-CRD APIVersion
//     ("aggexpstorage.aggexp.io/v1"). Otherwise kube-apiserver's
//     CRD field-manager sees an entry with a foreign apiVersion and
//     silently drops it on write. This is **the one finding that
//     required a fix in the backend**: managedFields entries are
//     group-scoped, and a facade that pass-through-copies them will
//     lose them at the CRD boundary.
func widgetToStorage(w *aggexp.Widget) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion(StorageAPIVersion)
	u.SetKind(StorageKind)
	// Clone ObjectMeta so our apiVersion rewrite doesn't mutate the
	// caller's Widget.
	om := w.ObjectMeta.DeepCopy()
	// Rewrite ManagedFieldsEntry.APIVersion to the backing CRD's
	// group/version. See package doc.
	if len(om.ManagedFields) > 0 {
		for i := range om.ManagedFields {
			if om.ManagedFields[i].APIVersion == "aggexp.io/v1" || om.ManagedFields[i].APIVersion == "" {
				om.ManagedFields[i].APIVersion = StorageAPIVersion
			}
		}
		// Also rewrite fieldsV1 JSON: replace f:counter -> f:storedCounter
		// so the fields referenced match the storage schema. Otherwise
		// the field manager holds pointers to names the backing row
		// doesn't contain.
		for i := range om.ManagedFields {
			if om.ManagedFields[i].FieldsV1 == nil {
				continue
			}
			om.ManagedFields[i].FieldsV1 = renameFieldsV1(om.ManagedFields[i].FieldsV1, "f:counter", "f:storedCounter")
		}
	}
	// Copy ObjectMeta through an unstructured conversion so all
	// fields (ManagedFields, Finalizers, OwnerReferences, Labels,
	// Annotations, UID, ResourceVersion, Name, etc.) round-trip.
	metaMap, err := objectMetaToMap(om)
	if err == nil {
		u.Object["metadata"] = metaMap
	}
	spec := map[string]interface{}{}
	if w.Spec.Description != "" {
		spec["description"] = w.Spec.Description
	}
	// Rename: exposed "counter" -> storage "storedCounter".
	spec["storedCounter"] = int64(w.Spec.Counter)
	if len(w.Spec.Tags) > 0 {
		tags := make(map[string]interface{}, len(w.Spec.Tags))
		for k, v := range w.Spec.Tags {
			tags[k] = v
		}
		spec["tags"] = tags
	}
	u.Object["spec"] = spec
	status := map[string]interface{}{
		"observedCounter": int64(w.Status.ObservedCounter),
	}
	u.Object["status"] = status
	return u
}

// storageToWidget is the inverse: WidgetStorage unstructured -> Widget.
func storageToWidget(u *unstructured.Unstructured) *aggexp.Widget {
	w := &aggexp.Widget{}
	w.TypeMeta.Kind = "Widget"
	w.TypeMeta.APIVersion = "aggexp.io/v1"

	if raw, ok := u.Object["metadata"].(map[string]interface{}); ok {
		_ = mapToObjectMeta(raw, &w.ObjectMeta)
	}
	// Rewrite ManagedFieldsEntry.APIVersion back to the exposed
	// group/version so clients see a consistent picture.
	if len(w.ManagedFields) > 0 {
		for i := range w.ManagedFields {
			if w.ManagedFields[i].APIVersion == StorageAPIVersion {
				w.ManagedFields[i].APIVersion = "aggexp.io/v1"
			}
		}
		for i := range w.ManagedFields {
			if w.ManagedFields[i].FieldsV1 == nil {
				continue
			}
			w.ManagedFields[i].FieldsV1 = renameFieldsV1(w.ManagedFields[i].FieldsV1, "f:storedCounter", "f:counter")
		}
	}

	if spec, ok := u.Object["spec"].(map[string]interface{}); ok {
		if v, ok := spec["description"].(string); ok {
			w.Spec.Description = v
		}
		if v, ok := spec["storedCounter"]; ok {
			w.Spec.Counter = int32(toInt64(v))
		}
		if tagsRaw, ok := spec["tags"].(map[string]interface{}); ok {
			tags := make(map[string]string, len(tagsRaw))
			for k, v := range tagsRaw {
				if s, ok := v.(string); ok {
					tags[k] = s
				}
			}
			if len(tags) > 0 {
				w.Spec.Tags = tags
			}
		}
	}
	if status, ok := u.Object["status"].(map[string]interface{}); ok {
		if v, ok := status["observedCounter"]; ok {
			w.Status.ObservedCounter = int32(toInt64(v))
		}
	}
	return w
}

// applyIdentityTransform mutates w in place based on the caller's
// identity. The transformation here is deliberately simple and
// arbitrary: it proves identity reaches the backend.
func applyIdentityTransform(w *aggexp.Widget, u user.Info) {
	if w == nil || u == nil {
		return
	}
	name := u.GetName()
	if !strings.HasPrefix(name, "alice-") {
		return
	}
	if len(w.Spec.Tags) == 0 {
		return
	}
	filtered := map[string]string{}
	for k, v := range w.Spec.Tags {
		if strings.HasPrefix(k, "alice-") {
			filtered[k] = v
		}
	}
	if len(filtered) == 0 {
		w.Spec.Tags = nil
	} else {
		w.Spec.Tags = filtered
	}
}

// ---- small helpers ----

func logUser(verb string, u user.Info, fields ...interface{}) {
	if u == nil {
		klog.V(2).InfoS("crd-backend", append([]interface{}{"verb", verb}, fields...)...)
		return
	}
	kv := append([]interface{}{"verb", verb, "user", u.GetName(), "groups", u.GetGroups()}, fields...)
	klog.V(2).InfoS("crd-backend", kv...)
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

// timerAfter is a tiny helper so the watch-loop file reads as
// self-contained. Uses time.After internally but exposes it as a
// unit-testable hook if we ever need to stub it.
func timerAfter(seconds int) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		<-timeAfterSeconds(seconds)
		close(ch)
	}()
	return ch
}

// Compile-time assertions.
var (
	_ runtimestorage.Backend         = (*Backend)(nil)
	_ runtimestorage.WritableBackend = (*Backend)(nil)
)
