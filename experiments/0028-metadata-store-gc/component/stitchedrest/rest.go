// Package stitchedrest is the 0024 REST adapter. It wraps the
// runtime/component grpcbackend protocol client with a metadata
// overlay backed by a shared cluster-scoped ResourceMetadata CRD.
//
// Design:
//
//   - Get: backend.Get(name) → business JSON; metastore.Get(ref) →
//     Record or nil. Stitch: overlay Record onto decoded object's
//     metadata. If no Record exists yet, synthesize in-memory
//     defaults (fresh UID, current RV) without writing.
//
//   - List: backend.List() → []business; one metastore.List()
//     filtered by (group, resource); stitch each item.
//
//   - Create: metastore.Put(new Record with fresh UID + initial
//     managedFields) → backend.Create(spec/status) → stitch and
//     return. Reverse order is also viable; we go metastore-first
//     so the Record persists even if the backend is flaky, giving
//     the caller a visible "pending" surface.
//
//   - Update / SSA: library gives us objInfo.UpdatedObject(current);
//     current is the stitched prior state. The library's field
//     manager has already computed managedFields on the returned
//     object. We split: metadata-fields → metastore.Put; spec/status
//     → backend.Update. Return the stitched result.
//
//   - Delete: metastore.Get; if finalizers non-empty, set
//     deletionTimestamp on the Record and return the stitched
//     object (no backend delete). Otherwise backend.Delete +
//     metastore.Delete.
//
//   - Watch: two upstream watch streams (one on the backend, one on
//     metastore) funnel into a single broadcaster. Each event
//     re-fetches the counterpart and stitches.
//
// The RV of a stitched response is the metastore Record's own
// resourceVersion (the CRD's RV, from host etcd). This is
// monotonic by construction and non-synthetic, so informers can
// do optimistic concurrency without us bookkeeping a counter.
package stitchedrest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/klog/v2"

	componentpb "github.com/cheeseandcereal/aggexp/runtime/component/proto"
	componentscheme "github.com/cheeseandcereal/aggexp/runtime/component/scheme"

	"github.com/cheeseandcereal/aggexp/experiments/0028-metadata-store-gc/component/metastore"
)

// Descriptor captures resource identity.
type Descriptor struct {
	GroupVersion  schema.GroupVersion
	Resource      string
	Kind          string
	Singular      string
	Namespaced    bool
	Writable      bool
	Columns       []metav1.TableColumnDefinition
	RowFields     []string
	GroupResource schema.GroupResource
}

// REST is the stitched adapter.
type REST struct {
	desc   Descriptor
	client componentpb.BackendClient
	store  *metastore.Store

	// fallback RV counter for watch events that aren't synced to
	// the metastore's RV. Primed to 1 at construction.
	rv atomic.Uint64

	bcaster *watch.Broadcaster
}

// New constructs a stitched REST.
func New(desc Descriptor, client componentpb.BackendClient, store *metastore.Store) *REST {
	r := &REST{
		desc:    desc,
		client:  client,
		store:   store,
		bcaster: watch.NewBroadcaster(100, watch.DropIfChannelFull),
	}
	r.rv.Store(1)
	if desc.GroupResource == (schema.GroupResource{}) {
		desc.GroupResource = schema.GroupResource{Group: desc.GroupVersion.Group, Resource: desc.Resource}
		r.desc = desc
	}
	return r
}

// Shutdown stops the broadcaster.
func (r *REST) Shutdown() {
	if r.bcaster != nil {
		r.bcaster.Shutdown()
	}
}

// ---- identity / shape (unchanged from grpcbackend) ----

func (r *REST) New() runtime.Object {
	gvk := r.desc.GroupVersion.WithKind(r.desc.Kind)
	obj := &componentscheme.Object{}
	obj.GetObjectKind().SetGroupVersionKind(gvk)
	return obj
}

func (r *REST) NewList() runtime.Object {
	listGVK := r.desc.GroupVersion.WithKind(r.desc.Kind + "List")
	l := &componentscheme.ObjectList{}
	l.GetObjectKind().SetGroupVersionKind(listGVK)
	return l
}

func (r *REST) Destroy()                {}
func (r *REST) NamespaceScoped() bool   { return r.desc.Namespaced }
func (r *REST) Kind() string            { return r.desc.Kind }
func (r *REST) GetSingularName() string { return r.desc.Singular }

// ---- Get ----

func (r *REST) Get(ctx context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	bizResp, err := r.client.Get(ctx, &componentpb.GetRequest{
		User:      userFromCtx(ctx),
		Namespace: ns,
		Name:      name,
	})
	if err != nil {
		return nil, r.translateErr(err, name)
	}
	rec, merr := r.store.Get(ctx, r.refFor(ns, name))
	if merr != nil {
		klog.Warningf("middleware:metastore:get failed ns=%s name=%s err=%v (returning stitched object without persisted metadata)", ns, name, merr)
	}
	obj, err := r.stitch(bizResp.GetObjectJson(), rec, ns, name)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

// ---- List ----

func (r *REST) List(ctx context.Context, opts *metainternalversion.ListOptions) (runtime.Object, error) {
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	bizResp, err := r.client.List(ctx, &componentpb.ListRequest{
		User:          userFromCtx(ctx),
		Namespace:     ns,
		LabelSelector: selectorString(opts),
	})
	if err != nil {
		return nil, r.translateErr(err, "")
	}
	records, merr := r.store.List(ctx, r.desc.GroupResource.Group, r.desc.GroupResource.Resource)
	if merr != nil {
		klog.Warningf("middleware:metastore:list failed (proceeding without metadata overlay): %v", merr)
	}
	byKey := map[string]*metastore.Record{}
	maxRV := uint64(0)
	for _, rec := range records {
		if r.desc.Namespaced && rec.Ref.Namespace != ns && ns != "" {
			continue
		}
		byKey[rec.Ref.Namespace+"/"+rec.Ref.Name] = rec
		if n, err := strconv.ParseUint(rec.RecordResourceVersion, 10, 64); err == nil && n > maxRV {
			maxRV = n
		}
	}

	list := r.NewList().(*componentscheme.ObjectList)
	for _, raw := range bizResp.GetItemsJson() {
		// Decode just enough to get name/namespace for ref lookup.
		nm, ens := nameNamespaceFromJSON(raw)
		if ens == "" {
			ens = ns
		}
		rec := byKey[ens+"/"+nm]
		obj, err := r.stitch(raw, rec, ens, nm)
		if err != nil {
			return nil, err
		}
		if !r.matchesLabels(obj, opts) {
			continue
		}
		list.Items = append(list.Items, *obj)
	}
	// Stamp the list-level RV with the max Record RV we saw (or
	// the internal counter if the metastore was empty).
	if maxRV > 0 {
		list.SetResourceVersion(strconv.FormatUint(maxRV, 10))
	} else {
		list.SetResourceVersion(strconv.FormatUint(r.rv.Load(), 10))
	}
	return list, nil
}

// ---- Watch ----

// Watch streams events. Initial state is replayed as ADDED events
// from List. Live updates come from the fanout goroutines started
// by Start().
func (r *REST) Watch(ctx context.Context, opts *metainternalversion.ListOptions) (watch.Interface, error) {
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	initial, err := r.List(ctx, opts)
	if err != nil {
		return nil, err
	}
	list := initial.(*componentscheme.ObjectList)
	prefix := make([]watch.Event, 0, len(list.Items))
	for i := range list.Items {
		prefix = append(prefix, watch.Event{Type: watch.Added, Object: &list.Items[i]})
	}
	w, err := r.bcaster.WatchWithPrefix(prefix)
	if err != nil {
		return nil, err
	}
	sel := selectorFromOpts(opts)
	if (sel == nil || sel.Empty()) && ns == "" {
		return w, nil
	}
	return watch.Filter(w, func(ev watch.Event) (watch.Event, bool) {
		if acc, err := meta.Accessor(ev.Object); err == nil {
			if ns != "" && acc.GetNamespace() != ns {
				return ev, false
			}
			if sel != nil && !sel.Empty() && !sel.Matches(labels.Set(acc.GetLabels())) {
				return ev, false
			}
		}
		return ev, true
	}), nil
}

// ---- Create ----

func (r *REST) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, opts *metav1.CreateOptions) (runtime.Object, error) {
	if !r.desc.Writable {
		return nil, apierrors.NewMethodNotSupported(r.desc.GroupResource, "create")
	}
	if createValidation != nil {
		if err := createValidation(ctx, obj); err != nil {
			return nil, err
		}
	}
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	acc, err := meta.Accessor(obj)
	if err != nil {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("accessor: %v", err))
	}
	name := acc.GetName()

	// --- Step 1: persist metadata Record first. This gives us a
	// stable UID + resourceVersion we can embed in the backend
	// create call's object JSON so consumers see the final state
	// on the response.
	rec := recordFromLibraryObject(obj, r.refFor(ns, name))
	if rec.UID == "" {
		rec.UID = uuid.NewString()
	}
	if rec.CreationTimestamp.IsZero() {
		rec.CreationTimestamp = metav1.NewTime(time.Now().UTC())
	}
	klog.V(2).Infof("middleware:metastore:put (create) ref=%s uid=%s", refLog(rec.Ref), rec.UID)
	storedRec, merr := r.store.Put(ctx, rec)
	if merr != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("metastore.Put: %w", merr))
	}

	// --- Step 2: call the backend with the caller's spec/status.
	bizJSON, err := businessJSON(obj)
	if err != nil {
		// Best effort: roll back the Record we just created.
		_ = r.store.Delete(ctx, rec.Ref)
		return nil, apierrors.NewBadRequest(err.Error())
	}
	klog.V(2).Infof("middleware:backend:create ns=%s name=%s", ns, name)
	bizResp, err := r.client.Create(ctx, &componentpb.CreateRequest{
		User:         userFromCtx(ctx),
		Namespace:    ns,
		ObjectJson:   bizJSON,
		FieldManager: createFM(opts),
	})
	if err != nil {
		// Roll back the metastore Record so the caller can retry
		// cleanly. If rollback fails the record becomes orphaned;
		// a later GC sweep would clean it up (out of scope here).
		_ = r.store.Delete(ctx, rec.Ref)
		return nil, r.translateErr(err, name)
	}

	stitched, err := r.stitch(bizResp.GetObjectJson(), storedRec, ns, name)
	if err != nil {
		return nil, err
	}
	r.publish(watch.Added, stitched)
	return stitched, nil
}

// ---- Update (handles SSA too via rest.Patcher + rest.Updater) ----

func (r *REST) Update(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	opts *metav1.UpdateOptions,
) (runtime.Object, bool, error) {
	if !r.desc.Writable {
		return nil, false, apierrors.NewMethodNotSupported(r.desc.GroupResource, "update")
	}
	ns, _ := genericapirequest.NamespaceFrom(ctx)

	// Fetch current stitched state to present to the library's
	// UpdatedObjectInfo. If the object doesn't exist, current = nil
	// and forceAllowCreate controls the upsert path.
	current, gerr := r.Get(ctx, name, &metav1.GetOptions{})
	if gerr != nil {
		if !apierrors.IsNotFound(gerr) {
			return nil, false, gerr
		}
		current = nil
	}

	updated, err := objInfo.UpdatedObject(ctx, current)
	if err != nil {
		return nil, false, err
	}

	if current == nil {
		if !forceAllowCreate {
			return nil, false, apierrors.NewNotFound(r.desc.GroupResource, name)
		}
		if createValidation != nil {
			if err := createValidation(ctx, updated); err != nil {
				return nil, false, err
			}
		}
		// Upsert path: delegate to Create. Stamp fieldManager.
		co, err := r.Create(ctx, updated, nil, &metav1.CreateOptions{FieldManager: updateFM(opts)})
		if err != nil {
			return nil, false, err
		}
		return co, true, nil
	}
	if updateValidation != nil {
		if err := updateValidation(ctx, updated, current); err != nil {
			return nil, false, err
		}
	}

	// Pull out the (already-merged-by-the-library) metadata and
	// persist it.
	rec := recordFromLibraryObject(updated, r.refFor(ns, name))
	// Preserve server-managed fields from the prior Record where
	// the library can't touch them.
	var priorRec *metastore.Record
	if cur, _ := r.store.Get(ctx, rec.Ref); cur != nil {
		priorRec = cur
		if rec.UID == "" {
			rec.UID = cur.UID
		}
		if rec.CreationTimestamp.IsZero() {
			rec.CreationTimestamp = cur.CreationTimestamp
		}
		// deletionTimestamp is immutable once set except via
		// finalizer clearing (handled below).
		if cur.DeletionTimestamp != nil && rec.DeletionTimestamp == nil {
			rec.DeletionTimestamp = cur.DeletionTimestamp
		}
	}
	// If the current object carried a synthetic UID (because the
	// backend object exists out-of-band with no Record yet), mint
	// a real one now. "synthetic-" is our in-memory marker from
	// stitch(); it must never land in persisted metadata.
	if strings.HasPrefix(rec.UID, "synthetic-") || rec.UID == "" {
		rec.UID = uuid.NewString()
	}
	if rec.CreationTimestamp.IsZero() {
		rec.CreationTimestamp = metav1.NewTime(time.Now().UTC())
	}

	// Finalizer-clear trigger: if the prior record had a
	// DeletionTimestamp + finalizers, and the update clears
	// finalizers, finish the delete now. Matches the Kubernetes
	// convention ("once deletionTimestamp is set and finalizers
	// become empty, the object is removed").
	if priorRec != nil && priorRec.DeletionTimestamp != nil &&
		len(priorRec.Finalizers) > 0 && len(rec.Finalizers) == 0 {
		klog.V(2).Infof("middleware:finalizer-cleared ref=%s -> completing delete", refLog(rec.Ref))
		// Backend delete + metastore delete.
		_, derr := r.client.Delete(ctx, &componentpb.DeleteRequest{
			User:      userFromCtx(ctx),
			Namespace: ns,
			Name:      name,
		})
		if derr != nil {
			if st, ok := grpcstatus.FromError(derr); !ok || st.Code() != codes.NotFound {
				return nil, false, r.translateErr(derr, name)
			}
		}
		if merr := r.store.Delete(ctx, rec.Ref); merr != nil {
			return nil, false, apierrors.NewInternalError(merr)
		}
		// Return the "just about to vanish" stitched object with the
		// cleared finalizer set and no trailing metadata.
		bizJSON, err := businessJSON(updated)
		if err == nil {
			if stitched, err := r.stitch(bizJSON, nil, ns, name); err == nil {
				r.publish(watch.Deleted, stitched)
				return stitched, false, nil
			}
		}
		r.publish(watch.Deleted, updated)
		return updated, false, nil
	}

	klog.V(2).Infof("middleware:metastore:put (update) ref=%s uid=%s finalizers=%v", refLog(rec.Ref), rec.UID, rec.Finalizers)
	storedRec, merr := r.store.Put(ctx, rec)
	if merr != nil {
		return nil, false, apierrors.NewInternalError(fmt.Errorf("metastore.Put: %w", merr))
	}

	// Forward spec/status to the backend.
	bizJSON, err := businessJSON(updated)
	if err != nil {
		return nil, false, apierrors.NewBadRequest(err.Error())
	}
	klog.V(2).Infof("middleware:backend:update ns=%s name=%s", ns, name)
	bizResp, err := r.client.Update(ctx, &componentpb.UpdateRequest{
		User:             userFromCtx(ctx),
		Namespace:        ns,
		Name:             name,
		ObjectJson:       bizJSON,
		ForceAllowCreate: false,
		FieldManager:     updateFM(opts),
	})
	if err != nil {
		return nil, false, r.translateErr(err, name)
	}
	stitched, err := r.stitch(bizResp.GetObjectJson(), storedRec, ns, name)
	if err != nil {
		return nil, false, err
	}
	r.publish(watch.Modified, stitched)
	return stitched, false, nil
}

// ---- Delete ----

func (r *REST) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, _ *metav1.DeleteOptions) (runtime.Object, bool, error) {
	if !r.desc.Writable {
		return nil, false, apierrors.NewMethodNotSupported(r.desc.GroupResource, "delete")
	}
	ns, _ := genericapirequest.NamespaceFrom(ctx)

	// Fetch current stitched object — also the deleteValidation
	// input.
	prior, gerr := r.Get(ctx, name, &metav1.GetOptions{})
	if gerr != nil {
		return nil, false, gerr
	}
	if deleteValidation != nil {
		if err := deleteValidation(ctx, prior); err != nil {
			return nil, false, err
		}
	}

	ref := r.refFor(ns, name)
	rec, merr := r.store.Get(ctx, ref)
	if merr != nil {
		return nil, false, apierrors.NewInternalError(fmt.Errorf("metastore.Get: %w", merr))
	}

	// Finalizer-blocking semantics: if the Record has finalizers,
	// set DeletionTimestamp (if not already set) and return the
	// still-present object. Backend is NOT called.
	if rec != nil && len(rec.Finalizers) > 0 {
		if rec.DeletionTimestamp == nil {
			now := metav1.NewTime(time.Now().UTC())
			rec.DeletionTimestamp = &now
			klog.V(2).Infof("middleware:metastore:delete-blocked-by-finalizers ref=%s finalizers=%v", refLog(ref), rec.Finalizers)
			storedRec, perr := r.store.Put(ctx, rec)
			if perr != nil {
				return nil, false, apierrors.NewInternalError(perr)
			}
			// Re-stitch with the updated Record.
			bizResp, berr := r.client.Get(ctx, &componentpb.GetRequest{
				User:      userFromCtx(ctx),
				Namespace: ns,
				Name:      name,
			})
			if berr == nil {
				if stitched, serr := r.stitch(bizResp.GetObjectJson(), storedRec, ns, name); serr == nil {
					r.publish(watch.Modified, stitched)
					return stitched, false, nil
				}
			}
			return prior, false, nil
		}
		// deletionTimestamp already set; still blocked. Return the prior.
		return prior, false, nil
	}

	// Actual delete: backend first, then metastore. If the backend
	// 404s (already gone), continue to metastore cleanup — the
	// caller's intent is to remove the record.
	klog.V(2).Infof("middleware:backend:delete ns=%s name=%s", ns, name)
	_, err := r.client.Delete(ctx, &componentpb.DeleteRequest{
		User:      userFromCtx(ctx),
		Namespace: ns,
		Name:      name,
	})
	if err != nil {
		if st, ok := grpcstatus.FromError(err); !ok || st.Code() != codes.NotFound {
			return nil, false, r.translateErr(err, name)
		}
		klog.V(2).Infof("middleware:backend:delete already-gone ns=%s name=%s (proceeding with metastore cleanup)", ns, name)
	}
	klog.V(2).Infof("middleware:metastore:delete ref=%s", refLog(ref))
	if derr := r.store.Delete(ctx, ref); derr != nil {
		return nil, false, apierrors.NewInternalError(derr)
	}
	r.publish(watch.Deleted, prior)
	return prior, true, nil
}

// ---- TableConvertor ----

func (r *REST) ConvertToTable(_ context.Context, object runtime.Object, _ runtime.Object) (*metav1.Table, error) {
	t := &metav1.Table{ColumnDefinitions: r.desc.Columns}
	rows, err := r.rowsFor(object)
	if err != nil {
		return nil, err
	}
	t.Rows = rows
	if list, ok := object.(metav1.ListInterface); ok {
		t.ResourceVersion = list.GetResourceVersion()
	}
	return t, nil
}

func (r *REST) rowsFor(obj runtime.Object) ([]metav1.TableRow, error) {
	switch o := obj.(type) {
	case *componentscheme.ObjectList:
		rows := make([]metav1.TableRow, 0, len(o.Items))
		for i := range o.Items {
			row, err := r.rowFromContent(o.Items[i].AsMap(), &o.Items[i])
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
		return rows, nil
	case *componentscheme.Object:
		row, err := r.rowFromContent(o.AsMap(), o)
		if err != nil {
			return nil, err
		}
		return []metav1.TableRow{row}, nil
	case *unstructured.UnstructuredList:
		rows := make([]metav1.TableRow, 0, len(o.Items))
		for i := range o.Items {
			row, err := r.rowFromContent(o.Items[i].Object, &o.Items[i])
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
		return rows, nil
	case *unstructured.Unstructured:
		row, err := r.rowFromContent(o.Object, o)
		if err != nil {
			return nil, err
		}
		return []metav1.TableRow{row}, nil
	default:
		return nil, fmt.Errorf("rowsFor: unexpected type %T", obj)
	}
}

func (r *REST) rowFromContent(content map[string]any, objForRow runtime.Object) (metav1.TableRow, error) {
	cells := make([]interface{}, len(r.desc.RowFields))
	for i, path := range r.desc.RowFields {
		v := lookupField(content, path)
		if path == ".metadata.creationTimestamp" {
			if s, ok := v.(string); ok && s != "" {
				v = ageOf(s)
			}
		}
		cells[i] = v
	}
	return metav1.TableRow{
		Cells:  cells,
		Object: runtime.RawExtension{Object: objForRow},
	}, nil
}

// ---- upstream watch fanout ----

// StartUpstreamWatches starts two goroutines: one watching the
// backend gRPC stream for business-data events, one watching the
// metastore CRD for metadata events. Every event is re-stitched
// and republished.
func (r *REST) StartUpstreamWatches(ctx context.Context) {
	go r.runBackendWatch(ctx)
	go r.runMetastoreWatch(ctx)
}

func (r *REST) runBackendWatch(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		err := r.backendWatchOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			klog.Warningf("middleware: backend upstream watch disconnected: %v; retrying in 2s", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (r *REST) backendWatchOnce(ctx context.Context) error {
	stream, err := r.client.Watch(ctx, &componentpb.WatchRequest{})
	if err != nil {
		return err
	}
	klog.Infof("middleware: backend upstream watch opened for %s", r.desc.GroupResource)
	for {
		ev, err := stream.Recv()
		if err != nil {
			return err
		}
		nm, ens := nameNamespaceFromJSON(ev.GetObjectJson())
		rec, merr := r.store.Get(ctx, r.refFor(ens, nm))
		if merr != nil {
			klog.Warningf("middleware:metastore:get failed during backend-watch ref=%s/%s err=%v", ens, nm, merr)
		}
		obj, serr := r.stitch(ev.GetObjectJson(), rec, ens, nm)
		if serr != nil {
			klog.Warningf("middleware: stitch failed on backend event: %v", serr)
			continue
		}
		switch ev.GetType() {
		case componentpb.EventType_EVENT_ADDED:
			r.publish(watch.Added, obj)
		case componentpb.EventType_EVENT_MODIFIED:
			r.publish(watch.Modified, obj)
		case componentpb.EventType_EVENT_DELETED:
			r.publish(watch.Deleted, obj)
		}
	}
}

func (r *REST) runMetastoreWatch(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		err := r.metastoreWatchOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			klog.Warningf("middleware: metastore watch disconnected: %v; retrying in 2s", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (r *REST) metastoreWatchOnce(ctx context.Context) error {
	w, err := r.store.Watch(ctx, "")
	if err != nil {
		return err
	}
	klog.Infof("middleware: metastore watch opened")
	defer w.Stop()
	for ev := range w.ResultChan() {
		u, ok := ev.Object.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		ref := metastore.RefFromUnstructured(u)
		if ref.Group != r.desc.GroupResource.Group || ref.Resource != r.desc.GroupResource.Resource {
			continue
		}
		bizResp, err := r.client.Get(ctx, &componentpb.GetRequest{
			User:      nil,
			Namespace: ref.Namespace,
			Name:      ref.Name,
		})
		if err != nil {
			// Backend object vanished under us; skip.
			continue
		}
		rec, _ := r.store.Get(ctx, ref)
		obj, serr := r.stitch(bizResp.GetObjectJson(), rec, ref.Namespace, ref.Name)
		if serr != nil {
			klog.Warningf("middleware: stitch failed on metastore event: %v", serr)
			continue
		}
		switch ev.Type {
		case watch.Added:
			r.publish(watch.Added, obj)
		case watch.Modified:
			r.publish(watch.Modified, obj)
		case watch.Deleted:
			// Metastore record removed; republish as MODIFIED so
			// clients refresh their view (the backend object may
			// still exist, but with synthesized fresh metadata).
			r.publish(watch.Modified, obj)
		}
	}
	return fmt.Errorf("metastore watch channel closed")
}

func (r *REST) publish(t watch.EventType, obj runtime.Object) {
	_ = r.bcaster.Action(t, obj)
}

// ---- stitch ----

// stitch takes a business JSON payload and a metastore Record and
// returns a *componentscheme.Object with metadata overlaid.
// If rec is nil, synthesizes in-memory defaults (fresh UID + RV)
// without writing.
func (r *REST) stitch(bizJSON []byte, rec *metastore.Record, ns, name string) (*componentscheme.Object, error) {
	obj := &componentscheme.Object{}
	if err := obj.UnmarshalJSON(bizJSON); err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("decode business: %w", err))
	}
	gvk := r.desc.GroupVersion.WithKind(r.desc.Kind)
	obj.GetObjectKind().SetGroupVersionKind(gvk)
	// The backend's metadata.name wins; backfill if empty.
	if obj.Name == "" {
		obj.Name = name
	}
	if r.desc.Namespaced && obj.Namespace == "" {
		obj.Namespace = ns
	}
	if !r.desc.Namespaced {
		// Force-clear namespace: the caller's URL may have carried
		// one through a dynamic client quirk, but cluster-scoped
		// resources should never have one in the response.
		obj.Namespace = ""
	}
	if rec == nil {
		// Synthesize minimal metadata without persistence.
		if obj.UID == "" {
			obj.UID = types.UID("synthetic-" + uuid.NewString())
		}
		if obj.ResourceVersion == "" {
			obj.ResourceVersion = strconv.FormatUint(r.rv.Load(), 10)
		}
		if obj.CreationTimestamp.IsZero() {
			// If the backend ships one, keep it; else now.
			obj.CreationTimestamp = metav1.NewTime(time.Now().UTC())
		}
		return obj, nil
	}
	// Overlay from Record.
	obj.UID = types.UID(rec.UID)
	obj.ResourceVersion = rec.RecordResourceVersion
	if !rec.CreationTimestamp.IsZero() {
		obj.CreationTimestamp = rec.CreationTimestamp
	}
	obj.DeletionTimestamp = rec.DeletionTimestamp
	obj.Labels = mapCopy(rec.Labels)
	obj.Annotations = mapCopy(rec.Annotations)
	obj.Finalizers = append([]string(nil), rec.Finalizers...)
	if len(rec.ManagedFields) > 0 {
		var mf []metav1.ManagedFieldsEntry
		if err := json.Unmarshal(rec.ManagedFields, &mf); err == nil {
			obj.ManagedFields = mf
		}
	} else {
		obj.ManagedFields = nil
	}
	if len(rec.OwnerReferences) > 0 {
		var or []metav1.OwnerReference
		if err := json.Unmarshal(rec.OwnerReferences, &or); err == nil {
			obj.OwnerReferences = or
		}
	}
	return obj, nil
}

// recordFromLibraryObject extracts a metastore Record from an
// object emitted by the library's patch / update machinery. The
// library has populated ObjectMeta including managedFields.
func recordFromLibraryObject(obj runtime.Object, ref metastore.ResourceRef) *metastore.Record {
	acc, err := meta.Accessor(obj)
	if err != nil {
		return &metastore.Record{Ref: ref}
	}
	rec := &metastore.Record{
		Ref:               ref,
		UID:               string(acc.GetUID()),
		CreationTimestamp: acc.GetCreationTimestamp(),
		Labels:            mapCopy(acc.GetLabels()),
		Annotations:       mapCopy(acc.GetAnnotations()),
		Finalizers:        append([]string(nil), acc.GetFinalizers()...),
	}
	if dt := acc.GetDeletionTimestamp(); dt != nil && !dt.IsZero() {
		rec.DeletionTimestamp = dt
	}
	if mf := acc.GetManagedFields(); len(mf) > 0 {
		raw, err := json.Marshal(mf)
		if err == nil {
			rec.ManagedFields = raw
		}
	}
	if or := acc.GetOwnerReferences(); len(or) > 0 {
		raw, err := json.Marshal(or)
		if err == nil {
			rec.OwnerReferences = raw
		}
	}
	return rec
}

// businessJSON renders only the backend-relevant portion: apiVersion,
// kind, metadata.name (and namespace), spec, status. Everything
// else is stripped.
func businessJSON(obj runtime.Object) ([]byte, error) {
	var bizMeta struct {
		APIVersion string         `json:"apiVersion,omitempty"`
		Kind       string         `json:"kind,omitempty"`
		Metadata   map[string]any `json:"metadata"`
		Spec       any            `json:"spec,omitempty"`
		Status     any            `json:"status,omitempty"`
	}
	// Round-trip through JSON to avoid caring about the source type.
	raw, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("marshal obj: %w", err)
	}
	var full map[string]any
	if err := json.Unmarshal(raw, &full); err != nil {
		return nil, fmt.Errorf("unmarshal obj: %w", err)
	}
	bizMeta.APIVersion, _ = full["apiVersion"].(string)
	bizMeta.Kind, _ = full["kind"].(string)
	md, _ := full["metadata"].(map[string]any)
	slim := map[string]any{}
	if md != nil {
		if v, ok := md["name"]; ok {
			slim["name"] = v
		}
		if v, ok := md["namespace"]; ok && v != "" {
			slim["namespace"] = v
		}
	}
	bizMeta.Metadata = slim
	bizMeta.Spec = full["spec"]
	bizMeta.Status = full["status"]
	return json.Marshal(bizMeta)
}

// ---- helpers ----

func (r *REST) refFor(ns, name string) metastore.ResourceRef {
	return metastore.ResourceRef{
		Group:     r.desc.GroupResource.Group,
		Resource:  r.desc.GroupResource.Resource,
		Namespace: ns,
		Name:      name,
	}
}

func (r *REST) translateErr(err error, name string) error {
	st, ok := grpcstatus.FromError(err)
	if !ok {
		return apierrors.NewInternalError(err)
	}
	switch st.Code() {
	case codes.NotFound:
		return apierrors.NewNotFound(r.desc.GroupResource, name)
	case codes.AlreadyExists:
		return apierrors.NewAlreadyExists(r.desc.GroupResource, name)
	case codes.InvalidArgument:
		return apierrors.NewBadRequest(st.Message())
	case codes.PermissionDenied:
		return apierrors.NewForbidden(r.desc.GroupResource, name, errors.New(st.Message()))
	case codes.Unavailable:
		return apierrors.NewServiceUnavailable(st.Message())
	default:
		return apierrors.NewInternalError(fmt.Errorf("backend: %s", st.Message()))
	}
}

func userFromCtx(ctx context.Context) *componentpb.UserInfo {
	v, ok := genericapirequest.UserFrom(ctx)
	if !ok || v == nil {
		return nil
	}
	return userToProto(v)
}

func userToProto(u user.Info) *componentpb.UserInfo {
	out := &componentpb.UserInfo{
		Name:   u.GetName(),
		Uid:    u.GetUID(),
		Groups: u.GetGroups(),
		Extra:  map[string]*componentpb.StringList{},
	}
	for k, v := range u.GetExtra() {
		out.Extra[k] = &componentpb.StringList{Values: v}
	}
	return out
}

func selectorString(opts *metainternalversion.ListOptions) string {
	if opts == nil || opts.LabelSelector == nil || opts.LabelSelector.Empty() {
		return ""
	}
	return opts.LabelSelector.String()
}

func selectorFromOpts(opts *metainternalversion.ListOptions) labels.Selector {
	if opts == nil || opts.LabelSelector == nil {
		return labels.Everything()
	}
	return opts.LabelSelector
}

func (r *REST) matchesLabels(obj runtime.Object, opts *metainternalversion.ListOptions) bool {
	sel := selectorFromOpts(opts)
	if sel.Empty() {
		return true
	}
	acc, err := meta.Accessor(obj)
	if err != nil {
		return true
	}
	return sel.Matches(labels.Set(acc.GetLabels()))
}

func updateFM(o *metav1.UpdateOptions) string {
	if o == nil {
		return ""
	}
	return o.FieldManager
}

func createFM(o *metav1.CreateOptions) string {
	if o == nil {
		return ""
	}
	return o.FieldManager
}

func mapCopy(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func nameNamespaceFromJSON(raw []byte) (string, string) {
	var m struct {
		Metadata struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return "", ""
	}
	return m.Metadata.Name, m.Metadata.Namespace
}

func lookupField(obj map[string]any, path string) any {
	if path == "" {
		return nil
	}
	parts := strings.Split(strings.TrimPrefix(path, "."), ".")
	var cur any = obj
	for _, p := range parts {
		m, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur = m[p]
	}
	if cur == nil {
		return ""
	}
	return cur
}

func ageOf(ts string) string {
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return "<unknown>"
	}
	d := time.Since(t).Round(time.Second)
	return durationShort(d)
}

func durationShort(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

func refLog(r metastore.ResourceRef) string {
	ns := r.Namespace
	if ns == "" {
		ns = "cluster"
	}
	return fmt.Sprintf("%s/%s/%s/%s", r.Group, r.Resource, ns, r.Name)
}

// Compile-time interface assertions.
var (
	_ rest.Storage              = (*REST)(nil)
	_ rest.Scoper               = (*REST)(nil)
	_ rest.KindProvider         = (*REST)(nil)
	_ rest.SingularNameProvider = (*REST)(nil)
	_ rest.Getter               = (*REST)(nil)
	_ rest.Lister               = (*REST)(nil)
	_ rest.Watcher              = (*REST)(nil)
	_ rest.TableConvertor       = (*REST)(nil)
	_ rest.Creater              = (*REST)(nil)
	_ rest.Updater              = (*REST)(nil)
	_ rest.Patcher              = (*REST)(nil)
	_ rest.GracefulDeleter      = (*REST)(nil)
)
