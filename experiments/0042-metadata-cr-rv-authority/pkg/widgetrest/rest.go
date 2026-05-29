// Package widgetrest is experiment 0042's stitched, host-RV rest.Storage
// for aggexp.io/v1 Widget. It is the synthesis the experiment exists to
// test:
//
//   - The business body (spec + status) comes from an in-memory backend
//     (pkg/backend) that is RV-blind.
//   - The KRM metadata + the authoritative resourceVersion come from a
//     cluster-scoped metadata CR on the host cluster (pkg/metastore).
//   - Every served object's metadata.resourceVersion is the host etcd
//     RV of its metadata CR — NEVER a backend RV, NEVER a per-replica
//     counter.
//   - Watch events are driven by the metadata CRD informer and carry
//     the metadata CR's RV. List stamps ListMeta.resourceVersion from
//     the metastore's high-water RV.
//   - Unknown resume RV replays current list-state (the 0034 contract),
//     never 410.
//
// It is a parallel implementation to runtime/storage.REST and 0034's
// shared.REST, intentionally not a wrapper: the substrate adapter's
// per-replica RV stamping (atomic.Uint64) is exactly what 0042
// bypasses. Code is duplicated from 0024/0034 per the lab ethos.
package widgetrest

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/klog/v2"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0042-metadata-cr-rv-authority/pkg/apis/aggexp"
	aggexpv1 "github.com/cheeseandcereal/aggexp/experiments/0042-metadata-cr-rv-authority/pkg/apis/aggexp/v1"
	"github.com/cheeseandcereal/aggexp/experiments/0042-metadata-cr-rv-authority/pkg/backend"
	"github.com/cheeseandcereal/aggexp/experiments/0042-metadata-cr-rv-authority/pkg/metastore"
)

const (
	servedGroup    = aggexpv1.GroupName
	servedResource = "widgets"
	fieldManager   = "aggexp-widgets"
)

var groupResource = schema.GroupResource{Group: servedGroup, Resource: servedResource}

// REST is the stitched host-RV adapter.
type REST struct {
	store     *metastore.Store
	bodies    *backend.InMem
	replicaID string

	bcaster *watch.Broadcaster

	mu    sync.RWMutex
	curRV string // last observed metadata-CR RV (host etcd authority)
}

// New constructs a REST. broadcasterSize defaults to 100.
func New(store *metastore.Store, bodies *backend.InMem, replicaID string, broadcasterSize int) *REST {
	if broadcasterSize <= 0 {
		broadcasterSize = 100
	}
	return &REST{
		store:     store,
		bodies:    bodies,
		replicaID: replicaID,
		bcaster:   watch.NewBroadcaster(broadcasterSize, watch.DropIfChannelFull),
	}
}

// Shutdown stops the broadcaster.
func (r *REST) Shutdown() { r.bcaster.Shutdown() }

// ---- metastore.EventSink ----

func (r *REST) Action(et watch.EventType, obj runtime.Object) {
	if err := r.bcaster.Action(et, obj); err != nil {
		klog.V(2).InfoS("broadcaster-action-failed", "err", err)
	}
}

func (r *REST) CurrentResourceVersion() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.curRV
}

func (r *REST) SetCurrentResourceVersion(rv string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rvLess(r.curRV, rv) {
		r.curRV = rv
	}
}

// ---- metastore.Stitcher ----

// StitchForRef is called by the informer event path. It fetches the
// body from the in-memory backend and overlays the Record's metadata
// (including the host RV). Returns (nil, false) if the body is absent.
func (r *REST) StitchForRef(ref metastore.ResourceRef, rec *metastore.Record) (runtime.Object, bool) {
	body, ok := r.bodies.Get(ref.Namespace, ref.Name)
	if !ok {
		return nil, false
	}
	return r.stitch(ref.Namespace, ref.Name, body, rec), true
}

// ---- identity / shape ----

func (r *REST) New() runtime.Object     { return &aggexp.Widget{} }
func (r *REST) NewList() runtime.Object { return &aggexp.WidgetList{} }
func (r *REST) Destroy()                {}
func (r *REST) NamespaceScoped() bool   { return true }
func (r *REST) Kind() string            { return "Widget" }
func (r *REST) GetSingularName() string { return "widget" }

// ---- Getter ----

func (r *REST) Get(ctx context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	body, ok := r.bodies.Get(ns, name)
	if !ok {
		return nil, apierrors.NewNotFound(groupResource, name)
	}
	ref := r.refFor(ns, name)
	rec, err := r.store.GetFromCache(ref)
	if err != nil {
		klog.Warningf("metastore cache get failed ref=%s: %v", refLog(ref), err)
	}
	return r.stitch(ns, name, body, rec), nil
}

// ---- Lister ----

func (r *REST) List(ctx context.Context, opts *metainternalversion.ListOptions) (runtime.Object, error) {
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	sel := selectorFrom(opts)

	records, maxRV, err := r.store.ListFromCache()
	if err != nil {
		klog.Warningf("metastore list failed (proceeding with synthesized metadata): %v", err)
	}
	byKey := map[string]*metastore.Record{}
	for _, rec := range records {
		byKey[rec.Ref.Namespace+"/"+rec.Ref.Name] = rec
	}

	list := &aggexp.WidgetList{}
	for _, br := range r.bodies.List(ns) {
		rec := byKey[br.Namespace+"/"+br.Name]
		obj := r.stitch(br.Namespace, br.Name, br.Body, rec)
		if !sel.Empty() && !sel.Matches(labels.Set(obj.Labels)) {
			continue
		}
		list.Items = append(list.Items, *obj)
	}

	// Stamp the list RV from the metastore high-water mark — the host
	// etcd RV authority. Prefer the informer-observed high-water mark
	// (curRV), fall back to the max record RV from this list.
	listRV := r.CurrentResourceVersion()
	if rvLess(listRV, maxRV) {
		listRV = maxRV
	}
	list.ResourceVersion = listRV
	return list, nil
}

// ---- Watcher ----

func (r *REST) Watch(ctx context.Context, opts *metainternalversion.ListOptions) (watch.Interface, error) {
	requested := ""
	if opts != nil {
		requested = opts.ResourceVersion
	}
	r.mu.RLock()
	cur := r.curRV
	r.mu.RUnlock()
	if requested != "" && requested != "0" {
		// 0034 contract: tolerate any host RV. We keep no event log,
		// so a resume replays current list-state as ADDED prefix
		// events. Never 410; never silently miss events. This is what
		// makes cross-replica resume-by-RV work — replica B can honor
		// an RV minted while the client was talking to replica A
		// because both observe the same etcd RV stream.
		klog.V(3).InfoS("watch-resume", "replica", r.replicaID, "requestedRV", requested, "currentRV", cur)
	}

	initial, err := r.List(ctx, opts)
	if err != nil {
		return nil, err
	}
	list := initial.(*aggexp.WidgetList)
	prefix := make([]watch.Event, 0, len(list.Items))
	for i := range list.Items {
		prefix = append(prefix, watch.Event{Type: watch.Added, Object: &list.Items[i]})
	}

	w, err := r.bcaster.WatchWithPrefix(prefix)
	if err != nil {
		return nil, err
	}

	ns, _ := genericapirequest.NamespaceFrom(ctx)
	sel := selectorFrom(opts)
	if sel.Empty() && ns == "" {
		return w, nil
	}
	return watch.Filter(w, func(ev watch.Event) (watch.Event, bool) {
		acc, aerr := meta.Accessor(ev.Object)
		if aerr != nil {
			return ev, true
		}
		if ns != "" && acc.GetNamespace() != ns {
			return ev, false
		}
		if !sel.Empty() && !sel.Matches(labels.Set(acc.GetLabels())) {
			return ev, false
		}
		return ev, true
	}), nil
}

// ---- TableConvertor ----

func (r *REST) ConvertToTable(_ context.Context, object runtime.Object, _ runtime.Object) (*metav1.Table, error) {
	t := &metav1.Table{ColumnDefinitions: []metav1.TableColumnDefinition{
		{Name: "Name", Type: "string", Format: "name"},
		{Name: "Color", Type: "string"},
		{Name: "Size", Type: "integer"},
		{Name: "Phase", Type: "string"},
		{Name: "Age", Type: "string"},
	}}
	row := func(w *aggexp.Widget) metav1.TableRow {
		age := ""
		if !w.CreationTimestamp.IsZero() {
			age = time.Since(w.CreationTimestamp.Time).Round(time.Second).String()
		}
		return metav1.TableRow{
			Cells:  []interface{}{w.Name, w.Spec.Color, int64(w.Spec.Size), w.Status.Phase, age},
			Object: runtime.RawExtension{Object: w},
		}
	}
	switch v := object.(type) {
	case *aggexp.Widget:
		t.Rows = []metav1.TableRow{row(v)}
	case *aggexp.WidgetList:
		t.Rows = make([]metav1.TableRow, 0, len(v.Items))
		for i := range v.Items {
			t.Rows = append(t.Rows, row(&v.Items[i]))
		}
		t.ListMeta.ResourceVersion = v.ResourceVersion
	default:
		return nil, fmt.Errorf("ConvertToTable: unexpected type %T", object)
	}
	return t, nil
}

// ---- Create ----

func (r *REST) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, _ *metav1.CreateOptions) (runtime.Object, error) {
	if createValidation != nil {
		if err := createValidation(ctx, obj); err != nil {
			return nil, err
		}
	}
	w, ok := obj.(*aggexp.Widget)
	if !ok {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("expected Widget, got %T", obj))
	}
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	if w.Namespace == "" {
		w.Namespace = ns
	}
	if w.Name == "" {
		return nil, apierrors.NewBadRequest("metadata.name is required")
	}
	if _, exists := r.bodies.Get(w.Namespace, w.Name); exists {
		return nil, apierrors.NewAlreadyExists(groupResource, w.Name)
	}

	ref := r.refFor(w.Namespace, w.Name)

	// Step 1: persist the metadata CR first so we obtain its host RV.
	rec := recordFromObject(w, ref)
	if rec.UID == "" {
		rec.UID = uuid.NewString()
	}
	if rec.CreationTimestamp.IsZero() {
		rec.CreationTimestamp = metav1.NewTime(time.Now().UTC())
	}
	storedRec, err := r.store.Put(ctx, rec)
	if err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("metastore.Put: %w", err))
	}

	// Step 2: store the body in the RV-blind backend.
	r.bodies.Put(w.Namespace, w.Name, backend.BodyFromWidget(w))

	stitched := r.stitch(w.Namespace, w.Name, backend.BodyFromWidget(w), storedRec)
	// Do NOT publish here: the metadata-CRD informer will fire the
	// broadcaster with the host-RV-stamped object, so the writer's
	// own watch sees the event the same way cross-replica watchers
	// do (0034 dedup contract).
	return stitched, nil
}

// ---- Update / Patch ----

func (r *REST) Update(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	_ *metav1.UpdateOptions,
) (runtime.Object, bool, error) {
	ns, _ := genericapirequest.NamespaceFrom(ctx)

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
	w, ok := updated.(*aggexp.Widget)
	if !ok {
		return nil, false, apierrors.NewBadRequest(fmt.Sprintf("expected Widget, got %T", updated))
	}
	if w.Namespace == "" {
		w.Namespace = ns
	}
	if w.Name == "" {
		w.Name = name
	}

	if current == nil {
		if !forceAllowCreate {
			return nil, false, apierrors.NewNotFound(groupResource, name)
		}
		if createValidation != nil {
			if err := createValidation(ctx, updated); err != nil {
				return nil, false, err
			}
		}
		created, cerr := r.Create(ctx, updated, nil, &metav1.CreateOptions{})
		if cerr != nil {
			return nil, false, cerr
		}
		return created, true, nil
	}
	if updateValidation != nil {
		if err := updateValidation(ctx, updated, current); err != nil {
			return nil, false, err
		}
	}

	ref := r.refFor(w.Namespace, name)
	rec := recordFromObject(w, ref)
	// Preserve server-managed fields from the prior Record.
	if prior, _ := r.store.GetFromCache(ref); prior != nil {
		if rec.UID == "" {
			rec.UID = prior.UID
		}
		if rec.CreationTimestamp.IsZero() {
			rec.CreationTimestamp = prior.CreationTimestamp
		}
		if prior.DeletionTimestamp != nil && rec.DeletionTimestamp == nil {
			rec.DeletionTimestamp = prior.DeletionTimestamp
		}
	}
	if rec.UID == "" {
		rec.UID = uuid.NewString()
	}
	if rec.CreationTimestamp.IsZero() {
		rec.CreationTimestamp = metav1.NewTime(time.Now().UTC())
	}

	storedRec, perr := r.store.Put(ctx, rec)
	if perr != nil {
		return nil, false, apierrors.NewInternalError(fmt.Errorf("metastore.Put: %w", perr))
	}
	r.bodies.Put(w.Namespace, name, backend.BodyFromWidget(w))

	stitched := r.stitch(w.Namespace, name, backend.BodyFromWidget(w), storedRec)
	// Informer publishes the MODIFIED event with host RV.
	return stitched, false, nil
}

// ---- Delete ----

func (r *REST) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, _ *metav1.DeleteOptions) (runtime.Object, bool, error) {
	ns, _ := genericapirequest.NamespaceFrom(ctx)
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
	// Delete the body first (so a racing StitchForRef sees it gone),
	// then the metadata CR (whose informer DELETE drives the watch
	// event). We publish a DELETED here as well as a safety net since
	// once the body is gone the informer's StitchForRef would return
	// not-present.
	r.bodies.Delete(ns, name)
	if derr := r.store.Delete(ctx, ref); derr != nil {
		return nil, false, apierrors.NewInternalError(derr)
	}
	r.Action(watch.Deleted, prior)
	return prior, true, nil
}

// ---- stitch ----

// stitch overlays a Record's metadata (and host RV) onto a body to
// produce the served Widget. If rec is nil, synthesizes in-memory
// defaults WITHOUT a real RV (uses the current informer high-water
// mark) — this only happens transiently before the metadata CR's
// informer event lands.
func (r *REST) stitch(namespace, name string, body backend.Body, rec *metastore.Record) *aggexp.Widget {
	w := &aggexp.Widget{}
	w.TypeMeta.Kind = "Widget"
	w.TypeMeta.APIVersion = servedGroup + "/v1"
	w.Name = name
	w.Namespace = namespace
	backend.ApplyBody(w, body)

	if rec == nil {
		w.UID = types.UID("synthetic-" + uuid.NewString())
		w.ResourceVersion = r.CurrentResourceVersion()
		w.CreationTimestamp = metav1.NewTime(time.Now().UTC())
		return w
	}
	w.UID = types.UID(rec.UID)
	w.ResourceVersion = rec.RecordRV // <-- THE load-bearing line.
	if !rec.CreationTimestamp.IsZero() {
		w.CreationTimestamp = rec.CreationTimestamp
	}
	w.DeletionTimestamp = rec.DeletionTimestamp
	w.Labels = mapCopy(rec.Labels)
	w.Annotations = mapCopy(rec.Annotations)
	w.Finalizers = append([]string(nil), rec.Finalizers...)
	if len(rec.ManagedFields) > 0 {
		var mf []metav1.ManagedFieldsEntry
		if err := json.Unmarshal(rec.ManagedFields, &mf); err == nil {
			w.ManagedFields = mf
		}
	}
	if len(rec.OwnerReferences) > 0 {
		var or []metav1.OwnerReference
		if err := json.Unmarshal(rec.OwnerReferences, &or); err == nil {
			w.OwnerReferences = or
		}
	}
	return w
}

// ---- helpers ----

func (r *REST) refFor(ns, name string) metastore.ResourceRef {
	return metastore.ResourceRef{Group: servedGroup, Resource: servedResource, Namespace: ns, Name: name}
}

func recordFromObject(w *aggexp.Widget, ref metastore.ResourceRef) *metastore.Record {
	rec := &metastore.Record{
		Ref:               ref,
		UID:               string(w.UID),
		CreationTimestamp: w.CreationTimestamp,
		Labels:            mapCopy(w.Labels),
		Annotations:       mapCopy(w.Annotations),
		Finalizers:        append([]string(nil), w.Finalizers...),
	}
	if dt := w.DeletionTimestamp; dt != nil && !dt.IsZero() {
		rec.DeletionTimestamp = dt
	}
	if mf := w.ManagedFields; len(mf) > 0 {
		if raw, err := json.Marshal(mf); err == nil {
			rec.ManagedFields = raw
		}
	}
	if or := w.OwnerReferences; len(or) > 0 {
		if raw, err := json.Marshal(or); err == nil {
			rec.OwnerReferences = raw
		}
	}
	return rec
}

func selectorFrom(opts *metainternalversion.ListOptions) labels.Selector {
	if opts == nil || opts.LabelSelector == nil {
		return labels.Everything()
	}
	return opts.LabelSelector
}

func rvLess(a, b string) bool {
	if a == "" {
		return b != ""
	}
	if b == "" {
		return false
	}
	an, aerr := strconv.ParseUint(a, 10, 64)
	bn, berr := strconv.ParseUint(b, 10, 64)
	if aerr != nil || berr != nil {
		return a < b
	}
	return an < bn
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

func refLog(r metastore.ResourceRef) string {
	ns := r.Namespace
	if ns == "" {
		ns = "cluster"
	}
	return fmt.Sprintf("%s/%s/%s/%s", r.Group, r.Resource, ns, r.Name)
}



// Compile-time assertions.
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
	_ metastore.EventSink       = (*REST)(nil)
	_ metastore.Stitcher        = (*REST)(nil)
)
