// Package widgetrest is experiment 0044's PER-WATCHER, identity-aware
// rest.Storage for aggexp.io/v1 Widget. It builds on the 0042 stitched
// host-RV core and INVERTS the single-global watch:
//
//   - The business body (spec + status + owner) comes from a shared,
//     cross-replica body CRD backend (pkg/backend) that is RV-blind
//     and owner-filtered.
//   - The KRM metadata + the authoritative resourceVersion come from a
//     cluster-scoped metadata CR on the host cluster (pkg/metastore).
//   - Every served object's metadata.resourceVersion is the host etcd
//     RV of its metadata CR.
//   - Each client Watch subscription gets its OWN per-watcher pipeline
//     (pkg/watch.Hub) carrying that caller's user.Info and its own
//     backend access (push: Backend.WatchFor; poll: Backend.ListFor).
//     The shared metadata informer is the RV authority + cross-replica
//     trigger; each metadata event drives a per-watcher Backend.GetFor
//     (the watcher's identity), deduped within the fan-out by
//     (identity, ns, name).
//   - SharedPoll mode recovers the single-global-watch cost at the
//     price of per-user authz.
//
// Code is duplicated from 0042/0034/0024 per the lab ethos.
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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	apiwatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/klog/v2"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0044-per-watcher-watch-identity/pkg/apis/aggexp"
	aggexpv1 "github.com/cheeseandcereal/aggexp/experiments/0044-per-watcher-watch-identity/pkg/apis/aggexp/v1"
	"github.com/cheeseandcereal/aggexp/experiments/0044-per-watcher-watch-identity/pkg/backend"
	"github.com/cheeseandcereal/aggexp/experiments/0044-per-watcher-watch-identity/pkg/metastore"
	perwatch "github.com/cheeseandcereal/aggexp/experiments/0044-per-watcher-watch-identity/pkg/watch"
)

const (
	servedGroup    = aggexpv1.GroupName
	servedResource = "widgets"
	fieldManager   = "aggexp-widgets"
)

var groupResource = schema.GroupResource{Group: servedGroup, Resource: servedResource}

// REST is the per-watcher, identity-aware stitched host-RV adapter.
type REST struct {
	store     *metastore.Store
	bodies    *backend.Store
	replicaID string
	hub       *perwatch.Hub
	mode      perwatch.Mode

	mu    sync.RWMutex
	curRV string // last observed metadata-CR RV (host etcd authority)
}

// New constructs a REST plus its per-watcher Hub.
func New(store *metastore.Store, bodies *backend.Store, replicaID string, mode perwatch.Mode, sharedPoll bool, pollInterval time.Duration, bufferSize int) *REST {
	r := &REST{
		store:     store,
		bodies:    bodies,
		replicaID: replicaID,
		mode:      mode,
	}
	r.hub = perwatch.NewHub(perwatch.HubOptions{
		Backend:      bodies,
		Stitcher:     r,
		SharedPoll:   sharedPoll,
		PollInterval: pollInterval,
		BufferSize:   bufferSize,
	})
	return r
}

// Hub exposes the per-watcher hub (for the shared-poll loop start and
// instrumentation logging).
func (r *REST) Hub() *perwatch.Hub { return r.hub }

// Shutdown is a no-op (per-watcher pipelines stop with their request
// contexts); kept for symmetry with the 0042 adapter.
func (r *REST) Shutdown() {}

// ---- metastore.RawSink ----

// OnMetadataEvent forwards a metadata-CR informer event to the hub's
// per-watcher fan-out.
func (r *REST) OnMetadataEvent(et apiwatch.EventType, ref metastore.ResourceRef, rv string) {
	r.mu.Lock()
	if rvLess(r.curRV, rv) {
		r.curRV = rv
	}
	r.mu.Unlock()
	r.hub.OnMetadataEvent(et, ref, rv)
}

func (r *REST) CurrentResourceVersion() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.curRV
}

// ---- perwatch.Stitcher ----

// StitchFor implements perwatch.Stitcher: fetch the body via the
// backend using the WATCHER's identity (Backend.GetFor — applies
// per-user authz), overlay the metadata CR's RV. Returns (nil,false)
// if the caller may not see the object or it is absent.
func (r *REST) StitchFor(u user.Info, ns, name string) (runtime.Object, bool) {
	body, ok := r.bodies.GetFor(u, ns, name)
	if !ok {
		return nil, false
	}
	ref := r.refFor(ns, name)
	rec, _ := r.store.GetFromCache(ref)
	if rec == nil {
		rec, _ = r.store.GetDirect(context.Background(), ref)
	}
	return r.stitch(ns, name, body, rec), true
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
	u, _ := genericapirequest.UserFrom(ctx)
	// Identity-aware read: a caller may only Get a Widget it owns
	// (system identities see all). This makes per-user authz
	// observable on the unary path too.
	body, ok := r.bodies.GetFor(u, ns, name)
	if !ok {
		// Cache may lag a very recent write; fall back to a direct
		// owner-checked read.
		raw, present := r.bodies.GetDirect(ctx, ns, name)
		if !present || !maySee(u, raw) {
			return nil, apierrors.NewNotFound(groupResource, name)
		}
		body = raw
	}
	ref := r.refFor(ns, name)
	rec, err := r.store.GetFromCache(ref)
	if err != nil {
		klog.Warningf("metastore cache get failed ref=%s: %v", refLog(ref), err)
	}
	if rec == nil {
		rec, _ = r.store.GetDirect(ctx, ref)
	}
	return r.stitch(ns, name, body, rec), nil
}

// ---- Lister ----

func (r *REST) List(ctx context.Context, opts *metainternalversion.ListOptions) (runtime.Object, error) {
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	u, _ := genericapirequest.UserFrom(ctx)
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
	// Identity-aware List: ListFor returns only bodies the caller may
	// see (owner match or system identity).
	for _, br := range r.bodies.ListFor(u, ns) {
		rec := byKey[br.Namespace+"/"+br.Name]
		obj := r.stitch(br.Namespace, br.Name, br.Body, rec)
		if !sel.Empty() && !sel.Matches(labels.Set(obj.Labels)) {
			continue
		}
		list.Items = append(list.Items, *obj)
	}

	listRV := r.CurrentResourceVersion()
	if rvLess(listRV, maxRV) {
		listRV = maxRV
	}
	list.ResourceVersion = listRV
	return list, nil
}

// ---- Watcher (per-watcher inversion) ----

func (r *REST) Watch(ctx context.Context, opts *metainternalversion.ListOptions) (apiwatch.Interface, error) {
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	u, _ := genericapirequest.UserFrom(ctx)
	sel := selectorFrom(opts)

	requested := ""
	if opts != nil {
		requested = opts.ResourceVersion
	}
	if requested != "" && requested != "0" {
		// 0034 contract: tolerate any host RV; replay current state.
		klog.V(3).InfoS("watch-resume", "replica", r.replicaID, "requestedRV", requested, "user", userName(u))
	}

	// Initial replay: owner-filtered, RV-stamped current state. This
	// is the per-watcher Backend.ListFor read (poll-mode initial; push
	// mode also replays before live events).
	initial := r.initialReplay(u, ns, sel)

	w := r.hub.NewWatch(ctx, u, ns, sel, r.mode, initial)
	return w, nil
}

// initialReplay builds the owner-filtered ADDED prefix for a new
// per-watcher subscription.
func (r *REST) initialReplay(u user.Info, ns string, sel labels.Selector) []runtime.Object {
	records, _, _ := r.store.ListFromCache()
	byKey := map[string]*metastore.Record{}
	for _, rec := range records {
		byKey[rec.Ref.Namespace+"/"+rec.Ref.Name] = rec
	}
	out := []runtime.Object{}
	// In SharedPoll mode the replay is NOT owner-filtered (no per-user
	// authz); in per-watcher mode it is.
	var refs []backend.Ref
	if r.hub.SharedPoll() {
		refs = r.bodies.List(ns)
	} else {
		refs = r.bodies.ListFor(u, ns)
	}
	for _, br := range refs {
		rec := byKey[br.Namespace+"/"+br.Name]
		obj := r.stitch(br.Namespace, br.Name, br.Body, rec)
		if !sel.Empty() && !sel.Matches(labels.Set(obj.Labels)) {
			continue
		}
		out = append(out, obj)
	}
	return out
}

// ---- TableConvertor ----

func (r *REST) ConvertToTable(_ context.Context, object runtime.Object, _ runtime.Object) (*metav1.Table, error) {
	t := &metav1.Table{ColumnDefinitions: []metav1.TableColumnDefinition{
		{Name: "Name", Type: "string", Format: "name"},
		{Name: "Owner", Type: "string"},
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
			Cells:  []interface{}{w.Name, w.Spec.Owner, w.Spec.Color, int64(w.Spec.Size), w.Status.Phase, age},
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
	// Server-stamp the owner from the caller's user.Info. A
	// client-supplied owner is overwritten — identity is authoritative.
	if u, has := genericapirequest.UserFrom(ctx); has && u != nil {
		w.Spec.Owner = u.GetName()
	}
	if _, exists := r.bodies.GetDirect(ctx, w.Namespace, w.Name); exists {
		return nil, apierrors.NewAlreadyExists(groupResource, w.Name)
	}

	ref := r.refFor(w.Namespace, w.Name)

	if berr := r.bodies.Put(ctx, w.Namespace, w.Name, backend.BodyFromWidget(w)); berr != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("backend.Put: %w", berr))
	}

	rec := recordFromObject(w, ref)
	if rec.UID == "" {
		rec.UID = uuid.NewString()
	}
	if rec.CreationTimestamp.IsZero() {
		rec.CreationTimestamp = metav1.NewTime(time.Now().UTC())
	}
	storedRec, err := r.store.Put(ctx, rec)
	if err != nil {
		_ = r.bodies.Delete(ctx, w.Namespace, w.Name)
		return nil, apierrors.NewInternalError(fmt.Errorf("metastore.Put: %w", err))
	}

	stitched := r.stitch(w.Namespace, w.Name, backend.BodyFromWidget(w), storedRec)
	// The metadata-CR informer fires the per-watcher fan-out with the
	// host-RV-stamped object; do not publish here.
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

	// Preserve the original owner on update (identity stamped at
	// Create is authoritative; an Update keeps it).
	if cur, isCur := current.(*aggexp.Widget); isCur && cur.Spec.Owner != "" {
		w.Spec.Owner = cur.Spec.Owner
	}

	ref := r.refFor(w.Namespace, name)
	rec := recordFromObject(w, ref)
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

	if berr := r.bodies.Put(ctx, w.Namespace, name, backend.BodyFromWidget(w)); berr != nil {
		return nil, false, apierrors.NewInternalError(fmt.Errorf("backend.Put: %w", berr))
	}
	storedRec, perr := r.store.Put(ctx, rec)
	if perr != nil {
		return nil, false, apierrors.NewInternalError(fmt.Errorf("metastore.Put: %w", perr))
	}

	stitched := r.stitch(w.Namespace, name, backend.BodyFromWidget(w), storedRec)
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
	if derr := r.store.Delete(ctx, ref); derr != nil {
		return nil, false, apierrors.NewInternalError(derr)
	}
	if berr := r.bodies.Delete(ctx, ns, name); berr != nil {
		klog.Warningf("backend.Delete failed ns=%s name=%s: %v", ns, name, berr)
	}
	// The metadata-CR DELETE informer event drives the per-watcher
	// fan-out (owner-filtered). No direct publish.
	return prior, true, nil
}

// ---- stitch ----

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
	w.ResourceVersion = rec.RecordRV // host etcd RV authority.
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

func maySee(u user.Info, b backend.Body) bool {
	if u == nil {
		return true
	}
	for _, g := range u.GetGroups() {
		if g == "system:masters" {
			return true
		}
	}
	n := u.GetName()
	if n == "system:kube-aggregator" || n == "system:apiserver" {
		return true
	}
	if len(n) >= len("system:serviceaccount:") && n[:len("system:serviceaccount:")] == "system:serviceaccount:" {
		return true
	}
	return b.Owner == n
}

func userName(u user.Info) string {
	if u == nil {
		return "<nil>"
	}
	return u.GetName()
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
	_ metastore.RawSink         = (*REST)(nil)
	_ perwatch.Stitcher         = (*REST)(nil)
)
