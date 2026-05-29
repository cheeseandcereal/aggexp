// Package widgetrest is experiment 0048's CAPSTONE rest.Storage for
// widgets.aggexp.io/v1 Widget. It composes every mechanism the
// multi-replica library arc validated in isolation onto ONE adapter:
//
//   - 0042 host-CR RV authority: the KRM metadata + authoritative
//     resourceVersion live on a cluster-scoped metadata CR; the
//     business body lives on a shared body CRD; every served object's
//     metadata.resourceVersion is the host etcd RV of its metadata CR.
//   - 0043 embedded lock + pre-acquire OCC: each write acquires a
//     CAS lock embedded on the metadata CR (acquire → put body →
//     commit-release in two CR writes), and the OCC check runs against
//     the PRE-acquire RV so lock churn never spuriously 409s.
//   - 0044 per-watcher, identity-aware watch: each Watch subscription
//     gets its own pipeline carrying the caller's identity; the
//     metadata informer is the RV authority + cross-replica trigger;
//     the re-homed 0043 emission filter (pkg/watch) keeps lock churn
//     out of every stream.
//   - 0045 read-path reconcile: Get/List treat the backend as the
//     source of truth for existence (GetAuthoritative/ListAuthoritative
//     direct host reads), adopting unknown backend objects and
//     collecting orphan records inline (subject to minAge). No
//     tolerant-Get.
//   - 0046 generated types: the served Widget is the oapigen-GENERATED
//     widgets.aggexp.io/v1.Widget, consumed verbatim.
//
// Code is duplicated from 0042/0043/0044/0045 per the lab ethos.
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
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	apiwatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/klog/v2"

	widgetsv1 "github.com/cheeseandcereal/aggexp/experiments/0048-library-multireplica-vertical-slice/pkg/apis/widgets/v1"
	"github.com/cheeseandcereal/aggexp/experiments/0048-library-multireplica-vertical-slice/pkg/backend"
	"github.com/cheeseandcereal/aggexp/experiments/0048-library-multireplica-vertical-slice/pkg/locking"
	"github.com/cheeseandcereal/aggexp/experiments/0048-library-multireplica-vertical-slice/pkg/metastore"
	"github.com/cheeseandcereal/aggexp/experiments/0048-library-multireplica-vertical-slice/pkg/metrics"
	perwatch "github.com/cheeseandcereal/aggexp/experiments/0048-library-multireplica-vertical-slice/pkg/watch"
)

const (
	servedGroup    = widgetsv1.GroupName
	servedResource = "widgets"
	fieldManager   = "aggexp-widgets"
)

var groupResource = schema.GroupResource{Group: servedGroup, Resource: servedResource}

// Policy is the runtime-toggleable read-path reconcile policy (0045).
type Policy struct {
	MinAgeSeconds float64 `json:"minAgeSeconds"`
	AdoptEnabled  bool    `json:"adoptEnabled"`
	GCEnabled     bool    `json:"gcEnabled"`
}

// REST is the capstone composed adapter.
type REST struct {
	store     *metastore.Store
	bodies    *backend.Store
	locker    *locking.Locker
	counters  *metrics.Counters
	replicaID string
	hub       *perwatch.Hub
	mode      perwatch.Mode

	mu    sync.RWMutex
	curRV string // last observed metadata-CR RV (host etcd authority)

	polMu        sync.RWMutex
	minAge       time.Duration
	adoptEnabled bool
	gcEnabled    bool
}

// Config configures a REST.
type Config struct {
	Store        *metastore.Store
	Bodies       *backend.Store
	Locker       *locking.Locker
	Counters     *metrics.Counters
	ReplicaID    string
	Mode         perwatch.Mode
	SharedPoll   bool
	PollInterval time.Duration
	BufferSize   int
	MinAge       time.Duration
	AdoptEnabled bool
	GCEnabled    bool
}

// New constructs a REST plus its per-watcher Hub.
func New(cfg Config) *REST {
	if cfg.MinAge <= 0 {
		cfg.MinAge = 30 * time.Second
	}
	r := &REST{
		store:        cfg.Store,
		bodies:       cfg.Bodies,
		locker:       cfg.Locker,
		counters:     cfg.Counters,
		replicaID:    cfg.ReplicaID,
		mode:         cfg.Mode,
		minAge:       cfg.MinAge,
		adoptEnabled: cfg.AdoptEnabled,
		gcEnabled:    cfg.GCEnabled,
	}
	if r.counters == nil {
		r.counters = &metrics.Counters{}
	}
	r.hub = perwatch.NewHub(perwatch.HubOptions{
		Backend:      cfg.Bodies,
		Stitcher:     r,
		SharedPoll:   cfg.SharedPoll,
		PollInterval: cfg.PollInterval,
		BufferSize:   cfg.BufferSize,
	})
	return r
}

// Hub exposes the per-watcher hub (shared-poll start + instrumentation).
func (r *REST) Hub() *perwatch.Hub { return r.hub }

// Shutdown is a no-op (per-watcher pipelines stop with their request
// contexts).
func (r *REST) Shutdown() {}

// ---- 0045 policy toggles ----

// GetPolicy returns the current read-path reconcile policy.
func (r *REST) GetPolicy() Policy {
	r.polMu.RLock()
	defer r.polMu.RUnlock()
	return Policy{MinAgeSeconds: r.minAge.Seconds(), AdoptEnabled: r.adoptEnabled, GCEnabled: r.gcEnabled}
}

// SetAdopt toggles adoption (inline + sweep).
func (r *REST) SetAdopt(b bool) { r.polMu.Lock(); r.adoptEnabled = b; r.polMu.Unlock() }

// SetGC toggles collection (inline + sweep).
func (r *REST) SetGC(b bool) { r.polMu.Lock(); r.gcEnabled = b; r.polMu.Unlock() }

func (r *REST) policy() (minAge time.Duration, adopt, gc bool) {
	r.polMu.RLock()
	defer r.polMu.RUnlock()
	return r.minAge, r.adoptEnabled, r.gcEnabled
}

// ---- metastore.RawSink ----

// OnMetadataEvent forwards a metadata-CR informer event to the hub's
// per-watcher fan-out. The hub applies the re-homed 0043 emission
// filter keyed on the record's VisibleSignature.
func (r *REST) OnMetadataEvent(et apiwatch.EventType, ref metastore.ResourceRef, rec *metastore.Record, rv string) {
	r.mu.Lock()
	if rvLess(r.curRV, rv) {
		r.curRV = rv
	}
	r.mu.Unlock()
	r.hub.OnMetadataEvent(et, ref, rec, rv)
}

func (r *REST) CurrentResourceVersion() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.curRV
}

// ---- perwatch.Stitcher ----

// StitchFor fetches the body via the backend using the WATCHER's
// identity (Backend.GetFor — applies per-user authz), overlays the
// metadata CR's RV. Returns (nil,false) if the caller may not see the
// object or it is absent.
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

// NewBookmark returns an empty served Widget (the 0046-generated type)
// for use as a BOOKMARK carrier (must be a scheme-registered served
// type, not PartialObjectMetadata, or the watch encoder closes the
// stream silently — FINDINGS/0044).
func (r *REST) NewBookmark() runtime.Object {
	w := &widgetsv1.Widget{}
	w.TypeMeta.Kind = "Widget"
	w.TypeMeta.APIVersion = servedGroup + "/v1"
	return w
}

// ---- identity / shape ----

func (r *REST) New() runtime.Object     { return &widgetsv1.Widget{} }
func (r *REST) NewList() runtime.Object { return &widgetsv1.WidgetList{} }
func (r *REST) Destroy()                {}
func (r *REST) NamespaceScoped() bool   { return true }
func (r *REST) Kind() string            { return "Widget" }
func (r *REST) GetSingularName() string { return "widget" }

// ---- Getter (0045 read-path reconcile + 0044 identity gate) ----

func (r *REST) Get(ctx context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	r.counters.ServedGet.Add(1)
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	u, _ := genericapirequest.UserFrom(ctx)
	ref := r.refFor(ns, name)

	// 0045: the backend is the SOURCE OF TRUTH for existence. Always
	// reach the host apiserver directly (no store-miss short-circuit).
	body, present := r.bodies.GetAuthoritative(ctx, ns, name)
	if !present {
		// Backend has no object. Collect any orphan record (no
		// tolerant-Get; finalizers do not block — 0045).
		rec, _ := r.store.GetFromCache(ref)
		if rec != nil {
			r.collect(ctx, ref, rec, false)
		}
		return nil, apierrors.NewNotFound(groupResource, name)
	}

	// 0044: identity gate. A caller may only Get a Widget it owns
	// (system identities see all).
	if !maySee(u, body) {
		return nil, apierrors.NewNotFound(groupResource, name)
	}

	rec, _ := r.store.GetFromCache(ref)
	if rec == nil {
		rec, _ = r.store.GetDirect(ctx, ref)
	}
	if rec == nil {
		// Backend object with no record: adopt (0045) if enabled.
		_, adoptOn, _ := r.policy()
		if !adoptOn {
			return nil, apierrors.NewNotFound(groupResource, name)
		}
		rec = r.adopt(ctx, ref, false)
	}
	return r.stitch(ns, name, body, rec), nil
}

// ---- Lister (0045 read-path reconcile + 0044 identity gate) ----

func (r *REST) List(ctx context.Context, opts *metainternalversion.ListOptions) (runtime.Object, error) {
	r.counters.ServedList.Add(1)
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	u, _ := genericapirequest.UserFrom(ctx)
	sel := selectorFrom(opts)

	// 0045: authoritative backend set + inline reconcile.
	backendRefs, recByKey := r.ReconcileList(ctx, ns, false)
	_, adoptOn, _ := r.policy()

	list := &widgetsv1.WidgetList{}
	for _, br := range backendRefs {
		key := br.Namespace + "/" + br.Name
		rec := recByKey[key]
		if rec == nil && !adoptOn {
			// Unadopted foreign backend object: suppress (0045).
			continue
		}
		// 0044: identity gate.
		if !maySee(u, br.Body) {
			continue
		}
		obj := r.stitch(br.Namespace, br.Name, br.Body, rec)
		if !sel.Empty() && !sel.Matches(labels.Set(obj.Labels)) {
			continue
		}
		list.Items = append(list.Items, *obj)
	}

	listRV := r.CurrentResourceVersion()
	if hw := r.store.HighWaterRV(); rvLess(listRV, hw) {
		listRV = hw
	}
	list.ResourceVersion = listRV
	return list, nil
}

// ReconcileList reads the authoritative backend set, adopts unknown
// backend objects and collects orphan records (subject to minAge),
// then returns the backend set + the surviving records by key. The
// sweep calls the SAME method (fromSweep=true), so inline and sweep
// agree by construction (0045).
func (r *REST) ReconcileList(ctx context.Context, ns string, fromSweep bool) ([]backend.Ref, map[string]*metastore.Record) {
	_, adoptOn, gcOn := r.policy()

	backendRefs, lerr := r.bodies.ListAuthoritative(ctx, ns)
	if lerr != nil {
		klog.Warningf("backend ListAuthoritative failed (falling back to cache): %v", lerr)
		backendRefs = r.bodies.List(ns)
	}
	backendKeys := map[string]struct{}{}
	for _, br := range backendRefs {
		backendKeys[br.Namespace+"/"+br.Name] = struct{}{}
	}

	records, _, err := r.store.ListFromCache()
	if err != nil {
		klog.Warningf("metastore list failed: %v", err)
	}
	recByKey := map[string]*metastore.Record{}
	for _, rec := range records {
		if ns != "" && rec.Ref.Namespace != ns {
			continue
		}
		recByKey[rec.Ref.Namespace+"/"+rec.Ref.Name] = rec
	}

	if adoptOn {
		for _, br := range backendRefs {
			key := br.Namespace + "/" + br.Name
			if recByKey[key] == nil {
				if rec := r.adopt(ctx, r.refFor(br.Namespace, br.Name), fromSweep); rec != nil {
					recByKey[key] = rec
				}
			}
		}
	}
	if gcOn {
		for key, rec := range recByKey {
			if _, present := backendKeys[key]; !present {
				if r.collect(ctx, rec.Ref, rec, fromSweep) {
					delete(recByKey, key)
				}
			}
		}
	}
	return backendRefs, recByKey
}

// adopt synthesizes and persists a metadata record for a backend
// object that has none (0045).
func (r *REST) adopt(ctx context.Context, ref metastore.ResourceRef, fromSweep bool) *metastore.Record {
	_, adoptOn, _ := r.policy()
	if !adoptOn {
		return nil
	}
	if ref.Namespace == "" {
		ref.Namespace = "default"
	}
	rec := &metastore.Record{
		Ref:               ref,
		UID:               uuid.NewString(),
		CreationTimestamp: metav1.NewTime(time.Now().UTC()),
		Annotations: map[string]string{
			"aggexp.io/adopted":    "true",
			"aggexp.io/adopted-by": adoptedBy(fromSweep),
		},
	}
	stored, err := r.store.Put(ctx, rec)
	if err != nil {
		klog.Warningf("adopt: metastore.Put failed ref=%s: %v", refLog(ref), err)
		return nil
	}
	if fromSweep {
		r.counters.AdoptSweep.Add(1)
	} else {
		r.counters.AdoptInline.Add(1)
	}
	klog.InfoS("adopt", "ref", refLog(ref), "from", adoptedBy(fromSweep), "rv", stored.RecordRV)
	return stored
}

// collect deletes an orphan record once the backend confirms absence —
// subject to the minAge grace window. Finalizers do NOT block (0045:
// no tolerant-Get, so the metadata store has no independent opinion
// about existence).
func (r *REST) collect(ctx context.Context, ref metastore.ResourceRef, rec *metastore.Record, fromSweep bool) bool {
	minAge, _, gcOn := r.policy()
	if !gcOn {
		return false
	}
	if rec != nil && !rec.CreationTimestamp.IsZero() {
		if age := time.Since(rec.CreationTimestamp.Time); age < minAge {
			r.counters.CollectSkippedAge.Add(1)
			klog.InfoS("collect-skip-age", "ref", refLog(ref), "age", age.Round(time.Millisecond).String(), "minAge", minAge.String())
			return false
		}
	}
	if err := r.store.Delete(ctx, ref); err != nil {
		klog.Warningf("collect: metastore.Delete failed ref=%s: %v", refLog(ref), err)
		return false
	}
	if fromSweep {
		r.counters.CollectSweep.Add(1)
	} else {
		r.counters.CollectInline.Add(1)
	}
	klog.InfoS("collect", "ref", refLog(ref), "from", adoptedBy(fromSweep), "finalizers", rec.Finalizers)
	return true
}

func adoptedBy(fromSweep bool) string {
	if fromSweep {
		return "sweep"
	}
	return "read"
}

// ---- Watcher (per-watcher inversion, 0044) ----

func (r *REST) Watch(ctx context.Context, opts *metainternalversion.ListOptions) (apiwatch.Interface, error) {
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	u, _ := genericapirequest.UserFrom(ctx)
	sel := selectorFrom(opts)

	requested := ""
	if opts != nil {
		requested = opts.ResourceVersion
	}
	if requested != "" && requested != "0" {
		klog.V(3).InfoS("watch-resume", "replica", r.replicaID, "requestedRV", requested, "user", userName(u))
	}

	initial := r.initialReplay(u, ns, sel)
	w := r.hub.NewWatch(ctx, u, ns, sel, r.mode, initial)
	return w, nil
}

func (r *REST) initialReplay(u user.Info, ns string, sel labels.Selector) []runtime.Object {
	records, _, _ := r.store.ListFromCache()
	byKey := map[string]*metastore.Record{}
	for _, rec := range records {
		byKey[rec.Ref.Namespace+"/"+rec.Ref.Name] = rec
	}
	out := []runtime.Object{}
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
		{Name: "Color", Type: "string"},
		{Name: "Size", Type: "integer"},
		{Name: "Phase", Type: "string"},
		{Name: "Age", Type: "string"},
	}}
	row := func(w *widgetsv1.Widget) metav1.TableRow {
		age := ""
		if !w.CreationTimestamp.IsZero() {
			age = time.Since(w.CreationTimestamp.Time).Round(time.Second).String()
		}
		return metav1.TableRow{
			Cells:  []interface{}{w.Name, string(w.Spec.Color), int64(w.Spec.Size), string(w.Status.Phase), age},
			Object: runtime.RawExtension{Object: w},
		}
	}
	switch v := object.(type) {
	case *widgetsv1.Widget:
		t.Rows = []metav1.TableRow{row(v)}
	case *widgetsv1.WidgetList:
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

// ---- Create (0043 lock) ----

func (r *REST) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, _ *metav1.CreateOptions) (runtime.Object, error) {
	if createValidation != nil {
		if err := createValidation(ctx, obj); err != nil {
			return nil, err
		}
	}
	w, ok := obj.(*widgetsv1.Widget)
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
	if _, exists := r.bodies.GetDirect(ctx, w.Namespace, w.Name); exists {
		return nil, apierrors.NewAlreadyExists(groupResource, w.Name)
	}

	// Owner is server-stamped from identity (0044). Carried only on the
	// body CR, not surfaced on the generated Widget.
	owner := ""
	if u, has := genericapirequest.UserFrom(ctx); has && u != nil {
		owner = u.GetName()
	}

	ref := r.refFor(w.Namespace, w.Name)

	// 0043: acquire the embedded lock BEFORE touching the body. On
	// Create the metadata CR does not exist, so Acquire CAS-creates it
	// carrying only the lock.
	h, lerr := r.locker.Acquire(ctx, ref)
	if lerr != nil {
		return nil, lerr // 409 on contention
	}

	body := backend.BodyFromWidget(w)
	body.Owner = owner
	if berr := r.bodies.Put(ctx, w.Namespace, w.Name, body); berr != nil {
		h.Release(ctx)
		return nil, apierrors.NewInternalError(fmt.Errorf("backend.Put: %w", berr))
	}

	rec := recordFromObject(w, ref)
	if rec.UID == "" {
		rec.UID = uuid.NewString()
	}
	if rec.CreationTimestamp.IsZero() {
		rec.CreationTimestamp = metav1.NewTime(time.Now().UTC())
	}
	rec.BodyHash = backend.HashBody(body)
	storedRec, err := h.Commit(ctx, rec)
	if err != nil {
		_ = r.bodies.Delete(ctx, w.Namespace, w.Name)
		return nil, apierrors.NewInternalError(fmt.Errorf("metastore.Commit: %w", err))
	}

	stitched := r.stitch(w.Namespace, w.Name, body, storedRec)
	return stitched, nil
}

// ---- Update / Patch (0043 pre-acquire OCC + lock) ----

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
	w, ok := updated.(*widgetsv1.Widget)
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

	// 0043 PRE-ACQUIRE OCC ORDERING. Capture the served object's RV
	// BEFORE acquiring the lock (acquire bumps the CR's RV).
	preRec, _ := r.store.GetDirect(ctx, ref)
	preRV := ""
	if preRec != nil {
		preRV = preRec.RecordRV
	}
	clientRV := w.ResourceVersion
	if clientRV != "" && preRV != "" && clientRV != preRV {
		return nil, false, apierrors.NewConflict(groupResource, name,
			fmt.Errorf("the object has been modified; please apply your changes to the latest version and try again"))
	}

	h, lerr := r.locker.Acquire(ctx, ref)
	if lerr != nil {
		return nil, false, lerr // 409 on contention
	}

	// Preserve the owner from the prior body (identity at Create is
	// authoritative; an Update keeps it).
	owner := ""
	if prior, ok := r.bodies.GetDirect(ctx, w.Namespace, name); ok {
		owner = prior.Owner
	}

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

	body := backend.BodyFromWidget(w)
	body.Owner = owner
	if berr := r.bodies.Put(ctx, w.Namespace, name, body); berr != nil {
		h.Release(ctx)
		return nil, false, apierrors.NewInternalError(fmt.Errorf("backend.Put: %w", berr))
	}
	rec.BodyHash = backend.HashBody(body)

	storedRec, perr := h.Commit(ctx, rec)
	if perr != nil {
		return nil, false, apierrors.NewInternalError(fmt.Errorf("metastore.Commit: %w", perr))
	}

	stitched := r.stitch(w.Namespace, name, body, storedRec)
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
	// Delete the metadata CR (releases the embedded lock atomically —
	// 0043) then the body.
	if derr := r.store.Delete(ctx, ref); derr != nil {
		return nil, false, apierrors.NewInternalError(derr)
	}
	if berr := r.bodies.Delete(ctx, ns, name); berr != nil {
		klog.Warningf("backend.Delete failed ns=%s name=%s: %v", ns, name, berr)
	}
	return prior, true, nil
}

// ---- stitch ----

func (r *REST) stitch(namespace, name string, body backend.Body, rec *metastore.Record) *widgetsv1.Widget {
	w := &widgetsv1.Widget{}
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

func recordFromObject(w *widgetsv1.Widget, ref metastore.ResourceRef) *metastore.Record {
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
	if len(n) >= len("system:node:") && n[:len("system:node:")] == "system:node:" {
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
