// Package widgetrest is experiment 0045's stitched, host-RV
// rest.Storage for aggexp.io/v1 Widget, with READ-PATH RECONCILE.
//
// It inherits the 0042 stitch (business body on a shared body CRD,
// KRM metadata + authoritative RV on a metadata CR) but changes the
// read semantics: the BACKEND IS THE SOURCE OF TRUTH FOR EXISTENCE,
// reconciled inline on every Get and List.
//
//	Get(ns,name):
//	   obj, ok := backend.GetAuthoritative(ns,name)  # always hits backend
//	   ├─ ok, record present → stitch + return
//	   ├─ ok, no record      → ADOPT (synthesize+persist record) → stitch
//	   └─ 404                 → COLLECT record (minAge guard) → 404
//	                            (NO tolerant-Get: a 404 is a 404 even
//	                             with finalizers present)
//
//	List(ns):
//	   backendObjs := backend.ListAuthoritative(ns)   # authoritative set
//	   reconcile: adopt (backendObjs − records),
//	              collect (records − backendObjs, minAge)
//	   then stitch the authoritative set.
//
// Adoption and GC are independently toggleable (both default on); the
// toggle applies to BOTH this inline path and the periodic sweep
// (pkg/sweep), which calls ReconcileList. There is no store-miss
// short-circuit 404 — every Get reaches the backend — which is the
// read amplification the experiment measures (pkg/metrics).
//
// Code is duplicated from 0042/0024/0028 per the lab ethos.
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

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0045-read-path-reconcile-amplification/pkg/apis/aggexp"
	aggexpv1 "github.com/cheeseandcereal/aggexp/experiments/0045-read-path-reconcile-amplification/pkg/apis/aggexp/v1"
	"github.com/cheeseandcereal/aggexp/experiments/0045-read-path-reconcile-amplification/pkg/backend"
	"github.com/cheeseandcereal/aggexp/experiments/0045-read-path-reconcile-amplification/pkg/metrics"
	"github.com/cheeseandcereal/aggexp/experiments/0045-read-path-reconcile-amplification/pkg/metastore"
)

const (
	servedGroup    = aggexpv1.GroupName
	servedResource = "widgets"
	fieldManager   = "aggexp-widgets"
)

var groupResource = schema.GroupResource{Group: servedGroup, Resource: servedResource}

// REST is the stitched host-RV adapter with read-path reconcile.
type REST struct {
	store     *metastore.Store
	bodies    *backend.Store
	replicaID string
	counters  *metrics.Counters

	bcaster *watch.Broadcaster

	mu    sync.RWMutex
	curRV string // last observed metadata-CR RV (host etcd authority)

	// Reconcile policy. minAge is the grace window before an orphan
	// record is collected. adoptEnabled / gcEnabled toggle adoption
	// and collection respectively; the toggles govern BOTH the inline
	// read path and the periodic sweep.
	polMu        sync.RWMutex
	minAge       time.Duration
	adoptEnabled bool
	gcEnabled    bool
}

// Config carries the reconcile policy into New.
type Config struct {
	Counters     *metrics.Counters
	MinAge       time.Duration
	AdoptEnabled bool
	GCEnabled    bool
}

// New constructs a REST. broadcasterSize defaults to 100.
func New(store *metastore.Store, bodies *backend.Store, replicaID string, broadcasterSize int, cfg Config) *REST {
	if broadcasterSize <= 0 {
		broadcasterSize = 100
	}
	if cfg.Counters == nil {
		cfg.Counters = &metrics.Counters{}
	}
	if cfg.MinAge <= 0 {
		cfg.MinAge = 30 * time.Second
	}
	return &REST{
		store:        store,
		bodies:       bodies,
		replicaID:    replicaID,
		counters:     cfg.Counters,
		bcaster:      watch.NewBroadcaster(broadcasterSize, watch.DropIfChannelFull),
		minAge:       cfg.MinAge,
		adoptEnabled: cfg.AdoptEnabled,
		gcEnabled:    cfg.GCEnabled,
	}
}

// Shutdown stops the broadcaster.
func (r *REST) Shutdown() { r.bcaster.Shutdown() }

// ---- reconcile policy accessors (debug endpoint flips these) ----

// Policy is a snapshot of the reconcile toggles.
type Policy struct {
	MinAgeSeconds float64 `json:"minAgeSeconds"`
	AdoptEnabled  bool    `json:"adoptEnabled"`
	GCEnabled     bool    `json:"gcEnabled"`
}

func (r *REST) GetPolicy() Policy {
	r.polMu.RLock()
	defer r.polMu.RUnlock()
	return Policy{MinAgeSeconds: r.minAge.Seconds(), AdoptEnabled: r.adoptEnabled, GCEnabled: r.gcEnabled}
}

func (r *REST) SetAdopt(b bool) { r.polMu.Lock(); r.adoptEnabled = b; r.polMu.Unlock() }
func (r *REST) SetGC(b bool)    { r.polMu.Lock(); r.gcEnabled = b; r.polMu.Unlock() }

func (r *REST) policy() (minAge time.Duration, adopt, gc bool) {
	r.polMu.RLock()
	defer r.polMu.RUnlock()
	return r.minAge, r.adoptEnabled, r.gcEnabled
}

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
// body from the backend cache and overlays the Record's metadata.
func (r *REST) StitchForRef(ref metastore.ResourceRef, rec *metastore.Record) (runtime.Object, bool) {
	body, ok := r.bodies.Get(ref.Namespace, ref.Name)
	if !ok {
		body, ok = r.bodies.GetDirect(context.Background(), ref.Namespace, ref.Name)
		if !ok {
			return nil, false
		}
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

// ---- Getter (read-path reconcile) ----

func (r *REST) Get(ctx context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	r.counters.ServedGet.Add(1)
	ns, _ := genericapirequest.NamespaceFrom(ctx)

	// THE load-bearing read: always query the backend. No store-miss
	// short-circuit; the backend is the source of truth for existence.
	body, ok := r.bodies.GetAuthoritative(ctx, ns, name)
	ref := r.refFor(ns, name)
	rec, err := r.store.GetFromCache(ref)
	if err != nil {
		klog.Warningf("metastore cache get failed ref=%s: %v", refLog(ref), err)
	}
	if rec == nil {
		rec, _ = r.store.GetDirect(ctx, ref)
	}

	if ok {
		if rec == nil {
			// Backend has it but there's no record. ADOPT
			// (synthesize+persist a record) if adoption is on;
			// otherwise the object is UNKNOWN and is NOT served — an
			// unadopted foreign backend object 404s (this is the
			// adoption-off path that suppresses shared-backend noise).
			_, adoptOn, _ := r.policy()
			if !adoptOn {
				return nil, apierrors.NewNotFound(groupResource, name)
			}
			rec = r.adopt(ctx, ref, false)
		}
		return r.stitch(ns, name, body, rec), nil
	}

	// Backend 404. NO tolerant-Get — even if a record (with
	// finalizers) exists, a backend 404 is a 404. Collect the orphan
	// record subject to minAge.
	if rec != nil {
		r.collect(ctx, ref, rec, false)
	}
	return nil, apierrors.NewNotFound(groupResource, name)
}

// ---- Lister (read-path reconcile) ----

func (r *REST) List(ctx context.Context, opts *metainternalversion.ListOptions) (runtime.Object, error) {
	r.counters.ServedList.Add(1)
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	sel := selectorFrom(opts)

	// Authoritative backend set + inline reconcile (adopt unknown,
	// collect orphans). Falls back to the cache view if the
	// authoritative list errors.
	backendRefs, recByKey := r.ReconcileList(ctx, ns, false)

	_, adoptOn, _ := r.policy()
	list := &aggexp.WidgetList{}
	for _, br := range backendRefs {
		rec := recByKey[br.Namespace+"/"+br.Name]
		if rec == nil && !adoptOn {
			// Adoption off: a backend object with no record is unknown
			// and is OMITTED from the list (suppresses shared-backend
			// noise). With adoption on, ReconcileList already adopted
			// it, so rec is non-nil here.
			continue
		}
		obj := r.stitch(br.Namespace, br.Name, br.Body, rec)
		if !sel.Empty() && !sel.Matches(labels.Set(obj.Labels)) {
			continue
		}
		list.Items = append(list.Items, *obj)
	}

	listRV := r.CurrentResourceVersion()
	if mrv := r.store.HighWaterRV(); rvLess(listRV, mrv) {
		listRV = mrv
	}
	list.ResourceVersion = listRV
	return list, nil
}

// ReconcileList performs the List-side reconcile: it reads the
// authoritative backend set, adopts unknown backend objects and
// collects orphan records (subject to minAge), then returns the
// authoritative set and a map of records keyed by ns/name (post-
// reconcile). fromSweep selects which counters to bump. Shared by the
// inline List path and the periodic sweep.
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

	// Adopt: backend objects with no record.
	if adoptOn {
		for _, br := range backendRefs {
			key := br.Namespace + "/" + br.Name
			if recByKey[key] == nil {
				rec := r.adopt(ctx, r.refFor(br.Namespace, br.Name), fromSweep)
				if rec != nil {
					recByKey[key] = rec
				}
			}
		}
	}

	// Collect: records whose backend object is gone (subject to minAge).
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
// object that has none. Returns the stored record (or nil on
// failure / adoption disabled). Adopted objects with no namespace
// land in `default` per the README decision.
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

// collect deletes an orphan record once the backend confirms its
// object is absent — subject to the minAge grace window. Returns true
// if the record was deleted. Finalizers do NOT block collection:
// there is no tolerant-Get path, so once the backend confirms absence
// the record goes (this is the 0028 sharp edge being removed). The
// minAge guard still protects the freshly-created race.
func (r *REST) collect(ctx context.Context, ref metastore.ResourceRef, rec *metastore.Record, fromSweep bool) bool {
	minAge, _, gcOn := r.policy()
	if !gcOn {
		return false
	}
	if rec != nil && !rec.CreationTimestamp.IsZero() {
		age := time.Since(rec.CreationTimestamp.Time)
		if age < minAge {
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
	r.Action(watch.Deleted, prior)
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
	w.ResourceVersion = rec.RecordRV
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
