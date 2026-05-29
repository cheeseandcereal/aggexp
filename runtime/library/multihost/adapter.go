package multihost

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
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
)

// REST is the multi-host enhanced rest.Storage adapter. It composes the
// seven capabilities the 0042-0049 arc validated onto one adapter:
// host-RV-authority metadata store, shared body store, embedded lock
// with the validated 0049 transactional write path, emission filtering,
// pre-acquire OCC, per-watcher identity-carrying watch, and (opt-in,
// default off) inline read-path reconcile.
//
// It is the multi-replica analogue of runtime/library.REST. The
// consumer supplies a Converter; the substrate does not try to be
// magically generic over the served type.
type REST struct {
	store     *MetaStore
	bodies    *BodyStore
	locker    *Locker
	hub       *Hub
	conv      Converter
	gate      IdentityGate
	counters  *Counters
	gr        schema.GroupResource
	group     string
	resource  string
	replicaID string
	bodyGVR   schema.GroupVersionResource
	bodyKind  string
	mode      WatchMode

	// read-path reconcile policy (0045).
	reconcile bool
	minAge    time.Duration
	adopt     bool
	gc        bool
}

// New constructs the multi-host stores, locker, hub, and REST adapter
// from Options, wires them together, and returns the REST. Call
// Start(ctx) before serving to spin up the informers.
func New(opts Options) *REST {
	if opts.Dynamic == nil {
		panic("multihost.New: Dynamic is required")
	}
	if opts.Converter == nil {
		panic("multihost.New: Converter is required")
	}
	replicaID := opts.ReplicaID
	if replicaID == "" {
		replicaID = os.Getenv("POD_NAME")
		if replicaID == "" {
			replicaID, _ = os.Hostname()
		}
	}
	fieldMgr := opts.FieldManager
	if fieldMgr == "" {
		fieldMgr = opts.GroupResource.Resource
	}
	gate := opts.IdentityGate
	if gate == nil {
		gate = DefaultIdentityGate
	}
	metaKind := opts.MetaKind
	if metaKind == "" {
		metaKind = "ResourceMetadata"
	}
	counters := &Counters{}
	minAge := opts.CollectMinAge
	if minAge <= 0 {
		minAge = defaultCollectMinAge
	}

	store := NewMetaStore(MetaStoreOptions{
		Dynamic:      opts.Dynamic,
		GVR:          opts.MetaGVR,
		Kind:         metaKind,
		FieldManager: fieldMgr,
		Group:        opts.GroupResource.Group,
		Resource:     opts.GroupResource.Resource,
		ReplicaID:    replicaID,
		ResyncPeriod: opts.ResyncPeriod,
	})
	bodies := NewBodyStore(BodyStoreOptions{
		Dynamic:        opts.Dynamic,
		GVR:            opts.BodyGVR,
		Kind:           opts.BodyKind,
		FieldManager:   fieldMgr,
		ReplicaID:      replicaID,
		ResyncPeriod:   opts.ResyncPeriod,
		UpstreamBudget: opts.UpstreamBudget,
		Metrics:        counters,
		IdentityGate:   gate,
	})

	r := &REST{
		store:     store,
		bodies:    bodies,
		conv:      opts.Converter,
		gate:      gate,
		counters:  counters,
		gr:        opts.GroupResource,
		group:     opts.GroupResource.Group,
		resource:  opts.GroupResource.Resource,
		replicaID: replicaID,
		bodyGVR:   opts.BodyGVR,
		bodyKind:  opts.BodyKind,
		mode:      opts.WatchModeSelect,
		reconcile: opts.ReadPathReconcile,
		minAge:    minAge,
		adopt:     opts.Adopt,
		gc:        opts.GCEnabled,
	}

	if opts.Lock {
		renew := opts.LockRenew
		if !opts.DisableLockRenew && !opts.LockRenew {
			renew = true // default on when Lock is on
		}
		if opts.DisableLockRenew {
			renew = false
		}
		r.locker = NewLocker(LockerOptions{
			Store:               store,
			GroupResource:       opts.GroupResource,
			Identity:            replicaID,
			LeaseDuration:       opts.LeaseDuration,
			RenewEnabled:        renew,
			TransactionalWrite:  opts.TransactionalWrite,
			TransactionAttempts: opts.TransactionAttempts,
		})
	}

	r.hub = NewHub(HubOptions{
		Backend:      bodies,
		Stitcher:     r,
		IdentityGate: gate,
		SharedPoll:   opts.SharedPoll,
		PollInterval: opts.PollInterval,
		BufferSize:   opts.WatchBufferSize,
	})
	store.SetRawSink(r)
	return r
}

// Start spins up the metadata and body informers (blocks until both
// initial caches sync) and, in SharedPoll mode, the shared poll loop.
func (r *REST) Start(ctx context.Context) error {
	if err := r.bodies.Start(ctx); err != nil {
		return err
	}
	if err := r.store.Start(ctx); err != nil {
		return err
	}
	if r.hub.SharedPoll() {
		go r.hub.RunSharedPoll(ctx)
	}
	return nil
}

// Shutdown is a no-op (per-watcher pipelines stop with their request
// contexts).
func (r *REST) Shutdown() {}

// Hub exposes the per-watcher hub for instrumentation.
func (r *REST) Hub() *Hub { return r.hub }

// Counters exposes the shared instrument.
func (r *REST) Counters() *Counters { return r.counters }

// Locker exposes the locker (nil if locking disabled).
func (r *REST) Locker() *Locker { return r.locker }

// ---- RawSink (metadata informer → hub) ----

func (r *REST) OnMetadataEvent(et apiwatch.EventType, ref ResourceRef, rec *Record, rv string) {
	r.hub.OnMetadataEvent(et, ref, rec, rv)
}

// CurrentResourceVersion returns the host-RV authority high-water mark.
func (r *REST) CurrentResourceVersion() string {
	rv := r.hub.CurrentResourceVersion()
	if hw := r.store.HighWaterRV(); rvLess(rv, hw) {
		rv = hw
	}
	return rv
}

// ---- WatchStitcher ----

// StitchFor fetches the body via the BodyStore using the WATCHER's
// identity (applies per-user authz), overlays the metadata CR's RV.
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
	return r.stitch(ref, body, rec), true
}

// NewBookmark returns an empty served object (a scheme-registered
// served type) for use as a BOOKMARK carrier.
func (r *REST) NewBookmark() runtime.Object { return r.conv.New() }

// ---- identity / shape ----

func (r *REST) New() runtime.Object     { return r.conv.New() }
func (r *REST) NewList() runtime.Object { return r.conv.NewList() }
func (r *REST) Destroy()                {}
func (r *REST) NamespaceScoped() bool   { return true }
func (r *REST) Kind() string            { return r.gr.Resource }
func (r *REST) GetSingularName() string { return r.resource }

// ---- Getter ----

func (r *REST) Get(ctx context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	r.counters.ServedGet.Add(1)
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	u, _ := genericapirequest.UserFrom(ctx)
	ref := r.refFor(ns, name)

	var body Body
	var present bool
	if r.reconcile {
		// 0045: backend is source of truth for existence (direct read).
		body, present = r.bodies.GetAuthoritative(ctx, ns, name)
		if !present {
			rec, _ := r.store.GetFromCache(ref)
			if rec == nil {
				rec, _ = r.store.GetDirect(ctx, ref)
			}
			if rec != nil {
				r.collect(ctx, ref, rec, false)
			}
			return nil, apierrors.NewNotFound(r.gr, name)
		}
	} else {
		// Cache read (no read-path reconcile).
		body, present = r.bodies.Get(ns, name)
		if !present {
			return nil, apierrors.NewNotFound(r.gr, name)
		}
	}

	// Identity gate.
	if !r.gate(u, body.Owner) {
		return nil, apierrors.NewNotFound(r.gr, name)
	}

	rec, _ := r.store.GetFromCache(ref)
	if rec == nil {
		rec, _ = r.store.GetDirect(ctx, ref)
	}
	if rec == nil {
		if r.reconcile && r.adopt {
			rec = r.adoptRecord(ctx, ref, false)
		} else if r.reconcile {
			return nil, apierrors.NewNotFound(r.gr, name)
		}
	}
	return r.stitch(ref, body, rec), nil
}

// ---- Lister ----

func (r *REST) List(ctx context.Context, opts *metainternalversion.ListOptions) (runtime.Object, error) {
	r.counters.ServedList.Add(1)
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	u, _ := genericapirequest.UserFrom(ctx)
	sel := selectorFrom(opts)

	list := r.conv.NewList()
	items := []runtime.Object{}

	if r.reconcile {
		backendRefs, recByKey := r.ReconcileList(ctx, ns, false)
		for _, br := range backendRefs {
			key := br.Namespace + "/" + br.Name
			rec := recByKey[key]
			if rec == nil && !r.adopt {
				continue // unadopted foreign object: suppress (0045).
			}
			if !r.gate(u, br.Body.Owner) {
				continue
			}
			obj := r.stitch(r.refFor(br.Namespace, br.Name), br.Body, rec)
			if !matchesSel(obj, sel) {
				continue
			}
			items = append(items, obj)
		}
	} else {
		records, _, _ := r.store.ListFromCache()
		recByKey := map[string]*Record{}
		for _, rec := range records {
			if ns != "" && rec.Ref.Namespace != ns {
				continue
			}
			recByKey[rec.Ref.Namespace+"/"+rec.Ref.Name] = rec
		}
		for _, br := range r.bodies.ListFor(u, ns) {
			rec := recByKey[br.Namespace+"/"+br.Name]
			obj := r.stitch(r.refFor(br.Namespace, br.Name), br.Body, rec)
			if !matchesSel(obj, sel) {
				continue
			}
			items = append(items, obj)
		}
	}

	if err := meta.SetList(list, items); err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("set list items: %w", err))
	}
	if li, ok := list.(metav1.ListInterface); ok {
		li.SetResourceVersion(r.CurrentResourceVersion())
	}
	return list, nil
}

// ReconcileList reads the authoritative backend set, adopts unknown
// backend objects and collects orphan records (subject to minAge), then
// returns the backend set + surviving records by key. The same method
// serves inline (fromSweep=false) and the periodic sweep
// (fromSweep=true), so they agree by construction (0045).
func (r *REST) ReconcileList(ctx context.Context, ns string, fromSweep bool) ([]BodyRef, map[string]*Record) {
	backendRefs, lerr := r.bodies.ListAuthoritative(ctx, ns)
	if lerr != nil {
		klog.Warningf("bodies ListAuthoritative failed (falling back to cache): %v", lerr)
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
	recByKey := map[string]*Record{}
	for _, rec := range records {
		if ns != "" && rec.Ref.Namespace != ns {
			continue
		}
		recByKey[rec.Ref.Namespace+"/"+rec.Ref.Name] = rec
	}

	if r.adopt {
		for _, br := range backendRefs {
			key := br.Namespace + "/" + br.Name
			if recByKey[key] == nil {
				if rec := r.adoptRecord(ctx, r.refFor(br.Namespace, br.Name), fromSweep); rec != nil {
					recByKey[key] = rec
				}
			}
		}
	}
	if r.gc {
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

func (r *REST) adoptRecord(ctx context.Context, ref ResourceRef, fromSweep bool) *Record {
	if !r.adopt {
		return nil
	}
	if ref.Namespace == "" {
		ref.Namespace = "default"
	}
	rec := &Record{
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
		klog.Warningf("adopt: metastore.Put failed ref=%s: %v", ref.String(), err)
		return nil
	}
	if fromSweep {
		r.counters.AdoptSweep.Add(1)
	} else {
		r.counters.AdoptInline.Add(1)
	}
	return stored
}

func (r *REST) collect(ctx context.Context, ref ResourceRef, rec *Record, fromSweep bool) bool {
	if !r.gc {
		return false
	}
	if rec != nil && !rec.CreationTimestamp.IsZero() {
		if age := time.Since(rec.CreationTimestamp.Time); age < r.minAge {
			r.counters.CollectSkippedAge.Add(1)
			return false
		}
	}
	if err := r.store.Delete(ctx, ref); err != nil {
		klog.Warningf("collect: metastore.Delete failed ref=%s: %v", ref.String(), err)
		return false
	}
	if fromSweep {
		r.counters.CollectSweep.Add(1)
	} else {
		r.counters.CollectInline.Add(1)
	}
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
	initial := r.initialReplay(u, ns, sel)
	w := r.hub.NewWatch(ctx, u, ns, sel, r.mode, initial)
	return w, nil
}

func (r *REST) initialReplay(u user.Info, ns string, sel labels.Selector) []runtime.Object {
	records, _, _ := r.store.ListFromCache()
	byKey := map[string]*Record{}
	for _, rec := range records {
		byKey[rec.Ref.Namespace+"/"+rec.Ref.Name] = rec
	}
	out := []runtime.Object{}
	var refs []BodyRef
	if r.hub.SharedPoll() {
		refs = r.bodies.List(ns)
	} else {
		refs = r.bodies.ListFor(u, ns)
	}
	for _, br := range refs {
		rec := byKey[br.Namespace+"/"+br.Name]
		obj := r.stitch(r.refFor(br.Namespace, br.Name), br.Body, rec)
		if !matchesSel(obj, sel) {
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
		{Name: "Age", Type: "string"},
	}}
	row := func(o runtime.Object) (metav1.TableRow, error) {
		acc, err := meta.Accessor(o)
		if err != nil {
			return metav1.TableRow{}, err
		}
		age := ""
		if ct := acc.GetCreationTimestamp(); !ct.IsZero() {
			age = time.Since(ct.Time).Round(time.Second).String()
		}
		return metav1.TableRow{Cells: []interface{}{acc.GetName(), age}, Object: runtime.RawExtension{Object: o}}, nil
	}
	if meta.IsListType(object) {
		items, err := meta.ExtractList(object)
		if err != nil {
			return nil, err
		}
		for _, it := range items {
			tr, rerr := row(it)
			if rerr != nil {
				return nil, rerr
			}
			t.Rows = append(t.Rows, tr)
		}
		if li, ok := object.(metav1.ListInterface); ok {
			t.ListMeta.ResourceVersion = li.GetResourceVersion()
		}
		return t, nil
	}
	tr, err := row(object)
	if err != nil {
		return nil, err
	}
	t.Rows = []metav1.TableRow{tr}
	return t, nil
}

// ---- Create (0043 lock + 0049 transaction) ----

func (r *REST) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, _ *metav1.CreateOptions) (runtime.Object, error) {
	if createValidation != nil {
		if err := createValidation(ctx, obj); err != nil {
			return nil, err
		}
	}
	acc, err := meta.Accessor(obj)
	if err != nil {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("cannot access object metadata: %v", err))
	}
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	if acc.GetNamespace() == "" {
		acc.SetNamespace(ns)
	}
	name := acc.GetName()
	if name == "" {
		return nil, apierrors.NewBadRequest("metadata.name is required")
	}
	objNS := acc.GetNamespace()
	if _, exists := r.bodies.GetDirect(ctx, objNS, name); exists {
		return nil, apierrors.NewAlreadyExists(r.gr, name)
	}

	owner := ""
	if u, has := genericapirequest.UserFrom(ctx); has && u != nil {
		owner = u.GetName()
	}
	ref := r.refFor(objNS, name)

	body := r.conv.BodyFromObject(obj)
	body.Owner = owner

	res, terr := r.write(ctx, ref, objNS, name, obj, body, "")
	if terr != nil {
		// On a hard failure clean up any body so a client retry is clean.
		_ = r.bodies.Delete(ctx, objNS, name)
		if apierrors.IsConflict(terr) {
			r.counters.WriteConflict.Add(1)
		}
		return nil, terr
	}
	r.recordWriteOutcome(res.Depth)
	return r.stitch(ref, body, res.Record), nil
}

// ---- Update / Patch (0043 pre-acquire OCC + lock + 0049 txn) ----

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
	acc, aerr := meta.Accessor(updated)
	if aerr != nil {
		return nil, false, apierrors.NewBadRequest(fmt.Sprintf("cannot access updated object metadata: %v", aerr))
	}
	if acc.GetNamespace() == "" {
		acc.SetNamespace(ns)
	}
	if acc.GetName() == "" {
		acc.SetName(name)
	}
	objNS := acc.GetNamespace()

	if current == nil {
		if !forceAllowCreate {
			return nil, false, apierrors.NewNotFound(r.gr, name)
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

	ref := r.refFor(objNS, name)
	clientRV := acc.GetResourceVersion()

	// Preserve the owner from the prior body (identity at Create wins).
	owner := ""
	if prior, ok := r.bodies.GetDirect(ctx, objNS, name); ok {
		owner = prior.Owner
	}
	body := r.conv.BodyFromObject(updated)
	body.Owner = owner

	res, terr := r.write(ctx, ref, objNS, name, updated, body, clientRV)
	if terr != nil {
		if apierrors.IsConflict(terr) {
			r.counters.WriteConflict.Add(1)
		}
		return nil, false, terr
	}
	r.recordWriteOutcome(res.Depth)
	return r.stitch(ref, body, res.Record), false, nil
}

// write runs the locked-write transaction (0049) when a locker is
// configured, or a plain single-shot body+commit when not. clientRV is
// the OCC precondition ("" disables OCC, e.g. on Create).
func (r *REST) write(ctx context.Context, ref ResourceRef, ns, name string, obj runtime.Object, body Body, clientRV string) (*TxnResult, error) {
	buildRecord := func(_ context.Context, _ int) (*Record, error) {
		rec := r.conv.RecordFromObject(obj, ref)
		if rec == nil {
			rec = &Record{Ref: ref}
		}
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
		rec.BodyHash = HashBody(body)
		return rec, nil
	}
	writeBody := func(ctx context.Context) error {
		return r.bodies.Put(ctx, ns, name, body)
	}
	occ := func(curRV string) error {
		if clientRV != "" && curRV != "" && clientRV != curRV {
			return apierrors.NewConflict(r.gr, name,
				fmt.Errorf("the object has been modified; please apply your changes to the latest version and try again"))
		}
		return nil
	}

	if r.locker != nil {
		return r.locker.WriteTxn(ctx, ref, WriteOp{
			WriteBody:   writeBody,
			BuildRecord: buildRecord,
			OnConflictRetry: func(attempt int) {
				r.counters.WriteRetry.Add(1)
				klog.V(2).InfoS("write-txn-retry", "replica", r.replicaID, "ref", ref.String(), "attempt", attempt)
			},
		}, occ)
	}

	// No lock: single-shot. Still honor pre-write OCC.
	if cerr := occ(currentRVOf(r, ref)); cerr != nil {
		return nil, cerr
	}
	if berr := writeBody(ctx); berr != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("backend.Put: %w", berr))
	}
	rec, _ := buildRecord(ctx, 0)
	stored, perr := r.store.Put(ctx, rec)
	if perr != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("metastore.Put: %w", perr))
	}
	return &TxnResult{Record: stored, Depth: 0}, nil
}

func currentRVOf(r *REST, ref ResourceRef) string {
	if rec, _ := r.store.GetDirect(context.Background(), ref); rec != nil {
		return rec.RecordRV
	}
	return ""
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
	// Delete the metadata CR (releases the embedded lock atomically),
	// then the body.
	if derr := r.store.Delete(ctx, ref); derr != nil {
		return nil, false, apierrors.NewInternalError(derr)
	}
	if berr := r.bodies.Delete(ctx, ns, name); berr != nil {
		klog.Warningf("bodies.Delete failed ns=%s name=%s: %v", ns, name, berr)
	}
	return prior, true, nil
}

func (r *REST) recordWriteOutcome(depth int) {
	r.counters.WriteOK.Add(1)
	for {
		cur := r.counters.MaxWriteDepth.Load()
		if uint64(depth) <= cur {
			break
		}
		if r.counters.MaxWriteDepth.CompareAndSwap(cur, uint64(depth)) {
			break
		}
	}
}

// ---- stitch ----

func (r *REST) stitch(ref ResourceRef, body Body, rec *Record) runtime.Object {
	obj := r.conv.Stitch(ref, body, rec)
	acc, err := meta.Accessor(obj)
	if err != nil {
		return obj
	}
	if acc.GetName() == "" {
		acc.SetName(ref.Name)
	}
	if acc.GetNamespace() == "" {
		acc.SetNamespace(ref.Namespace)
	}
	if rec == nil {
		// Adopted / synthetic object: stamp a synthetic UID + the
		// current high-water RV.
		if acc.GetUID() == "" {
			acc.SetUID(types.UID("synthetic-" + uuid.NewString()))
		}
		if acc.GetResourceVersion() == "" {
			acc.SetResourceVersion(r.CurrentResourceVersion())
		}
		if ct := acc.GetCreationTimestamp(); ct.IsZero() {
			acc.SetCreationTimestamp(metav1.NewTime(time.Now().UTC()))
		}
		return obj
	}
	// Host-RV authority: every served object's RV is the metadata CR's
	// host etcd RV.
	acc.SetUID(types.UID(rec.UID))
	acc.SetResourceVersion(rec.RecordRV)
	if !rec.CreationTimestamp.IsZero() {
		acc.SetCreationTimestamp(rec.CreationTimestamp)
	}
	acc.SetDeletionTimestamp(rec.DeletionTimestamp)
	acc.SetLabels(mapCopy(rec.Labels))
	acc.SetAnnotations(mapCopy(rec.Annotations))
	acc.SetFinalizers(append([]string(nil), rec.Finalizers...))
	if len(rec.ManagedFields) > 0 {
		var mf []metav1.ManagedFieldsEntry
		if err := json.Unmarshal(rec.ManagedFields, &mf); err == nil {
			acc.SetManagedFields(mf)
		}
	}
	if len(rec.OwnerReferences) > 0 {
		var or []metav1.OwnerReference
		if err := json.Unmarshal(rec.OwnerReferences, &or); err == nil {
			acc.SetOwnerReferences(or)
		}
	}
	return obj
}

// ---- helpers ----

func (r *REST) refFor(ns, name string) ResourceRef {
	return ResourceRef{Group: r.group, Resource: r.resource, Namespace: ns, Name: name}
}

func selectorFrom(opts *metainternalversion.ListOptions) labels.Selector {
	if opts == nil || opts.LabelSelector == nil {
		return labels.Everything()
	}
	return opts.LabelSelector
}

func matchesSel(obj runtime.Object, sel labels.Selector) bool {
	if sel == nil || sel.Empty() {
		return true
	}
	acc, err := meta.Accessor(obj)
	if err != nil {
		return true
	}
	return sel.Matches(labels.Set(acc.GetLabels()))
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
	_ RawSink                   = (*REST)(nil)
	_ WatchStitcher             = (*REST)(nil)
)
