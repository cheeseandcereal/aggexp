// Package grpcbackend is the v2 component-server REST adapter. It
// wires a resource-agnostic Kubernetes CRUD+watch surface to any
// componentv2pb.BackendClient (gRPC or HTTP, per transport).
//
// v2 differences from runtime/component/grpcbackend:
//
//   - Metadata-store integration (optional). When a MetadataStore is
//     wired, every Get/List/Create/Update/SSA/Delete/Watch path
//     stitches KRM metadata (uid, RV, managedFields, finalizers,
//     ownerReferences, labels, annotations, deletionTimestamp) onto
//     the backend's spec/status response. This recovers all the
//     library features a stateless AA loses — per FINDINGS/0024.
//   - Unified resourceVersion authority. The middleware's monotonic
//     counter is the single source of truth for RV — on Get, List,
//     and Watch. Backend-supplied RVs are advisory. Closes the
//     Get-vs-Watch RV split FINDINGS/0025 surfaced. When a Record
//     exists the Record's RecordResourceVersion (authoritative, from
//     host etcd) is preferred over the local counter.
//   - initial-events-end BOOKMARK. Watch emits an unconditional
//     BOOKMARK event at the tail of the initial-state replay, with
//     the `k8s.io/initial-events-end=true` annotation, so
//     `kubectl wait --for=jsonpath` works (closes FINDINGS/0011,
//     per the FINDINGS/0025 implementation).
//   - Admission hook. An optional admission.Engine runs first (CEL +
//     JSONPath); backend Validate/Mutate RPCs run second; identical
//     422 multi-cause wire shape. Per FINDINGS/0020 and 0029.
//   - Watch capability dispatch. ModePush calls backend.Watch and
//     forwards events; ModePoll drives its own list-loop. The mode
//     is set at construction from the backend's advertised
//     WatchCapability (with a runtime probe possible).
//
// The REST is a single type; the dial helpers for the two transport
// implementations live with their respective packages
// (runtime/component/v2/grpcbackend.Dial for gRPC,
// runtime/component/v2/httpbackend.New for HTTP).
package grpcbackend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
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
	utilwatch "k8s.io/apimachinery/pkg/watch"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/klog/v2"

	"github.com/google/uuid"

	"github.com/cheeseandcereal/aggexp/runtime/component/v2/admission"
	"github.com/cheeseandcereal/aggexp/runtime/component/v2/metadatastore"
	componentv2pb "github.com/cheeseandcereal/aggexp/runtime/component/v2/proto"
	componentscheme "github.com/cheeseandcereal/aggexp/runtime/component/v2/scheme"
)

// Mode selects watch-source behavior.
type Mode string

const (
	// ModePoll drives a middleware-local list-diff loop against the
	// backend. Safe fallback for any backend.
	ModePoll Mode = "poll"
	// ModePush consumes the backend's streaming Watch RPC.
	ModePush Mode = "push"
)

// Descriptor is the resource identity the REST serves.
type Descriptor struct {
	GroupVersion            schema.GroupVersion
	Resource                string
	Kind                    string
	Singular                string
	Namespaced              bool
	Writable                bool
	SupportsServerSideApply bool
	// UseTypedWrapper must match the Scheme.
	UseTypedWrapper bool
	Columns         []metav1.TableColumnDefinition
	RowFields       []string
	GroupResource   schema.GroupResource

	// SupportsValidation / SupportsMutation toggle backend Validate
	// and Mutate RPCs. Safe to leave false if the backend doesn't
	// implement them; admission.Engine (below) is independent.
	SupportsValidation bool
	SupportsMutation   bool

	// WatchMode selects push vs poll. Default is ModePoll for safety.
	WatchMode Mode
	// PollInterval is the period between poll snapshots. Only used
	// for ModePoll. Default 15s.
	PollInterval time.Duration
}

// REST is the rest.Storage implementation.
type REST struct {
	desc   Descriptor
	client componentv2pb.BackendClient

	// Optional integrations. All three are safe to leave nil.
	store     *metadatastore.Store
	admission *admission.Engine

	// Middleware-owned monotonic RV. Primary when no Record exists;
	// Record.RecordResourceVersion takes precedence when it does.
	rv      atomic.Uint64
	bcaster *utilwatch.Broadcaster

	// Poll-mode state.
	pollCache atomic.Pointer[map[string]pollEntry] // key = ns/name
}

type pollEntry struct {
	raw  []byte
	hash string
}

// New constructs a REST.
func New(d Descriptor, c componentv2pb.BackendClient) *REST {
	r := &REST{
		desc:    d,
		client:  c,
		bcaster: utilwatch.NewBroadcaster(100, utilwatch.DropIfChannelFull),
	}
	r.rv.Store(1)
	if r.desc.GroupResource == (schema.GroupResource{}) {
		r.desc.GroupResource = schema.GroupResource{Group: d.GroupVersion.Group, Resource: d.Resource}
	}
	if r.desc.WatchMode == "" {
		r.desc.WatchMode = ModePoll
	}
	if r.desc.PollInterval <= 0 {
		r.desc.PollInterval = 15 * time.Second
	}
	empty := map[string]pollEntry{}
	r.pollCache.Store(&empty)
	return r
}

// WithMetadataStore attaches a MetadataStore for KRM stitching.
// Safe to omit; the REST then behaves like v1 (backend owns all
// object state).
func (r *REST) WithMetadataStore(s *metadatastore.Store) *REST {
	r.store = s
	return r
}

// WithAdmission attaches a declarative-admission engine.
func (r *REST) WithAdmission(e *admission.Engine) *REST {
	r.admission = e
	return r
}

// Shutdown stops the broadcaster. Safe to call multiple times.
func (r *REST) Shutdown() {
	if r.bcaster != nil {
		r.bcaster.Shutdown()
	}
}

// ---- rest.Storage / rest.Scoper / rest.KindProvider ----

func (r *REST) New() runtime.Object {
	gvk := r.desc.GroupVersion.WithKind(r.desc.Kind)
	if r.desc.UseTypedWrapper {
		o := &componentscheme.Object{}
		o.GetObjectKind().SetGroupVersionKind(gvk)
		return o
	}
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	return u
}

func (r *REST) NewList() runtime.Object {
	listGVK := r.desc.GroupVersion.WithKind(r.desc.Kind + "List")
	if r.desc.UseTypedWrapper {
		l := &componentscheme.ObjectList{}
		l.GetObjectKind().SetGroupVersionKind(listGVK)
		return l
	}
	u := &unstructured.UnstructuredList{}
	u.SetGroupVersionKind(listGVK)
	return u
}

func (r *REST) Destroy()                {}
func (r *REST) NamespaceScoped() bool   { return r.desc.Namespaced }
func (r *REST) Kind() string            { return r.desc.Kind }
func (r *REST) GetSingularName() string { return r.desc.Singular }

// ---- Get ----

func (r *REST) Get(ctx context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	resp, err := r.client.Get(ctx, &componentv2pb.GetRequest{
		User:      userFromCtx(ctx),
		Namespace: ns,
		Name:      name,
	})
	if err != nil {
		return nil, r.translateErr(err, name)
	}
	return r.assembleOne(ctx, resp.GetObjectJson(), ns, name)
}

// ---- List ----

func (r *REST) List(ctx context.Context, opts *metainternalversion.ListOptions) (runtime.Object, error) {
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	resp, err := r.client.List(ctx, &componentv2pb.ListRequest{
		User:          userFromCtx(ctx),
		Namespace:     ns,
		LabelSelector: selectorString(opts),
	})
	if err != nil {
		return nil, r.translateErr(err, "")
	}
	records := r.listRecords(ctx, ns)
	listRV := uint64(0)

	if r.desc.UseTypedWrapper {
		list := r.NewList().(*componentscheme.ObjectList)
		for _, raw := range resp.GetItemsJson() {
			obj, rv, err := r.stitchObj(raw, records, ns, "")
			if err != nil {
				return nil, err
			}
			if !r.matchesLabels(obj, opts) {
				continue
			}
			list.Items = append(list.Items, *(obj.(*componentscheme.Object)))
			if rv > listRV {
				listRV = rv
			}
		}
		list.SetResourceVersion(r.finalRV(listRV))
		return list, nil
	}
	list := r.NewList().(*unstructured.UnstructuredList)
	for _, raw := range resp.GetItemsJson() {
		obj, rv, err := r.stitchObj(raw, records, ns, "")
		if err != nil {
			return nil, err
		}
		if !r.matchesLabels(obj, opts) {
			continue
		}
		list.Items = append(list.Items, *(obj.(*unstructured.Unstructured)))
		if rv > listRV {
			listRV = rv
		}
	}
	list.SetResourceVersion(r.finalRV(listRV))
	return list, nil
}

// finalRV picks the larger of listRV and the local counter as the
// list's resourceVersion. Keeps watches anchored on a single
// monotonic sequence even if the metastore has no records yet.
func (r *REST) finalRV(listRV uint64) string {
	local := r.rv.Load()
	if listRV > local {
		return strconv.FormatUint(listRV, 10)
	}
	return strconv.FormatUint(local, 10)
}

// ---- Watch (with initial-events-end BOOKMARK) ----

func (r *REST) Watch(ctx context.Context, opts *metainternalversion.ListOptions) (utilwatch.Interface, error) {
	if opts != nil && opts.ResourceVersion != "" && opts.ResourceVersion != "0" {
		reqN, perr := strconv.ParseUint(opts.ResourceVersion, 10, 64)
		if perr != nil || reqN > r.rv.Load() {
			return nil, apierrors.NewResourceExpired(fmt.Sprintf(
				"too old resource version: %s (current %s)", opts.ResourceVersion, r.CurrentResourceVersion()))
		}
	}
	ns, _ := genericapirequest.NamespaceFrom(ctx)

	initial, err := r.List(ctx, opts)
	if err != nil {
		return nil, err
	}

	items := extractListItems(initial)
	prefix := make([]utilwatch.Event, 0, len(items)+1)
	for _, it := range items {
		prefix = append(prefix, utilwatch.Event{Type: utilwatch.Added, Object: it})
	}

	// initial-events-end BOOKMARK (closes FINDINGS/0011 per 0025).
	// Emitted unconditionally regardless of watch capability. The
	// library augments this with a `kubernetes.io/initial-events-list-blueprint`
	// annotation for WatchList-aware clients.
	prefix = append(prefix, utilwatch.Event{
		Type:   utilwatch.Bookmark,
		Object: r.bookmarkObject(),
	})

	w, err := r.bcaster.WatchWithPrefix(prefix)
	if err != nil {
		return nil, err
	}
	sel := selectorFromOpts(opts)
	if (sel == nil || sel.Empty()) && ns == "" {
		return w, nil
	}
	return utilwatch.Filter(w, func(ev utilwatch.Event) (utilwatch.Event, bool) {
		// Never filter BOOKMARK events — they carry their own
		// RV/annotation payload, not the per-resource identity
		// selectors below.
		if ev.Type == utilwatch.Bookmark {
			return ev, true
		}
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

// bookmarkObject builds the initial-events-end BOOKMARK object. The
// `k8s.io/initial-events-end=true` annotation is what kubectl and
// WatchList-aware clients key off.
func (r *REST) bookmarkObject() runtime.Object {
	gvk := r.desc.GroupVersion.WithKind(r.desc.Kind)
	rv := r.CurrentResourceVersion()
	if r.desc.UseTypedWrapper {
		o := &componentscheme.Object{}
		o.TypeMeta = metav1.TypeMeta{APIVersion: gvk.GroupVersion().String(), Kind: gvk.Kind}
		o.Annotations = map[string]string{"k8s.io/initial-events-end": "true"}
		o.ResourceVersion = rv
		return o
	}
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetAnnotations(map[string]string{"k8s.io/initial-events-end": "true"})
	u.SetResourceVersion(rv)
	return u
}

// StartUpstreamWatch launches the background loop that feeds the
// broadcaster from the backend — push or poll per the descriptor.
// Call from a PostStart hook; the goroutine exits when ctx is
// cancelled.
func (r *REST) StartUpstreamWatch(ctx context.Context) {
	if r.desc.WatchMode == ModePush {
		go r.runPushLoop(ctx)
		return
	}
	go r.runPollLoop(ctx)
}

func (r *REST) runPushLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		err := r.runPushOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			klog.Warningf("v2: push watch disconnected: %v; retrying in 2s", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (r *REST) runPushOnce(ctx context.Context) error {
	stream, err := r.client.Watch(ctx, &componentv2pb.WatchRequest{})
	if err != nil {
		return err
	}
	klog.Infof("v2: push watch opened for %s", r.desc.GroupResource)
	for {
		ev, err := stream.Recv()
		if err != nil {
			return err
		}
		obj, err := r.fromJSONAssembled(ctx, ev.GetObjectJson(), "", "")
		if err != nil {
			klog.Warningf("v2: decode/stitch upstream watch event: %v", err)
			continue
		}
		switch ev.GetType() {
		case componentv2pb.EventType_EVENT_ADDED:
			r.publish(utilwatch.Added, obj)
		case componentv2pb.EventType_EVENT_MODIFIED:
			r.publish(utilwatch.Modified, obj)
		case componentv2pb.EventType_EVENT_DELETED:
			r.publish(utilwatch.Deleted, obj)
		}
	}
}

func (r *REST) runPollLoop(ctx context.Context) {
	klog.Infof("v2: poll watch starting for %s interval=%s", r.desc.GroupResource, r.desc.PollInterval)
	tick := time.NewTimer(0)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if err := r.pollOnce(ctx); err != nil {
			klog.Warningf("v2: poll error: %v", err)
		}
		tick.Reset(r.desc.PollInterval)
	}
}

func (r *REST) pollOnce(ctx context.Context) error {
	resp, err := r.client.List(ctx, &componentv2pb.ListRequest{})
	if err != nil {
		return err
	}
	now := map[string]pollEntry{}
	for _, raw := range resp.GetItemsJson() {
		nm, ens := nameNamespaceFromJSON(raw)
		key := ens + "/" + nm
		now[key] = pollEntry{raw: raw, hash: quickHash(raw)}
	}
	prev := *r.pollCache.Load()
	r.pollCache.Store(&now)

	for key, pe := range now {
		if old, ok := prev[key]; !ok {
			obj, err := r.fromJSONAssembled(ctx, pe.raw, "", "")
			if err == nil {
				r.publish(utilwatch.Added, obj)
			}
		} else if old.hash != pe.hash {
			obj, err := r.fromJSONAssembled(ctx, pe.raw, "", "")
			if err == nil {
				r.publish(utilwatch.Modified, obj)
			}
		}
	}
	for key, old := range prev {
		if _, ok := now[key]; !ok {
			obj, err := r.fromJSONAssembled(ctx, old.raw, "", "")
			if err == nil {
				r.publish(utilwatch.Deleted, obj)
			}
		}
	}
	return nil
}

func (r *REST) publish(t utilwatch.EventType, obj runtime.Object) {
	// Always advance the local counter on publish. Stamp it onto
	// the object (overwriting the stitched-in-advance RV) so the
	// Watch stream and local counter stay in lockstep. When a
	// Record supplies its own RV that's still the authoritative
	// RV for Get/List; we just make sure watch sees a strictly
	// monotonic sequence.
	next := r.rv.Add(1)
	if acc, err := meta.Accessor(obj); err == nil {
		acc.SetResourceVersion(strconv.FormatUint(next, 10))
	}
	_ = r.bcaster.Action(t, obj)
}

// CurrentResourceVersion returns the middleware's RV as decimal.
func (r *REST) CurrentResourceVersion() string {
	return strconv.FormatUint(r.rv.Load(), 10)
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

	// Middleware-declared admission first.
	mutatedRaw, err := r.runAdmission(ctx, obj, nil, "CREATE")
	if err != nil {
		return nil, err
	}
	// Backend-RPC admission second (opt-in per schema flags).
	afterBackend, err := r.backendMutateValidate(ctx, mutatedRaw, nil, ns, name, "CREATE")
	if err != nil {
		return nil, err
	}

	// Persist metadata Record first (if store is wired) — gives us
	// a stable UID + RV to surface on the response.
	var storedRec *metadatastore.Record
	if r.store != nil {
		rec := recordFromLibraryObject(obj, r.refFor(ns, name))
		if rec.UID == "" {
			rec.UID = uuid.NewString()
		}
		if rec.CreationTimestamp.IsZero() {
			rec.CreationTimestamp = metav1.NewTime(time.Now().UTC())
		}
		storedRec, err = r.store.Put(ctx, rec)
		if err != nil {
			return nil, apierrors.NewInternalError(fmt.Errorf("metastore.Put: %w", err))
		}
	}

	// Only spec/status go to the backend; metadata stays in the Record.
	bizJSON, err := businessJSONFromBytes(afterBackend)
	if err != nil {
		r.rollbackRecord(ctx, ns, name)
		return nil, apierrors.NewBadRequest(err.Error())
	}
	resp, err := r.client.Create(ctx, &componentv2pb.CreateRequest{
		User:         userFromCtx(ctx),
		Namespace:    ns,
		ObjectJson:   bizJSON,
		FieldManager: createFM(opts),
	})
	if err != nil {
		r.rollbackRecord(ctx, ns, name)
		return nil, r.translateErr(err, name)
	}
	stitched, err := r.stitchFromRecord(resp.GetObjectJson(), storedRec, ns, name)
	if err != nil {
		return nil, err
	}
	r.publish(utilwatch.Added, stitched)
	return stitched, nil
}

// ---- Update (also handles SSA via rest.Patcher) ----

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
		co, cerr := r.Create(ctx, updated, nil, &metav1.CreateOptions{FieldManager: updateFM(opts)})
		if cerr != nil {
			return nil, false, cerr
		}
		return co, true, nil
	}
	if updateValidation != nil {
		if err := updateValidation(ctx, updated, current); err != nil {
			return nil, false, err
		}
	}

	// Admission.
	oldRaw, _ := encodeObject(current)
	newRaw, err := r.runAdmission(ctx, updated, oldRaw, "UPDATE")
	if err != nil {
		return nil, false, err
	}
	afterBackend, err := r.backendMutateValidate(ctx, newRaw, oldRaw, ns, name, "UPDATE")
	if err != nil {
		return nil, false, err
	}

	// Metastore sync (if wired): extract metadata, persist Record,
	// handle finalizer-clear-triggers-delete.
	var storedRec *metadatastore.Record
	if r.store != nil {
		rec := recordFromLibraryObject(updated, r.refFor(ns, name))
		priorRec, _ := r.store.Get(ctx, rec.Ref)
		if priorRec != nil {
			if rec.UID == "" || strings.HasPrefix(rec.UID, "synthetic-") {
				rec.UID = priorRec.UID
			}
			if rec.CreationTimestamp.IsZero() {
				rec.CreationTimestamp = priorRec.CreationTimestamp
			}
			if priorRec.DeletionTimestamp != nil && rec.DeletionTimestamp == nil {
				rec.DeletionTimestamp = priorRec.DeletionTimestamp
			}
		}
		if rec.UID == "" || strings.HasPrefix(rec.UID, "synthetic-") {
			rec.UID = uuid.NewString()
		}
		if rec.CreationTimestamp.IsZero() {
			rec.CreationTimestamp = metav1.NewTime(time.Now().UTC())
		}

		// Finalizer-clear-completes-delete.
		if priorRec != nil && priorRec.DeletionTimestamp != nil &&
			len(priorRec.Finalizers) > 0 && len(rec.Finalizers) == 0 {
			klog.V(2).Infof("v2: finalizer cleared, completing delete ref=%s/%s/%s/%s",
				rec.Ref.Group, rec.Ref.Resource, rec.Ref.Namespace, rec.Ref.Name)
			_, derr := r.client.Delete(ctx, &componentv2pb.DeleteRequest{
				User: userFromCtx(ctx), Namespace: ns, Name: name,
			})
			if derr != nil {
				if st, ok := grpcstatus.FromError(derr); !ok || st.Code() != codes.NotFound {
					return nil, false, r.translateErr(derr, name)
				}
			}
			_ = r.store.Delete(ctx, rec.Ref)
			bizNoMeta, _ := businessJSON(updated)
			vanishing, err := r.stitchFromRecord(bizNoMeta, nil, ns, name)
			if err == nil {
				r.publish(utilwatch.Deleted, vanishing)
				return vanishing, false, nil
			}
			r.publish(utilwatch.Deleted, updated)
			return updated, false, nil
		}

		storedRec, err = r.store.Put(ctx, rec)
		if err != nil {
			return nil, false, apierrors.NewInternalError(fmt.Errorf("metastore.Put: %w", err))
		}
	}

	bizJSON, err := businessJSONFromBytes(afterBackend)
	if err != nil {
		return nil, false, apierrors.NewBadRequest(err.Error())
	}
	resp, err := r.client.Update(ctx, &componentv2pb.UpdateRequest{
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
	stitched, err := r.stitchFromRecord(resp.GetObjectJson(), storedRec, ns, name)
	if err != nil {
		return nil, false, err
	}
	r.publish(utilwatch.Modified, stitched)
	return stitched, false, nil
}

// ---- Delete ----

func (r *REST) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, _ *metav1.DeleteOptions) (runtime.Object, bool, error) {
	if !r.desc.Writable {
		return nil, false, apierrors.NewMethodNotSupported(r.desc.GroupResource, "delete")
	}
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

	if r.store != nil {
		ref := r.refFor(ns, name)
		rec, merr := r.store.Get(ctx, ref)
		if merr != nil {
			return nil, false, apierrors.NewInternalError(fmt.Errorf("metastore.Get: %w", merr))
		}
		if rec != nil && len(rec.Finalizers) > 0 {
			if rec.DeletionTimestamp == nil {
				now := metav1.NewTime(time.Now().UTC())
				rec.DeletionTimestamp = &now
				stored, perr := r.store.Put(ctx, rec)
				if perr != nil {
					return nil, false, apierrors.NewInternalError(perr)
				}
				bizResp, berr := r.client.Get(ctx, &componentv2pb.GetRequest{
					User: userFromCtx(ctx), Namespace: ns, Name: name,
				})
				if berr == nil {
					if stitched, err := r.stitchFromRecord(bizResp.GetObjectJson(), stored, ns, name); err == nil {
						r.publish(utilwatch.Modified, stitched)
						return stitched, false, nil
					}
				}
				return prior, false, nil
			}
			return prior, false, nil
		}
	}

	_, err := r.client.Delete(ctx, &componentv2pb.DeleteRequest{
		User: userFromCtx(ctx), Namespace: ns, Name: name,
	})
	if err != nil {
		if st, ok := grpcstatus.FromError(err); !ok || st.Code() != codes.NotFound {
			return nil, false, r.translateErr(err, name)
		}
	}
	if r.store != nil {
		_ = r.store.Delete(ctx, r.refFor(ns, name))
	}
	r.publish(utilwatch.Deleted, prior)
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
		v := LookupField(content, path)
		if path == ".metadata.creationTimestamp" {
			if s, ok := v.(string); ok && s != "" {
				v = ageOf(s)
			}
		}
		cells[i] = v
	}
	return metav1.TableRow{Cells: cells, Object: runtime.RawExtension{Object: objForRow}}, nil
}

// ---- assembly helpers ----

// assembleOne returns the stitched object for a single Get.
func (r *REST) assembleOne(ctx context.Context, raw []byte, ns, name string) (runtime.Object, error) {
	if r.store == nil {
		// No metastore → stamp an RV from the local counter and
		// return the decoded object.
		obj, err := r.decodeOne(raw)
		if err != nil {
			return nil, err
		}
		if acc, err := meta.Accessor(obj); err == nil && acc.GetResourceVersion() == "" {
			acc.SetResourceVersion(r.CurrentResourceVersion())
		}
		return obj, nil
	}
	rec, merr := r.store.Get(ctx, r.refFor(ns, name))
	if merr != nil {
		klog.Warningf("v2: metastore.Get failed ns=%s name=%s: %v", ns, name, merr)
	}
	return r.stitchFromRecord(raw, rec, ns, name)
}

// stitchObj works for List: it takes the item raw and a records map
// keyed by "ns/name".
func (r *REST) stitchObj(raw []byte, records map[string]*metadatastore.Record, ns, name string) (runtime.Object, uint64, error) {
	nm, ens := nameNamespaceFromJSON(raw)
	if ens == "" {
		ens = ns
	}
	if name != "" {
		nm = name
	}
	var rec *metadatastore.Record
	if records != nil {
		rec = records[ens+"/"+nm]
	}
	obj, err := r.stitchFromRecord(raw, rec, ens, nm)
	if err != nil {
		return nil, 0, err
	}
	var rv uint64
	if rec != nil {
		if n, err := strconv.ParseUint(rec.RecordResourceVersion, 10, 64); err == nil {
			rv = n
		}
	}
	return obj, rv, nil
}

// stitchFromRecord assembles a runtime.Object from a business JSON
// + optional Record. When store is nil rec will always be nil and
// we fall back to local-counter RV stamping.
func (r *REST) stitchFromRecord(bizJSON []byte, rec *metadatastore.Record, ns, name string) (runtime.Object, error) {
	obj, err := r.decodeOne(bizJSON)
	if err != nil {
		return nil, err
	}
	acc, aerr := meta.Accessor(obj)
	if aerr != nil {
		return nil, apierrors.NewInternalError(aerr)
	}
	gvk := r.desc.GroupVersion.WithKind(r.desc.Kind)
	obj.GetObjectKind().SetGroupVersionKind(gvk)

	if acc.GetName() == "" {
		acc.SetName(name)
	}
	if r.desc.Namespaced && acc.GetNamespace() == "" {
		acc.SetNamespace(ns)
	}
	if !r.desc.Namespaced {
		acc.SetNamespace("")
	}

	if rec == nil {
		// Synthesize minimal metadata without persistence.
		if acc.GetUID() == "" {
			acc.SetUID(types.UID("synthetic-" + uuid.NewString()))
		}
		if acc.GetResourceVersion() == "" {
			acc.SetResourceVersion(r.CurrentResourceVersion())
		}
		if ts := acc.GetCreationTimestamp(); ts.IsZero() {
			acc.SetCreationTimestamp(metav1.NewTime(time.Now().UTC()))
		}
		return obj, nil
	}
	acc.SetUID(types.UID(rec.UID))
	// Unified RV authority: Record's is preferred (host-etcd
	// monotonic); fall back to our counter when the Record doesn't
	// supply one yet.
	if rec.RecordResourceVersion != "" {
		acc.SetResourceVersion(rec.RecordResourceVersion)
	} else {
		acc.SetResourceVersion(r.CurrentResourceVersion())
	}
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
	} else {
		acc.SetManagedFields(nil)
	}
	if len(rec.OwnerReferences) > 0 {
		var or []metav1.OwnerReference
		if err := json.Unmarshal(rec.OwnerReferences, &or); err == nil {
			acc.SetOwnerReferences(or)
		}
	}
	return obj, nil
}

// fromJSONAssembled decodes a JSON blob and stitches metadata on
// the fly. Convenience for upstream watch fanout.
func (r *REST) fromJSONAssembled(ctx context.Context, raw []byte, ns, name string) (runtime.Object, error) {
	if len(name) == 0 {
		nm, ens := nameNamespaceFromJSON(raw)
		name = nm
		if ns == "" {
			ns = ens
		}
	}
	if r.store == nil {
		return r.decodeOne(raw)
	}
	rec, _ := r.store.Get(ctx, r.refFor(ns, name))
	return r.stitchFromRecord(raw, rec, ns, name)
}

// decodeOne decodes a JSON blob to the correct runtime type for the
// REST's descriptor (typed wrapper or unstructured).
func (r *REST) decodeOne(raw []byte) (runtime.Object, error) {
	if r.desc.UseTypedWrapper {
		o := &componentscheme.Object{}
		if err := o.UnmarshalJSON(raw); err != nil {
			return nil, apierrors.NewInternalError(fmt.Errorf("decode object: %w", err))
		}
		return o, nil
	}
	u := &unstructured.Unstructured{}
	if err := u.UnmarshalJSON(raw); err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("decode object: %w", err))
	}
	return u, nil
}

func (r *REST) listRecords(ctx context.Context, ns string) map[string]*metadatastore.Record {
	if r.store == nil {
		return nil
	}
	records, err := r.store.List(ctx, r.desc.GroupResource.Group, r.desc.GroupResource.Resource)
	if err != nil {
		klog.Warningf("v2: metastore.List failed (proceeding unstitched): %v", err)
		return nil
	}
	out := map[string]*metadatastore.Record{}
	for _, rec := range records {
		if r.desc.Namespaced && rec.Ref.Namespace != ns && ns != "" {
			continue
		}
		out[rec.Ref.Namespace+"/"+rec.Ref.Name] = rec
	}
	return out
}

func (r *REST) refFor(ns, name string) metadatastore.ResourceRef {
	return metadatastore.ResourceRef{
		Group: r.desc.GroupResource.Group, Resource: r.desc.GroupResource.Resource,
		Namespace: ns, Name: name,
	}
}

func (r *REST) rollbackRecord(ctx context.Context, ns, name string) {
	if r.store == nil {
		return
	}
	_ = r.store.Delete(ctx, r.refFor(ns, name))
}

// ---- admission composition (middleware + backend) ----

// runAdmission applies the declarative admission engine. Returns
// the possibly-mutated JSON of obj.
func (r *REST) runAdmission(ctx context.Context, obj runtime.Object, oldRaw []byte, op string) ([]byte, error) {
	raw, err := encodeObject(obj)
	if err != nil {
		return nil, apierrors.NewBadRequest(err.Error())
	}
	if r.admission == nil {
		return raw, nil
	}
	// Mutate first.
	mutated, _, err := r.admission.Mutate(op, raw)
	if err != nil {
		return nil, apierrors.NewInternalError(err)
	}
	// Validate second.
	failures, err := r.admission.Validate(op, mutated, oldRaw)
	if err != nil {
		return nil, apierrors.NewInternalError(err)
	}
	if len(failures) > 0 {
		return nil, failuresToInvalid(r.desc.GroupResource, nameOfRaw(mutated), failures)
	}
	return mutated, nil
}

// backendMutateValidate runs backend Mutate / Validate RPCs (opt-in
// per descriptor) after middleware admission.
func (r *REST) backendMutateValidate(ctx context.Context, raw, oldRaw []byte, ns, name, op string) ([]byte, error) {
	if r.desc.SupportsMutation {
		resp, err := r.client.Mutate(ctx, &componentv2pb.MutateRequest{
			User:          userFromCtx(ctx),
			Namespace:     ns,
			Name:          name,
			ObjectJson:    raw,
			OldObjectJson: oldRaw,
			Operation:     op,
		})
		if err != nil {
			return nil, r.translateErr(err, name)
		}
		if len(resp.GetMutatedObjectJson()) > 0 {
			raw = resp.GetMutatedObjectJson()
		}
	}
	if r.desc.SupportsValidation {
		resp, err := r.client.Validate(ctx, &componentv2pb.ValidateRequest{
			User:          userFromCtx(ctx),
			Namespace:     ns,
			Name:          name,
			ObjectJson:    raw,
			OldObjectJson: oldRaw,
			Operation:     op,
		})
		if err != nil {
			return nil, r.translateErr(err, name)
		}
		if !resp.GetAllowed() {
			return nil, protoCausesToInvalid(r.desc.GroupResource, name, resp)
		}
	}
	return raw, nil
}

// ---- helpers ----

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

func (r *REST) translateErr(err error, name string) error {
	if errors.Is(err, io.EOF) {
		return apierrors.NewServiceUnavailable("backend stream closed")
	}
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
)

// Dial is a convenience constructor that dials a gRPC backend with
// insecure-h2c credentials. Callers needing mTLS or SPIFFE should
// build their own grpc.ClientConn.
func Dial(ctx context.Context, addr string) (componentv2pb.BackendClient, *grpc.ClientConn, error) {
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, nil, err
	}
	return componentv2pb.NewBackendClient(conn), conn, nil
}
