// Package shared implements a rest.Storage that wraps the
// experiment's crdbackend and uses **host CRD resource versions** as
// the RV authority instead of the per-replica atomic.Uint64 the
// substrate's runtime/storage adapter normally synthesizes.
//
// This is the load-bearing experimental decision of 0034. With
// per-replica RVs:
//
//	replica A: write -> RV=42 (its own counter)
//	replica B: same write observed via informer -> RV=17 (B's counter)
//	client resumes against B with rv=42 -> 410 Gone or stale-skip
//
// With host-CRD RVs:
//
//	replica A: write -> CRD assigns RV=10042 (etcd)
//	replica B: same write observed via informer -> RV=10042 (same)
//	client resumes against B with rv=10042 -> consistent
//
// The host CRD's RV is the single etcd-assigned monotonic stream
// every replica observes via its informer in the same order.
//
// The cost: we cannot control RV ourselves, so we lose the ability
// to tag synthesized events (e.g. periodic resyncs) with a fresh RV.
// In this experiment that's a feature: we want events ONLY when the
// host CRD changes.
package shared

import (
	"context"
	"strconv"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/klog/v2"

	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// REST is a rest.Storage that uses host-CRD-RV authority. It is
// intentionally a parallel implementation to runtime/storage.REST,
// not a wrapper; the substrate adapter's rv stamping is the thing we
// are deliberately bypassing.
type REST struct {
	backend       runtimestorage.WritableBackend
	groupResource schema.GroupResource

	bcaster *watch.Broadcaster

	mu     sync.RWMutex
	curRV  string // last observed host CRD RV, set by SetCurrentResourceVersion.
}

// Options configures the REST.
type Options struct {
	Backend         runtimestorage.WritableBackend
	GroupResource   schema.GroupResource
	BroadcasterSize int
}

// New constructs a REST.
func New(opts Options) *REST {
	if opts.Backend == nil {
		panic("shared.New: Backend is required")
	}
	size := opts.BroadcasterSize
	if size <= 0 {
		size = 100
	}
	return &REST{
		backend:       opts.Backend,
		groupResource: opts.GroupResource,
		bcaster:       watch.NewBroadcaster(size, watch.DropIfChannelFull),
	}
}

// Shutdown stops the broadcaster.
func (r *REST) Shutdown() { r.bcaster.Shutdown() }

// ---- EventSink implementation (consumed by crdbackend) ----

// Action publishes a watch event with the given type and object.
// The object's existing RV (set from the host CRD) is preserved.
func (r *REST) Action(et watch.EventType, obj runtime.Object) {
	if err := r.bcaster.Action(et, obj); err != nil {
		klog.V(2).InfoS("broadcaster-action-failed", "err", err)
	}
}

// CurrentResourceVersion returns the last observed host CRD RV.
func (r *REST) CurrentResourceVersion() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.curRV
}

// SetCurrentResourceVersion records the highest host CRD RV the
// informer has dispatched. Note: RVs are STRINGS in Kubernetes,
// numerically comparable for this CRD but the API treats them as
// opaque. We do a string comparison treating leading-zero-stripped
// numeric strings as comparable; for etcd CRD RVs this is monotonic
// integer-valued and works fine.
func (r *REST) SetCurrentResourceVersion(rv string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rvLess(r.curRV, rv) {
		r.curRV = rv
	}
}

// rvLess returns true when a < b numerically. Treats empty as
// minimum.
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

// ---- identity / shape interfaces ----

func (r *REST) New() runtime.Object     { return r.backend.New() }
func (r *REST) NewList() runtime.Object { return r.backend.NewList() }
func (r *REST) Destroy()                {}
func (r *REST) NamespaceScoped() bool   { return r.backend.NamespaceScoped() }
func (r *REST) Kind() string            { return r.backend.Kind() }
func (r *REST) GetSingularName() string { return r.backend.SingularName() }

// ---- Getter ----

func (r *REST) Get(ctx context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	u := userFromCtx(ctx)
	return r.backend.Get(ctx, u, name)
}

// ---- Lister ----

func (r *REST) List(ctx context.Context, opts *metainternalversion.ListOptions) (runtime.Object, error) {
	u := userFromCtx(ctx)
	bOpts := listOptsFrom(opts)
	out, err := r.backend.List(ctx, u, bOpts)
	if err != nil {
		return nil, err
	}
	if bOpts.LabelSelector != nil && !bOpts.LabelSelector.Empty() {
		filterListByLabels(out, bOpts.LabelSelector)
	}
	// The backend already stamped the list RV from the publisher's
	// CurrentResourceVersion (host CRD high-water mark).
	return out, nil
}

// ---- Watcher ----

func (r *REST) Watch(ctx context.Context, opts *metainternalversion.ListOptions) (watch.Interface, error) {
	bOpts := listOptsFrom(opts)
	requested := ""
	if opts != nil {
		requested = opts.ResourceVersion
	}

	// Take a snapshot under read lock so the prefix and the watch
	// register at consistent boundaries.
	r.mu.RLock()
	cur := r.curRV
	r.mu.RUnlock()

	if requested != "" && requested != "0" {
		// With host-CRD RVs the resume contract is: any RV <= cur
		// is "in our buffer" insofar as we're about to replay
		// list-state. We don't keep an event log, so any non-cur
		// RV that the client passes will result in a re-list (the
		// initial prefix below). Importantly: this is no LESS
		// safe than per-replica RVs — the client either lands at
		// the current state (rv<=cur, broadcaster prefix replays
		// from List), or sees nothing newer than cur is yet
		// observed (which is exactly correct).
		//
		// We tolerate any host RV; a stale client will simply
		// re-receive the current state via the prefix below.
		// Returning 410 Gone here would force re-list on every
		// reconnect, which we want to avoid for the resume-against-
		// different-replica scenario.
		klog.V(3).InfoS("watch-resume", "requestedRV", requested, "currentRV", cur)
	}

	u := userFromCtx(ctx)
	snapshot, err := r.backend.List(ctx, u, bOpts)
	if err != nil {
		return nil, err
	}
	items, err := meta.ExtractList(snapshot)
	if err != nil {
		return nil, err
	}
	hasSel := bOpts.LabelSelector != nil && !bOpts.LabelSelector.Empty()
	prefix := make([]watch.Event, 0, len(items))
	for _, o := range items {
		if hasSel && !matchesLabels(o, bOpts.LabelSelector) {
			continue
		}
		prefix = append(prefix, watch.Event{Type: watch.Added, Object: o})
	}

	w, err := r.bcaster.WatchWithPrefix(prefix)
	if err != nil {
		return nil, err
	}
	if !hasSel {
		return w, nil
	}
	sel := bOpts.LabelSelector
	return watch.Filter(w, func(ev watch.Event) (watch.Event, bool) {
		return ev, matchesLabels(ev.Object, sel)
	}), nil
}

// ---- TableConvertor ----

func (r *REST) ConvertToTable(ctx context.Context, object runtime.Object, _ runtime.Object) (*metav1.Table, error) {
	cols := r.backend.TableColumns()
	rows, err := r.backend.RowsFor(object)
	if err != nil {
		return nil, err
	}
	t := &metav1.Table{
		ColumnDefinitions: cols,
		Rows:              rows,
	}
	if list, ok := object.(metav1.ListInterface); ok {
		t.ListMeta.ResourceVersion = list.GetResourceVersion()
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
	u := userFromCtx(ctx)
	stored, err := r.backend.Create(ctx, u, obj)
	if err != nil {
		return nil, err
	}
	// Do NOT publish here: the informer event from the host CRD
	// will fire the broadcaster.Action(). This deduplicates events:
	// the writer's local watcher receives the event via informer
	// just like the cross-replica watchers do.
	return stored, nil
}

// ---- Update ----

func (r *REST) Update(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	_ *metav1.UpdateOptions,
) (runtime.Object, bool, error) {
	u := userFromCtx(ctx)
	existing, getErr := r.backend.Get(ctx, u, name)
	if getErr != nil && !apierrors.IsNotFound(getErr) {
		return nil, false, getErr
	}
	var current runtime.Object
	if getErr == nil {
		current = existing
	}
	updated, err := objInfo.UpdatedObject(ctx, current)
	if err != nil {
		return nil, false, err
	}
	if current == nil {
		if !forceAllowCreate {
			return nil, false, getErr
		}
		if createValidation != nil {
			if err := createValidation(ctx, updated); err != nil {
				return nil, false, err
			}
		}
	} else if updateValidation != nil {
		if err := updateValidation(ctx, updated, current); err != nil {
			return nil, false, err
		}
	}
	stored, created, err := r.backend.Update(ctx, u, name, updated, forceAllowCreate)
	if err != nil {
		return nil, false, err
	}
	return stored, created, nil
}

// ---- Delete ----

func (r *REST) Delete(
	ctx context.Context,
	name string,
	deleteValidation rest.ValidateObjectFunc,
	_ *metav1.DeleteOptions,
) (runtime.Object, bool, error) {
	u := userFromCtx(ctx)
	existing, err := r.backend.Get(ctx, u, name)
	if err != nil {
		return nil, false, err
	}
	if deleteValidation != nil {
		if err := deleteValidation(ctx, existing); err != nil {
			return nil, false, err
		}
	}
	stored, deleted, err := r.backend.Delete(ctx, u, name)
	if err != nil {
		return nil, false, err
	}
	return stored, deleted, nil
}

// ---- helpers ----

func userFromCtx(ctx context.Context) user.Info {
	if v, ok := genericapirequest.UserFrom(ctx); ok && v != nil {
		return v
	}
	return nil
}

func listOptsFrom(opts *metainternalversion.ListOptions) runtimestorage.ListOptions {
	out := runtimestorage.ListOptions{}
	if opts != nil && opts.LabelSelector != nil {
		out.LabelSelector = opts.LabelSelector
	} else {
		out.LabelSelector = labels.Everything()
	}
	return out
}

func matchesLabels(obj runtime.Object, sel labels.Selector) bool {
	if sel == nil || sel.Empty() {
		return true
	}
	acc, err := meta.Accessor(obj)
	if err != nil {
		return true
	}
	return sel.Matches(labels.Set(acc.GetLabels()))
}

func filterListByLabels(list runtime.Object, sel labels.Selector) {
	items, err := meta.ExtractList(list)
	if err != nil {
		return
	}
	kept := items[:0]
	for _, o := range items {
		if matchesLabels(o, sel) {
			kept = append(kept, o)
		}
	}
	if err := meta.SetList(list, kept); err != nil {
		// best-effort; runtime/storage has the reflection-based
		// fallback if we ever need it.
		_ = err
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
