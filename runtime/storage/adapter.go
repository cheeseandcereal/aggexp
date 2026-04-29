package storage

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
)

// REST wraps a Backend into a full rest.Storage implementation. It
// always implements:
//
//	rest.Storage, rest.Scoper, rest.KindProvider,
//	rest.SingularNameProvider, rest.Getter, rest.Lister,
//	rest.Watcher, rest.TableConvertor
//
// Additionally, when the wrapped backend implements WritableBackend,
// the REST satisfies rest.Creater, rest.Updater, rest.Patcher, and
// rest.GracefulDeleter.
type REST struct {
	backend       Backend
	writable      WritableBackend // non-nil iff backend implements WritableBackend
	groupResource schema.GroupResource

	rv      atomic.Uint64
	bcaster *watch.Broadcaster
}

// Options configures a REST adapter.
type Options struct {
	// Backend is the resource-specific data plane. Required.
	Backend Backend
	// GroupResource is used only in error messages when writes are
	// refused on a read-only backend. Optional but recommended.
	GroupResource schema.GroupResource
	// BroadcasterSize is the watch.Broadcaster queue size.
	// Defaults to 100 if 0. Chosen to match 0002/0004's setting.
	BroadcasterSize int
}

// New constructs a REST adapter. It starts at RV=1 so the first
// emitted event arrives at RV=2.
func New(opts Options) *REST {
	if opts.Backend == nil {
		panic("storage.New: Backend is required")
	}
	size := opts.BroadcasterSize
	if size <= 0 {
		size = 100
	}
	r := &REST{
		backend:       opts.Backend,
		groupResource: opts.GroupResource,
		bcaster:       watch.NewBroadcaster(size, watch.DropIfChannelFull),
	}
	if w, ok := opts.Backend.(WritableBackend); ok {
		r.writable = w
	}
	r.rv.Store(1)
	return r
}

// Shutdown stops the internal broadcaster.
func (r *REST) Shutdown() { r.bcaster.Shutdown() }

// Backend returns the wrapped backend, useful in tests.
func (r *REST) Backend() Backend { return r.backend }

// Writable returns whether the backend supports writes.
func (r *REST) Writable() bool { return r.writable != nil }

// CurrentResourceVersion returns the current RV as a decimal string.
func (r *REST) CurrentResourceVersion() string {
	return strconv.FormatUint(r.rv.Load(), 10)
}

// NextResourceVersion bumps and returns the next RV.
func (r *REST) NextResourceVersion() string {
	return strconv.FormatUint(r.rv.Add(1), 10)
}

// ---- Publisher ----

// PublishAdded bumps RV, stamps it on the object, and emits ADDED.
func (r *REST) PublishAdded(obj runtime.Object) {
	stampRV(obj, r.NextResourceVersion())
	_ = r.bcaster.Action(watch.Added, obj)
}

// PublishModified bumps RV, stamps it, and emits MODIFIED.
func (r *REST) PublishModified(obj runtime.Object) {
	stampRV(obj, r.NextResourceVersion())
	_ = r.bcaster.Action(watch.Modified, obj)
}

// PublishDeleted bumps RV, stamps it, and emits DELETED.
func (r *REST) PublishDeleted(obj runtime.Object) {
	stampRV(obj, r.NextResourceVersion())
	_ = r.bcaster.Action(watch.Deleted, obj)
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
		filterList(out, bOpts.LabelSelector)
	}
	setListRV(out, r.CurrentResourceVersion())
	return out, nil
}

// ---- Watcher ----

func (r *REST) Watch(ctx context.Context, opts *metainternalversion.ListOptions) (watch.Interface, error) {
	bOpts := listOptsFrom(opts)
	requested := ""
	if opts != nil {
		requested = opts.ResourceVersion
	}
	if requested != "" && requested != "0" {
		reqN, perr := strconv.ParseUint(requested, 10, 64)
		if perr != nil || reqN != r.rv.Load() {
			return nil, apierrors.NewResourceExpired(fmt.Sprintf(
				"too old resource version: %s (current %s)", requested, r.CurrentResourceVersion()))
		}
	}

	u := userFromCtx(ctx)
	snapshot, err := r.backend.List(ctx, u, bOpts)
	if err != nil {
		return nil, err
	}
	items, err := listItems(snapshot)
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
	t := &metav1.Table{
		ColumnDefinitions: r.backend.TableColumns(),
	}
	rows, err := r.backend.RowsFor(object)
	if err != nil {
		return nil, err
	}
	t.Rows = rows
	if list, ok := object.(metav1.ListInterface); ok {
		t.ListMeta.ResourceVersion = list.GetResourceVersion()
	}
	return t, nil
}

// ---- writable path ----

// Create implements rest.Creater when the backend is writable.
func (r *REST) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, _ *metav1.CreateOptions) (runtime.Object, error) {
	if r.writable == nil {
		return nil, apierrors.NewMethodNotSupported(r.groupResource, "create")
	}
	if createValidation != nil {
		if err := createValidation(ctx, obj); err != nil {
			return nil, err
		}
	}
	u := userFromCtx(ctx)
	stored, err := r.writable.Create(ctx, u, obj)
	if err != nil {
		return nil, err
	}
	r.PublishAdded(stored)
	return stored, nil
}

// Update implements rest.Updater (and thus rest.Patcher).
func (r *REST) Update(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	_ *metav1.UpdateOptions,
) (runtime.Object, bool, error) {
	if r.writable == nil {
		return nil, false, apierrors.NewMethodNotSupported(r.groupResource, "update")
	}
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
			return nil, false, getErr // NotFound
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
	stored, created, err := r.writable.Update(ctx, u, name, updated, forceAllowCreate)
	if err != nil {
		return nil, false, err
	}
	if created {
		r.PublishAdded(stored)
	} else {
		r.PublishModified(stored)
	}
	return stored, created, nil
}

// Delete implements rest.GracefulDeleter.
func (r *REST) Delete(
	ctx context.Context,
	name string,
	deleteValidation rest.ValidateObjectFunc,
	_ *metav1.DeleteOptions,
) (runtime.Object, bool, error) {
	if r.writable == nil {
		return nil, false, apierrors.NewMethodNotSupported(r.groupResource, "delete")
	}
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
	stored, deleted, err := r.writable.Delete(ctx, u, name)
	if err != nil {
		return nil, false, err
	}
	if deleted {
		r.PublishDeleted(stored)
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

func listOptsFrom(opts *metainternalversion.ListOptions) ListOptions {
	out := ListOptions{}
	if opts != nil && opts.LabelSelector != nil {
		out.LabelSelector = opts.LabelSelector
	} else {
		out.LabelSelector = labels.Everything()
	}
	return out
}

// Compile-time interface assertions for the read path.
var (
	_ rest.Storage              = (*REST)(nil)
	_ rest.Scoper               = (*REST)(nil)
	_ rest.KindProvider         = (*REST)(nil)
	_ rest.SingularNameProvider = (*REST)(nil)
	_ rest.Getter               = (*REST)(nil)
	_ rest.Lister               = (*REST)(nil)
	_ rest.Watcher              = (*REST)(nil)
	_ rest.TableConvertor       = (*REST)(nil)
)
