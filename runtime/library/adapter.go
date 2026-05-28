package library

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"

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

	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// REST is an enhanced rest.Storage adapter that composes all library
// capabilities on top of a runtime/storage.Backend. It extends the
// v1 runtime/storage.REST with:
//
//   - Deterministic UIDs
//   - Pagination (limit+continue)
//   - Field selectors
//   - Optimistic concurrency control
//   - WatchList BOOKMARK emission
//
// Each capability is independently enabled via Options.
type REST struct {
	backend       runtimestorage.Backend
	writable      runtimestorage.WritableBackend
	groupResource schema.GroupResource

	rv      atomic.Uint64
	bcaster *watch.Broadcaster

	// Capabilities (nil if disabled).
	occ     *occTracker
	fieldCfg *fieldSelectorConfig

	// Flags.
	deterministicUIDs bool
	pagination        bool
	bookmark          bool
}

// New constructs an enhanced REST adapter.
func New(opts Options) *REST {
	if opts.Backend == nil {
		panic("library.New: Backend is required")
	}
	size := opts.BroadcasterSize
	if size <= 0 {
		size = 100
	}
	r := &REST{
		backend:           opts.Backend,
		groupResource:     opts.GroupResource,
		bcaster:           watch.NewBroadcaster(size, watch.DropIfChannelFull),
		deterministicUIDs: opts.DeterministicUIDs,
		pagination:        opts.Pagination,
		bookmark:          opts.Bookmark,
	}
	if w, ok := opts.Backend.(runtimestorage.WritableBackend); ok {
		r.writable = w
	}
	if opts.OptimisticConcurrency {
		r.occ = newOCCTracker(opts.GroupResource)
	}
	if opts.FieldSelectors != nil {
		r.fieldCfg = &fieldSelectorConfig{
			selectableFields: opts.FieldSelectors.SelectableFields,
			accessor:         opts.FieldSelectors.Accessor,
		}
	}
	r.rv.Store(1)
	return r
}

// Shutdown stops the internal broadcaster.
func (r *REST) Shutdown() { r.bcaster.Shutdown() }

// Backend returns the wrapped backend.
func (r *REST) Backend() runtimestorage.Backend { return r.backend }

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
	if r.occ != nil {
		r.occ.recordRV(obj)
	}
	_ = r.bcaster.Action(watch.Added, obj)
}

// PublishModified bumps RV, stamps it, and emits MODIFIED.
func (r *REST) PublishModified(obj runtime.Object) {
	stampRV(obj, r.NextResourceVersion())
	if r.occ != nil {
		r.occ.recordRV(obj)
	}
	_ = r.bcaster.Action(watch.Modified, obj)
}

// PublishDeleted bumps RV, stamps it, and emits DELETED.
func (r *REST) PublishDeleted(obj runtime.Object) {
	stampRV(obj, r.NextResourceVersion())
	if r.occ != nil {
		name := objectNameFromObj(obj)
		r.occ.deleteRV(name)
	}
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
	obj, err := r.backend.Get(ctx, u, name)
	if err != nil {
		return nil, err
	}
	if r.occ != nil {
		r.occ.stampRV(obj, name)
	}
	return obj, nil
}

// ---- Lister ----

func (r *REST) List(ctx context.Context, opts *metainternalversion.ListOptions) (runtime.Object, error) {
	// Validate field selector early.
	var fieldSel interface{ Empty() bool }
	if r.fieldCfg != nil && opts != nil && opts.FieldSelector != nil && !opts.FieldSelector.Empty() {
		if err := r.fieldCfg.validate(opts.FieldSelector); err != nil {
			return nil, err
		}
		fieldSel = opts.FieldSelector
	} else if opts != nil && opts.FieldSelector != nil && !opts.FieldSelector.Empty() && r.fieldCfg == nil {
		// Field selectors not configured — reject.
		reqs := opts.FieldSelector.Requirements()
		for _, req := range reqs {
			if req.Field != "metadata.name" && req.Field != "metadata.namespace" {
				return nil, apierrors.NewBadRequest(
					fmt.Sprintf("field label not supported: %s", req.Field))
			}
		}
		// Only metadata.name/namespace used — we can handle those without FieldAccessor.
		fieldSel = opts.FieldSelector
	}

	u := userFromCtx(ctx)
	bOpts := listOptsFrom(opts)
	out, err := r.backend.List(ctx, u, bOpts)
	if err != nil {
		return nil, err
	}

	// Apply label filtering.
	if bOpts.LabelSelector != nil && !bOpts.LabelSelector.Empty() {
		filterList(out, bOpts.LabelSelector)
	}

	// Apply field selector filtering.
	if fieldSel != nil {
		if r.fieldCfg != nil {
			r.fieldCfg.filterListByField(out, opts.FieldSelector)
		} else {
			// Handle metadata.name/namespace without a FieldAccessor.
			basicFieldCfg := &fieldSelectorConfig{}
			basicFieldCfg.filterListByField(out, opts.FieldSelector)
		}
	}

	currentRV := r.CurrentResourceVersion()

	// Apply pagination if enabled.
	if r.pagination {
		var limit int64
		var continueToken string
		if opts != nil {
			limit = opts.Limit
			continueToken = opts.Continue
		}
		if err := paginateList(out, limit, continueToken, currentRV); err != nil {
			return nil, err
		}
	} else {
		setListRV(out, currentRV)
	}

	// Stamp per-object RVs if OCC is enabled.
	if r.occ != nil {
		r.occ.stampListRVs(out)
	}

	return out, nil
}

// ---- Watcher ----

func (r *REST) Watch(ctx context.Context, opts *metainternalversion.ListOptions) (watch.Interface, error) {
	// Validate field selector.
	if r.fieldCfg != nil && opts != nil && opts.FieldSelector != nil && !opts.FieldSelector.Empty() {
		if err := r.fieldCfg.validate(opts.FieldSelector); err != nil {
			return nil, err
		}
	}

	bOpts := listOptsFrom(opts)
	requested := ""
	allowBookmarks := false
	if opts != nil {
		requested = opts.ResourceVersion
		if opts.AllowWatchBookmarks {
			allowBookmarks = true
		}
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

	hasLabelSel := bOpts.LabelSelector != nil && !bOpts.LabelSelector.Empty()
	hasFieldSel := r.fieldCfg != nil && opts != nil && opts.FieldSelector != nil && !opts.FieldSelector.Empty()

	prefix := make([]watch.Event, 0, len(items)+1)
	for _, o := range items {
		if hasLabelSel && !matchesLabels(o, bOpts.LabelSelector) {
			continue
		}
		if hasFieldSel && !r.fieldCfg.matchesField(o, opts.FieldSelector) {
			continue
		}
		prefix = append(prefix, watch.Event{Type: watch.Added, Object: o})
	}

	// Emit BOOKMARK at tail of prefix if enabled and allowed.
	if r.bookmark && shouldEmitBookmark(allowBookmarks) {
		bmEvent := makeBookmarkEvent(r.backend.New, r.CurrentResourceVersion())
		prefix = append(prefix, bmEvent)
	}

	w, err := r.bcaster.WatchWithPrefix(prefix)
	if err != nil {
		return nil, err
	}

	if !hasLabelSel && !hasFieldSel {
		return w, nil
	}
	labelSel := bOpts.LabelSelector
	return watch.Filter(w, func(ev watch.Event) (watch.Event, bool) {
		if ev.Type == watch.Bookmark {
			return ev, true
		}
		if hasLabelSel && !matchesLabels(ev.Object, labelSel) {
			return ev, false
		}
		if hasFieldSel && !r.fieldCfg.matchesField(ev.Object, opts.FieldSelector) {
			return ev, false
		}
		return ev, true
	}), nil
}

// ---- TableConvertor ----

func (r *REST) ConvertToTable(ctx context.Context, object runtime.Object, tableOpts runtime.Object) (*metav1.Table, error) {
	t := &metav1.Table{
		ColumnDefinitions: r.backend.TableColumns(),
	}
	rows, err := r.backend.RowsFor(object)
	if err != nil {
		return nil, err
	}
	t.Rows = rows

	// Populate Object on each row for PartialObjectMetadata extraction.
	if meta.IsListType(object) {
		items, extractErr := meta.ExtractList(object)
		if extractErr == nil && len(items) == len(t.Rows) {
			for i := range t.Rows {
				t.Rows[i].Object = runtime.RawExtension{Object: items[i]}
			}
		}
		if li, ok := object.(metav1.ListInterface); ok {
			t.ListMeta.ResourceVersion = li.GetResourceVersion()
			t.ListMeta.Continue = li.GetContinue()
			t.ListMeta.RemainingItemCount = li.GetRemainingItemCount()
		}
	} else {
		if len(t.Rows) == 1 {
			t.Rows[0].Object = runtime.RawExtension{Object: object}
		}
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

	// Stamp deterministic UID if enabled.
	if r.deterministicUIDs {
		acc, err := meta.Accessor(obj)
		if err == nil {
			uid := DeterministicUID(r.groupResource, acc.GetNamespace(), acc.GetName())
			acc.SetUID(uid)
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
		// Stamp the stored RV on current so objInfo.UpdatedObject sees it.
		if r.occ != nil {
			r.occ.stampRV(current, name)
		}
	}

	updated, err := objInfo.UpdatedObject(ctx, current)
	if err != nil {
		return nil, false, err
	}

	// OCC check.
	if r.occ != nil && current != nil {
		if err := r.occ.checkConflict(name, current, updated); err != nil {
			return nil, false, err
		}
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
		// Stamp deterministic UID on create-on-update.
		if r.deterministicUIDs {
			acc, aerr := meta.Accessor(updated)
			if aerr == nil {
				uid := DeterministicUID(r.groupResource, acc.GetNamespace(), acc.GetName())
				acc.SetUID(uid)
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

func listOptsFrom(opts *metainternalversion.ListOptions) runtimestorage.ListOptions {
	out := runtimestorage.ListOptions{}
	if opts != nil && opts.LabelSelector != nil {
		out.LabelSelector = opts.LabelSelector
	} else {
		out.LabelSelector = labels.Everything()
	}
	return out
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
	_ runtimestorage.Publisher  = (*REST)(nil)
)
