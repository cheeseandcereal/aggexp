// Package storage provides a REST adapter that extends runtime/storage.REST
// with field-selector support. This is a local fork/wrapper for experiment
// 0037; it does NOT modify the substrate.
//
// Approach: wrap runtimestorage.REST, override List and Watch to parse
// FieldSelector from metainternalversion.ListOptions, validate against
// declared selectable fields, and apply defensive filtering.
package storage

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"sync/atomic"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"

	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// FieldAccessor extracts a string value for a given field path from obj.
// Returns ("", false) if the field is unrecognized.
type FieldAccessor func(obj runtime.Object, field string) (string, bool)

// REST wraps a Backend into a full rest.Storage with field selector support.
type REST struct {
	backend          runtimestorage.Backend
	writable         runtimestorage.WritableBackend
	groupResource    schema.GroupResource
	selectableFields []string
	fieldAccessor    FieldAccessor

	rv      atomic.Uint64
	bcaster *watch.Broadcaster
}

// Options configures the field-selector-aware REST adapter.
type Options struct {
	Backend          runtimestorage.Backend
	GroupResource    schema.GroupResource
	BroadcasterSize  int
	SelectableFields []string
	FieldAccessor    FieldAccessor
}

// New constructs a field-selector-aware REST adapter.
func New(opts Options) *REST {
	if opts.Backend == nil {
		panic("storage.New: Backend is required")
	}
	size := opts.BroadcasterSize
	if size <= 0 {
		size = 100
	}
	r := &REST{
		backend:          opts.Backend,
		groupResource:    opts.GroupResource,
		selectableFields: opts.SelectableFields,
		fieldAccessor:    opts.FieldAccessor,
		bcaster:          watch.NewBroadcaster(size, watch.DropIfChannelFull),
	}
	if w, ok := opts.Backend.(runtimestorage.WritableBackend); ok {
		r.writable = w
	}
	r.rv.Store(1)
	return r
}

func (r *REST) Shutdown() { r.bcaster.Shutdown() }

func (r *REST) CurrentResourceVersion() string {
	return strconv.FormatUint(r.rv.Load(), 10)
}

func (r *REST) NextResourceVersion() string {
	return strconv.FormatUint(r.rv.Add(1), 10)
}

// ---- Publisher ----

func (r *REST) PublishAdded(obj runtime.Object) {
	stampRV(obj, r.NextResourceVersion())
	_ = r.bcaster.Action(watch.Added, obj)
}

func (r *REST) PublishModified(obj runtime.Object) {
	stampRV(obj, r.NextResourceVersion())
	_ = r.bcaster.Action(watch.Modified, obj)
}

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

// ---- Lister (with field selector support) ----

func (r *REST) List(ctx context.Context, opts *metainternalversion.ListOptions) (runtime.Object, error) {
	// Parse and validate field selector
	fieldSel, err := r.parseAndValidateFieldSelector(opts)
	if err != nil {
		return nil, err
	}

	u := userFromCtx(ctx)
	bOpts := listOptsFrom(opts)
	out, err := r.backend.List(ctx, u, bOpts)
	if err != nil {
		return nil, err
	}

	// Apply label filtering
	if bOpts.LabelSelector != nil && !bOpts.LabelSelector.Empty() {
		filterList(out, bOpts.LabelSelector)
	}

	// Apply field selector filtering
	if fieldSel != nil && !fieldSel.Empty() {
		r.filterListByField(out, fieldSel)
	}

	setListRV(out, r.CurrentResourceVersion())
	return out, nil
}

// ---- Watcher (with field selector support) ----

func (r *REST) Watch(ctx context.Context, opts *metainternalversion.ListOptions) (watch.Interface, error) {
	// Parse and validate field selector
	fieldSel, err := r.parseAndValidateFieldSelector(opts)
	if err != nil {
		return nil, err
	}

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

	hasLabelSel := bOpts.LabelSelector != nil && !bOpts.LabelSelector.Empty()
	hasFieldSel := fieldSel != nil && !fieldSel.Empty()

	prefix := make([]watch.Event, 0, len(items))
	for _, o := range items {
		if hasLabelSel && !matchesLabels(o, bOpts.LabelSelector) {
			continue
		}
		if hasFieldSel && !r.matchesField(o, fieldSel) {
			continue
		}
		prefix = append(prefix, watch.Event{Type: watch.Added, Object: o})
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
		if hasLabelSel && !matchesLabels(ev.Object, labelSel) {
			return ev, false
		}
		if hasFieldSel && !r.matchesField(ev.Object, fieldSel) {
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

	// Set Object on each row to the source object (for partial object metadata inclusion).
	// For single objects, we set the same object on the only row.
	// For lists, we correlate rows with list items.
	switch v := object.(type) {
	case metav1.ListInterface:
		t.ListMeta.ResourceVersion = v.GetResourceVersion()
		items, _ := meta.ExtractList(object)
		for i := range t.Rows {
			if i < len(items) {
				t.Rows[i].Object = runtime.RawExtension{Object: items[i]}
			}
		}
	default:
		if len(t.Rows) == 1 {
			t.Rows[0].Object = runtime.RawExtension{Object: object}
		}
	}
	return t, nil
}

// ---- writable path (unchanged from substrate) ----

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

// ---- field selector helpers ----

func (r *REST) parseAndValidateFieldSelector(opts *metainternalversion.ListOptions) (fields.Selector, error) {
	if opts == nil || opts.FieldSelector == nil || opts.FieldSelector.Empty() {
		return nil, nil
	}
	sel := opts.FieldSelector
	// Validate all fields in the selector are known
	known := map[string]bool{
		"metadata.name":      true,
		"metadata.namespace": true,
	}
	for _, f := range r.selectableFields {
		known[f] = true
	}
	reqs := sel.Requirements()
	for _, req := range reqs {
		if !known[req.Field] {
			return nil, apierrors.NewBadRequest(
				fmt.Sprintf("field label not supported: %s", req.Field))
		}
	}
	return sel, nil
}

func (r *REST) matchesField(obj runtime.Object, sel fields.Selector) bool {
	if sel == nil || sel.Empty() {
		return true
	}
	reqs := sel.Requirements()
	acc, _ := meta.Accessor(obj)
	for _, req := range reqs {
		var val string
		switch req.Field {
		case "metadata.name":
			if acc != nil {
				val = acc.GetName()
			}
		case "metadata.namespace":
			if acc != nil {
				val = acc.GetNamespace()
			}
		default:
			if r.fieldAccessor != nil {
				v, ok := r.fieldAccessor(obj, req.Field)
				if !ok {
					return false
				}
				val = v
			} else {
				return false
			}
		}
		switch req.Operator {
		case "=", "==":
			if val != req.Value {
				return false
			}
		case "!=":
			if val == req.Value {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func (r *REST) filterListByField(list runtime.Object, sel fields.Selector) {
	items, err := meta.ExtractList(list)
	if err != nil {
		return
	}
	kept := items[:0]
	for _, o := range items {
		if r.matchesField(o, sel) {
			kept = append(kept, o)
		}
	}
	if err := meta.SetList(list, kept); err != nil {
		setListItemsReflect(list, kept)
	}
}

// ---- shared helpers (from substrate, not modifying it) ----

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

func stampRV(obj runtime.Object, rv string) {
	if obj == nil {
		return
	}
	if acc, err := meta.Accessor(obj); err == nil {
		acc.SetResourceVersion(rv)
	}
}

func setListRV(list runtime.Object, rv string) {
	if list == nil {
		return
	}
	if li, ok := list.(metav1.ListInterface); ok {
		li.SetResourceVersion(rv)
	}
}

func listItems(list runtime.Object) ([]runtime.Object, error) {
	if list == nil {
		return nil, nil
	}
	return meta.ExtractList(list)
}

func filterList(list runtime.Object, sel labels.Selector) {
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
		setListItemsReflect(list, kept)
	}
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

// setListItemsReflect replaces list.Items via reflection.
func setListItemsReflect(list runtime.Object, kept []runtime.Object) {
	v := reflect.ValueOf(list)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return
	}
	v = v.Elem()
	field := v.FieldByName("Items")
	if !field.IsValid() || !field.CanSet() || field.Kind() != reflect.Slice {
		return
	}
	elemType := field.Type().Elem()
	newSlice := reflect.MakeSlice(field.Type(), len(kept), len(kept))
	for i, o := range kept {
		ov := reflect.ValueOf(o)
		if elemType.Kind() == reflect.Ptr {
			newSlice.Index(i).Set(ov)
			continue
		}
		if ov.Kind() == reflect.Ptr {
			ov = ov.Elem()
		}
		newSlice.Index(i).Set(ov)
	}
	field.Set(newSlice)
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
)
