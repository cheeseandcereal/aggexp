// Package grpcbackend implements a rest.Storage that proxies to the
// gRPC backend. It is the inverse of runtime/storage: where that
// package wraps a typed, in-process Go Backend, this one wraps a
// network call to a separate process that doesn't know about
// k8s.io/apiserver.
//
// Objects on the wire are JSON bytes. Inside the component server
// we use *unstructured.Unstructured for everything. This sidesteps
// compile-time type registration but loses Server-Side Apply's
// typed-merge semantics and narrows what kubectl explain can do.
package grpcbackend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/klog/v2"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	krmv1 "github.com/cheeseandcereal/aggexp/experiments/0017-krm-protocol-refinement/gen/aggexp/krm/v1"
	"github.com/cheeseandcereal/aggexp/experiments/0017-krm-protocol-refinement/component/pkg/dyn"
)

// Descriptor is the subset of the schema needed by this package.
type Descriptor struct {
	GroupVersion schema.GroupVersion
	Resource     string
	Kind         string
	Singular     string
	Namespaced   bool
	Writable     bool
	// SupportsServerSideApply, if true, indicates the backend has
	// explicitly declared SSA compatibility. The component server
	// still implements rest.Patcher regardless (because merge and
	// json patch are always available once Update is available);
	// SSA is a library decision keyed off Patcher + OpenAPIV3Config.
	SupportsServerSideApply bool
	// UseTypedWrapper, when true, makes New/NewList return
	// *dyn.Object / *dyn.ObjectList (typed wrappers that unblock
	// the library's SSA empty-object-GVK path). When false, the
	// classic *unstructured.Unstructured path is used.
	UseTypedWrapper bool
	Columns         []metav1.TableColumnDefinition
	RowFields       []string // jsonpath-style lookups; same length as Columns
	GroupResource   schema.GroupResource
}

// REST is the rest.Storage that proxies to the gRPC backend. It
// maintains its own resourceVersion counter and watch.Broadcaster
// so the component server generates conformant watch streams even
// if the backend's protocol never surfaces its own RVs.
type REST struct {
	desc   Descriptor
	client krmv1.BackendClient

	rv      atomic.Uint64
	bcaster *watch.Broadcaster
}

// New constructs a REST.
func New(desc Descriptor, client krmv1.BackendClient) *REST {
	r := &REST{
		desc:    desc,
		client:  client,
		bcaster: watch.NewBroadcaster(100, watch.DropIfChannelFull),
	}
	r.rv.Store(1)
	if desc.GroupResource == (schema.GroupResource{}) {
		desc.GroupResource = schema.GroupResource{Group: desc.GroupVersion.Group, Resource: desc.Resource}
		r.desc = desc
	}
	return r
}

// Shutdown stops the broadcaster. Called on server-stop hook.
func (r *REST) Shutdown() {
	if r.bcaster != nil {
		r.bcaster.Shutdown()
	}
}

// ---- identity / shape ----

func (r *REST) New() runtime.Object {
	gvk := r.desc.GroupVersion.WithKind(r.desc.Kind)
	if r.desc.UseTypedWrapper {
		obj := &dyn.Object{}
		obj.GetObjectKind().SetGroupVersionKind(gvk)
		return obj
	}
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	return u
}

func (r *REST) NewList() runtime.Object {
	listGVK := r.desc.GroupVersion.WithKind(r.desc.Kind + "List")
	if r.desc.UseTypedWrapper {
		l := &dyn.ObjectList{}
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

// ---- Getter ----

func (r *REST) Get(ctx context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	resp, err := r.client.Get(ctx, &krmv1.GetRequest{
		User:      userFromCtx(ctx),
		Namespace: ns,
		Name:      name,
	})
	if err != nil {
		return nil, r.translateErr(err, name)
	}
	return r.objFromJSON(resp.GetObjectJson())
}

// ---- Lister ----

func (r *REST) List(ctx context.Context, opts *metainternalversion.ListOptions) (runtime.Object, error) {
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	sel := selectorString(opts)
	resp, err := r.client.List(ctx, &krmv1.ListRequest{
		User:          userFromCtx(ctx),
		Namespace:     ns,
		LabelSelector: sel,
	})
	if err != nil {
		return nil, r.translateErr(err, "")
	}
	if r.desc.UseTypedWrapper {
		list := r.NewList().(*dyn.ObjectList)
		for _, raw := range resp.GetItemsJson() {
			o, err := decodeDyn(raw)
			if err != nil {
				return nil, err
			}
			if !matchesLabelsDyn(o, opts) {
				continue
			}
			list.Items = append(list.Items, *o)
		}
		list.SetResourceVersion(r.CurrentResourceVersion())
		return list, nil
	}
	list := r.NewList().(*unstructured.UnstructuredList)
	for _, raw := range resp.GetItemsJson() {
		u, err := decodeUnstructured(raw)
		if err != nil {
			return nil, err
		}
		if !matchesLabels(u, opts) {
			continue
		}
		list.Items = append(list.Items, *u)
	}
	list.SetResourceVersion(r.CurrentResourceVersion())
	return list, nil
}

// ---- Watcher ----

func (r *REST) Watch(ctx context.Context, opts *metainternalversion.ListOptions) (watch.Interface, error) {
	// Per substrate convention: a non-current RV results in
	// ResourceExpired.
	if opts != nil && opts.ResourceVersion != "" && opts.ResourceVersion != "0" {
		reqN, perr := strconv.ParseUint(opts.ResourceVersion, 10, 64)
		if perr != nil || reqN != r.rv.Load() {
			return nil, apierrors.NewResourceExpired(fmt.Sprintf(
				"too old resource version: %s (current %s)", opts.ResourceVersion, r.CurrentResourceVersion()))
		}
	}
	// Seed the watcher with the current list, consistent with the
	// library's expectation: a LIST-then-WATCH pair.
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	snapshot, err := r.client.List(ctx, &krmv1.ListRequest{
		User:          userFromCtx(ctx),
		Namespace:     ns,
		LabelSelector: selectorString(opts),
	})
	if err != nil {
		return nil, r.translateErr(err, "")
	}
	prefix := make([]watch.Event, 0, len(snapshot.GetItemsJson()))
	for _, raw := range snapshot.GetItemsJson() {
		obj, err := r.objFromJSON(raw)
		if err != nil {
			return nil, err
		}
		if !r.matchesLabels(obj, opts) {
			continue
		}
		prefix = append(prefix, watch.Event{Type: watch.Added, Object: obj})
	}
	w, err := r.bcaster.WatchWithPrefix(prefix)
	if err != nil {
		return nil, err
	}
	// Apply label filter to live events.
	sel := selectorFromOpts(opts)
	if sel == nil || sel.Empty() {
		return w, nil
	}
	return watch.Filter(w, func(ev watch.Event) (watch.Event, bool) {
		if acc, err := meta.Accessor(ev.Object); err == nil {
			return ev, sel.Matches(labels.Set(acc.GetLabels()))
		}
		return ev, true
	}), nil
}

// StartUpstreamWatch opens a single long-lived Watch stream to the
// backend and fans incoming events into the broadcaster. Called once
// from a post-start hook. Retries on disconnect.
func (r *REST) StartUpstreamWatch(ctx context.Context) {
	go func() {
		for {
			if ctx.Err() != nil {
				return
			}
			err := r.runUpstreamWatch(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				klog.Warningf("upstream watch disconnected: %v; retrying in 2s", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	}()
}

func (r *REST) runUpstreamWatch(ctx context.Context) error {
	stream, err := r.client.Watch(ctx, &krmv1.WatchRequest{})
	if err != nil {
		return err
	}
	klog.Infof("upstream watch opened for %s", r.desc.GroupResource)
	for {
		ev, err := stream.Recv()
		if err != nil {
			return err
		}
		obj, derr := r.objFromJSON(ev.GetObjectJson())
		if derr != nil {
			klog.Warningf("upstream watch: decode failed: %v", derr)
			continue
		}
		switch ev.GetType() {
		case krmv1.EventType_EVENT_ADDED:
			r.publishAdded(obj)
		case krmv1.EventType_EVENT_MODIFIED:
			r.publishModified(obj)
		case krmv1.EventType_EVENT_DELETED:
			r.publishDeleted(obj)
		default:
			// bookmarks / unspecified: ignore for now.
		}
	}
}

func (r *REST) publishAdded(obj runtime.Object) {
	r.stamp(obj)
	_ = r.bcaster.Action(watch.Added, obj)
}
func (r *REST) publishModified(obj runtime.Object) {
	r.stamp(obj)
	_ = r.bcaster.Action(watch.Modified, obj)
}
func (r *REST) publishDeleted(obj runtime.Object) {
	r.stamp(obj)
	_ = r.bcaster.Action(watch.Deleted, obj)
}

func (r *REST) stamp(obj runtime.Object) {
	if acc, err := meta.Accessor(obj); err == nil {
		acc.SetResourceVersion(r.nextRV())
	}
}

func (r *REST) nextRV() string { return strconv.FormatUint(r.rv.Add(1), 10) }

// CurrentResourceVersion returns the RV as decimal.
func (r *REST) CurrentResourceVersion() string {
	return strconv.FormatUint(r.rv.Load(), 10)
}

// ---- Writer ----

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
	raw, err := encodeUnstructured(obj)
	if err != nil {
		return nil, apierrors.NewBadRequest(err.Error())
	}
	resp, err := r.client.Create(ctx, &krmv1.CreateRequest{
		User:         userFromCtx(ctx),
		Namespace:    ns,
		ObjectJson:   raw,
		FieldManager: createOptsFieldManager(opts),
	})
	if err != nil {
		return nil, r.translateErr(err, nameOf(obj))
	}
	stored, err := r.objFromJSON(resp.GetObjectJson())
	if err != nil {
		return nil, err
	}
	// The backend pushes a watch event via its own Watch stream;
	// don't double-publish here.
	return stored, nil
}

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
	// Fetch current so PATCH has something to merge against.
	var current runtime.Object
	cur, err := r.client.Get(ctx, &krmv1.GetRequest{
		User:      userFromCtx(ctx),
		Namespace: ns,
		Name:      name,
	})
	if err != nil {
		if st, ok := grpcstatus.FromError(err); ok && st.Code() == codes.NotFound {
			if !forceAllowCreate {
				return nil, false, apierrors.NewNotFound(r.desc.GroupResource, name)
			}
			// fall through to upsert path with current == nil
		} else {
			return nil, false, r.translateErr(err, name)
		}
	} else {
		current, err = r.objFromJSON(cur.GetObjectJson())
		if err != nil {
			return nil, false, err
		}
	}
	updated, err := objInfo.UpdatedObject(ctx, current)
	if err != nil {
		return nil, false, err
	}
	if current == nil {
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
	raw, err := encodeUnstructured(updated)
	if err != nil {
		return nil, false, apierrors.NewBadRequest(err.Error())
	}
	resp, err := r.client.Update(ctx, &krmv1.UpdateRequest{
		User:             userFromCtx(ctx),
		Namespace:        ns,
		Name:             name,
		ObjectJson:       raw,
		ForceAllowCreate: forceAllowCreate,
		FieldManager:     optsFieldManager(opts),
	})
	if err != nil {
		return nil, false, r.translateErr(err, name)
	}
	stored, err := r.objFromJSON(resp.GetObjectJson())
	if err != nil {
		return nil, false, err
	}
	return stored, resp.GetCreated(), nil
}

func (r *REST) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, _ *metav1.DeleteOptions) (runtime.Object, bool, error) {
	if !r.desc.Writable {
		return nil, false, apierrors.NewMethodNotSupported(r.desc.GroupResource, "delete")
	}
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	if deleteValidation != nil {
		cur, err := r.client.Get(ctx, &krmv1.GetRequest{
			User:      userFromCtx(ctx),
			Namespace: ns,
			Name:      name,
		})
		if err != nil {
			return nil, false, r.translateErr(err, name)
		}
		obj, err := r.objFromJSON(cur.GetObjectJson())
		if err != nil {
			return nil, false, err
		}
		if err := deleteValidation(ctx, obj); err != nil {
			return nil, false, err
		}
	}
	resp, err := r.client.Delete(ctx, &krmv1.DeleteRequest{
		User:      userFromCtx(ctx),
		Namespace: ns,
		Name:      name,
	})
	if err != nil {
		return nil, false, r.translateErr(err, name)
	}
	stored, err := r.objFromJSON(resp.GetObjectJson())
	if err != nil {
		return nil, false, err
	}
	return stored, resp.GetDeleted(), nil
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
	case *dyn.ObjectList:
		rows := make([]metav1.TableRow, 0, len(o.Items))
		for i := range o.Items {
			row, err := r.rowFromContent(o.Items[i].AsMap(), &o.Items[i])
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
		return rows, nil
	case *dyn.Object:
		row, err := r.rowFromContent(o.AsMap(), o)
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
		v := lookupField(content, path)
		if path == ".metadata.creationTimestamp" {
			if s, ok := v.(string); ok {
				v = ageOf(s)
			}
		}
		cells[i] = v
	}
	return metav1.TableRow{
		Cells:  cells,
		Object: runtime.RawExtension{Object: objForRow},
	}, nil
}

// ---- helpers ----

func (r *REST) objFromJSON(raw []byte) (runtime.Object, error) {
	if r.desc.UseTypedWrapper {
		return decodeDyn(raw)
	}
	return decodeUnstructured(raw)
}

func (r *REST) translateErr(err error, name string) error {
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

func decodeUnstructured(raw []byte) (*unstructured.Unstructured, error) {
	u := &unstructured.Unstructured{}
	if err := u.UnmarshalJSON(raw); err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("decode object: %w", err))
	}
	return u, nil
}

func decodeDyn(raw []byte) (*dyn.Object, error) {
	o := &dyn.Object{}
	if err := o.UnmarshalJSON(raw); err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("decode object: %w", err))
	}
	return o, nil
}

func encodeUnstructured(obj runtime.Object) ([]byte, error) {
	if u, ok := obj.(*unstructured.Unstructured); ok {
		return u.MarshalJSON()
	}
	if o, ok := obj.(*dyn.Object); ok {
		return o.MarshalJSON()
	}
	// Fallback: marshal whatever this is.
	return json.Marshal(obj)
}

func matchesLabelsDyn(o *dyn.Object, opts *metainternalversion.ListOptions) bool {
	sel := selectorFromOpts(opts)
	if sel.Empty() {
		return true
	}
	return sel.Matches(labels.Set(o.GetLabels()))
}

func optsFieldManager(o *metav1.UpdateOptions) string {
	if o == nil {
		return ""
	}
	return o.FieldManager
}

func createOptsFieldManager(o *metav1.CreateOptions) string {
	if o == nil {
		return ""
	}
	return o.FieldManager
}

func userFromCtx(ctx context.Context) *krmv1.UserInfo {
	v, ok := genericapirequest.UserFrom(ctx)
	if !ok || v == nil {
		return nil
	}
	return userToProto(v)
}

func userToProto(u user.Info) *krmv1.UserInfo {
	out := &krmv1.UserInfo{
		Name:   u.GetName(),
		Uid:    u.GetUID(),
		Groups: u.GetGroups(),
		Extra:  map[string]*krmv1.StringList{},
	}
	for k, v := range u.GetExtra() {
		out.Extra[k] = &krmv1.StringList{Values: v}
	}
	return out
}

func selectorString(opts *metainternalversion.ListOptions) string {
	if opts == nil || opts.LabelSelector == nil || opts.LabelSelector.Empty() {
		return ""
	}
	return opts.LabelSelector.String()
}

func selectorFromOpts(opts *metainternalversion.ListOptions) labels.Selector {
	if opts == nil || opts.LabelSelector == nil {
		return labels.Everything()
	}
	return opts.LabelSelector
}

func matchesLabels(u *unstructured.Unstructured, opts *metainternalversion.ListOptions) bool {
	sel := selectorFromOpts(opts)
	if sel.Empty() {
		return true
	}
	return sel.Matches(labels.Set(u.GetLabels()))
}

// matchesLabels (method form) accepts either decode target.
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

// lookupField implements a tiny JSONPath-ish "." lookup. It accepts
// strings like ".metadata.name". Returns nil for missing paths.
func lookupField(obj map[string]any, path string) any {
	if path == "" {
		return nil
	}
	parts := strings.Split(strings.TrimPrefix(path, "."), ".")
	var cur any = obj
	for _, p := range parts {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[p]
	}
	if cur == nil {
		return ""
	}
	return cur
}

func ageOf(ts string) string {
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return "<unknown>"
	}
	d := time.Since(t).Round(time.Second)
	return durationShort(d)
}

func durationShort(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

func nameOf(obj runtime.Object) string {
	if acc, err := meta.Accessor(obj); err == nil {
		return acc.GetName()
	}
	return ""
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
	_ rest.Creater              = (*REST)(nil)
	_ rest.Updater              = (*REST)(nil)
	_ rest.Patcher              = (*REST)(nil)
	_ rest.GracefulDeleter      = (*REST)(nil)
)
