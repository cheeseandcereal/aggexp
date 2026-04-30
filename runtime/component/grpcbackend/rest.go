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
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	componentpb "github.com/cheeseandcereal/aggexp/runtime/component/proto"
	componentscheme "github.com/cheeseandcereal/aggexp/runtime/component/scheme"
)

// Descriptor is the subset of the backend's GetSchemaResponse that
// the REST adapter needs to serve the resource.
type Descriptor struct {
	GroupVersion schema.GroupVersion
	Resource     string
	Kind         string
	Singular     string
	Namespaced   bool
	Writable     bool
	// SupportsServerSideApply mirrors the backend flag. The REST
	// implements rest.Patcher unconditionally; this flag is
	// observational.
	SupportsServerSideApply bool
	// UseTypedWrapper switches New/NewList between
	// *unstructured.Unstructured and *scheme.Object. Must match the
	// mode used to build the Scheme.
	UseTypedWrapper bool
	Columns         []metav1.TableColumnDefinition
	RowFields       []string // jsonpath-style; same length as Columns
	GroupResource   schema.GroupResource
}

// REST proxies rest.Storage traffic to a gRPC backend.
type REST struct {
	desc   Descriptor
	client componentpb.BackendClient

	rv      atomic.Uint64
	bcaster *watch.Broadcaster
}

// New constructs a REST. The broadcaster is sized at 100 events; at
// lab scale this has been sufficient. Production callers may want
// configurable sizing.
func New(desc Descriptor, client componentpb.BackendClient) *REST {
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

// Shutdown stops the broadcaster. Safe to call multiple times.
func (r *REST) Shutdown() {
	if r.bcaster != nil {
		r.bcaster.Shutdown()
	}
}

// ---- identity / shape ----

func (r *REST) New() runtime.Object {
	gvk := r.desc.GroupVersion.WithKind(r.desc.Kind)
	if r.desc.UseTypedWrapper {
		obj := &componentscheme.Object{}
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

// ---- Getter ----

func (r *REST) Get(ctx context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	resp, err := r.client.Get(ctx, &componentpb.GetRequest{
		User:      userFromCtx(ctx),
		Namespace: ns,
		Name:      name,
	})
	if err != nil {
		return nil, r.translateErr(err, name)
	}
	obj, derr := r.objFromJSON(resp.GetObjectJson())
	if derr != nil {
		return nil, derr
	}
	// Stamp a resourceVersion so clients can use optimistic
	// concurrency even on single-object GETs. Uses the current
	// counter rather than incrementing — this is a read.
	if acc, err := meta.Accessor(obj); err == nil {
		if acc.GetResourceVersion() == "" {
			acc.SetResourceVersion(r.CurrentResourceVersion())
		}
	}
	return obj, nil
}

// ---- Lister ----

func (r *REST) List(ctx context.Context, opts *metainternalversion.ListOptions) (runtime.Object, error) {
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	resp, err := r.client.List(ctx, &componentpb.ListRequest{
		User:          userFromCtx(ctx),
		Namespace:     ns,
		LabelSelector: selectorString(opts),
	})
	if err != nil {
		return nil, r.translateErr(err, "")
	}
	if r.desc.UseTypedWrapper {
		list := r.NewList().(*componentscheme.ObjectList)
		for _, raw := range resp.GetItemsJson() {
			o, err := decodeDyn(raw)
			if err != nil {
				return nil, err
			}
			if !r.matchesLabels(o, opts) {
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
		if !r.matchesLabels(u, opts) {
			continue
		}
		list.Items = append(list.Items, *u)
	}
	list.SetResourceVersion(r.CurrentResourceVersion())
	return list, nil
}

// ---- Watcher ----

func (r *REST) Watch(ctx context.Context, opts *metainternalversion.ListOptions) (watch.Interface, error) {
	if opts != nil && opts.ResourceVersion != "" && opts.ResourceVersion != "0" {
		reqN, perr := strconv.ParseUint(opts.ResourceVersion, 10, 64)
		if perr != nil || reqN != r.rv.Load() {
			return nil, apierrors.NewResourceExpired(fmt.Sprintf(
				"too old resource version: %s (current %s)", opts.ResourceVersion, r.CurrentResourceVersion()))
		}
	}
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	snapshot, err := r.client.List(ctx, &componentpb.ListRequest{
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

// StartUpstreamWatch opens a long-lived Watch stream to the backend
// and fans incoming events into the broadcaster. Retries on
// disconnect with a 2s backoff.
//
// Call from a post-start hook; goroutine exits when ctx is cancelled.
func (r *REST) StartUpstreamWatch(ctx context.Context) {
	go func() {
		for {
			if ctx.Err() != nil {
				return
			}
			err := r.runUpstreamWatch(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				klog.Warningf("component: upstream watch disconnected: %v; retrying in 2s", err)
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
	stream, err := r.client.Watch(ctx, &componentpb.WatchRequest{})
	if err != nil {
		return err
	}
	klog.Infof("component: upstream watch opened for %s", r.desc.GroupResource)
	for {
		ev, err := stream.Recv()
		if err != nil {
			return err
		}
		obj, derr := r.objFromJSON(ev.GetObjectJson())
		if derr != nil {
			klog.Warningf("component: upstream watch decode failed: %v", derr)
			continue
		}
		switch ev.GetType() {
		case componentpb.EventType_EVENT_ADDED:
			r.publish(watch.Added, obj)
		case componentpb.EventType_EVENT_MODIFIED:
			r.publish(watch.Modified, obj)
		case componentpb.EventType_EVENT_DELETED:
			r.publish(watch.Deleted, obj)
		}
	}
}

func (r *REST) publish(t watch.EventType, obj runtime.Object) {
	if acc, err := meta.Accessor(obj); err == nil {
		acc.SetResourceVersion(strconv.FormatUint(r.rv.Add(1), 10))
	}
	_ = r.bcaster.Action(t, obj)
}

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
	raw, err := encodeObject(obj)
	if err != nil {
		return nil, apierrors.NewBadRequest(err.Error())
	}
	resp, err := r.client.Create(ctx, &componentpb.CreateRequest{
		User:         userFromCtx(ctx),
		Namespace:    ns,
		ObjectJson:   raw,
		FieldManager: createFM(opts),
	})
	if err != nil {
		return nil, r.translateErr(err, nameOf(obj))
	}
	stored, err := r.objFromJSON(resp.GetObjectJson())
	if err != nil {
		return nil, err
	}
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
	var current runtime.Object
	cur, err := r.client.Get(ctx, &componentpb.GetRequest{
		User:      userFromCtx(ctx),
		Namespace: ns,
		Name:      name,
	})
	if err != nil {
		if st, ok := grpcstatus.FromError(err); ok && st.Code() == codes.NotFound {
			if !forceAllowCreate {
				return nil, false, apierrors.NewNotFound(r.desc.GroupResource, name)
			}
			// upsert path with current == nil
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
	raw, err := encodeObject(updated)
	if err != nil {
		return nil, false, apierrors.NewBadRequest(err.Error())
	}
	resp, err := r.client.Update(ctx, &componentpb.UpdateRequest{
		User:             userFromCtx(ctx),
		Namespace:        ns,
		Name:             name,
		ObjectJson:       raw,
		ForceAllowCreate: forceAllowCreate,
		FieldManager:     updateFM(opts),
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
		cur, err := r.client.Get(ctx, &componentpb.GetRequest{
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
	resp, err := r.client.Delete(ctx, &componentpb.DeleteRequest{
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
	default:
		return nil, fmt.Errorf("rowsFor: unexpected type %T", obj)
	}
}

func (r *REST) rowFromContent(content map[string]any, objForRow runtime.Object) (metav1.TableRow, error) {
	cells := make([]interface{}, len(r.desc.RowFields))
	for i, path := range r.desc.RowFields {
		v := LookupField(content, path)
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

func decodeDyn(raw []byte) (*componentscheme.Object, error) {
	o := &componentscheme.Object{}
	if err := o.UnmarshalJSON(raw); err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("decode object: %w", err))
	}
	return o, nil
}

func encodeObject(obj runtime.Object) ([]byte, error) {
	if u, ok := obj.(*unstructured.Unstructured); ok {
		return u.MarshalJSON()
	}
	if o, ok := obj.(*componentscheme.Object); ok {
		return o.MarshalJSON()
	}
	return json.Marshal(obj)
}

func updateFM(o *metav1.UpdateOptions) string {
	if o == nil {
		return ""
	}
	return o.FieldManager
}

func createFM(o *metav1.CreateOptions) string {
	if o == nil {
		return ""
	}
	return o.FieldManager
}

func userFromCtx(ctx context.Context) *componentpb.UserInfo {
	v, ok := genericapirequest.UserFrom(ctx)
	if !ok || v == nil {
		return nil
	}
	return userToProto(v)
}

func userToProto(u user.Info) *componentpb.UserInfo {
	out := &componentpb.UserInfo{
		Name:   u.GetName(),
		Uid:    u.GetUID(),
		Groups: u.GetGroups(),
		Extra:  map[string]*componentpb.StringList{},
	}
	for k, v := range u.GetExtra() {
		out.Extra[k] = &componentpb.StringList{Values: v}
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

// LookupField implements a tiny "." JSONPath lookup against an
// already-decoded object map. Exported so tests and helpers can
// reuse it; the format matches what the backend's RowFields ships.
// Returns "" for unresolvable paths (kubectl table cells expect a
// value, not nil).
func LookupField(obj map[string]any, path string) any {
	if path == "" {
		return nil
	}
	parts := strings.Split(strings.TrimPrefix(path, "."), ".")
	var cur any = obj
	for _, p := range parts {
		m, ok := cur.(map[string]any)
		if !ok {
			return ""
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
