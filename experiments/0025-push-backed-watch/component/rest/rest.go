// Package rest implements a fork of runtime/component/grpcbackend.REST
// for experiment 0025. The fork differs from the substrate version in
// two specific places:
//
//  1. Watch capability switch. At NewREST time the component probes
//     the backend: it opens a backend.Watch stream and reads one
//     event (or an error). If the error is codes.Unimplemented the
//     REST flips into POLL mode and runs its own list-poll loop.
//     Otherwise it flips into PUSH mode and forwards backend events
//     verbatim.
//
//  2. initial-events-end BOOKMARK. The substrate's Watch() builds a
//     prefix of ADDED events from a List snapshot and hands them to
//     the broadcaster. This fork additionally appends a BOOKMARK
//     event whose object carries
//     metadata.annotations["k8s.io/initial-events-end"]="true".
//     That's the signal WatchList-aware clients (kubectl wait
//     --for=jsonpath, client-go 1.31+ informers) use to consider
//     the watch "synced" — the gap 0011 found.
//
// Everything else (Get, List, Create, Update, Delete, ConvertToTable,
// the typed-wrapper / unstructured split, the library-facing
// rest.Storage interfaces) mirrors the substrate's behavior. The
// fork is deliberately self-contained: it's ~250 semantic lines and
// the experiment needs both pieces of custom behavior in one place.
package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// Mode is the watch mode the component settled on after probing.
type Mode int

const (
	ModePush Mode = iota
	ModePoll
)

func (m Mode) String() string {
	if m == ModePush {
		return "push"
	}
	return "poll"
}

// Descriptor mirrors runtime/component/grpcbackend.Descriptor.
type Descriptor struct {
	GroupVersion            schema.GroupVersion
	Resource                string
	Kind                    string
	Singular                string
	Namespaced              bool
	Writable                bool
	SupportsServerSideApply bool
	Columns                 []metav1.TableColumnDefinition
	RowFields               []string
	GroupResource           schema.GroupResource
	// PollInterval is the interval the POLL-mode list loop runs at.
	// Ignored in PUSH mode.
	PollInterval time.Duration
}

type REST struct {
	desc    Descriptor
	client  componentpb.BackendClient
	mode    Mode
	rv      atomic.Uint64
	bcaster *watch.Broadcaster

	// cached last-known state keyed by ns/name; used in poll mode to
	// diff against the next list response.
	mu       sync.Mutex
	cache    map[string]*cachedObject
	prevList []string // ordered keys for deterministic diffing
}

type cachedObject struct {
	raw []byte
	uid string
}

// New constructs a REST in the mode dictated by probeMode. Callers
// get the chosen mode back via Mode() for logging.
func New(desc Descriptor, client componentpb.BackendClient, mode Mode) *REST {
	r := &REST{
		desc:    desc,
		client:  client,
		mode:    mode,
		bcaster: watch.NewBroadcaster(100, watch.DropIfChannelFull),
		cache:   map[string]*cachedObject{},
	}
	r.rv.Store(1)
	if desc.GroupResource == (schema.GroupResource{}) {
		desc.GroupResource = schema.GroupResource{Group: desc.GroupVersion.Group, Resource: desc.Resource}
		r.desc = desc
	}
	return r
}

// ProbeMode attempts to open a Watch stream on the backend; returns
// ModePoll if the backend responds with codes.Unimplemented (or a
// clearly-not-a-stream error), otherwise ModePush. The probe Watch
// is closed before returning.
//
// Deliberately synchronous with a short timeout so startup is bounded.
func ProbeMode(ctx context.Context, client componentpb.BackendClient, timeout time.Duration) (Mode, error) {
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stream, err := client.Watch(probeCtx, &componentpb.WatchRequest{})
	if err != nil {
		if st, ok := grpcstatus.FromError(err); ok && st.Code() == codes.Unimplemented {
			return ModePoll, nil
		}
		return ModePoll, fmt.Errorf("probe Watch: %w", err)
	}
	// Try to read the first event. The backend may produce nothing
	// for an empty store; that's fine — just cancel.
	done := make(chan error, 1)
	go func() {
		_, recvErr := stream.Recv()
		done <- recvErr
	}()
	select {
	case <-probeCtx.Done():
		// Timed out waiting; treat as push-capable (backend accepted
		// the stream; it's just idle because state is empty).
		return ModePush, nil
	case err := <-done:
		if err == nil || errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
			return ModePush, nil
		}
		if st, ok := grpcstatus.FromError(err); ok {
			if st.Code() == codes.Unimplemented {
				return ModePoll, nil
			}
			if st.Code() == codes.Canceled || st.Code() == codes.DeadlineExceeded {
				return ModePush, nil
			}
		}
		return ModePoll, fmt.Errorf("probe Watch recv: %w", err)
	}
}

func (r *REST) Mode() Mode { return r.mode }

func (r *REST) Shutdown() {
	if r.bcaster != nil {
		r.bcaster.Shutdown()
	}
}

// ---- identity / shape ----

func (r *REST) New() runtime.Object {
	gvk := r.desc.GroupVersion.WithKind(r.desc.Kind)
	obj := &componentscheme.Object{}
	obj.GetObjectKind().SetGroupVersionKind(gvk)
	return obj
}

func (r *REST) NewList() runtime.Object {
	listGVK := r.desc.GroupVersion.WithKind(r.desc.Kind + "List")
	l := &componentscheme.ObjectList{}
	l.GetObjectKind().SetGroupVersionKind(listGVK)
	return l
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
	obj, derr := r.decodeObject(resp.GetObjectJson())
	if derr != nil {
		return nil, derr
	}
	if acc, err := meta.Accessor(obj); err == nil {
		if acc.GetResourceVersion() == "" {
			acc.SetResourceVersion(r.currentRV())
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
	list.SetResourceVersion(r.currentRV())
	return list, nil
}

// ---- Watcher ----

// Watch constructs the initial-events prefix from a List snapshot,
// appends a BOOKMARK event carrying the
// metadata.annotations["k8s.io/initial-events-end"]="true" signal,
// then subscribes to the broadcaster.
func (r *REST) Watch(ctx context.Context, opts *metainternalversion.ListOptions) (watch.Interface, error) {
	if opts != nil && opts.ResourceVersion != "" && opts.ResourceVersion != "0" {
		reqN, perr := strconv.ParseUint(opts.ResourceVersion, 10, 64)
		if perr != nil || reqN != r.rv.Load() {
			return nil, apierrors.NewResourceExpired(fmt.Sprintf(
				"too old resource version: %s (current %s)", opts.ResourceVersion, r.currentRV()))
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
	prefix := make([]watch.Event, 0, len(snapshot.GetItemsJson())+1)
	for _, raw := range snapshot.GetItemsJson() {
		obj, err := r.decodeObject(raw)
		if err != nil {
			return nil, err
		}
		if !r.matchesLabels(obj, opts) {
			continue
		}
		prefix = append(prefix, watch.Event{Type: watch.Added, Object: obj})
	}
	// Initial-events-end BOOKMARK. We emit one regardless of the
	// caller's allowWatchBookmarks preference — kubectl wait and
	// WatchList-aware informers set that flag; clients that don't
	// honor BOOKMARK events safely ignore them per the wire
	// contract.
	bookmark := r.newBookmarkObject(r.currentRV())
	prefix = append(prefix, watch.Event{Type: watch.Bookmark, Object: bookmark})
	klog.V(2).Infof("component: Watch opened mode=%s prefix=%d (incl. 1 BOOKMARK) rv=%s",
		r.mode, len(prefix), r.currentRV())

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

func (r *REST) newBookmarkObject(rv string) runtime.Object {
	obj := r.New().(*componentscheme.Object)
	obj.SetResourceVersion(rv)
	obj.SetAnnotations(map[string]string{"k8s.io/initial-events-end": "true"})
	return obj
}

// StartUpstreamWatch runs the push-mode upstream watch or the
// poll-mode list-diff loop. Exits when ctx is cancelled.
func (r *REST) StartUpstreamWatch(ctx context.Context) {
	if r.mode == ModePush {
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
			klog.Warningf("component: upstream watch disconnected: %v; retrying in 2s", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (r *REST) runPushOnce(ctx context.Context) error {
	stream, err := r.client.Watch(ctx, &componentpb.WatchRequest{})
	if err != nil {
		return err
	}
	klog.Infof("component: upstream watch (push) opened for %s", r.desc.GroupResource)
	for {
		ev, err := stream.Recv()
		if err != nil {
			return err
		}
		obj, derr := r.decodeObject(ev.GetObjectJson())
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
		case componentpb.EventType_EVENT_BOOKMARK:
			// Forward backend-supplied bookmarks — e.g. an
			// initial-events-end backend may re-emit bookmarks
			// on reconnect. Poll mode ignores backend bookmarks
			// since the poll loop invented its own snapshot.
			r.publish(watch.Bookmark, obj)
		}
	}
}

func (r *REST) runPollLoop(ctx context.Context) {
	interval := r.desc.PollInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	klog.Infof("component: upstream watch (poll) interval=%s for %s", interval, r.desc.GroupResource)
	// Immediate first tick so we seed cache promptly.
	tick := time.NewTimer(0)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			start := time.Now()
			if err := r.pollAndDiff(ctx); err != nil {
				klog.Warningf("component: poll failed: %v", err)
			} else {
				klog.V(2).Infof("component: poll complete in %s", time.Since(start))
			}
			tick.Reset(interval)
		}
	}
}

// pollAndDiff calls backend.List, compares against the cached set,
// and publishes ADDED / MODIFIED / DELETED events to the broadcaster.
func (r *REST) pollAndDiff(ctx context.Context) error {
	resp, err := r.client.List(ctx, &componentpb.ListRequest{})
	if err != nil {
		return err
	}
	now := map[string]*cachedObject{}
	keys := make([]string, 0, len(resp.GetItemsJson()))
	for _, raw := range resp.GetItemsJson() {
		var head struct {
			Metadata struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
				UID       string `json:"uid"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal(raw, &head); err != nil {
			klog.Warningf("poll: skip raw: %v", err)
			continue
		}
		k := head.Metadata.Namespace + "/" + head.Metadata.Name
		now[k] = &cachedObject{raw: raw, uid: head.Metadata.UID}
		keys = append(keys, k)
	}

	r.mu.Lock()
	prev := r.cache
	// Diff additions / modifications.
	var adds, mods, dels [][]byte
	for k, cur := range now {
		old, ok := prev[k]
		if !ok {
			adds = append(adds, cur.raw)
		} else if !bytesEqual(old.raw, cur.raw) {
			mods = append(mods, cur.raw)
		}
	}
	for k, old := range prev {
		if _, ok := now[k]; !ok {
			dels = append(dels, old.raw)
		}
	}
	r.cache = now
	r.mu.Unlock()

	for _, raw := range adds {
		if obj, err := r.decodeObject(raw); err == nil {
			r.publish(watch.Added, obj)
		}
	}
	for _, raw := range mods {
		if obj, err := r.decodeObject(raw); err == nil {
			r.publish(watch.Modified, obj)
		}
	}
	for _, raw := range dels {
		if obj, err := r.decodeObject(raw); err == nil {
			r.publish(watch.Deleted, obj)
		}
	}
	if len(adds)+len(mods)+len(dels) > 0 {
		klog.V(2).Infof("poll: added=%d modified=%d deleted=%d total=%d",
			len(adds), len(mods), len(dels), len(now))
	}
	return nil
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (r *REST) publish(t watch.EventType, obj runtime.Object) {
	if acc, err := meta.Accessor(obj); err == nil {
		if t != watch.Bookmark {
			// The middleware assigns monotonic RVs in push mode;
			// in poll mode we also assign since the backend didn't.
			acc.SetResourceVersion(strconv.FormatUint(r.rv.Add(1), 10))
		}
	}
	_ = r.bcaster.Action(t, obj)
}

func (r *REST) currentRV() string {
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
	stored, err := r.decodeObject(resp.GetObjectJson())
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
		} else {
			return nil, false, r.translateErr(err, name)
		}
	} else {
		current, err = r.decodeObject(cur.GetObjectJson())
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
	stored, err := r.decodeObject(resp.GetObjectJson())
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
		obj, err := r.decodeObject(cur.GetObjectJson())
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
	stored, err := r.decodeObject(resp.GetObjectJson())
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
	case *componentscheme.ObjectList:
		rows := make([]metav1.TableRow, 0, len(o.Items))
		for i := range o.Items {
			row := r.rowFromContent(o.Items[i].AsMap(), &o.Items[i])
			rows = append(rows, row)
		}
		return rows, nil
	case *componentscheme.Object:
		return []metav1.TableRow{r.rowFromContent(o.AsMap(), o)}, nil
	default:
		return nil, fmt.Errorf("rowsFor: unexpected type %T", obj)
	}
}

func (r *REST) rowFromContent(content map[string]any, objForRow runtime.Object) metav1.TableRow {
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
	}
}

// ---- helpers ----

func (r *REST) decodeObject(raw []byte) (runtime.Object, error) {
	return decodeDyn(raw)
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

func decodeDyn(raw []byte) (*componentscheme.Object, error) {
	o := &componentscheme.Object{}
	if err := o.UnmarshalJSON(raw); err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("decode object: %w", err))
	}
	return o, nil
}

func encodeObject(obj runtime.Object) ([]byte, error) {
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

func lookupField(obj map[string]any, path string) any {
	if path == "" {
		return nil
	}
	// Simple dot-delimited lookup; matches the substrate's helper.
	cur := any(obj)
	for _, p := range splitDot(path) {
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

func splitDot(s string) []string {
	if len(s) > 0 && s[0] == '.' {
		s = s[1:]
	}
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}

func ageOf(ts string) string {
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return "<unknown>"
	}
	d := time.Since(t).Round(time.Second)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
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
