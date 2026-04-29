// Package hello provides the in-memory rest.Storage implementation
// for Hello objects. State is a sync-guarded map keyed by name;
// resourceVersions are drawn from a monotonic atomic counter; watch
// events are fanned out via watch.Broadcaster.
//
// No etcd, no persistence, no leader election. When the pod restarts,
// every Hello is gone. That's part of the experiment: how much of
// Kubernetes just keeps working under these constraints.
package hello

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/klog/v2"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0002-hello-aggregated/pkg/apis/aggexp"
)

// REST satisfies the set of rest.* interfaces required for a
// read+write+watch+patch resource:
//
//	rest.Storage               // New, Destroy
//	rest.Scoper                // cluster-scoped
//	rest.KindProvider          // "Hello"
//	rest.SingularNameProvider  // "hello"
//	rest.Getter, rest.Lister   // reads
//	rest.Creater, rest.Updater // writes
//	rest.GracefulDeleter       // deletes
//	rest.Watcher               // watches
//	rest.Patcher               // Getter + Updater (no extra methods)
//	rest.TableConvertor        // kubectl get table rendering
type REST struct {
	mu    sync.RWMutex
	items map[string]*aggexp.Hello

	rv      atomic.Uint64
	bcaster *watch.Broadcaster
}

// NewREST constructs a ready-to-install storage. Call Start to begin
// emitting bookmarks; the returned storage is usable immediately.
func NewREST() *REST {
	r := &REST{
		items:   make(map[string]*aggexp.Hello),
		bcaster: watch.NewBroadcaster(100, watch.DropIfChannelFull),
	}
	r.rv.Store(1) // items begin at rv=2 so list rv=1 is a valid starting point
	return r
}

// Start launches the background bookmark loop. Cancel the context to
// stop it (and shut down the broadcaster). Intended to be called from
// PostStartHook.
func (r *REST) Start(ctx context.Context) {
	go r.bookmarkLoop(ctx)
	go func() {
		<-ctx.Done()
		r.bcaster.Shutdown()
	}()
}

// ---- identity / shape interfaces ----

func (r *REST) New() runtime.Object      { return &aggexp.Hello{} }
func (r *REST) NewList() runtime.Object  { return &aggexp.HelloList{} }
func (r *REST) Destroy()                 {}
func (r *REST) NamespaceScoped() bool    { return false }
func (r *REST) Kind() string             { return "Hello" }
func (r *REST) GetSingularName() string  { return "hello" }

// ---- rv helpers ----

func (r *REST) nextRV() string {
	return strconv.FormatUint(r.rv.Add(1), 10)
}

func (r *REST) currentRV() string {
	return strconv.FormatUint(r.rv.Load(), 10)
}

// ---- Getter ----

func (r *REST) Get(ctx context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	obj, ok := r.items[name]
	if !ok {
		return nil, errors.NewNotFound(aggexp.Resource("hellos"), name)
	}
	return obj.DeepCopy(), nil
}

// ---- Lister ----

func (r *REST) List(ctx context.Context, opts *metainternalversion.ListOptions) (runtime.Object, error) {
	sel := labels.Everything()
	if opts != nil && opts.LabelSelector != nil {
		sel = opts.LabelSelector
	}
	r.mu.RLock()
	list := &aggexp.HelloList{Items: make([]aggexp.Hello, 0, len(r.items))}
	for _, o := range r.items {
		if !sel.Matches(labels.Set(o.Labels)) {
			continue
		}
		list.Items = append(list.Items, *o.DeepCopy())
	}
	list.ListMeta.ResourceVersion = r.currentRV()
	r.mu.RUnlock()
	return list, nil
}

// ---- Watcher ----

func (r *REST) Watch(ctx context.Context, opts *metainternalversion.ListOptions) (watch.Interface, error) {
	sel := labels.Everything()
	if opts != nil && opts.LabelSelector != nil {
		sel = opts.LabelSelector
	}

	// If the client asked to resume from a specific RV older than our
	// in-memory state, we cannot replay those events. Return Gone so
	// the reflector relists. RV=="" or "0" means "from current state".
	requested := ""
	if opts != nil {
		requested = opts.ResourceVersion
	}
	if requested != "" && requested != "0" {
		reqN, err := strconv.ParseUint(requested, 10, 64)
		if err != nil || reqN < r.rv.Load() {
			// We have no event buffer; any older RV is unsatisfiable.
			// Equal to current RV is fine (no events to replay).
			if err != nil || reqN != r.rv.Load() {
				return nil, errors.NewResourceExpired(fmt.Sprintf(
					"too old resource version: %s (current %s)", requested, r.currentRV()))
			}
		}
	}

	// Snapshot current state as initial ADDED events so the watcher is
	// level-consistent from the first event.
	r.mu.RLock()
	prefix := make([]watch.Event, 0, len(r.items))
	for _, o := range r.items {
		if !sel.Matches(labels.Set(o.Labels)) {
			continue
		}
		prefix = append(prefix, watch.Event{Type: watch.Added, Object: o.DeepCopy()})
	}
	r.mu.RUnlock()

	w, err := r.bcaster.WatchWithPrefix(prefix)
	if err != nil {
		return nil, err
	}
	// Filter out events whose object doesn't match the label selector.
	return watch.Filter(w, func(ev watch.Event) (watch.Event, bool) {
		h, ok := ev.Object.(*aggexp.Hello)
		if !ok {
			return ev, true // pass non-Hello (e.g. Status) through
		}
		return ev, sel.Matches(labels.Set(h.Labels))
	}), nil
}

// ---- Creater ----

func (r *REST) Create(
	ctx context.Context,
	obj runtime.Object,
	createValidation rest.ValidateObjectFunc,
	_ *metav1.CreateOptions,
) (runtime.Object, error) {
	h, ok := obj.(*aggexp.Hello)
	if !ok {
		return nil, errors.NewBadRequest(fmt.Sprintf("expected Hello, got %T", obj))
	}
	if h.Name == "" {
		return nil, errors.NewBadRequest("metadata.name is required")
	}
	if createValidation != nil {
		if err := createValidation(ctx, obj); err != nil {
			return nil, err
		}
	}

	r.mu.Lock()
	if _, exists := r.items[h.Name]; exists {
		r.mu.Unlock()
		return nil, errors.NewAlreadyExists(aggexp.Resource("hellos"), h.Name)
	}
	stored := h.DeepCopy()
	stored.UID = types.UID(uuid.New().String())
	stored.CreationTimestamp = metav1.Now()
	stored.ResourceVersion = r.nextRV()
	stored.Status.ObservedGreeting = stored.Spec.Greeting
	r.items[h.Name] = stored
	r.mu.Unlock()

	logRequest(ctx, "create", stored)
	_ = r.bcaster.Action(watch.Added, stored.DeepCopy())
	return stored.DeepCopy(), nil
}

// ---- Updater (also satisfies Patcher via Getter+Updater) ----

func (r *REST) Update(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	_ *metav1.UpdateOptions,
) (runtime.Object, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	existing, exists := r.items[name]
	var current runtime.Object
	if exists {
		current = existing.DeepCopy()
	}

	updated, err := objInfo.UpdatedObject(ctx, current)
	if err != nil {
		return nil, false, err
	}
	h, ok := updated.(*aggexp.Hello)
	if !ok {
		return nil, false, errors.NewBadRequest(fmt.Sprintf("expected Hello, got %T", updated))
	}
	h.Name = name // defensive

	created := false
	if !exists {
		if !forceAllowCreate {
			return nil, false, errors.NewNotFound(aggexp.Resource("hellos"), name)
		}
		if createValidation != nil {
			if err := createValidation(ctx, h); err != nil {
				return nil, false, err
			}
		}
		h.UID = types.UID(uuid.New().String())
		h.CreationTimestamp = metav1.Now()
		created = true
	} else if updateValidation != nil {
		if err := updateValidation(ctx, h, existing); err != nil {
			return nil, false, err
		}
	}

	h.ResourceVersion = r.nextRV()
	h.Status.ObservedGreeting = h.Spec.Greeting
	r.items[name] = h

	evt := watch.Modified
	if created {
		evt = watch.Added
	}
	logRequest(ctx, string(evt), h)
	_ = r.bcaster.Action(evt, h.DeepCopy())
	return h.DeepCopy(), created, nil
}

// ---- GracefulDeleter ----

func (r *REST) Delete(
	ctx context.Context,
	name string,
	deleteValidation rest.ValidateObjectFunc,
	_ *metav1.DeleteOptions,
) (runtime.Object, bool, error) {
	r.mu.Lock()
	existing, ok := r.items[name]
	if !ok {
		r.mu.Unlock()
		return nil, false, errors.NewNotFound(aggexp.Resource("hellos"), name)
	}
	if deleteValidation != nil {
		if err := deleteValidation(ctx, existing); err != nil {
			r.mu.Unlock()
			return nil, false, err
		}
	}
	delete(r.items, name)
	// Bump RV so a subsequent watch cannot accidentally think nothing happened.
	existing = existing.DeepCopy()
	existing.ResourceVersion = r.nextRV()
	r.mu.Unlock()

	logRequest(ctx, "delete", existing)
	_ = r.bcaster.Action(watch.Deleted, existing.DeepCopy())
	return existing, true, nil
}

// ---- TableConvertor ----

func (r *REST) ConvertToTable(ctx context.Context, object runtime.Object, _ runtime.Object) (*metav1.Table, error) {
	t := &metav1.Table{
		ColumnDefinitions: []metav1.TableColumnDefinition{
			{Name: "Name", Type: "string", Format: "name", Description: "Name of the Hello."},
			{Name: "Greeting", Type: "string", Description: "The greeting string."},
			{Name: "Age", Type: "date", Description: "Age since creation."},
		},
	}
	row := func(h *aggexp.Hello) metav1.TableRow {
		return metav1.TableRow{
			Cells: []interface{}{
				h.Name,
				h.Spec.Greeting,
				translateTimestampSince(h.CreationTimestamp),
			},
			Object: runtime.RawExtension{Object: h},
		}
	}
	switch obj := object.(type) {
	case *aggexp.Hello:
		t.Rows = []metav1.TableRow{row(obj)}
	case *aggexp.HelloList:
		for i := range obj.Items {
			t.Rows = append(t.Rows, row(&obj.Items[i]))
		}
		t.ListMeta.ResourceVersion = obj.ResourceVersion
	default:
		return nil, fmt.Errorf("unexpected object type %T", object)
	}
	return t, nil
}

// ---- bookmark loop ----

func (r *REST) bookmarkLoop(ctx context.Context) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			bm := &aggexp.Hello{}
			bm.ResourceVersion = r.currentRV()
			_ = r.bcaster.Action(watch.Bookmark, bm)
		}
	}
}

// ---- helpers ----

func translateTimestampSince(t metav1.Time) string {
	if t.IsZero() {
		return "<unknown>"
	}
	d := time.Since(t.Time)
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

// logRequest writes a single structured line per mutation including
// identity info harvested from the request context, so we can see
// who did what in pod logs even though this experiment does not
// make per-request authz decisions.
func logRequest(ctx context.Context, verb string, h *aggexp.Hello) {
	u, _ := genericapirequest.UserFrom(ctx)
	name := "?"
	groups := []string{}
	if u != nil {
		name = u.GetName()
		groups = u.GetGroups()
	}
	klog.V(2).InfoS("hello-mutation", "verb", verb, "name", h.Name,
		"rv", h.ResourceVersion, "user", name, "groups", groups)
}

// Compile-time interface assertions. If any fail, Go will not build.
var (
	_ rest.Storage              = (*REST)(nil)
	_ rest.Scoper               = (*REST)(nil)
	_ rest.KindProvider         = (*REST)(nil)
	_ rest.SingularNameProvider = (*REST)(nil)
	_ rest.Getter               = (*REST)(nil)
	_ rest.Lister               = (*REST)(nil)
	_ rest.Watcher              = (*REST)(nil)
	_ rest.Creater              = (*REST)(nil)
	_ rest.Updater              = (*REST)(nil)
	_ rest.Patcher              = (*REST)(nil)
	_ rest.GracefulDeleter      = (*REST)(nil)
	_ rest.TableConvertor       = (*REST)(nil)
)
