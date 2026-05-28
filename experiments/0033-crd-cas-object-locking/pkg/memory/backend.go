// Package memory implements a writable, in-memory runtime/storage
// Backend for Gizmos. State is per-process and is intentionally not
// shared between AA replicas — that's 0034's problem. The locking
// layer (pkg/locking) wraps this backend to gate writes.
package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/authentication/user"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0033-crd-cas-object-locking/pkg/apis/aggexp"
	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// Backend is an in-memory writable backend for Gizmo. It is the
// "data plane" the locking layer wraps.
type Backend struct {
	mu    sync.RWMutex
	items map[string]*aggexp.Gizmo
	// PodName identifies which replica produced this object;
	// stamped into Status.LastWriter on every mutation.
	PodName string
}

// New constructs a Backend.
func New(podName string) *Backend {
	return &Backend{
		items:   map[string]*aggexp.Gizmo{},
		PodName: podName,
	}
}

// --- runtime/storage.Backend ---

func (b *Backend) New() runtime.Object     { return &aggexp.Gizmo{} }
func (b *Backend) NewList() runtime.Object { return &aggexp.GizmoList{} }
func (b *Backend) Kind() string            { return "Gizmo" }
func (b *Backend) SingularName() string    { return "gizmo" }
func (b *Backend) NamespaceScoped() bool   { return false }

func (b *Backend) Get(_ context.Context, _ user.Info, name string) (runtime.Object, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	g, ok := b.items[name]
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: "aggexp.io", Resource: "gizmos"}, name)
	}
	return g.DeepCopy(), nil
}

func (b *Backend) List(_ context.Context, _ user.Info, _ runtimestorage.ListOptions) (runtime.Object, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	list := &aggexp.GizmoList{Items: make([]aggexp.Gizmo, 0, len(b.items))}
	names := make([]string, 0, len(b.items))
	for n := range b.items {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		list.Items = append(list.Items, *b.items[n].DeepCopy())
	}
	return list, nil
}

func (b *Backend) TableColumns() []metav1.TableColumnDefinition {
	return []metav1.TableColumnDefinition{
		{Name: "Name", Type: "string", Format: "name", Description: "Gizmo name."},
		{Name: "Color", Type: "string", Description: "Spec.Color."},
		{Name: "Counter", Type: "integer", Description: "Spec.Counter."},
		{Name: "LastWriter", Type: "string", Description: "Pod that last wrote (replica identity)."},
		{Name: "Age", Type: "date", Description: "Time since creation."},
	}
}

func (b *Backend) RowsFor(obj runtime.Object) ([]metav1.TableRow, error) {
	row := func(g *aggexp.Gizmo) metav1.TableRow {
		return metav1.TableRow{
			Cells: []interface{}{
				g.Name,
				g.Spec.Color,
				g.Spec.Counter,
				g.Status.LastWriter,
				translateTimestampSince(g.CreationTimestamp),
			},
			Object: runtime.RawExtension{Object: g},
		}
	}
	switch v := obj.(type) {
	case *aggexp.Gizmo:
		return []metav1.TableRow{row(v)}, nil
	case *aggexp.GizmoList:
		rs := make([]metav1.TableRow, 0, len(v.Items))
		for i := range v.Items {
			rs = append(rs, row(&v.Items[i]))
		}
		return rs, nil
	}
	return nil, fmt.Errorf("unexpected object %T", obj)
}

// --- WritableBackend ---

func (b *Backend) Create(_ context.Context, _ user.Info, obj runtime.Object) (runtime.Object, error) {
	g, ok := obj.(*aggexp.Gizmo)
	if !ok {
		return nil, fmt.Errorf("Create expected *Gizmo, got %T", obj)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.items[g.Name]; exists {
		return nil, apierrors.NewAlreadyExists(schema.GroupResource{Group: "aggexp.io", Resource: "gizmos"}, g.Name)
	}
	out := g.DeepCopy()
	out.UID = types.UID(uuid.New().String())
	out.CreationTimestamp = metav1.NewTime(time.Now())
	out.Status.LastWriter = b.PodName
	b.items[out.Name] = out
	return out.DeepCopy(), nil
}

func (b *Backend) Update(_ context.Context, _ user.Info, name string, obj runtime.Object, forceAllowCreate bool) (runtime.Object, bool, error) {
	g, ok := obj.(*aggexp.Gizmo)
	if !ok {
		return nil, false, fmt.Errorf("Update expected *Gizmo, got %T", obj)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	existing, exists := b.items[name]
	if !exists {
		if !forceAllowCreate {
			return nil, false, apierrors.NewNotFound(schema.GroupResource{Group: "aggexp.io", Resource: "gizmos"}, name)
		}
		out := g.DeepCopy()
		out.Name = name
		out.UID = types.UID(uuid.New().String())
		out.CreationTimestamp = metav1.NewTime(time.Now())
		out.Status.LastWriter = b.PodName
		b.items[name] = out
		return out.DeepCopy(), true, nil
	}
	out := g.DeepCopy()
	out.Name = name
	out.UID = existing.UID
	out.CreationTimestamp = existing.CreationTimestamp
	out.Status.LastWriter = b.PodName
	b.items[name] = out
	return out.DeepCopy(), false, nil
}

func (b *Backend) Delete(_ context.Context, _ user.Info, name string) (runtime.Object, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	existing, exists := b.items[name]
	if !exists {
		return nil, false, apierrors.NewNotFound(schema.GroupResource{Group: "aggexp.io", Resource: "gizmos"}, name)
	}
	delete(b.items, name)
	return existing.DeepCopy(), true, nil
}

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
	default:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
}

// Compile-time assertions.
var (
	_ runtimestorage.Backend         = (*Backend)(nil)
	_ runtimestorage.WritableBackend = (*Backend)(nil)
)
