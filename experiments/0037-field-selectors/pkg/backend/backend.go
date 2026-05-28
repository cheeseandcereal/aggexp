// Package backend provides an in-memory WritableBackend for Widget
// resources, pre-populated with 10 widgets for field selector testing.
package backend

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubetypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/authentication/user"

	"github.com/cheeseandcereal/aggexp/experiments/0037-field-selectors/pkg/types"
	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

var gr = schema.GroupResource{Group: types.GroupName, Resource: "widgets"}

// MemBackend is an in-memory Backend for Widgets with field selector support.
type MemBackend struct {
	mu    sync.RWMutex
	store map[string]*types.Widget // keyed by name
}

func New() *MemBackend {
	b := &MemBackend{store: make(map[string]*types.Widget)}
	b.seed()
	return b
}

// SelectableFields declares which spec fields are selectable.
func (b *MemBackend) SelectableFields() []string {
	return []string{"spec.color", "spec.size", "spec.priority"}
}

// FieldAccessor returns the value of a spec field for a Widget.
func FieldAccessor(obj runtime.Object, field string) (string, bool) {
	w, ok := obj.(*types.Widget)
	if !ok {
		return "", false
	}
	switch field {
	case "spec.color":
		return w.Spec.Color, true
	case "spec.size":
		return w.Spec.Size, true
	case "spec.priority":
		return w.Spec.Priority, true
	default:
		return "", false
	}
}

func (b *MemBackend) seed() {
	widgets := []struct {
		name     string
		color    string
		size     string
		priority string
		labels   map[string]string
	}{
		{"widget-01", "red", "small", "high", map[string]string{"app": "demo", "tier": "frontend"}},
		{"widget-02", "blue", "medium", "low", map[string]string{"app": "demo", "tier": "backend"}},
		{"widget-03", "red", "large", "medium", map[string]string{"app": "demo", "tier": "frontend"}},
		{"widget-04", "green", "small", "high", map[string]string{"app": "other", "tier": "backend"}},
		{"widget-05", "blue", "large", "low", map[string]string{"app": "demo", "tier": "frontend"}},
		{"widget-06", "red", "medium", "high", map[string]string{"app": "demo", "tier": "backend"}},
		{"widget-07", "green", "large", "medium", map[string]string{"app": "other", "tier": "frontend"}},
		{"widget-08", "blue", "small", "high", map[string]string{"app": "demo", "tier": "backend"}},
		{"widget-09", "red", "large", "low", map[string]string{"app": "demo", "tier": "frontend"}},
		{"widget-10", "green", "medium", "medium", map[string]string{"app": "other", "tier": "backend"}},
	}
	for _, w := range widgets {
		b.store[w.name] = &types.Widget{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "widgets.aggexp.io/v1",
				Kind:       "Widget",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:              w.name,
				Namespace:         "default",
				UID:               kubetypes.UID(uuid.New().String()),
				CreationTimestamp: metav1.Time{Time: time.Now()},
				Labels:            w.labels,
			},
			Spec: types.WidgetSpec{
				Color:    w.color,
				Size:     w.size,
				Priority: w.priority,
			},
		}
	}
}

func (b *MemBackend) New() runtime.Object    { return &types.Widget{} }
func (b *MemBackend) NewList() runtime.Object { return &types.WidgetList{} }
func (b *MemBackend) Kind() string            { return "Widget" }
func (b *MemBackend) SingularName() string    { return "widget" }
func (b *MemBackend) NamespaceScoped() bool   { return true }

func (b *MemBackend) Get(_ context.Context, _ user.Info, name string) (runtime.Object, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	w, ok := b.store[name]
	if !ok {
		return nil, apierrors.NewNotFound(gr, name)
	}
	c := *w
	return &c, nil
}

func (b *MemBackend) List(_ context.Context, _ user.Info, _ runtimestorage.ListOptions) (runtime.Object, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	list := &types.WidgetList{}
	for _, w := range b.store {
		c := *w
		list.Items = append(list.Items, c)
	}
	return list, nil
}

func (b *MemBackend) TableColumns() []metav1.TableColumnDefinition {
	return []metav1.TableColumnDefinition{
		{Name: "Name", Type: "string"},
		{Name: "Color", Type: "string"},
		{Name: "Size", Type: "string"},
		{Name: "Priority", Type: "string"},
	}
}

func (b *MemBackend) RowsFor(obj runtime.Object) ([]metav1.TableRow, error) {
	switch v := obj.(type) {
	case *types.Widget:
		return []metav1.TableRow{{Cells: []interface{}{v.Name, v.Spec.Color, v.Spec.Size, v.Spec.Priority}}}, nil
	case *types.WidgetList:
		rows := make([]metav1.TableRow, len(v.Items))
		for i := range v.Items {
			rows[i] = metav1.TableRow{Cells: []interface{}{v.Items[i].Name, v.Items[i].Spec.Color, v.Items[i].Spec.Size, v.Items[i].Spec.Priority}}
		}
		return rows, nil
	}
	return nil, fmt.Errorf("unexpected type %T", obj)
}

func (b *MemBackend) Create(_ context.Context, _ user.Info, obj runtime.Object) (runtime.Object, error) {
	w := obj.(*types.Widget)
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.store[w.Name]; exists {
		return nil, apierrors.NewAlreadyExists(gr, w.Name)
	}
	w.UID = kubetypes.UID(uuid.New().String())
	w.CreationTimestamp = metav1.Time{Time: time.Now()}
	stored := *w
	b.store[w.Name] = &stored
	c := stored
	return &c, nil
}

func (b *MemBackend) Update(_ context.Context, _ user.Info, name string, obj runtime.Object, forceAllowCreate bool) (runtime.Object, bool, error) {
	w := obj.(*types.Widget)
	b.mu.Lock()
	defer b.mu.Unlock()
	_, exists := b.store[name]
	if !exists && !forceAllowCreate {
		return nil, false, apierrors.NewNotFound(gr, name)
	}
	if !exists {
		w.UID = kubetypes.UID(uuid.New().String())
		w.CreationTimestamp = metav1.Time{Time: time.Now()}
	}
	stored := *w
	b.store[name] = &stored
	c := stored
	return &c, !exists, nil
}

func (b *MemBackend) Delete(_ context.Context, _ user.Info, name string) (runtime.Object, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	w, ok := b.store[name]
	if !ok {
		return nil, false, apierrors.NewNotFound(gr, name)
	}
	delete(b.store, name)
	c := *w
	return &c, true, nil
}

// Compile-time check.
var _ runtimestorage.WritableBackend = (*MemBackend)(nil)
