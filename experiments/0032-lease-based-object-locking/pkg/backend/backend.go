// Package backend provides an in-memory WritableBackend for Widget resources.
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

	"github.com/cheeseandcereal/aggexp/experiments/0032-lease-based-object-locking/pkg/types"
	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

var gr = schema.GroupResource{Group: types.GroupName, Resource: "widgets"}

// MemBackend is an in-memory Backend for Widgets.
type MemBackend struct {
	mu    sync.RWMutex
	store map[string]*types.Widget // keyed by name
}

func New() *MemBackend {
	return &MemBackend{store: make(map[string]*types.Widget)}
}

func (b *MemBackend) New() runtime.Object     { return &types.Widget{} }
func (b *MemBackend) NewList() runtime.Object  { return &types.WidgetList{} }
func (b *MemBackend) Kind() string             { return "Widget" }
func (b *MemBackend) SingularName() string     { return "widget" }
func (b *MemBackend) NamespaceScoped() bool    { return true }

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
	}
}

func (b *MemBackend) RowsFor(obj runtime.Object) ([]metav1.TableRow, error) {
	switch v := obj.(type) {
	case *types.Widget:
		return []metav1.TableRow{{Cells: []interface{}{v.Name, v.Spec.Color, v.Spec.Size}}}, nil
	case *types.WidgetList:
		rows := make([]metav1.TableRow, len(v.Items))
		for i := range v.Items {
			rows[i] = metav1.TableRow{Cells: []interface{}{v.Items[i].Name, v.Items[i].Spec.Color, v.Items[i].Spec.Size}}
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
