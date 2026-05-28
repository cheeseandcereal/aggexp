// Package backend provides in-memory backends for experiment 0040.
// WidgetBackend is writable (push mode); GadgetBackend is read-only (poll mode).
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

	"github.com/cheeseandcereal/aggexp/experiments/0040-watchlist-and-consumer-watch/pkg/types"
	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// ---- Widget (push mode, writable) ----

var widgetGR = schema.GroupResource{Group: types.WidgetGroupName, Resource: "widgets"}

type WidgetBackend struct {
	mu    sync.RWMutex
	store map[string]*types.Widget
}

func NewWidgetBackend() *WidgetBackend {
	return &WidgetBackend{store: make(map[string]*types.Widget)}
}

func (b *WidgetBackend) New() runtime.Object         { return &types.Widget{} }
func (b *WidgetBackend) NewList() runtime.Object      { return &types.WidgetList{} }
func (b *WidgetBackend) Kind() string                 { return "Widget" }
func (b *WidgetBackend) SingularName() string         { return "widget" }
func (b *WidgetBackend) NamespaceScoped() bool        { return true }

func (b *WidgetBackend) Get(_ context.Context, _ user.Info, name string) (runtime.Object, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	w, ok := b.store[name]
	if !ok {
		return nil, apierrors.NewNotFound(widgetGR, name)
	}
	c := *w
	return &c, nil
}

func (b *WidgetBackend) List(_ context.Context, _ user.Info, _ runtimestorage.ListOptions) (runtime.Object, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	list := &types.WidgetList{}
	for _, w := range b.store {
		c := *w
		list.Items = append(list.Items, c)
	}
	return list, nil
}

func (b *WidgetBackend) TableColumns() []metav1.TableColumnDefinition {
	return []metav1.TableColumnDefinition{
		{Name: "Name", Type: "string"},
		{Name: "Color", Type: "string"},
		{Name: "Size", Type: "string"},
	}
}

func (b *WidgetBackend) RowsFor(obj runtime.Object) ([]metav1.TableRow, error) {
	switch v := obj.(type) {
	case *types.Widget:
		return []metav1.TableRow{{Cells: []interface{}{v.Name, v.Spec.Color, v.Spec.Size}, Object: runtime.RawExtension{Object: v}}}, nil
	case *types.WidgetList:
		rows := make([]metav1.TableRow, len(v.Items))
		for i := range v.Items {
			rows[i] = metav1.TableRow{Cells: []interface{}{v.Items[i].Name, v.Items[i].Spec.Color, v.Items[i].Spec.Size}, Object: runtime.RawExtension{Object: &v.Items[i]}}
		}
		return rows, nil
	}
	return nil, fmt.Errorf("unexpected type %T", obj)
}

func (b *WidgetBackend) Create(_ context.Context, _ user.Info, obj runtime.Object) (runtime.Object, error) {
	w := obj.(*types.Widget)
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.store[w.Name]; exists {
		return nil, apierrors.NewAlreadyExists(widgetGR, w.Name)
	}
	w.UID = kubetypes.UID(uuid.New().String())
	w.CreationTimestamp = metav1.Time{Time: time.Now()}
	stored := *w
	b.store[w.Name] = &stored
	c := stored
	return &c, nil
}

func (b *WidgetBackend) Update(_ context.Context, _ user.Info, name string, obj runtime.Object, forceAllowCreate bool) (runtime.Object, bool, error) {
	w := obj.(*types.Widget)
	b.mu.Lock()
	defer b.mu.Unlock()
	_, exists := b.store[name]
	if !exists && !forceAllowCreate {
		return nil, false, apierrors.NewNotFound(widgetGR, name)
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

func (b *WidgetBackend) Delete(_ context.Context, _ user.Info, name string) (runtime.Object, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	w, ok := b.store[name]
	if !ok {
		return nil, false, apierrors.NewNotFound(widgetGR, name)
	}
	delete(b.store, name)
	c := *w
	return &c, true, nil
}

var _ runtimestorage.WritableBackend = (*WidgetBackend)(nil)

// ---- Gadget (poll mode, read-only list source) ----

var gadgetGR = schema.GroupResource{Group: types.GadgetGroupName, Resource: "gadgets"}

// GadgetSource is the external data source for gadgets. It implements only
// List — the poll wrapper calls this periodically and diffs.
type GadgetSource struct {
	mu    sync.RWMutex
	store map[string]*types.Gadget
}

func NewGadgetSource() *GadgetSource {
	return &GadgetSource{store: make(map[string]*types.Gadget)}
}

// Put adds or updates a gadget in the source. Returns true if this was new.
func (s *GadgetSource) Put(g *types.Gadget) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, existed := s.store[g.Name]
	stored := *g
	s.store[g.Name] = &stored
	return !existed
}

// Remove deletes a gadget from the source. Returns true if it existed.
func (s *GadgetSource) Remove(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, existed := s.store[name]
	delete(s.store, name)
	return existed
}

// ListAll returns a snapshot of all gadgets.
func (s *GadgetSource) ListAll() []types.Gadget {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]types.Gadget, 0, len(s.store))
	for _, g := range s.store {
		out = append(out, *g)
	}
	return out
}

// GadgetBackend wraps GadgetSource to implement the read-only Backend
// interface from runtime/storage.
type GadgetBackend struct {
	source *GadgetSource
}

func NewGadgetBackend(source *GadgetSource) *GadgetBackend {
	return &GadgetBackend{source: source}
}

func (b *GadgetBackend) New() runtime.Object         { return &types.Gadget{} }
func (b *GadgetBackend) NewList() runtime.Object      { return &types.GadgetList{} }
func (b *GadgetBackend) Kind() string                 { return "Gadget" }
func (b *GadgetBackend) SingularName() string         { return "gadget" }
func (b *GadgetBackend) NamespaceScoped() bool        { return true }

func (b *GadgetBackend) Get(_ context.Context, _ user.Info, name string) (runtime.Object, error) {
	b.source.mu.RLock()
	defer b.source.mu.RUnlock()
	g, ok := b.source.store[name]
	if !ok {
		return nil, apierrors.NewNotFound(gadgetGR, name)
	}
	c := *g
	return &c, nil
}

func (b *GadgetBackend) List(_ context.Context, _ user.Info, _ runtimestorage.ListOptions) (runtime.Object, error) {
	items := b.source.ListAll()
	return &types.GadgetList{Items: items}, nil
}

func (b *GadgetBackend) TableColumns() []metav1.TableColumnDefinition {
	return []metav1.TableColumnDefinition{
		{Name: "Name", Type: "string"},
		{Name: "Model", Type: "string"},
		{Name: "Firmware", Type: "string"},
	}
}

func (b *GadgetBackend) RowsFor(obj runtime.Object) ([]metav1.TableRow, error) {
	switch v := obj.(type) {
	case *types.Gadget:
		return []metav1.TableRow{{Cells: []interface{}{v.Name, v.Spec.Model, v.Spec.Firmware}, Object: runtime.RawExtension{Object: v}}}, nil
	case *types.GadgetList:
		rows := make([]metav1.TableRow, len(v.Items))
		for i := range v.Items {
			rows[i] = metav1.TableRow{Cells: []interface{}{v.Items[i].Name, v.Items[i].Spec.Model, v.Items[i].Spec.Firmware}, Object: runtime.RawExtension{Object: &v.Items[i]}}
		}
		return rows, nil
	}
	return nil, fmt.Errorf("unexpected type %T", obj)
}

var _ runtimestorage.Backend = (*GadgetBackend)(nil)
