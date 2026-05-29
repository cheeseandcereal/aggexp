// Package backend is an in-memory WritableBackend over the generated
// Widget types. It exists only to verify wire parity of the
// oapigen-generated API package; it is not part of the generator.
package backend

import (
	"context"
	"fmt"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubetypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/authentication/user"

	v1 "github.com/cheeseandcereal/aggexp/experiments/0046-openapi-first-codegen/pkg/apis/widgets/v1"
	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

var gr = schema.GroupResource{Group: v1.GroupName, Resource: "widgets"}

// Mem is an in-memory Widget backend, seeded for selector demos.
type Mem struct {
	mu    sync.RWMutex
	store map[string]*v1.Widget
}

// New returns a seeded backend.
func New() *Mem {
	m := &Mem{store: map[string]*v1.Widget{}}
	m.seed()
	return m
}

func (m *Mem) seed() {
	seeds := []struct {
		name  string
		color v1.WidgetSpecColor
		size  int32
		phase v1.WidgetStatusPhase
	}{
		{"widget-01", v1.WidgetSpecColorRed, 1, v1.WidgetStatusPhaseReady},
		{"widget-02", v1.WidgetSpecColorBlue, 2, v1.WidgetStatusPhasePending},
		{"widget-03", v1.WidgetSpecColorRed, 3, v1.WidgetStatusPhaseFailed},
		{"widget-04", v1.WidgetSpecColorGreen, 4, v1.WidgetStatusPhaseReady},
		{"widget-05", v1.WidgetSpecColorRed, 5, v1.WidgetStatusPhaseReady},
	}
	for _, s := range seeds {
		m.store[s.name] = &v1.Widget{
			TypeMeta: metav1.TypeMeta{APIVersion: v1.GroupName + "/v1", Kind: "Widget"},
			ObjectMeta: metav1.ObjectMeta{
				Name:              s.name,
				Namespace:         "default",
				UID:               kubetypes.UID("seed-" + s.name),
				CreationTimestamp: metav1.NewTime(time.Unix(0, 0)),
			},
			Spec:   v1.WidgetSpec{Color: s.color, Size: s.size},
			Status: v1.WidgetStatus{Phase: s.phase},
		}
	}
}

func (m *Mem) New() runtime.Object     { return &v1.Widget{} }
func (m *Mem) NewList() runtime.Object  { return &v1.WidgetList{} }
func (m *Mem) Kind() string             { return "Widget" }
func (m *Mem) SingularName() string     { return "widget" }
func (m *Mem) NamespaceScoped() bool    { return true }

func (m *Mem) Get(_ context.Context, _ user.Info, name string) (runtime.Object, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	w, ok := m.store[name]
	if !ok {
		return nil, apierrors.NewNotFound(gr, name)
	}
	return w.DeepCopy(), nil
}

func (m *Mem) List(_ context.Context, _ user.Info, _ runtimestorage.ListOptions) (runtime.Object, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	list := &v1.WidgetList{}
	for _, w := range m.store {
		list.Items = append(list.Items, *w.DeepCopy())
	}
	return list, nil
}

func (m *Mem) TableColumns() []metav1.TableColumnDefinition {
	return []metav1.TableColumnDefinition{
		{Name: "Name", Type: "string"},
		{Name: "Color", Type: "string"},
		{Name: "Size", Type: "integer"},
		{Name: "Phase", Type: "string"},
	}
}

func (m *Mem) RowsFor(obj runtime.Object) ([]metav1.TableRow, error) {
	row := func(w *v1.Widget) metav1.TableRow {
		return metav1.TableRow{Cells: []interface{}{w.Name, string(w.Spec.Color), w.Spec.Size, string(w.Status.Phase)}}
	}
	switch v := obj.(type) {
	case *v1.Widget:
		return []metav1.TableRow{row(v)}, nil
	case *v1.WidgetList:
		rows := make([]metav1.TableRow, len(v.Items))
		for i := range v.Items {
			rows[i] = row(&v.Items[i])
		}
		return rows, nil
	}
	return nil, fmt.Errorf("unexpected type %T", obj)
}

func (m *Mem) Create(_ context.Context, _ user.Info, obj runtime.Object) (runtime.Object, error) {
	w := obj.(*v1.Widget)
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.store[w.Name]; ok {
		return nil, apierrors.NewAlreadyExists(gr, w.Name)
	}
	if w.UID == "" {
		w.UID = kubetypes.UID("uid-" + w.Name)
	}
	if w.CreationTimestamp.IsZero() {
		w.CreationTimestamp = metav1.NewTime(time.Unix(0, 0))
	}
	m.store[w.Name] = w.DeepCopy()
	return w.DeepCopy(), nil
}

func (m *Mem) Update(_ context.Context, _ user.Info, name string, obj runtime.Object, forceAllowCreate bool) (runtime.Object, bool, error) {
	w := obj.(*v1.Widget)
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.store[name]
	if !exists && !forceAllowCreate {
		return nil, false, apierrors.NewNotFound(gr, name)
	}
	if !exists {
		if w.UID == "" {
			w.UID = kubetypes.UID("uid-" + name)
		}
		if w.CreationTimestamp.IsZero() {
			w.CreationTimestamp = metav1.NewTime(time.Unix(0, 0))
		}
	}
	m.store[name] = w.DeepCopy()
	return w.DeepCopy(), !exists, nil
}

func (m *Mem) Delete(_ context.Context, _ user.Info, name string) (runtime.Object, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w, ok := m.store[name]
	if !ok {
		return nil, false, apierrors.NewNotFound(gr, name)
	}
	delete(m.store, name)
	return w.DeepCopy(), true, nil
}

var _ runtimestorage.WritableBackend = (*Mem)(nil)
