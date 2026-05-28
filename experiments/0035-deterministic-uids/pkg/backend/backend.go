// Package backend provides an in-memory WritableBackend for Widget resources
// with configurable UID generation (random vs deterministic).
package backend

import (
	"context"
	"crypto/sha256"
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

	"github.com/cheeseandcereal/aggexp/experiments/0035-deterministic-uids/pkg/types"
	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// UIDMode controls how UIDs are generated.
type UIDMode string

const (
	UIDModeRandom        UIDMode = "random"
	UIDModeDeterministic UIDMode = "deterministic"
)

var gr = schema.GroupResource{Group: types.GroupName, Resource: "widgets"}

// MemBackend is an in-memory Backend for Widgets.
type MemBackend struct {
	mu       sync.RWMutex
	store    map[string]*types.Widget // keyed by namespace/name
	uidMode  UIDMode
	resource schema.GroupResource
}

// SeedWidget defines a widget to pre-populate on startup (simulating
// an external backend that always has the same objects).
type SeedWidget struct {
	Namespace string
	Name      string
	Color     string
	Size      string
}

func New(mode UIDMode, seeds []SeedWidget) *MemBackend {
	b := &MemBackend{
		store:    make(map[string]*types.Widget),
		uidMode:  mode,
		resource: gr,
	}
	// Pre-populate from seeds (simulates reading from stable external source)
	for _, s := range seeds {
		w := &types.Widget{}
		w.Name = s.Name
		w.Namespace = s.Namespace
		w.Spec.Color = s.Color
		w.Spec.Size = s.Size
		w.UID = b.generateUID(s.Namespace, s.Name)
		w.CreationTimestamp = metav1.Now()
		w.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   types.GroupName,
			Version: "v1",
			Kind:    "Widget",
		})
		b.store[storeKey(s.Namespace, s.Name)] = w
	}
	return b
}

func (b *MemBackend) New() runtime.Object     { return &types.Widget{} }
func (b *MemBackend) NewList() runtime.Object  { return &types.WidgetList{} }
func (b *MemBackend) Kind() string             { return "Widget" }
func (b *MemBackend) SingularName() string     { return "widget" }
func (b *MemBackend) NamespaceScoped() bool    { return true }

// generateUID produces a UID based on the configured mode.
func (b *MemBackend) generateUID(namespace, name string) kubetypes.UID {
	switch b.uidMode {
	case UIDModeDeterministic:
		return deterministicUID(b.resource.Group, b.resource.Resource, namespace, name)
	default:
		return kubetypes.UID(uuid.New().String())
	}
}

// deterministicUID produces a UUID-formatted UID from SHA256(group/resource/namespace/name).
func deterministicUID(group, resource, namespace, name string) kubetypes.UID {
	input := group + "/" + resource + "/" + namespace + "/" + name
	hash := sha256.Sum256([]byte(input))
	// Format as 8-4-4-4-12 hex (standard UUID format)
	return kubetypes.UID(fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		hash[0:4], hash[4:6], hash[6:8], hash[8:10], hash[10:16]))
}

func storeKey(namespace, name string) string {
	return namespace + "/" + name
}

func (b *MemBackend) Get(_ context.Context, _ user.Info, name string) (runtime.Object, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	// Linear scan since we key by ns/name but Get only gets name.
	// For this experiment this is fine (small object counts).
	for _, w := range b.store {
		if w.Name == name {
			c := *w
			return &c, nil
		}
	}
	return nil, apierrors.NewNotFound(gr, name)
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
	key := storeKey(w.Namespace, w.Name)
	if _, exists := b.store[key]; exists {
		return nil, apierrors.NewAlreadyExists(gr, w.Name)
	}
	w.UID = b.generateUID(w.Namespace, w.Name)
	w.CreationTimestamp = metav1.Time{Time: time.Now()}
	stored := *w
	b.store[key] = &stored
	c := stored
	return &c, nil
}

func (b *MemBackend) Update(_ context.Context, _ user.Info, name string, obj runtime.Object, forceAllowCreate bool) (runtime.Object, bool, error) {
	w := obj.(*types.Widget)
	b.mu.Lock()
	defer b.mu.Unlock()
	key := storeKey(w.Namespace, w.Name)
	_, exists := b.store[key]
	if !exists && !forceAllowCreate {
		return nil, false, apierrors.NewNotFound(gr, name)
	}
	if !exists {
		w.UID = b.generateUID(w.Namespace, w.Name)
		w.CreationTimestamp = metav1.Time{Time: time.Now()}
	}
	stored := *w
	b.store[key] = &stored
	c := stored
	return &c, !exists, nil
}

func (b *MemBackend) Delete(_ context.Context, _ user.Info, name string) (runtime.Object, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for key, w := range b.store {
		if w.Name == name {
			delete(b.store, key)
			c := *w
			return &c, true, nil
		}
	}
	return nil, false, apierrors.NewNotFound(gr, name)
}

// Compile-time check.
var _ runtimestorage.WritableBackend = (*MemBackend)(nil)
