// Package statusrest provides a status-subresource REST adapter that
// implements rest.Getter + rest.Updater. On Update it preserves the
// spec from the stored object and only applies status changes.
package statusrest

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"

	"github.com/cheeseandcereal/aggexp/experiments/0038-subresources-status/pkg/types"
	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// StatusREST implements only Get + Update for the /status subresource.
// It shares the same backing store (via the main REST adapter) but on
// Update it preserves spec from the existing object.
type StatusREST struct {
	mainStore *runtimestorage.REST
}

// New creates a StatusREST that shares the main store.
func New(mainStore *runtimestorage.REST) *StatusREST {
	return &StatusREST{mainStore: mainStore}
}

func (s *StatusREST) New() runtime.Object  { return &types.Widget{} }
func (s *StatusREST) Destroy()             {}
func (s *StatusREST) Kind() string         { return "Widget" }
func (s *StatusREST) GetSingularName() string { return "widget" }
func (s *StatusREST) NamespaceScoped() bool { return true }

// Get delegates to the main store.
func (s *StatusREST) Get(ctx context.Context, name string, opts *metav1.GetOptions) (runtime.Object, error) {
	return s.mainStore.Get(ctx, name, opts)
}

// Update implements rest.Updater for the status subresource.
// It preserves spec from the existing object, only applying status changes.
func (s *StatusREST) Update(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	options *metav1.UpdateOptions,
) (runtime.Object, bool, error) {
	// Wrap objInfo to enforce status-only semantics
	wrappedInfo := &statusUpdateInfo{
		inner: objInfo,
	}
	return s.mainStore.Update(ctx, name, wrappedInfo, createValidation, updateValidation, forceAllowCreate, options)
}

// statusUpdateInfo wraps UpdatedObjectInfo to ensure only status changes
// pass through; spec is always restored from the existing object.
type statusUpdateInfo struct {
	inner rest.UpdatedObjectInfo
}

func (u *statusUpdateInfo) Preconditions() *metav1.Preconditions {
	return u.inner.Preconditions()
}

func (u *statusUpdateInfo) UpdatedObject(ctx context.Context, oldObj runtime.Object) (runtime.Object, error) {
	// Let the inner info compute the desired object (applies patches etc.)
	newObj, err := u.inner.UpdatedObject(ctx, oldObj)
	if err != nil {
		return nil, err
	}

	// Now enforce: preserve spec from oldObj, only take status from newObj
	newWidget, ok := newObj.(*types.Widget)
	if !ok {
		return nil, fmt.Errorf("expected *Widget, got %T", newObj)
	}

	if oldObj != nil {
		oldWidget, ok := oldObj.(*types.Widget)
		if !ok {
			return nil, fmt.Errorf("expected *Widget for old, got %T", oldObj)
		}
		// Preserve spec from the stored object
		newWidget.Spec = oldWidget.Spec
	}

	return newWidget, nil
}

// Compile-time interface checks
var (
	_ rest.Storage              = (*StatusREST)(nil)
	_ rest.Scoper               = (*StatusREST)(nil)
	_ rest.KindProvider         = (*StatusREST)(nil)
	_ rest.SingularNameProvider = (*StatusREST)(nil)
	_ rest.Getter               = (*StatusREST)(nil)
	_ rest.Updater              = (*StatusREST)(nil)
)
