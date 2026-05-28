// Package specrest provides a main-resource REST wrapper that preserves
// status from the existing object on Update (so spec-only writes via
// PUT don't zero out status).
package specrest

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"

	"github.com/cheeseandcereal/aggexp/experiments/0038-subresources-status/pkg/types"
	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// SpecREST wraps the main REST adapter to enforce spec-only Update
// semantics: on Update, status is preserved from the existing object.
// All other operations (Get, List, Watch, Create, Delete) are passed
// through unchanged.
type SpecREST struct {
	*runtimestorage.REST
}

// New creates a SpecREST wrapping the given main store.
func New(store *runtimestorage.REST) *SpecREST {
	return &SpecREST{REST: store}
}

// Update wraps the inner Update to preserve status from existing object.
func (s *SpecREST) Update(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	options *metav1.UpdateOptions,
) (runtime.Object, bool, error) {
	wrappedInfo := &specUpdateInfo{inner: objInfo}
	return s.REST.Update(ctx, name, wrappedInfo, createValidation, updateValidation, forceAllowCreate, options)
}

// specUpdateInfo wraps UpdatedObjectInfo to preserve status from oldObj.
type specUpdateInfo struct {
	inner rest.UpdatedObjectInfo
}

func (u *specUpdateInfo) Preconditions() *metav1.Preconditions {
	return u.inner.Preconditions()
}

func (u *specUpdateInfo) UpdatedObject(ctx context.Context, oldObj runtime.Object) (runtime.Object, error) {
	newObj, err := u.inner.UpdatedObject(ctx, oldObj)
	if err != nil {
		return nil, err
	}

	// Preserve status from the existing object
	newWidget, ok := newObj.(*types.Widget)
	if !ok {
		return nil, fmt.Errorf("expected *Widget, got %T", newObj)
	}

	if oldObj != nil {
		oldWidget, ok := oldObj.(*types.Widget)
		if !ok {
			return nil, fmt.Errorf("expected *Widget for old, got %T", oldObj)
		}
		// Preserve status from stored object — spec-only endpoint
		newWidget.Status = oldWidget.Status
	}

	return newWidget, nil
}

// Compile-time check that we still satisfy Updater.
var _ rest.Updater = (*SpecREST)(nil)
