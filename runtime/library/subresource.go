package library

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
)

// StatusUpdater is a function that preserves spec from the existing
// object and only applies status changes from the new object.
// Consumers provide a type-specific implementation.
type StatusUpdater func(newObj, oldObj runtime.Object) runtime.Object

// SpecUpdater is a function that preserves status from the existing
// object and only applies spec changes from the new object.
// Consumers provide a type-specific implementation.
type SpecUpdater func(newObj, oldObj runtime.Object) runtime.Object

// StatusREST provides a /status subresource implementation. It shares
// the same underlying store but enforces status-only semantics on Update:
// spec is always preserved from the existing (stored) object.
//
// Register as "resource/status" in the API group's Resources map.
type StatusREST struct {
	mainStore     rest.Getter
	mainUpdater   rest.Updater
	statusUpdater StatusUpdater
	newFunc       func() runtime.Object
	kind          string
	singularName  string
	nsScoped      bool
}

// StatusRESTOptions configures a StatusREST.
type StatusRESTOptions struct {
	// MainStore is the primary REST store (must satisfy rest.Getter).
	MainStore rest.Getter
	// MainUpdater is the primary REST store's Update method.
	MainUpdater rest.Updater
	// StatusUpdater preserves spec from oldObj, takes status from newObj.
	StatusUpdater StatusUpdater
	// NewFunc returns a new empty instance of the resource type.
	NewFunc func() runtime.Object
	// Kind is the resource Kind.
	Kind string
	// SingularName is the singular name.
	SingularName string
	// NamespaceScoped indicates if the resource is namespace-scoped.
	NamespaceScoped bool
}

// NewStatusREST creates a StatusREST.
func NewStatusREST(opts StatusRESTOptions) *StatusREST {
	return &StatusREST{
		mainStore:     opts.MainStore,
		mainUpdater:   opts.MainUpdater,
		statusUpdater: opts.StatusUpdater,
		newFunc:       opts.NewFunc,
		kind:          opts.Kind,
		singularName:  opts.SingularName,
		nsScoped:      opts.NamespaceScoped,
	}
}

func (s *StatusREST) New() runtime.Object     { return s.newFunc() }
func (s *StatusREST) Destroy()                {}
func (s *StatusREST) Kind() string            { return s.kind }
func (s *StatusREST) GetSingularName() string { return s.singularName }
func (s *StatusREST) NamespaceScoped() bool   { return s.nsScoped }

// Get delegates to the main store.
func (s *StatusREST) Get(ctx context.Context, name string, opts *metav1.GetOptions) (runtime.Object, error) {
	return s.mainStore.Get(ctx, name, opts)
}

// Update enforces status-only semantics.
func (s *StatusREST) Update(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	options *metav1.UpdateOptions,
) (runtime.Object, bool, error) {
	wrappedInfo := &subresourceUpdateInfo{
		inner:   objInfo,
		updater: s.statusUpdater,
	}
	return s.mainUpdater.Update(ctx, name, wrappedInfo, createValidation, updateValidation, forceAllowCreate, options)
}

// SpecREST wraps the main REST Update to enforce spec-only semantics:
// on Update, status is preserved from the existing object. All other
// operations pass through to the underlying store.
//
// This is typically used as the main resource's REST when a /status
// subresource is also registered.
type SpecREST struct {
	// Inner is the underlying REST store. All methods except Update
	// are delegated directly.
	Inner       rest.Storage
	SpecUpdater SpecUpdater
}

// Update wraps the inner Update to preserve status from the existing object.
func (s *SpecREST) Update(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	options *metav1.UpdateOptions,
) (runtime.Object, bool, error) {
	wrappedInfo := &subresourceUpdateInfo{
		inner:   objInfo,
		updater: s.SpecUpdater,
	}
	updater, ok := s.Inner.(rest.Updater)
	if !ok {
		return nil, false, nil
	}
	return updater.Update(ctx, name, wrappedInfo, createValidation, updateValidation, forceAllowCreate, options)
}

// subresourceUpdateInfo wraps UpdatedObjectInfo to apply a subresource
// update function that preserves one half (spec or status) from the old object.
type subresourceUpdateInfo struct {
	inner   rest.UpdatedObjectInfo
	updater func(newObj, oldObj runtime.Object) runtime.Object
}

func (u *subresourceUpdateInfo) Preconditions() *metav1.Preconditions {
	return u.inner.Preconditions()
}

func (u *subresourceUpdateInfo) UpdatedObject(ctx context.Context, oldObj runtime.Object) (runtime.Object, error) {
	newObj, err := u.inner.UpdatedObject(ctx, oldObj)
	if err != nil {
		return nil, err
	}
	if oldObj != nil && u.updater != nil {
		newObj = u.updater(newObj, oldObj)
	}
	return newObj, nil
}

// Compile-time interface checks for StatusREST.
var (
	_ rest.Storage              = (*StatusREST)(nil)
	_ rest.Scoper               = (*StatusREST)(nil)
	_ rest.KindProvider         = (*StatusREST)(nil)
	_ rest.SingularNameProvider = (*StatusREST)(nil)
	_ rest.Getter               = (*StatusREST)(nil)
	_ rest.Updater              = (*StatusREST)(nil)
)
