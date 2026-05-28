// Package occ provides an optimistic concurrency control wrapper for
// the runtime/storage.REST Update path. It intercepts the Update call,
// compares the incoming object's resourceVersion against the stored
// object's resourceVersion, and rejects the write with 409 Conflict
// if they don't match.
//
// This is the standard Kubernetes behavior: an Update with a stale
// metadata.resourceVersion is rejected.
//
// The substrate's REST adapter assigns RVs via PublishAdded/Modified
// but does not persist them back into the backend. This wrapper
// maintains a per-object RV map and stamps it on Get responses so
// clients see the RV and can pass it back on update.
package occ

import (
	"context"
	"fmt"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"

	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// Store wraps a *runtimestorage.REST and adds optimistic concurrency
// control to the Update path. It also maintains per-object RV tracking
// so that Get and List return objects with their assigned RVs.
type Store struct {
	*runtimestorage.REST
	gr schema.GroupResource

	mu  sync.RWMutex
	rvs map[string]string // name -> last assigned RV
}

// New creates an OCC-wrapped store.
func New(inner *runtimestorage.REST, gr schema.GroupResource) *Store {
	return &Store{
		REST: inner,
		gr:   gr,
		rvs:  make(map[string]string),
	}
}

// Get overrides the embedded REST.Get to stamp the per-object RV.
func (s *Store) Get(ctx context.Context, name string, opts *metav1.GetOptions) (runtime.Object, error) {
	obj, err := s.REST.Get(ctx, name, opts)
	if err != nil {
		return nil, err
	}
	s.stampStoredRV(obj, name)
	return obj, nil
}

// List overrides the embedded REST.List to stamp per-object RVs.
func (s *Store) List(ctx context.Context, opts *metainternalversion.ListOptions) (runtime.Object, error) {
	list, err := s.REST.List(ctx, opts)
	if err != nil {
		return nil, err
	}
	items, ierr := meta.ExtractList(list)
	if ierr == nil {
		for _, item := range items {
			acc, accErr := meta.Accessor(item)
			if accErr == nil {
				s.stampStoredRV(item, acc.GetName())
			}
		}
	}
	return list, nil
}

// Create overrides Create to track the assigned RV.
func (s *Store) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, opts *metav1.CreateOptions) (runtime.Object, error) {
	result, err := s.REST.Create(ctx, obj, createValidation, opts)
	if err != nil {
		return nil, err
	}
	// After PublishAdded has stamped the RV on result, record it.
	acc, accErr := meta.Accessor(result)
	if accErr == nil {
		s.mu.Lock()
		s.rvs[acc.GetName()] = acc.GetResourceVersion()
		s.mu.Unlock()
	}
	return result, nil
}

// Update implements rest.Updater with optimistic concurrency control.
func (s *Store) Update(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	_ *metav1.UpdateOptions,
) (runtime.Object, bool, error) {
	wb := s.writable()
	if wb == nil {
		return nil, false, apierrors.NewMethodNotSupported(s.gr, "update")
	}

	u := userFromCtx(ctx)

	// Get current state from backend
	existing, getErr := s.REST.Backend().Get(ctx, u, name)
	if getErr != nil && !apierrors.IsNotFound(getErr) {
		return nil, false, getErr
	}

	var current runtime.Object
	if getErr == nil {
		current = existing
		// Stamp the stored RV so UpdatedObject sees it
		s.stampStoredRV(current, name)
	}

	// Compute what the caller wants to write
	updated, err := objInfo.UpdatedObject(ctx, current)
	if err != nil {
		return nil, false, err
	}

	// OCC check: only on updates (not create-on-update)
	if current != nil {
		currentAcc, err := meta.Accessor(current)
		if err != nil {
			return nil, false, fmt.Errorf("cannot access current object metadata: %w", err)
		}
		updatedAcc, err := meta.Accessor(updated)
		if err != nil {
			return nil, false, fmt.Errorf("cannot access updated object metadata: %w", err)
		}

		incomingRV := updatedAcc.GetResourceVersion()
		storedRV := currentAcc.GetResourceVersion()

		// Standard kube behavior: empty RV on update is rejected.
		if incomingRV == "" {
			return nil, false, apierrors.NewConflict(
				s.gr, name,
				fmt.Errorf("resourceVersion must be specified for an update"),
			)
		}

		// Core OCC check: stale RV => 409 Conflict
		if incomingRV != storedRV {
			return nil, false, apierrors.NewConflict(
				s.gr, name,
				fmt.Errorf("the object has been modified; please apply your changes to the latest version and try again"),
			)
		}
	} else {
		// Object doesn't exist
		if !forceAllowCreate {
			return nil, false, getErr // propagate NotFound
		}
	}

	// Validation callbacks
	if current == nil {
		if createValidation != nil {
			if err := createValidation(ctx, updated); err != nil {
				return nil, false, err
			}
		}
	} else if updateValidation != nil {
		if err := updateValidation(ctx, updated, current); err != nil {
			return nil, false, err
		}
	}

	// Delegate to writable backend
	stored, created, err := wb.Update(ctx, u, name, updated, forceAllowCreate)
	if err != nil {
		return nil, false, err
	}
	if created {
		s.REST.PublishAdded(stored)
	} else {
		s.REST.PublishModified(stored)
	}

	// Record the assigned RV
	acc, accErr := meta.Accessor(stored)
	if accErr == nil {
		s.mu.Lock()
		s.rvs[acc.GetName()] = acc.GetResourceVersion()
		s.mu.Unlock()
	}
	return stored, created, nil
}

// Delete overrides Delete to clean up the RV map.
func (s *Store) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, opts *metav1.DeleteOptions) (runtime.Object, bool, error) {
	result, deleted, err := s.REST.Delete(ctx, name, deleteValidation, opts)
	if err != nil {
		return nil, false, err
	}
	if deleted {
		s.mu.Lock()
		delete(s.rvs, name)
		s.mu.Unlock()
	}
	return result, deleted, nil
}

// stampStoredRV stamps the tracked RV on obj if we have one for the given name.
func (s *Store) stampStoredRV(obj runtime.Object, name string) {
	s.mu.RLock()
	rv, ok := s.rvs[name]
	s.mu.RUnlock()
	if ok {
		if acc, err := meta.Accessor(obj); err == nil {
			acc.SetResourceVersion(rv)
		}
	}
}

// writable returns the WritableBackend if the backend implements it.
func (s *Store) writable() runtimestorage.WritableBackend {
	if wb, ok := s.REST.Backend().(runtimestorage.WritableBackend); ok {
		return wb
	}
	return nil
}

func userFromCtx(ctx context.Context) user.Info {
	if v, ok := genericapirequest.UserFrom(ctx); ok && v != nil {
		return v
	}
	return nil
}
