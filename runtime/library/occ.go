package library

import (
	"fmt"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// occTracker maintains per-object resource version tracking for
// optimistic concurrency control. On Update, it compares the incoming
// object's RV against the stored RV and rejects with 409 Conflict
// if they don't match.
type occTracker struct {
	mu  sync.RWMutex
	rvs map[string]string // name -> last assigned RV
	gr  schema.GroupResource
}

func newOCCTracker(gr schema.GroupResource) *occTracker {
	return &occTracker{
		rvs: make(map[string]string),
		gr:  gr,
	}
}

// recordRV stores the assigned RV for an object after publish.
func (t *occTracker) recordRV(obj runtime.Object) {
	acc, err := meta.Accessor(obj)
	if err != nil {
		return
	}
	t.mu.Lock()
	t.rvs[acc.GetName()] = acc.GetResourceVersion()
	t.mu.Unlock()
}

// stampRV sets the tracked RV on obj for the given name, so clients
// see the correct RV to pass back on Update.
func (t *occTracker) stampRV(obj runtime.Object, name string) {
	t.mu.RLock()
	rv, ok := t.rvs[name]
	t.mu.RUnlock()
	if ok {
		if acc, err := meta.Accessor(obj); err == nil {
			acc.SetResourceVersion(rv)
		}
	}
}

// stampListRVs stamps tracked RVs on each item in a list.
func (t *occTracker) stampListRVs(list runtime.Object) {
	items, err := meta.ExtractList(list)
	if err != nil {
		return
	}
	for _, item := range items {
		acc, accErr := meta.Accessor(item)
		if accErr == nil {
			t.stampRV(item, acc.GetName())
		}
	}
}

// checkConflict verifies the incoming update's RV matches the stored RV.
// Returns nil if the check passes, or a 409 Conflict error.
func (t *occTracker) checkConflict(name string, current, updated runtime.Object) error {
	currentAcc, err := meta.Accessor(current)
	if err != nil {
		return fmt.Errorf("cannot access current object metadata: %w", err)
	}
	updatedAcc, err := meta.Accessor(updated)
	if err != nil {
		return fmt.Errorf("cannot access updated object metadata: %w", err)
	}

	incomingRV := updatedAcc.GetResourceVersion()
	storedRV := currentAcc.GetResourceVersion()

	if incomingRV == "" {
		return apierrors.NewConflict(
			t.gr, name,
			fmt.Errorf("resourceVersion must be specified for an update"),
		)
	}
	if incomingRV != storedRV {
		return apierrors.NewConflict(
			t.gr, name,
			fmt.Errorf("the object has been modified; please apply your changes to the latest version and try again"),
		)
	}
	return nil
}

// deleteRV removes tracking for a deleted object.
func (t *occTracker) deleteRV(name string) {
	t.mu.Lock()
	delete(t.rvs, name)
	t.mu.Unlock()
}
