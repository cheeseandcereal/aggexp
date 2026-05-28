// Package locker wraps a WritableBackend with Lease-based locking.
package locker

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/cheeseandcereal/aggexp/experiments/0032-lease-based-object-locking/pkg/types"
	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// Mode selects the lock granularity.
type Mode string

const (
	ModePerObject   Mode = "per-object"
	ModePerResource Mode = "per-resource"
)

// LeaseDuration is how long a lease is valid without renewal.
const LeaseDuration = 15 * time.Second

// LockNamespace is where lock Leases are stored.
const LockNamespace = "aggexp-locks"

// LockedBackend wraps a WritableBackend with Lease-based locking.
type LockedBackend struct {
	inner    runtimestorage.WritableBackend
	client   kubernetes.Interface
	mode     Mode
	identity string
	gr       schema.GroupResource
}

// New creates a LockedBackend.
func New(inner runtimestorage.WritableBackend, client kubernetes.Interface, mode Mode, gr schema.GroupResource) *LockedBackend {
	id := os.Getenv("POD_NAME")
	if id == "" {
		h, _ := os.Hostname()
		id = h
	}
	return &LockedBackend{
		inner:    inner,
		client:   client,
		mode:     mode,
		identity: id,
		gr:       gr,
	}
}

// --- Forwarded read methods (no locking needed) ---

func (lb *LockedBackend) New() runtime.Object                                              { return lb.inner.New() }
func (lb *LockedBackend) NewList() runtime.Object                                          { return lb.inner.NewList() }
func (lb *LockedBackend) Kind() string                                                     { return lb.inner.Kind() }
func (lb *LockedBackend) SingularName() string                                             { return lb.inner.SingularName() }
func (lb *LockedBackend) NamespaceScoped() bool                                            { return lb.inner.NamespaceScoped() }
func (lb *LockedBackend) Get(ctx context.Context, u user.Info, name string) (runtime.Object, error) { return lb.inner.Get(ctx, u, name) }
func (lb *LockedBackend) List(ctx context.Context, u user.Info, opts runtimestorage.ListOptions) (runtime.Object, error) { return lb.inner.List(ctx, u, opts) }
func (lb *LockedBackend) TableColumns() []metav1.TableColumnDefinition                     { return lb.inner.TableColumns() }
func (lb *LockedBackend) RowsFor(obj runtime.Object) ([]metav1.TableRow, error)            { return lb.inner.RowsFor(obj) }

// --- Write methods (acquire lock first) ---

func (lb *LockedBackend) Create(ctx context.Context, u user.Info, obj runtime.Object) (runtime.Object, error) {
	w := obj.(*types.Widget)
	lockName := lb.lockName(w.Namespace, w.Name)
	if err := lb.acquire(ctx, lockName); err != nil {
		return nil, err
	}
	defer lb.release(ctx, lockName)
	return lb.inner.Create(ctx, u, obj)
}

func (lb *LockedBackend) Update(ctx context.Context, u user.Info, name string, obj runtime.Object, forceAllowCreate bool) (runtime.Object, bool, error) {
	w := obj.(*types.Widget)
	lockName := lb.lockName(w.Namespace, name)
	if err := lb.acquire(ctx, lockName); err != nil {
		return nil, false, err
	}
	defer lb.release(ctx, lockName)
	return lb.inner.Update(ctx, u, name, obj, forceAllowCreate)
}

func (lb *LockedBackend) Delete(ctx context.Context, u user.Info, name string) (runtime.Object, bool, error) {
	// For delete we don't have namespace from the object; use empty string.
	// In a real implementation we'd extract namespace from context.
	ns := "" // TODO: extract from ctx if namespace-scoped
	lockName := lb.lockName(ns, name)
	if err := lb.acquire(ctx, lockName); err != nil {
		return nil, false, err
	}
	defer lb.release(ctx, lockName)
	return lb.inner.Delete(ctx, u, name)
}

// lockName returns the Lease name for the given object or resource.
func (lb *LockedBackend) lockName(ns, name string) string {
	switch lb.mode {
	case ModePerResource:
		return fmt.Sprintf("%s.%s", lb.gr.Group, lb.gr.Resource)
	default: // per-object
		raw := fmt.Sprintf("%s.%s.%s.%s", lb.gr.Group, lb.gr.Resource, ns, name)
		if len(raw) <= 63 {
			return raw
		}
		// Truncate with SHA suffix for DNS compliance
		h := sha256.Sum256([]byte(raw))
		return raw[:40] + fmt.Sprintf("-%x", h[:11])
	}
}

// acquire tries to create or take ownership of the Lease.
func (lb *LockedBackend) acquire(ctx context.Context, lockName string) error {
	leases := lb.client.CoordinationV1().Leases(LockNamespace)
	dur := int32(LeaseDuration.Seconds())
	now := metav1.NewMicroTime(time.Now())

	// Retry loop handles the race between Create-fails-AlreadyExists
	// and a concurrent release (Delete) making the Lease disappear
	// between our Create-fail and our subsequent Get.
	for attempts := 0; attempts < 3; attempts++ {
		// Try to create the Lease (fast path: no contention)
		lease := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:      lockName,
				Namespace: LockNamespace,
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       &lb.identity,
				LeaseDurationSeconds: &dur,
				AcquireTime:          &now,
				RenewTime:            &now,
			},
		}
		_, err := leases.Create(ctx, lease, metav1.CreateOptions{})
		if err == nil {
			klog.V(4).Infof("lock acquired (create): %s by %s", lockName, lb.identity)
			return nil
		}
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("lock create failed: %w", err)
		}

		// Lease exists — check if we can take it (expired or we already hold it)
		existing, err := leases.Get(ctx, lockName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				// Race: someone deleted the Lease between our Create and Get.
				// Retry from the top.
				klog.V(4).Infof("lock race (deleted between create and get): %s, retrying", lockName)
				continue
			}
			return fmt.Errorf("lock get failed: %w", err)
		}

		holder := ""
		if existing.Spec.HolderIdentity != nil {
			holder = *existing.Spec.HolderIdentity
		}

		// Already ours?
		if holder == lb.identity {
			klog.V(4).Infof("lock already held: %s by %s", lockName, lb.identity)
			return nil
		}

		// Check expiry
		if existing.Spec.RenewTime != nil && existing.Spec.LeaseDurationSeconds != nil {
			expiry := existing.Spec.RenewTime.Time.Add(time.Duration(*existing.Spec.LeaseDurationSeconds) * time.Second)
			if time.Now().Before(expiry) {
				// Lock is held by someone else and not expired
				return apierrors.NewConflict(lb.gr, lockName,
					fmt.Errorf("lock held by %s (expires %s)", holder, expiry.Format(time.RFC3339)))
			}
		}

		// Expired — try to take over via CAS (update with resourceVersion)
		existing.Spec.HolderIdentity = &lb.identity
		existing.Spec.AcquireTime = &now
		existing.Spec.RenewTime = &now
		existing.Spec.LeaseDurationSeconds = &dur
		_, err = leases.Update(ctx, existing, metav1.UpdateOptions{})
		if err != nil {
			if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
				// CAS lost or Lease deleted mid-takeover; retry
				klog.V(4).Infof("lock CAS race: %s, retrying", lockName)
				continue
			}
			return fmt.Errorf("lock update failed: %w", err)
		}
		klog.V(4).Infof("lock acquired (takeover): %s by %s", lockName, lb.identity)
		return nil
	}
	return apierrors.NewConflict(lb.gr, lockName,
		fmt.Errorf("lock acquisition failed after retries (concurrent contention)"))
}

// release deletes or clears the Lease after a write completes.
func (lb *LockedBackend) release(ctx context.Context, lockName string) {
	leases := lb.client.CoordinationV1().Leases(LockNamespace)
	err := leases.Delete(ctx, lockName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		klog.Warningf("lock release failed: %s: %v", lockName, err)
	} else {
		klog.V(4).Infof("lock released: %s", lockName)
	}
}

// Compile-time check.
var _ runtimestorage.WritableBackend = (*LockedBackend)(nil)
