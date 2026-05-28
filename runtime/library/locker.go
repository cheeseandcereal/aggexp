package library

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

	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// LockMode selects the lock granularity.
type LockMode string

const (
	// LockPerObject creates one Lease per (group, resource, namespace, name).
	LockPerObject LockMode = "per-object"
	// LockPerResource creates one Lease per (group, resource).
	LockPerResource LockMode = "per-resource"
)

// DefaultLeaseDuration is how long a lease is valid without renewal.
const DefaultLeaseDuration = 15 * time.Second

// DefaultLockNamespace is where lock Leases are stored.
const DefaultLockNamespace = "aggexp-locks"

// LockerOptions configures a LockedBackend.
type LockerOptions struct {
	// Inner is the WritableBackend to wrap with locking.
	Inner runtimestorage.WritableBackend
	// Client is used to create/get/update/delete Lease objects.
	Client kubernetes.Interface
	// Mode selects per-object or per-resource locking.
	Mode LockMode
	// GroupResource identifies the resource for lock naming.
	GroupResource schema.GroupResource
	// LeaseDuration is how long a lease is valid. Defaults to 15s.
	LeaseDuration time.Duration
	// LockNamespace is where Leases are created. Defaults to "aggexp-locks".
	LockNamespace string
	// Identity identifies this replica. Defaults to POD_NAME or hostname.
	Identity string
}

// LockedBackend wraps a WritableBackend with Lease-based per-object
// or per-resource locking for multi-replica deployments. Reads pass
// through without locking; writes acquire a Lease before proceeding.
type LockedBackend struct {
	inner         runtimestorage.WritableBackend
	client        kubernetes.Interface
	mode          LockMode
	identity      string
	gr            schema.GroupResource
	leaseDuration time.Duration
	lockNamespace string
}

// NewLockedBackend creates a LockedBackend.
func NewLockedBackend(opts LockerOptions) *LockedBackend {
	if opts.Inner == nil {
		panic("LockedBackend: Inner is required")
	}
	if opts.Client == nil {
		panic("LockedBackend: Client is required")
	}
	dur := opts.LeaseDuration
	if dur == 0 {
		dur = DefaultLeaseDuration
	}
	ns := opts.LockNamespace
	if ns == "" {
		ns = DefaultLockNamespace
	}
	id := opts.Identity
	if id == "" {
		id = os.Getenv("POD_NAME")
		if id == "" {
			id, _ = os.Hostname()
		}
	}
	return &LockedBackend{
		inner:         opts.Inner,
		client:        opts.Client,
		mode:          opts.Mode,
		identity:      id,
		gr:            opts.GroupResource,
		leaseDuration: dur,
		lockNamespace: ns,
	}
}

// --- Forwarded read methods (no locking) ---

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
	name := objectNameFromObj(obj)
	ns := objectNamespaceFromObj(obj)
	lockName := lb.lockName(ns, name)
	if err := lb.acquire(ctx, lockName); err != nil {
		return nil, err
	}
	defer lb.release(ctx, lockName)
	return lb.inner.Create(ctx, u, obj)
}

func (lb *LockedBackend) Update(ctx context.Context, u user.Info, name string, obj runtime.Object, forceAllowCreate bool) (runtime.Object, bool, error) {
	ns := objectNamespaceFromObj(obj)
	lockName := lb.lockName(ns, name)
	if err := lb.acquire(ctx, lockName); err != nil {
		return nil, false, err
	}
	defer lb.release(ctx, lockName)
	return lb.inner.Update(ctx, u, name, obj, forceAllowCreate)
}

func (lb *LockedBackend) Delete(ctx context.Context, u user.Info, name string) (runtime.Object, bool, error) {
	lockName := lb.lockName("", name)
	if err := lb.acquire(ctx, lockName); err != nil {
		return nil, false, err
	}
	defer lb.release(ctx, lockName)
	return lb.inner.Delete(ctx, u, name)
}

// lockName returns the Lease name for the given object or resource.
func (lb *LockedBackend) lockName(ns, name string) string {
	switch lb.mode {
	case LockPerResource:
		return fmt.Sprintf("%s.%s", lb.gr.Group, lb.gr.Resource)
	default: // per-object
		raw := fmt.Sprintf("%s.%s.%s.%s", lb.gr.Group, lb.gr.Resource, ns, name)
		if len(raw) <= 63 {
			return raw
		}
		h := sha256.Sum256([]byte(raw))
		return raw[:40] + fmt.Sprintf("-%x", h[:11])
	}
}

// acquire tries to create or take ownership of the Lease.
func (lb *LockedBackend) acquire(ctx context.Context, lockName string) error {
	leases := lb.client.CoordinationV1().Leases(lb.lockNamespace)
	dur := int32(lb.leaseDuration.Seconds())
	now := metav1.NewMicroTime(time.Now())

	for attempts := 0; attempts < 3; attempts++ {
		lease := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:      lockName,
				Namespace: lb.lockNamespace,
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

		existing, err := leases.Get(ctx, lockName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("lock get failed: %w", err)
		}

		holder := ""
		if existing.Spec.HolderIdentity != nil {
			holder = *existing.Spec.HolderIdentity
		}

		if holder == lb.identity {
			return nil
		}

		// Check expiry.
		if existing.Spec.RenewTime != nil && existing.Spec.LeaseDurationSeconds != nil {
			expiry := existing.Spec.RenewTime.Time.Add(time.Duration(*existing.Spec.LeaseDurationSeconds) * time.Second)
			if time.Now().Before(expiry) {
				return apierrors.NewConflict(lb.gr, lockName,
					fmt.Errorf("lock held by %s (expires %s)", holder, expiry.Format(time.RFC3339)))
			}
		}

		// Expired — try to take over.
		existing.Spec.HolderIdentity = &lb.identity
		existing.Spec.AcquireTime = &now
		existing.Spec.RenewTime = &now
		existing.Spec.LeaseDurationSeconds = &dur
		_, err = leases.Update(ctx, existing, metav1.UpdateOptions{})
		if err != nil {
			if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("lock update failed: %w", err)
		}
		klog.V(4).Infof("lock acquired (takeover): %s by %s", lockName, lb.identity)
		return nil
	}
	return apierrors.NewConflict(lb.gr, lockName,
		fmt.Errorf("lock acquisition failed after retries"))
}

func (lb *LockedBackend) release(ctx context.Context, lockName string) {
	leases := lb.client.CoordinationV1().Leases(lb.lockNamespace)
	err := leases.Delete(ctx, lockName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		klog.Warningf("lock release failed: %s: %v", lockName, err)
	}
}

// Compile-time check.
var _ runtimestorage.WritableBackend = (*LockedBackend)(nil)
