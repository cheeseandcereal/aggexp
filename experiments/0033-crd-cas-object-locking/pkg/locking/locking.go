// Package locking implements the 0033 lock-CR + CAS locking layer.
// It wraps a runtime/storage.WritableBackend, acquiring a lock CR
// on the host kube-apiserver before each mutation. Two granularities
// are supported:
//
//   - Mode "per-object": one ObjectLock CR per (group, resource,
//     namespace, name) tuple. Concurrent writes to different objects
//     do not contend.
//   - Mode "per-resource": one ResourceLock CR per (group, resource).
//     All writes serialize.
//
// The CAS algorithm:
//
//  1. GET the lock CR.
//  2. Not-found => CREATE with our identity. Conflict on Create
//     means a concurrent Create won; retry the loop.
//  3. Found, lockedBy == ourID => we already hold it. If
//     lockExpires < TTL/3 from now, renew via UPDATE (CAS on RV).
//     Conflict on the renew => retry.
//  4. Found, lockedBy != ourID, lockExpires < now => steal via
//     UPDATE (CAS on RV). Conflict => retry.
//  5. Found, lockedBy != ourID, lockExpires >= now => return 409
//     Conflict to the caller (lock contention).
//
// Release is best-effort: on the happy path we set lockedBy="" and
// lockExpires=now-1s; on failure we leave the lock and rely on
// expiry.
//
// **Important**: this layer does NOT manage GC of stale lock CRs.
// Expired locks become invisible (other replicas can steal them) but
// the CRs remain in host etcd. 0028's pattern would apply; deferred.
package locking

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"

	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// Mode selects locking granularity.
type Mode string

const (
	ModePerObject   Mode = "per-object"
	ModePerResource Mode = "per-resource"
)

// GVR for the two lock CRDs.
var (
	ObjectLockGVR = schema.GroupVersionResource{
		Group:    "aggexp.io",
		Version:  "v1",
		Resource: "objectlocks",
	}
	ResourceLockGVR = schema.GroupVersionResource{
		Group:    "aggexp.io",
		Version:  "v1",
		Resource: "resourcelocks",
	}
)

// Options configures Backend.
type Options struct {
	// Inner is the wrapped writable backend.
	Inner runtimestorage.WritableBackend
	// Dyn is the dynamic client to the host apiserver.
	Dyn dynamic.Interface
	// Mode is the locking granularity.
	Mode Mode
	// Group/Resource describe the resource being locked.
	Group    string
	Resource string
	// PodName is this replica's identity. Defaults to POD_NAME env
	// var, falling back to hostname.
	PodName string
	// TTL is the lock expiry from acquisition.
	TTL time.Duration
	// MaxRetries is the bounded retry budget per acquire.
	MaxRetries int
	// RetrySleep is the fixed sleep between retries.
	RetrySleep time.Duration
}

// ObserveAttempts is a public counter incremented per CAS attempt
// (every dyn round trip in the acquire loop). The scenarios script
// scrapes a /debug endpoint for this; it's the experiment's
// retry-count probe.
type ObserveAttempts struct {
	Total       atomic.Uint64
	Successes   atomic.Uint64
	Conflicts   atomic.Uint64
	Stolen      atomic.Uint64
	Renewals    atomic.Uint64
	StaleStolen atomic.Uint64 // lock taken because it was expired
}

// Backend wraps a WritableBackend with lock CR CAS gating.
type Backend struct {
	runtimestorage.Backend // delegate read methods straight through

	inner    runtimestorage.WritableBackend
	dyn      dynamic.Interface
	mode     Mode
	group    string
	resource string
	pod      string
	ttl      time.Duration
	retries  int
	sleep    time.Duration
	stats    *ObserveAttempts
}

// New constructs the locking Backend.
func New(opts Options) *Backend {
	pod := opts.PodName
	if pod == "" {
		pod = os.Getenv("POD_NAME")
	}
	if pod == "" {
		h, _ := os.Hostname()
		pod = h
	}
	if pod == "" {
		pod = "unknown"
	}
	if opts.TTL == 0 {
		opts.TTL = 15 * time.Second
	}
	if opts.MaxRetries == 0 {
		opts.MaxRetries = 8
	}
	if opts.RetrySleep == 0 {
		opts.RetrySleep = 25 * time.Millisecond
	}
	return &Backend{
		Backend:  opts.Inner,
		inner:    opts.Inner,
		dyn:      opts.Dyn,
		mode:     opts.Mode,
		group:    opts.Group,
		resource: opts.Resource,
		pod:      pod,
		ttl:      opts.TTL,
		retries:  opts.MaxRetries,
		sleep:    opts.RetrySleep,
		stats:    &ObserveAttempts{},
	}
}

// Stats returns the cumulative stat counters.
func (b *Backend) Stats() *ObserveAttempts { return b.stats }

// PodName returns the replica's identity.
func (b *Backend) PodName() string { return b.pod }

// Mode returns the configured granularity.
func (b *Backend) Mode() Mode { return b.mode }

// --- WritableBackend ---

// Create acquires the lock for obj.Name (per-object mode) or for
// the resource (per-resource mode), then forwards to the inner
// backend. Releases the lock after.
func (b *Backend) Create(ctx context.Context, u user.Info, obj runtime.Object) (runtime.Object, error) {
	name := metaNameOf(obj)
	if name == "" {
		return nil, fmt.Errorf("locking: Create requires obj.Name")
	}
	rel, err := b.acquire(ctx, name)
	if err != nil {
		return nil, err
	}
	defer rel()
	return b.inner.Create(ctx, u, obj)
}

// Update acquires lock, then forwards.
func (b *Backend) Update(ctx context.Context, u user.Info, name string, obj runtime.Object, forceAllowCreate bool) (runtime.Object, bool, error) {
	rel, err := b.acquire(ctx, name)
	if err != nil {
		return nil, false, err
	}
	defer rel()
	return b.inner.Update(ctx, u, name, obj, forceAllowCreate)
}

// Delete acquires lock, then forwards.
func (b *Backend) Delete(ctx context.Context, u user.Info, name string) (runtime.Object, bool, error) {
	rel, err := b.acquire(ctx, name)
	if err != nil {
		return nil, false, err
	}
	defer rel()
	return b.inner.Delete(ctx, u, name)
}

// --- locking core ---

func (b *Backend) lockGVR() schema.GroupVersionResource {
	if b.mode == ModePerResource {
		return ResourceLockGVR
	}
	return ObjectLockGVR
}

// LockName returns the deterministic name for a lock CR.
func (b *Backend) LockName(objectName string) string {
	grp := strings.ReplaceAll(b.group, ".", "-")
	var candidate string
	if b.mode == ModePerResource {
		candidate = fmt.Sprintf("%s--%s", grp, b.resource)
	} else {
		// Cluster-scoped resources only in this experiment;
		// namespace segment hard-coded to "cluster".
		candidate = fmt.Sprintf("%s--%s--cluster--%s", grp, b.resource, objectName)
	}
	if len(candidate) <= 253 && isDNS1123Subdomain(candidate) {
		return candidate
	}
	h := sha256.New()
	h.Write([]byte(candidate))
	sum := hex.EncodeToString(h.Sum(nil))
	prefix := "objlock-"
	if b.mode == ModePerResource {
		prefix = "reslock-"
	}
	return prefix + sum[:24]
}

var dns1123 = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

func isDNS1123Subdomain(s string) bool {
	if len(s) == 0 || len(s) > 253 {
		return false
	}
	return dns1123.MatchString(s)
}

// acquire runs the CAS loop until either the lock is held or a
// terminal condition triggers (other-holder fresh, retry budget
// exhausted, ctx canceled, unrelated host-apiserver error).
//
// On success, returns a release func.
func (b *Backend) acquire(ctx context.Context, objectName string) (func(), error) {
	gvr := b.lockGVR()
	name := b.LockName(objectName)

	for attempt := 0; attempt < b.retries; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		b.stats.Total.Add(1)

		now := time.Now()
		expires := now.Add(b.ttl)

		cur, err := b.dyn.Resource(gvr).Get(ctx, name, metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("locking: get %s: %w", name, err)
		}

		if apierrors.IsNotFound(err) {
			created, cerr := b.dyn.Resource(gvr).Create(ctx, b.buildLock(name, expires, objectName), metav1.CreateOptions{})
			if cerr != nil {
				if apierrors.IsAlreadyExists(cerr) || apierrors.IsConflict(cerr) {
					b.stats.Conflicts.Add(1)
					b.sleepRetry(ctx, attempt)
					continue
				}
				return nil, fmt.Errorf("locking: create %s: %w", name, cerr)
			}
			b.stats.Successes.Add(1)
			klog.V(3).InfoS("lock-acquired", "name", name, "by", b.pod, "mode", b.mode, "via", "create", "rv", created.GetResourceVersion())
			return b.releaserFor(name, created.GetResourceVersion()), nil
		}

		// Inspect existing lock.
		holder := getString(cur.Object, "spec", "lockedBy")
		exp := getString(cur.Object, "spec", "lockExpires")
		expT, _ := time.Parse(time.RFC3339Nano, exp)

		if holder == b.pod {
			// We already hold it. Renew if close to expiring.
			if expT.Sub(now) < b.ttl/3 {
				cur = renewedLock(cur, expires, b.pod)
				updated, uerr := b.dyn.Resource(gvr).Update(ctx, cur, metav1.UpdateOptions{})
				if uerr != nil {
					if apierrors.IsConflict(uerr) {
						b.stats.Conflicts.Add(1)
						b.sleepRetry(ctx, attempt)
						continue
					}
					return nil, fmt.Errorf("locking: renew %s: %w", name, uerr)
				}
				b.stats.Renewals.Add(1)
				b.stats.Successes.Add(1)
				return b.releaserFor(name, updated.GetResourceVersion()), nil
			}
			b.stats.Successes.Add(1)
			return b.releaserFor(name, cur.GetResourceVersion()), nil
		}

		if expT.Before(now) {
			// Steal the expired lock via CAS.
			cur = renewedLock(cur, expires, b.pod)
			updated, uerr := b.dyn.Resource(gvr).Update(ctx, cur, metav1.UpdateOptions{})
			if uerr != nil {
				if apierrors.IsConflict(uerr) {
					b.stats.Conflicts.Add(1)
					b.sleepRetry(ctx, attempt)
					continue
				}
				return nil, fmt.Errorf("locking: steal %s: %w", name, uerr)
			}
			b.stats.StaleStolen.Add(1)
			b.stats.Successes.Add(1)
			klog.V(3).InfoS("lock-stolen", "name", name, "by", b.pod, "from", holder)
			return b.releaserFor(name, updated.GetResourceVersion()), nil
		}

		// Active holder, fresh lock; refuse.
		b.stats.Conflicts.Add(1)
		return nil, apierrors.NewConflict(
			schema.GroupResource{Group: b.group, Resource: b.resource},
			objectName,
			fmt.Errorf("lock held by %q (expires %s)", holder, exp),
		)
	}

	return nil, apierrors.NewConflict(
		schema.GroupResource{Group: b.group, Resource: b.resource},
		objectName,
		fmt.Errorf("CAS retry budget exhausted (%d attempts)", b.retries),
	)
}

func (b *Backend) sleepRetry(ctx context.Context, _ int) {
	t := time.NewTimer(b.sleep)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// releaserFor returns a function that best-effort releases the lock
// by setting lockedBy=""/lockExpires=now-1s. Conflicts on the
// release are silently dropped — the lock will expire on its own.
func (b *Backend) releaserFor(name, rv string) func() {
	return func() {
		gvr := b.lockGVR()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		// Re-fetch (RV may have moved if we held across renews).
		cur, err := b.dyn.Resource(gvr).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			klog.V(4).InfoS("lock-release-skip", "name", name, "err", err)
			return
		}
		// Only release if we still hold it.
		if getString(cur.Object, "spec", "lockedBy") != b.pod {
			return
		}
		past := time.Now().Add(-1 * time.Second).UTC().Format(time.RFC3339Nano)
		_ = unstructured.SetNestedField(cur.Object, "", "spec", "lockedBy")
		_ = unstructured.SetNestedField(cur.Object, past, "spec", "lockExpires")
		if _, err := b.dyn.Resource(gvr).Update(ctx, cur, metav1.UpdateOptions{}); err != nil {
			// 409 here means another writer raced our release;
			// fine, expiry handles it.
			klog.V(4).InfoS("lock-release-failed", "name", name, "err", err)
			_ = rv
			return
		}
		klog.V(3).InfoS("lock-released", "name", name, "by", b.pod)
	}
}

// buildLock constructs an unstructured lock CR. spec.resourceRef is
// descriptive only.
func (b *Backend) buildLock(name string, expires time.Time, objectName string) *unstructured.Unstructured {
	gvr := b.lockGVR()
	apiVersion := gvr.Group + "/" + gvr.Version
	kind := "ObjectLock"
	if b.mode == ModePerResource {
		kind = "ResourceLock"
	}
	u := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata":   map[string]interface{}{"name": name},
		"spec": map[string]interface{}{
			"lockedBy":    b.pod,
			"lockExpires": expires.UTC().Format(time.RFC3339Nano),
			"resourceRef": map[string]interface{}{
				"group":     b.group,
				"resource":  b.resource,
				"namespace": "",
				"name":      objectName,
			},
		},
	}}
	return u
}

func renewedLock(cur *unstructured.Unstructured, expires time.Time, by string) *unstructured.Unstructured {
	out := cur.DeepCopy()
	_ = unstructured.SetNestedField(out.Object, by, "spec", "lockedBy")
	_ = unstructured.SetNestedField(out.Object, expires.UTC().Format(time.RFC3339Nano), "spec", "lockExpires")
	return out
}

func getString(obj map[string]interface{}, fields ...string) string {
	v, ok, _ := unstructured.NestedString(obj, fields...)
	if !ok {
		return ""
	}
	return v
}

// metaNameOf extracts metadata.name from a runtime.Object via the
// metav1 accessor interface.
func metaNameOf(obj runtime.Object) string {
	if a, ok := obj.(metav1Object); ok {
		return a.GetName()
	}
	return ""
}

type metav1Object interface {
	GetName() string
}

// Compile-time interface assertion.
var _ runtimestorage.WritableBackend = (*Backend)(nil)
