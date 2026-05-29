// Package locking implements experiment 0043's EMBEDDED per-object
// write lock. The lock lives as a spec.lock subfield ON THE SAME
// metadata CR that carries the served object's KRM metadata (0042),
// CAS'd on that CR's own resourceVersion. This collapses the separate
// lock object validated by 0032 (a coordination.k8s.io Lease) and
// 0033 (a separate custom CR) into a single CAS surface with a single
// lifecycle.
//
// The contract is adapted directly from 0032/0033:
//
//   - Acquire CAS-writes the lock holder onto the CR. A fresh lock
//     held by another replica fails fast with 409 (0033 semantics) —
//     no acquirer-side spin. A CAS-level conflict (two replicas racing
//     the same write) is retried with a small budget.
//   - An expired lease (renewedAt + leaseDuration < now) is stealable:
//     the acquirer takes over via CAS, exactly as 0032/0033 did.
//   - A renewal goroutine re-stamps renewedAt every leaseDuration/3 so
//     a slow backend op does not lose its lease mid-operation. It is
//     on by default; the caller can disable it.
//
// The load-bearing 0043 difference from 0032/0033: because the lock
// shares the served object's CR, acquire/release/renewal advance the
// served object's resourceVersion and fire the metadata informer
// ("lock churn"). The emission filter (pkg/metastore) keeps that churn
// invisible to watchers, and the REST layer captures the pre-acquire
// RV for the OCC check before acquire bumps it.
//
// This code is intentionally NOT runtime/library/locker.go (which
// targets a separate Lease). It is a parallel, embedded-subfield
// implementation, duplicated per the lab ethos.
package locking

import (
	"context"
	"fmt"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"

	"github.com/cheeseandcereal/aggexp/experiments/0047-host-etcd-write-ceiling/pkg/metastore"
)

// Defaults adopted from 0032/0033 (recorded in the README Decisions).
const (
	DefaultLeaseDuration = 15 * time.Second
	retryAttempts        = 3
	retryBaseBackoff     = 25 * time.Millisecond
)

// Locker acquires the embedded lock on a served object's metadata CR.
type Locker struct {
	store         *metastore.Store
	gr            schema.GroupResource
	identity      string
	leaseDuration time.Duration
	renewEnabled  bool
}

// Options configures a Locker.
type Options struct {
	Store         *metastore.Store
	GroupResource schema.GroupResource
	Identity      string
	LeaseDuration time.Duration
	// RenewEnabled turns the renewal heartbeat on (default true via
	// New when left as the zero value of a *bool-free struct; callers
	// set it explicitly).
	RenewEnabled bool
}

// New constructs a Locker.
func New(opts Options) *Locker {
	if opts.Store == nil {
		panic("locking.New: Store is required")
	}
	dur := opts.LeaseDuration
	if dur == 0 {
		dur = DefaultLeaseDuration
	}
	return &Locker{
		store:         opts.Store,
		gr:            opts.GroupResource,
		identity:      opts.Identity,
		leaseDuration: dur,
		renewEnabled:  opts.RenewEnabled,
	}
}

// Handle represents a held embedded lock. It carries the raw CR (with
// the RV minted by the acquire write) so the commit/release CAS-writes
// against the right version, plus the pre-acquire RV the OCC check
// must use.
type Handle struct {
	locker *Locker
	ref    metastore.ResourceRef

	mu  sync.Mutex
	raw *unstructured.Unstructured // latest CR we hold, with current RV

	// PreAcquireRV is the served object's resourceVersion BEFORE this
	// acquire bumped it. "" if the CR did not exist (Create path).
	// The REST layer's OCC check compares the client's RV against
	// this, never the post-acquire RV.
	PreAcquireRV string

	// renewal lifecycle.
	stopRenew chan struct{}
	renewDone chan struct{}
	renewing  bool
}

// Acquire takes the embedded lock for ref. On success it returns a
// Handle whose PreAcquireRV is the served object's RV before the lock
// write. On a fresh lock held by another replica it returns a 409
// Conflict (apierrors.IsConflict). Renewal (if enabled) starts
// immediately.
func (l *Locker) Acquire(ctx context.Context, ref metastore.ResourceRef) (*Handle, error) {
	dur := int32(l.leaseDuration.Seconds())

	var lastErr error
	for attempt := 0; attempt < retryAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(retryBaseBackoff * time.Duration(1<<uint(attempt-1)))
		}

		raw, err := l.store.GetRawDirect(ctx, ref)
		if err != nil {
			lastErr = fmt.Errorf("lock get: %w", err)
			continue
		}

		now := metav1.NewTime(time.Now().UTC())
		desired := &metastore.LockState{
			HolderIdentity:       l.identity,
			AcquiredAt:           &now,
			RenewedAt:            &now,
			LeaseDurationSeconds: dur,
		}

		if raw == nil {
			// No CR yet (Create path). CAS-create with the lock. The
			// pre-acquire RV is empty: the object did not exist.
			created, cerr := l.store.CreateRawWithLock(ctx, ref, desired)
			if cerr != nil {
				if apierrors.IsAlreadyExists(cerr) {
					// Lost the create race; retry reads the CR.
					lastErr = cerr
					continue
				}
				return nil, apierrors.NewInternalError(fmt.Errorf("lock create: %w", cerr))
			}
			return l.newHandle(ref, created, ""), nil
		}

		preRV := raw.GetResourceVersion()
		existing := metastore.LockFrom(raw)

		// Already ours? (re-entrant acquire on the same replica) —
		// refresh the stamp and proceed.
		if existing != nil && existing.HolderIdentity == l.identity {
			metastore.SetLockOn(raw, desired)
			updated, uerr := l.store.UpdateRaw(ctx, raw)
			if uerr != nil {
				if apierrors.IsConflict(uerr) {
					lastErr = uerr
					continue
				}
				return nil, apierrors.NewInternalError(fmt.Errorf("lock re-stamp: %w", uerr))
			}
			l.store.WriteCounters.LockAcquireUpdate.Add(1)
			return l.newHandle(ref, updated, preRV), nil
		}

		// Held by someone else and still fresh? Fail fast (0033).
		if existing.Held() && !expired(existing) {
			klog.V(3).InfoS("lock-held-fresh-409", "replica", l.identity, "ref", refLog(ref), "holder", existing.HolderIdentity)
			return nil, apierrors.NewConflict(l.gr, ref.Name,
				fmt.Errorf("object %q is locked by %q", ref.Name, existing.HolderIdentity))
		}

		// Free or expired: take it via CAS on preRV.
		if existing.Held() {
			klog.V(2).InfoS("lock-takeover", "replica", l.identity, "ref", refLog(ref), "from", existing.HolderIdentity)
		}
		metastore.SetLockOn(raw, desired)
		updated, uerr := l.store.UpdateRaw(ctx, raw)
		if uerr != nil {
			if apierrors.IsConflict(uerr) {
				// CAS lost: someone wrote between our read and write.
				lastErr = uerr
				continue
			}
			return nil, apierrors.NewInternalError(fmt.Errorf("lock acquire update: %w", uerr))
		}
		l.store.WriteCounters.LockAcquireUpdate.Add(1)
		return l.newHandle(ref, updated, preRV), nil
	}
	if lastErr != nil && apierrors.IsConflict(lastErr) {
		// Exhausted the retry budget on CAS conflicts — surface 409.
		return nil, apierrors.NewConflict(l.gr, ref.Name,
			fmt.Errorf("lock acquisition for %q failed after %d attempts: %v", ref.Name, retryAttempts, lastErr))
	}
	return nil, apierrors.NewInternalError(fmt.Errorf("lock acquisition for %q failed: %v", ref.Name, lastErr))
}

func (l *Locker) newHandle(ref metastore.ResourceRef, raw *unstructured.Unstructured, preRV string) *Handle {
	h := &Handle{
		locker:       l,
		ref:          ref,
		raw:          raw,
		PreAcquireRV: preRV,
	}
	if l.renewEnabled {
		h.startRenew()
	}
	return h
}

// expired reports whether a lease's renewedAt + leaseDuration is in
// the past (stealable).
func expired(ls *metastore.LockState) bool {
	if ls == nil || ls.RenewedAt == nil || ls.LeaseDurationSeconds <= 0 {
		// No usable renewal info: treat as expired/stealable.
		return true
	}
	expiry := ls.RenewedAt.Time.Add(time.Duration(ls.LeaseDurationSeconds) * time.Second)
	return time.Now().After(expiry)
}

// startRenew launches the renewal heartbeat: re-stamp renewedAt every
// leaseDuration/3 via CAS until Commit/Release stops it.
func (h *Handle) startRenew() {
	h.stopRenew = make(chan struct{})
	h.renewDone = make(chan struct{})
	h.renewing = true
	interval := h.locker.leaseDuration / 3
	if interval <= 0 {
		interval = time.Second
	}
	go func() {
		defer close(h.renewDone)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-h.stopRenew:
				return
			case <-t.C:
				if err := h.renew(); err != nil {
					klog.Warningf("lock renew failed ref=%s: %v", refLog(h.ref), err)
				}
			}
		}
	}()
}

// renew re-stamps renewedAt on the held CR via CAS. On a CAS conflict
// it re-reads (someone else may have legitimately written the CR, e.g.
// our own commit racing — the renew just gives up that round).
func (h *Handle) renew() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.raw == nil {
		return nil
	}
	now := metav1.NewTime(time.Now().UTC())
	ls := &metastore.LockState{
		HolderIdentity:       h.locker.identity,
		AcquiredAt:           lockAcquiredAt(h.raw),
		RenewedAt:            &now,
		LeaseDurationSeconds: int32(h.locker.leaseDuration.Seconds()),
	}
	metastore.SetLockOn(h.raw, ls)
	updated, err := h.locker.store.UpdateRaw(context.Background(), h.raw)
	if err != nil {
		if apierrors.IsConflict(err) {
			// Re-read to recover the current RV for the next round.
			fresh, gerr := h.locker.store.GetRawDirect(context.Background(), h.ref)
			if gerr == nil && fresh != nil {
				h.raw = fresh
			}
			return nil
		}
		return err
	}
	h.raw = updated
	h.locker.store.WriteCounters.LockRenew.Add(1)
	klog.V(4).InfoS("lock-renew", "replica", h.locker.identity, "ref", refLog(h.ref), "rv", updated.GetResourceVersion())
	return nil
}

func lockAcquiredAt(u *unstructured.Unstructured) *metav1.Time {
	if ls := metastore.LockFrom(u); ls != nil && ls.AcquiredAt != nil {
		return ls.AcquiredAt
	}
	now := metav1.NewTime(time.Now().UTC())
	return &now
}

// stopRenewal halts the heartbeat (idempotent).
func (h *Handle) stopRenewal() {
	if h.renewing {
		close(h.stopRenew)
		<-h.renewDone
		h.renewing = false
	}
}

// Commit writes the body hash + KRM metadata onto the held CR and
// clears the lock in a SINGLE CR write (the "commit + release" of the
// embedded design), advancing the RV exactly once. It stops renewal
// first. Returns the committed Record (carrying the fresh authoritative
// RV).
func (h *Handle) Commit(ctx context.Context, rec *metastore.Record) (*metastore.Record, error) {
	h.stopRenewal()
	h.mu.Lock()
	defer h.mu.Unlock()
	// CAS on the RV we currently hold.
	committed, err := h.locker.store.PutBodyHashAndMeta(ctx, h.raw, rec)
	if err != nil {
		return nil, err
	}
	h.raw = nil
	return committed, nil
}

// Release clears the lock WITHOUT committing metadata (used on a
// backend failure so a retry is clean). Best-effort; stops renewal.
func (h *Handle) Release(ctx context.Context) {
	h.stopRenewal()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.raw == nil {
		return
	}
	metastore.SetLockOn(h.raw, nil)
	if _, err := h.locker.store.UpdateRaw(ctx, h.raw); err != nil {
		if !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) {
			klog.Warningf("lock release failed ref=%s: %v", refLog(h.ref), err)
		}
	} else {
		h.locker.store.WriteCounters.LockRelease.Add(1)
	}
	h.raw = nil
}

func refLog(r metastore.ResourceRef) string {
	ns := r.Namespace
	if ns == "" {
		ns = "cluster"
	}
	return fmt.Sprintf("%s/%s/%s/%s", r.Group, r.Resource, ns, r.Name)
}
