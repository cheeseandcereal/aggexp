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

	"github.com/cheeseandcereal/aggexp/experiments/0049-locked-write-transaction/pkg/metastore"
)

// Defaults adopted from 0032/0033 (recorded in the README Decisions).
const (
	DefaultLeaseDuration = 15 * time.Second
	retryAttempts        = 3
	retryBaseBackoff     = 25 * time.Millisecond

	// DefaultTxnAttempts is 0049's transaction-retry budget for the
	// post-acquire body+commit critical section. Distinct from the
	// 0043 acquire budget (retryAttempts=3): a contended commit may
	// need to re-read the served object after a peer commits, which is
	// a heavier round-trip than a bare acquire CAS, so we give it a
	// touch more headroom. 5 chosen arbitrarily; tune via measurement.
	DefaultTxnAttempts = 5
)

// Locker acquires the embedded lock on a served object's metadata CR.
type Locker struct {
	store         *metastore.Store
	gr            schema.GroupResource
	identity      string
	leaseDuration time.Duration
	renewEnabled  bool

	// 0049 transaction discipline.
	txnEnabled  bool
	txnAttempts int
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

	// TxnEnabled turns on 0049's locked-write TRANSACTION discipline:
	// the post-acquire body+commit writes run inside a retry loop that
	// re-reads on a CAS conflict and surfaces budget exhaustion as 409
	// (Conflict), never 500. When false, the locker reproduces the
	// 0048 single-shot post-acquire behavior (commit CAS conflict ->
	// caller turns it into a 500). The 0049 regression baseline
	// (scenario 1) runs with TxnEnabled=false.
	TxnEnabled bool
	// TxnAttempts is the transaction-retry budget (default
	// DefaultTxnAttempts when <= 0).
	TxnAttempts int
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
	attempts := opts.TxnAttempts
	if attempts <= 0 {
		attempts = DefaultTxnAttempts
	}
	return &Locker{
		store:         opts.Store,
		gr:            opts.GroupResource,
		identity:      opts.Identity,
		leaseDuration: dur,
		renewEnabled:  opts.RenewEnabled,
		txnEnabled:    opts.TxnEnabled,
		txnAttempts:   attempts,
	}
}

// TxnEnabled reports whether the transaction discipline is on.
func (l *Locker) TxnEnabled() bool { return l.txnEnabled }

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
	}
	h.raw = nil
}

// ---- 0049 locked-write transaction ----
//
// The 0048 capstone surfaced the gap this closes: Acquire serializes
// lock ACQUISITION, but the post-acquire body Put and commit-release
// writes were single-shot. Under genuine cross-replica contention a
// writer whose lease was stolen (takeover after expiry), or whose body
// Put raced a concurrent Put, lost its own CAS and the REST layer
// turned that into a 500 InternalError rather than a clean 409.
//
// WriteTxn wraps acquire -> body -> commit in one retriable
// transaction (README candidate (a): "commit-path retry under the held
// lock"). On a recoverable CAS conflict at ANY of the three steps it
// releases, re-reads, and retries the whole critical section within
// txnAttempts. Exhausting the budget surfaces a 409 Conflict, never a
// 500. The acquire step keeps its own 0043 budget; this is the OUTER
// budget covering the body+commit writes the 0043 path never retried.

// WriteOp describes one logical locked write. WriteBody performs the
// body-CR write (idempotent put); it must return an apierrors conflict
// (IsConflict) on a CAS loss so the transaction can retry. BuildRecord
// builds the metadata Record to commit; attempt is the 0-based retry
// index (so a caller can re-read prior UID/creationTimestamp from the
// authoritative store on a retry if it wishes).
type WriteOp struct {
	// WriteBody writes the business body. A CAS conflict MUST be
	// returned as an apierrors.IsConflict error to be retriable.
	WriteBody func(ctx context.Context) error
	// BuildRecord builds the metadata Record to commit. Called once
	// per attempt (after WriteBody) so it can reflect the latest body.
	BuildRecord func(ctx context.Context, attempt int) (*metastore.Record, error)
	// OnConflictRetry, if set, is called before each retry (attempt >
	// 0) with the attempt index, for instrumentation/logging.
	OnConflictRetry func(attempt int)
}

// TxnResult carries the committed record plus the retry depth observed
// (0 = succeeded on the first attempt).
type TxnResult struct {
	Record *metastore.Record
	Depth  int
}

// WriteTxn runs op as a locked-write transaction for ref. When the
// locker's transaction discipline is disabled it degrades to the 0048
// single-shot behavior (one acquire, one body, one commit; a commit
// CAS conflict is returned to the caller as-is, which the REST layer
// turns into a 500 — the regression baseline).
//
// preRVCheck, if non-nil, is invoked once per attempt with the served
// object's current authoritative RecordRV (before this attempt's
// acquire bumps it). It returns a non-nil error to abort the write
// (used for the 0043 pre-acquire OCC check). A genuine OCC failure
// (the object was modified by a peer) is a legitimate 409 and is NOT
// retried.
func (l *Locker) WriteTxn(ctx context.Context, ref metastore.ResourceRef, op WriteOp, preRVCheck func(curRV string) error) (*TxnResult, error) {
	attempts := 1
	if l.txnEnabled {
		attempts = l.txnAttempts
	}

	var lastConflict error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			if op.OnConflictRetry != nil {
				op.OnConflictRetry(attempt)
			}
			time.Sleep(retryBaseBackoff * time.Duration(1<<uint(min(attempt-1, 4))))
		}

		// 0043 pre-acquire OCC: capture the served RV BEFORE acquire.
		if preRVCheck != nil {
			curRV := ""
			if rec, _ := l.store.GetDirect(ctx, ref); rec != nil {
				curRV = rec.RecordRV
			}
			if cerr := preRVCheck(curRV); cerr != nil {
				return nil, cerr // genuine OCC conflict: do not retry.
			}
		}

		h, lerr := l.Acquire(ctx, ref)
		if lerr != nil {
			// Acquire already 409s on a fresh foreign lock or exhausted
			// its own budget. If the txn discipline is on, a 409 here
			// is retriable at the outer level (the holder may release
			// momentarily); otherwise surface it.
			if l.txnEnabled && apierrors.IsConflict(lerr) {
				lastConflict = lerr
				klog.V(3).InfoS("txn-acquire-conflict-retry", "replica", l.identity, "ref", refLog(ref), "attempt", attempt)
				continue
			}
			return nil, lerr
		}

		// Body write (idempotent put). A CAS conflict is retriable.
		if berr := op.WriteBody(ctx); berr != nil {
			h.Release(ctx)
			if l.txnEnabled && apierrors.IsConflict(berr) {
				lastConflict = berr
				klog.V(3).InfoS("txn-body-conflict-retry", "replica", l.identity, "ref", refLog(ref), "attempt", attempt)
				continue
			}
			return nil, apierrors.NewInternalError(fmt.Errorf("backend.Put: %w", berr))
		}

		rec, rerr := op.BuildRecord(ctx, attempt)
		if rerr != nil {
			h.Release(ctx)
			return nil, rerr
		}

		committed, perr := h.Commit(ctx, rec)
		if perr != nil {
			if l.txnEnabled && apierrors.IsConflict(perr) {
				// The held RV moved under us (lease takeover / renewal
				// race / peer commit). Re-read and retry the whole
				// critical section.
				lastConflict = perr
				klog.V(3).InfoS("txn-commit-conflict-retry", "replica", l.identity, "ref", refLog(ref), "attempt", attempt)
				continue
			}
			// Discipline off, or a non-conflict error: surface as the
			// caller expects (REST turns this into a 500 — baseline).
			if apierrors.IsConflict(perr) {
				return nil, perr
			}
			return nil, apierrors.NewInternalError(fmt.Errorf("metastore.Commit: %w", perr))
		}
		return &TxnResult{Record: committed, Depth: attempt}, nil
	}

	// Budget exhausted on CAS conflicts: a clean 409, never a 500.
	klog.V(2).InfoS("txn-budget-exhausted-409", "replica", l.identity, "ref", refLog(ref), "attempts", attempts)
	return nil, apierrors.NewConflict(l.gr, ref.Name,
		fmt.Errorf("locked write for %q failed after %d transaction attempts: %v", ref.Name, attempts, lastConflict))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func refLog(r metastore.ResourceRef) string {
	ns := r.Namespace
	if ns == "" {
		ns = "cluster"
	}
	return fmt.Sprintf("%s/%s/%s/%s", r.Group, r.Resource, ns, r.Name)
}
