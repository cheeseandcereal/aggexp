package multihost

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
)

const (
	lockRetryAttempts = 3
	lockRetryBackoff  = 25 * time.Millisecond
)

// Locker acquires the embedded per-object lock on a served object's
// metadata CR (spec.lock), CAS'd on the CR's own resourceVersion
// (0043). It also drives the 0049 transactional write path (WriteTxn).
//
// The lock is an ADMISSION GATE that reduces contention; the WriteTxn
// retry loop is what makes concurrent writes CORRECT (0049). The held
// lock alone is insufficient: it is re-entrant per replica, and a lease
// can be taken over mid-write, so the body-CR Put and the metadata
// commit are two independent CAS surfaces that must be retried as a
// pair on either one's conflict.
type Locker struct {
	store         *MetaStore
	gr            schema.GroupResource
	identity      string
	leaseDuration time.Duration
	renewEnabled  bool

	txnEnabled  bool
	txnAttempts int
}

// LockerOptions configures a Locker.
type LockerOptions struct {
	Store         *MetaStore
	GroupResource schema.GroupResource
	Identity      string
	LeaseDuration time.Duration
	RenewEnabled  bool

	// TransactionalWrite enables the 0049 locked-write transaction (the
	// validated, correct write path). When false the locker reproduces
	// the 0048 single-shot post-acquire behavior (a CAS race surfaces
	// as a 500); that mode exists only for the regression baseline.
	TransactionalWrite bool
	// TransactionAttempts is the retry budget for the post-acquire
	// body+commit critical section (default 5).
	TransactionAttempts int
}

// NewLocker constructs a Locker.
func NewLocker(opts LockerOptions) *Locker {
	if opts.Store == nil {
		panic("multihost.NewLocker: Store is required")
	}
	dur := opts.LeaseDuration
	if dur == 0 {
		dur = defaultLeaseDuration
	}
	attempts := opts.TransactionAttempts
	if attempts <= 0 {
		attempts = defaultTxnAttempts
	}
	return &Locker{
		store:         opts.Store,
		gr:            opts.GroupResource,
		identity:      opts.Identity,
		leaseDuration: dur,
		renewEnabled:  opts.RenewEnabled,
		txnEnabled:    opts.TransactionalWrite,
		txnAttempts:   attempts,
	}
}

// TransactionalWrite reports whether the transaction discipline is on.
func (l *Locker) TransactionalWrite() bool { return l.txnEnabled }

// Handle represents a held embedded lock. It carries the raw CR (with
// the RV minted by the acquire write) so commit/release CAS against the
// right version, plus the pre-acquire RV the OCC check must use.
type Handle struct {
	locker *Locker
	ref    ResourceRef

	mu  sync.Mutex
	raw *unstructured.Unstructured

	// PreAcquireRV is the served object's resourceVersion BEFORE this
	// acquire bumped it (empty if the CR did not exist — Create path).
	// The OCC check compares the client's RV against this, never the
	// post-acquire RV (0043).
	PreAcquireRV string

	stopRenew chan struct{}
	renewDone chan struct{}
	renewing  bool
}

// Acquire takes the embedded lock for ref. On a fresh lock held by
// another replica it returns a 409 Conflict (apierrors.IsConflict). A
// CAS-level conflict is retried within a small budget; exhausting it
// returns 409.
func (l *Locker) Acquire(ctx context.Context, ref ResourceRef) (*Handle, error) {
	dur := int32(l.leaseDuration.Seconds())

	var lastErr error
	for attempt := 0; attempt < lockRetryAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(lockRetryBackoff * time.Duration(1<<uint(attempt-1)))
		}

		raw, err := l.store.GetRawDirect(ctx, ref)
		if err != nil {
			lastErr = fmt.Errorf("lock get: %w", err)
			continue
		}

		now := metav1.NewTime(time.Now().UTC())
		desired := &LockState{
			HolderIdentity:       l.identity,
			AcquiredAt:           &now,
			RenewedAt:            &now,
			LeaseDurationSeconds: dur,
		}

		if raw == nil {
			created, cerr := l.store.CreateRawWithLock(ctx, ref, desired)
			if cerr != nil {
				if apierrors.IsAlreadyExists(cerr) {
					lastErr = cerr
					continue
				}
				return nil, apierrors.NewInternalError(fmt.Errorf("lock create: %w", cerr))
			}
			return l.newHandle(ref, created, ""), nil
		}

		preRV := raw.GetResourceVersion()
		existing := lockFromRaw(raw)

		// Re-entrant acquire on the same replica: refresh and proceed.
		if existing != nil && existing.HolderIdentity == l.identity {
			setLockOn(raw, desired)
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
		if existing.Held() && !lockExpired(existing) {
			klog.V(3).InfoS("lock-held-fresh-409", "replica", l.identity, "ref", ref.String(), "holder", existing.HolderIdentity)
			return nil, apierrors.NewConflict(l.gr, ref.Name,
				fmt.Errorf("object %q is locked by %q", ref.Name, existing.HolderIdentity))
		}

		// Free or expired: take it via CAS on preRV.
		if existing.Held() {
			klog.V(2).InfoS("lock-takeover", "replica", l.identity, "ref", ref.String(), "from", existing.HolderIdentity)
		}
		setLockOn(raw, desired)
		updated, uerr := l.store.UpdateRaw(ctx, raw)
		if uerr != nil {
			if apierrors.IsConflict(uerr) {
				lastErr = uerr
				continue
			}
			return nil, apierrors.NewInternalError(fmt.Errorf("lock acquire update: %w", uerr))
		}
		return l.newHandle(ref, updated, preRV), nil
	}
	if lastErr != nil && apierrors.IsConflict(lastErr) {
		return nil, apierrors.NewConflict(l.gr, ref.Name,
			fmt.Errorf("lock acquisition for %q failed after %d attempts: %v", ref.Name, lockRetryAttempts, lastErr))
	}
	return nil, apierrors.NewInternalError(fmt.Errorf("lock acquisition for %q failed: %v", ref.Name, lastErr))
}

func (l *Locker) newHandle(ref ResourceRef, raw *unstructured.Unstructured, preRV string) *Handle {
	h := &Handle{locker: l, ref: ref, raw: raw, PreAcquireRV: preRV}
	if l.renewEnabled {
		h.startRenew()
	}
	return h
}

func lockExpired(ls *LockState) bool {
	if ls == nil || ls.RenewedAt == nil || ls.LeaseDurationSeconds <= 0 {
		return true
	}
	expiry := ls.RenewedAt.Time.Add(time.Duration(ls.LeaseDurationSeconds) * time.Second)
	return time.Now().After(expiry)
}

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
					klog.Warningf("lock renew failed ref=%s: %v", h.ref.String(), err)
				}
			}
		}
	}()
}

func (h *Handle) renew() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.raw == nil {
		return nil
	}
	now := metav1.NewTime(time.Now().UTC())
	ls := &LockState{
		HolderIdentity:       h.locker.identity,
		AcquiredAt:           lockAcquiredAt(h.raw),
		RenewedAt:            &now,
		LeaseDurationSeconds: int32(h.locker.leaseDuration.Seconds()),
	}
	setLockOn(h.raw, ls)
	updated, err := h.locker.store.UpdateRaw(context.Background(), h.raw)
	if err != nil {
		if apierrors.IsConflict(err) {
			if fresh, gerr := h.locker.store.GetRawDirect(context.Background(), h.ref); gerr == nil && fresh != nil {
				h.raw = fresh
			}
			return nil
		}
		return err
	}
	h.raw = updated
	return nil
}

func lockAcquiredAt(u *unstructured.Unstructured) *metav1.Time {
	if ls := lockFromRaw(u); ls != nil && ls.AcquiredAt != nil {
		return ls.AcquiredAt
	}
	now := metav1.NewTime(time.Now().UTC())
	return &now
}

func (h *Handle) stopRenewal() {
	if h.renewing {
		close(h.stopRenew)
		<-h.renewDone
		h.renewing = false
	}
}

// Commit writes the body hash + KRM metadata onto the held CR and
// clears the lock in a SINGLE CR write, advancing the RV exactly once.
// Stops renewal first. Returns the committed Record.
func (h *Handle) Commit(ctx context.Context, rec *Record) (*Record, error) {
	h.stopRenewal()
	h.mu.Lock()
	defer h.mu.Unlock()
	committed, err := h.locker.store.CommitBodyHashAndMeta(ctx, h.raw, rec)
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
	setLockOn(h.raw, nil)
	if _, err := h.locker.store.UpdateRaw(ctx, h.raw); err != nil {
		if !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) {
			klog.Warningf("lock release failed ref=%s: %v", h.ref.String(), err)
		}
	}
	h.raw = nil
}

// ---- 0049 locked-write transaction ----

// WriteOp describes one logical locked write.
type WriteOp struct {
	// WriteBody writes the business body. A CAS conflict MUST be
	// returned as an apierrors.IsConflict error to be retriable.
	WriteBody func(ctx context.Context) error
	// BuildRecord builds the metadata Record to commit. Called once
	// per attempt (after WriteBody) so it can reflect the latest body.
	BuildRecord func(ctx context.Context, attempt int) (*Record, error)
	// OnConflictRetry, if set, is called before each retry (attempt >
	// 0) for instrumentation.
	OnConflictRetry func(attempt int)
}

// TxnResult carries the committed record plus the retry depth observed
// (0 = succeeded on the first attempt).
type TxnResult struct {
	Record *Record
	Depth  int
}

// WriteTxn runs op as a locked-write transaction for ref (0049). The
// post-acquire body Put and metadata commit run inside a retry loop
// covering BOTH CAS surfaces: on a recoverable conflict at acquire,
// body Put, OR commit it releases, re-reads, and retries the whole
// critical section within the budget. Exhausting the budget surfaces a
// clean 409 Conflict, NEVER a 500. On the fast (uncontended) path the
// loop runs exactly once and adds zero writes.
//
// preRVCheck, if non-nil, is the 0043 pre-acquire OCC check: invoked
// once per attempt with the served object's current authoritative RV
// (before this attempt's acquire bumps it). A genuine OCC failure (a
// peer committed a different RV) is a legitimate 409 and is NOT
// retried.
//
// When the locker's transaction discipline is disabled it degrades to
// the 0048 single-shot behavior (the regression baseline): one acquire,
// one body, one commit; a commit CAS conflict is returned as-is.
func (l *Locker) WriteTxn(ctx context.Context, ref ResourceRef, op WriteOp, preRVCheck func(curRV string) error) (*TxnResult, error) {
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
			time.Sleep(lockRetryBackoff * time.Duration(1<<uint(minInt(attempt-1, 4))))
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
			if l.txnEnabled && apierrors.IsConflict(lerr) {
				lastConflict = lerr
				continue
			}
			return nil, lerr
		}

		if berr := op.WriteBody(ctx); berr != nil {
			h.Release(ctx)
			if l.txnEnabled && apierrors.IsConflict(berr) {
				lastConflict = berr
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
				lastConflict = perr
				continue
			}
			if apierrors.IsConflict(perr) {
				return nil, perr // discipline off: caller surfaces as 500-equivalent.
			}
			return nil, apierrors.NewInternalError(fmt.Errorf("metastore.Commit: %w", perr))
		}
		return &TxnResult{Record: committed, Depth: attempt}, nil
	}

	klog.V(2).InfoS("txn-budget-exhausted-409", "replica", l.identity, "ref", ref.String(), "attempts", attempts)
	return nil, apierrors.NewConflict(l.gr, ref.Name,
		fmt.Errorf("locked write for %q failed after %d transaction attempts: %v", ref.Name, attempts, lastConflict))
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
