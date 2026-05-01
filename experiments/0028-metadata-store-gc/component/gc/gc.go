// Package gc is the 0028 metadata-store garbage collector. It
// reconciles the two state stores described in
// FINDINGS/0024-metadata-crd-store.md: the host-cluster
// ResourceMetadata CRD store (authoritative for KRM metadata) and
// the backend's business-data store (authoritative for spec+status).
//
// The invariant 0024 implicitly relies on is that every
// ResourceMetadata record has a corresponding backend object; when
// that invariant breaks (backend wiped, row deleted out of band,
// backend namespace collapsed) the Record is orphaned — it shows
// up in `kubectl get rmeta` and in the middleware's metastore.List
// overlay, but `kubectl get buckets <name>` will 404 because the
// backend doesn't know about it. 0024 flagged this as a follow-up
// (candidate 0028).
//
// This package:
//
//   - Runs a periodic sweep (configurable interval, default 5m).
//   - On each sweep, lists ResourceMetadata records filtered by
//     the single (group, resource) the middleware serves, lists
//     backend IDs via the existing Backend.List RPC, and computes
//     the orphan set.
//   - Deletes orphaned records, honoring a short grace window
//     (see the `minAge` field) so a record that is seconds-old
//     — because Create persisted the metastore row before the
//     backend create landed — is never collected mid-flight.
//   - Respects finalizers: a Record with non-empty Finalizers is
//     skipped regardless of orphan status. The user's intent is
//     "hold this". Same for ownerReferences: if the Record claims
//     an ownerRef, we skip. The full finalizer-on-orphan case
//     (should the middleware also observe the finalizer for
//     orphans, running finalizer controllers the way genericregistry
//     does?) is out of scope; see FINDINGS.
//   - Logs each decision with enough context to trace.
//   - Optional on-demand trigger via `HandleRun` — a tiny HTTP
//     handler that blocks until one sweep completes. Used by the
//     demo scenarios.
//
// Design decision (recorded in README "Decisions made"):
// no new RPC. The GC calls the backend's existing `List` RPC and
// diffs locally. Pros: zero proto change, zero backend work;
// works with every backend 0013/0017/0021 ever shipped. Cons:
// List returns the full object body (potentially heavy) when an
// `Exists(ids)` RPC could return just bool flags; at the scale
// this experiment cares about the difference is invisible.
package gc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"k8s.io/klog/v2"

	componentpb "github.com/cheeseandcereal/aggexp/runtime/component/proto"

	"github.com/cheeseandcereal/aggexp/experiments/0028-metadata-store-gc/component/metastore"
)

// Reconciler is the GC reconciler.
type Reconciler struct {
	store  *metastore.Store
	client componentpb.BackendClient

	group    string
	resource string

	// interval between sweeps.
	interval time.Duration
	// minAge is a grace window: records younger than this (based on
	// creation timestamp on the host CR, not the Record's own KRM
	// creationTimestamp) are skipped even if the backend hasn't
	// observed them yet. Prevents racing a brand-new Create whose
	// metastore.Put landed but whose backend.Create is still
	// in-flight, or whose polling backend hasn't re-listed.
	minAge time.Duration

	// ongoing serializes manual + periodic runs.
	mu      sync.Mutex
	running bool

	// history of the last run, for the debug endpoint.
	lastRun *RunResult
}

// Config is the knobs for NewReconciler.
type Config struct {
	Group    string
	Resource string
	Interval time.Duration
	MinAge   time.Duration
}

// New constructs a Reconciler.
func New(store *metastore.Store, client componentpb.BackendClient, cfg Config) *Reconciler {
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Minute
	}
	if cfg.MinAge <= 0 {
		// 30s minimum age. A metastore row created at T, whose
		// backend create is delayed by up to `backend-timeout`
		// (20s default) plus a poll cycle, should be safely past
		// this window on the second sweep.
		cfg.MinAge = 30 * time.Second
	}
	return &Reconciler{
		store:    store,
		client:   client,
		group:    cfg.Group,
		resource: cfg.Resource,
		interval: cfg.Interval,
		minAge:   cfg.MinAge,
	}
}

// Start launches the periodic sweep goroutine. Returns immediately.
// Stops when ctx is cancelled.
func (r *Reconciler) Start(ctx context.Context) {
	klog.Infof("gc: starting reconciler group=%s resource=%s interval=%s minAge=%s",
		r.group, r.resource, r.interval, r.minAge)
	go func() {
		// First sweep after a short delay so the apiserver is fully
		// up and serving before the GC tries anything.
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				klog.Infof("gc: stopped (context cancelled)")
				return
			case <-timer.C:
			}
			_ = r.runOnce(ctx, "periodic")
			timer.Reset(r.interval)
		}
	}()
}

// RunResult captures one sweep outcome for debug display.
type RunResult struct {
	Trigger   string          `json:"trigger"`
	StartedAt time.Time       `json:"startedAt"`
	Duration  string          `json:"duration"`
	Seen      int             `json:"seenRecords"`
	Backend   int             `json:"backendObjects"`
	Orphans   int             `json:"orphansIdentified"`
	Deleted   int             `json:"deleted"`
	Skipped   []SkippedRecord `json:"skipped,omitempty"`
	Deletions []string        `json:"deletions,omitempty"`
	Error     string          `json:"error,omitempty"`
}

// SkippedRecord records why GC did not delete an orphan.
type SkippedRecord struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

// LastRun returns a copy of the most recent run's result, or nil
// if no run has happened yet.
func (r *Reconciler) LastRun() *RunResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastRun == nil {
		return nil
	}
	cp := *r.lastRun
	return &cp
}

// runOnce executes a single sweep and records the result.
func (r *Reconciler) runOnce(ctx context.Context, trigger string) *RunResult {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		klog.V(2).Infof("gc: sweep already in progress, skipping %s trigger", trigger)
		return nil
	}
	r.running = true
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.running = false
		r.mu.Unlock()
	}()

	res := &RunResult{Trigger: trigger, StartedAt: time.Now().UTC()}
	defer func() {
		res.Duration = time.Since(res.StartedAt).Round(time.Millisecond).String()
		r.mu.Lock()
		r.lastRun = res
		r.mu.Unlock()
	}()

	klog.Infof("gc: sweep start trigger=%s group=%s resource=%s", trigger, r.group, r.resource)

	// 1. List metastore records for our (group, resource).
	records, err := r.store.List(ctx, r.group, r.resource)
	if err != nil {
		klog.Warningf("gc: metastore.List failed: %v", err)
		res.Error = fmt.Sprintf("metastore.List: %v", err)
		return res
	}
	res.Seen = len(records)

	// 2. List backend objects. The `namespace` field is empty: the
	// backend returns everything it knows about. (Our backend is
	// cluster-scoped; namespaced resources would need a per-namespace
	// list.)
	backendResp, err := r.client.List(ctx, &componentpb.ListRequest{
		User:      nil,
		Namespace: "",
	})
	if err != nil {
		klog.Warningf("gc: backend.List failed: %v", err)
		res.Error = fmt.Sprintf("backend.List: %v", err)
		return res
	}
	backendByKey := map[string]bool{}
	for _, raw := range backendResp.GetItemsJson() {
		nm, ens := nameNamespaceFromJSON(raw)
		backendByKey[ens+"/"+nm] = true
	}
	res.Backend = len(backendByKey)

	// 3. Compute orphans & delete.
	now := time.Now().UTC()
	orphans := make([]*metastore.Record, 0)
	for _, rec := range records {
		key := rec.Ref.Namespace + "/" + rec.Ref.Name
		if backendByKey[key] {
			continue
		}
		orphans = append(orphans, rec)
	}
	sort.Slice(orphans, func(i, j int) bool {
		return orphans[i].Ref.Name < orphans[j].Ref.Name
	})
	res.Orphans = len(orphans)

	for _, rec := range orphans {
		decision, reason := r.decide(rec, now)
		name := metastore.RecordName(rec.Ref)
		fqn := fmt.Sprintf("%s/%s", rec.Ref.Namespace, rec.Ref.Name)
		if rec.Ref.Namespace == "" {
			fqn = rec.Ref.Name
		}
		switch decision {
		case decideDelete:
			if err := r.store.Delete(ctx, rec.Ref); err != nil {
				klog.Warningf("gc: delete orphan record=%s ref=%s failed: %v", name, fqn, err)
				res.Skipped = append(res.Skipped, SkippedRecord{Name: name, Reason: "delete-error: " + err.Error()})
				continue
			}
			klog.Infof("gc: DELETED orphan record=%s ref=%s uid=%s (backend absent)", name, fqn, rec.UID)
			res.Deletions = append(res.Deletions, name)
			res.Deleted++
		case decideSkip:
			klog.Infof("gc: SKIP orphan record=%s ref=%s uid=%s reason=%s", name, fqn, rec.UID, reason)
			res.Skipped = append(res.Skipped, SkippedRecord{Name: name, Reason: reason})
		}
	}

	klog.Infof("gc: sweep end trigger=%s seen=%d backend=%d orphans=%d deleted=%d skipped=%d duration=%s",
		trigger, res.Seen, res.Backend, res.Orphans, res.Deleted, len(res.Skipped),
		time.Since(res.StartedAt).Round(time.Millisecond))
	return res
}

// gcDecision is a tiny enum so decide() can return a code + reason
// without allocating a struct per call.
type gcDecision int

const (
	decideDelete gcDecision = iota
	decideSkip
)

// decide is the policy: given an orphan Record, should GC delete or
// skip? The current policy is conservative — skip on any finalizer,
// ownerReference, or deletion-timestamp-already-set (the user is
// trying to delete but a finalizer is blocking), and delete otherwise.
//
// Rationale:
//   - Finalizers express "don't let this disappear yet." Whatever
//     controller registered the finalizer is expected to handle
//     external state on its own schedule. If that controller is
//     gone, the Record leaks; an operator must clear the finalizer
//     manually. This matches Kubernetes' own CRD GC semantics.
//   - OwnerReferences point at *something extant*. Deleting the
//     orphan record would likely break a controller relying on
//     lifecycle hooks. We skip; full cascade-aware GC is its own
//     (large) problem.
//   - A Record with deletionTimestamp already set is in the middle
//     of a delete; don't race the DELETE path. It will be cleared
//     by the finalizer-clear handler in stitchedrest.Update.
//   - The minAge grace window is applied universally.
func (r *Reconciler) decide(rec *metastore.Record, now time.Time) (gcDecision, string) {
	// Grace window — look at the CR's own creationTimestamp (from
	// the host apiserver) rather than the embedded
	// rec.CreationTimestamp (which is the KRM metadata's creation
	// time; the two can diverge when the backend object existed
	// pre-Record). The host's CR time is a ceiling.
	//
	// We approximate by using rec.CreationTimestamp as a proxy; if
	// it's within minAge, skip. (The Record API doesn't expose the
	// host CR's metadata.creationTimestamp directly, and decoding
	// it through the Store would cost a second Get. Acceptable
	// consequent.)
	if !rec.CreationTimestamp.IsZero() {
		if now.Sub(rec.CreationTimestamp.Time) < r.minAge {
			return decideSkip, fmt.Sprintf("age<%s", r.minAge)
		}
	}
	if len(rec.Finalizers) > 0 {
		return decideSkip, fmt.Sprintf("finalizers=%v", rec.Finalizers)
	}
	if len(rec.OwnerReferences) > 0 {
		return decideSkip, "has-ownerReferences"
	}
	if rec.DeletionTimestamp != nil {
		// Delete in progress (no finalizers blocking now, but RV
		// dance still in flight). Let the normal delete path
		// finish rather than racing.
		return decideSkip, "deletionTimestamp-set"
	}
	return decideDelete, ""
}

// HandleRun is an HTTP handler (GET or POST) that triggers one sweep
// synchronously and returns the result as JSON. Not authenticated;
// the debug port is bound to 127.0.0.1:<n> in main.
func (r *Reconciler) HandleRun(w http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithTimeout(req.Context(), 60*time.Second)
	defer cancel()
	res := r.runOnce(ctx, "manual")
	if res == nil {
		http.Error(w, "another sweep is already in progress", http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(res)
}

// HandleLast is an HTTP handler (GET) returning the last run's
// result or 404 if no sweep has run yet.
func (r *Reconciler) HandleLast(w http.ResponseWriter, req *http.Request) {
	last := r.LastRun()
	if last == nil {
		http.Error(w, "no sweeps yet", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(last)
}

// nameNamespaceFromJSON is duplicated from stitchedrest to avoid an
// import cycle. Experiments tolerate duplication (per AGENTS.md) —
// fewer than twenty lines across the two copies.
func nameNamespaceFromJSON(raw []byte) (string, string) {
	var m struct {
		Metadata struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return "", ""
	}
	return m.Metadata.Name, m.Metadata.Namespace
}
