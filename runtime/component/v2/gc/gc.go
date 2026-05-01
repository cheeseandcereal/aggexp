// Package gc is the v2 metadata-store garbage collector. It
// reconciles the two state stores the split-state axis carries
// (FINDINGS/0024, FINDINGS/0028): the metadata store (host
// ResourceMetadata CRD) and the backend's business-data store.
// Neither is authoritative for the other's existence, so orphans
// are possible on either side.
//
// This reconciler:
//
//   - Runs a periodic sweep (default 5m).
//   - Lists metastore Records filtered by a single (group, resource).
//   - Lists backend objects via the existing Backend.List RPC, diffs
//     by (namespace, name), and deletes Records whose backend
//     counterpart is absent — subject to policy.
//   - Policy: skip on finalizer, ownerReferences, deletionTimestamp
//     already set, or age under a grace window (default 30s).
//   - Exposes HTTP /gc/run (trigger) and /gc/last (result) on an
//     optional debug server for on-demand operator control.
//
// A list-based approach is deliberate (see FINDINGS/0028): it uses
// only the existing Backend.List RPC and works with every backend.
// At ≥10⁴-record scale a dedicated Exists(ids) RPC becomes
// necessary; that's a future proto concern.
//
// A push-capable backend (0025) can shrink the grace window to near
// zero because the backend learns about new Creates synchronously.
// v2 exposes the window as a knob; the caller sets it per backend.
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

	componentv2pb "github.com/cheeseandcereal/aggexp/runtime/component/v2/proto"
	"github.com/cheeseandcereal/aggexp/runtime/component/v2/metadatastore"
)

// Config is the Reconciler's parameters.
type Config struct {
	Group    string
	Resource string
	// Interval between periodic sweeps. Default 5m.
	Interval time.Duration
	// MinAge is the grace window. Records younger than this are
	// skipped regardless of orphan status. Default 30s.
	MinAge time.Duration
	// InitialDelay before the first sweep. Default 10s (lets the
	// apiserver finish PrepareRun before GC runs).
	InitialDelay time.Duration
}

// Reconciler is the GC reconciler.
type Reconciler struct {
	store   *metadatastore.Store
	backend componentv2pb.BackendClient

	cfg Config

	mu      sync.Mutex
	running bool
	lastRun *RunResult
}

// RunResult captures one sweep's outcome.
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

// New builds a Reconciler.
func New(store *metadatastore.Store, backend componentv2pb.BackendClient, cfg Config) *Reconciler {
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Minute
	}
	if cfg.MinAge <= 0 {
		cfg.MinAge = 30 * time.Second
	}
	if cfg.InitialDelay <= 0 {
		cfg.InitialDelay = 10 * time.Second
	}
	return &Reconciler{
		store:   store,
		backend: backend,
		cfg:     cfg,
	}
}

// Start launches the periodic sweep goroutine. Returns immediately.
func (r *Reconciler) Start(ctx context.Context) {
	klog.Infof("gc: starting group=%s resource=%s interval=%s minAge=%s",
		r.cfg.Group, r.cfg.Resource, r.cfg.Interval, r.cfg.MinAge)
	go func() {
		timer := time.NewTimer(r.cfg.InitialDelay)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				klog.Infof("gc: stopping (context cancelled)")
				return
			case <-timer.C:
			}
			_ = r.runOnce(ctx, "periodic")
			timer.Reset(r.cfg.Interval)
		}
	}()
}

// LastRun returns the last sweep result, or nil if none has run.
func (r *Reconciler) LastRun() *RunResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastRun == nil {
		return nil
	}
	cp := *r.lastRun
	return &cp
}

// RunOnce triggers a single sweep synchronously. Returns nil when
// another sweep is already in progress.
func (r *Reconciler) RunOnce(ctx context.Context) *RunResult {
	return r.runOnce(ctx, "manual")
}

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

	records, err := r.store.List(ctx, r.cfg.Group, r.cfg.Resource)
	if err != nil {
		res.Error = fmt.Sprintf("metastore.List: %v", err)
		return res
	}
	res.Seen = len(records)

	backendResp, err := r.backend.List(ctx, &componentv2pb.ListRequest{Namespace: ""})
	if err != nil {
		res.Error = fmt.Sprintf("backend.List: %v", err)
		return res
	}
	backendByKey := map[string]bool{}
	for _, raw := range backendResp.GetItemsJson() {
		nm, ens := nameNamespaceFromJSON(raw)
		backendByKey[ens+"/"+nm] = true
	}
	res.Backend = len(backendByKey)

	now := time.Now().UTC()
	orphans := make([]*metadatastore.Record, 0)
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
		name := metadatastore.RecordName(rec.Ref)
		if decision == decideDelete {
			if err := r.store.Delete(ctx, rec.Ref); err != nil {
				klog.Warningf("gc: delete %s failed: %v", name, err)
				res.Skipped = append(res.Skipped, SkippedRecord{Name: name, Reason: "delete-error: " + err.Error()})
				continue
			}
			klog.Infof("gc: DELETED orphan record=%s uid=%s", name, rec.UID)
			res.Deletions = append(res.Deletions, name)
			res.Deleted++
		} else {
			klog.Infof("gc: SKIP orphan record=%s reason=%s", name, reason)
			res.Skipped = append(res.Skipped, SkippedRecord{Name: name, Reason: reason})
		}
	}
	klog.Infof("gc: sweep end trigger=%s seen=%d backend=%d orphans=%d deleted=%d skipped=%d duration=%s",
		trigger, res.Seen, res.Backend, res.Orphans, res.Deleted, len(res.Skipped),
		time.Since(res.StartedAt).Round(time.Millisecond))
	return res
}

type gcDecision int

const (
	decideDelete gcDecision = iota
	decideSkip
)

func (r *Reconciler) decide(rec *metadatastore.Record, now time.Time) (gcDecision, string) {
	if !rec.CreationTimestamp.IsZero() {
		if now.Sub(rec.CreationTimestamp.Time) < r.cfg.MinAge {
			return decideSkip, fmt.Sprintf("age<%s", r.cfg.MinAge)
		}
	}
	if len(rec.Finalizers) > 0 {
		return decideSkip, fmt.Sprintf("finalizers=%v", rec.Finalizers)
	}
	if len(rec.OwnerReferences) > 0 {
		return decideSkip, "has-ownerReferences"
	}
	if rec.DeletionTimestamp != nil {
		return decideSkip, "deletionTimestamp-set"
	}
	return decideDelete, ""
}

// HandleRun is an HTTP handler that triggers a sync sweep and
// returns the RunResult as JSON.
func (r *Reconciler) HandleRun(w http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithTimeout(req.Context(), 60*time.Second)
	defer cancel()
	res := r.RunOnce(ctx)
	if res == nil {
		http.Error(w, "another sweep is already in progress", http.StatusConflict)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// HandleLast is an HTTP handler returning the last sweep's result.
func (r *Reconciler) HandleLast(w http.ResponseWriter, req *http.Request) {
	last := r.LastRun()
	if last == nil {
		http.Error(w, "no sweeps yet", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, last)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

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
