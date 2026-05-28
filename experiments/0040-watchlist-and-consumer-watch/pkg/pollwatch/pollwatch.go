// Package pollwatch provides a wrapper that turns a read-only Backend.List
// into a full watch-capable REST by periodically polling and diffing snapshots.
//
// The consumer implements only List (read-only); the library does the rest.
// Events (ADDED/MODIFIED/DELETED) are emitted via the REST's PublishAdded/
// PublishModified/PublishDeleted methods as diffs are detected.
package pollwatch

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
)

// Publisher is the subset of *runtimestorage.REST that the poll loop needs.
type Publisher interface {
	PublishAdded(obj runtime.Object)
	PublishModified(obj runtime.Object)
	PublishDeleted(obj runtime.Object)
}

// Lister is the minimal interface a poll-mode backend must implement.
// In practice it's satisfied by runtimestorage.Backend.List via a thin adapter.
type Lister interface {
	List(ctx context.Context) ([]runtime.Object, error)
}

// ListerFunc adapts a function to the Lister interface.
type ListerFunc func(ctx context.Context) ([]runtime.Object, error)

func (f ListerFunc) List(ctx context.Context) ([]runtime.Object, error) { return f(ctx) }

// PollWatcher periodically calls Lister.List, diffs against a cached
// snapshot, and emits Added/Modified/Deleted events via Publisher.
type PollWatcher struct {
	lister   Lister
	pub      Publisher
	interval time.Duration

	mu       sync.Mutex
	snapshot map[string]entry // keyed by object name
}

type entry struct {
	obj  runtime.Object
	hash string // JSON-serialized spec for diff comparison
}

// New creates a PollWatcher. Call Run to start polling.
func New(lister Lister, pub Publisher, interval time.Duration) *PollWatcher {
	return &PollWatcher{
		lister:   lister,
		pub:      pub,
		interval: interval,
		snapshot: make(map[string]entry),
	}
}

// Run starts the poll loop. It blocks until ctx is cancelled.
// The first poll fires immediately (at t=0).
func (pw *PollWatcher) Run(ctx context.Context) {
	pw.poll(ctx) // immediate first poll
	ticker := time.NewTicker(pw.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pw.poll(ctx)
		}
	}
}

func (pw *PollWatcher) poll(ctx context.Context) {
	items, err := pw.lister.List(ctx)
	if err != nil {
		klog.Errorf("pollwatch: list failed: %v", err)
		return
	}

	pw.mu.Lock()
	defer pw.mu.Unlock()

	seen := make(map[string]bool, len(items))
	for _, obj := range items {
		name := objectName(obj)
		if name == "" {
			continue
		}
		seen[name] = true
		h := hashObject(obj)
		prev, existed := pw.snapshot[name]
		if !existed {
			pw.snapshot[name] = entry{obj: obj, hash: h}
			pw.pub.PublishAdded(obj)
		} else if prev.hash != h {
			pw.snapshot[name] = entry{obj: obj, hash: h}
			pw.pub.PublishModified(obj)
		}
		// else unchanged — no event
	}

	// Detect deletions
	for name, e := range pw.snapshot {
		if !seen[name] {
			delete(pw.snapshot, name)
			pw.pub.PublishDeleted(e.obj)
		}
	}
}

func objectName(obj runtime.Object) string {
	acc, err := meta.Accessor(obj)
	if err != nil {
		return ""
	}
	return acc.GetName()
}

func hashObject(obj runtime.Object) string {
	b, _ := json.Marshal(obj)
	return string(b)
}
