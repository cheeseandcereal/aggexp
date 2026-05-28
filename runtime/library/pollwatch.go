package library

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"

	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// PollPublisher is the subset of Publisher that the poll loop needs.
// Satisfied by *runtimestorage.REST and by *REST in this package.
type PollPublisher interface {
	PublishAdded(obj runtime.Object)
	PublishModified(obj runtime.Object)
	PublishDeleted(obj runtime.Object)
}

// PollLister is the minimal interface a poll-mode backend must implement.
// It returns a flat slice of objects from whatever source the backend reads.
type PollLister interface {
	List(ctx context.Context) ([]runtime.Object, error)
}

// PollListerFunc adapts a function to the PollLister interface.
type PollListerFunc func(ctx context.Context) ([]runtime.Object, error)

// List calls the underlying function.
func (f PollListerFunc) List(ctx context.Context) ([]runtime.Object, error) { return f(ctx) }

// BackendPollLister adapts a runtime/storage.Backend to PollLister by
// calling List with nil user and empty options.
func BackendPollLister(b runtimestorage.Backend) PollLister {
	return PollListerFunc(func(ctx context.Context) ([]runtime.Object, error) {
		list, err := b.List(ctx, nil, runtimestorage.ListOptions{})
		if err != nil {
			return nil, err
		}
		items, err := meta.ExtractList(list)
		if err != nil {
			return nil, err
		}
		return items, nil
	})
}

// PollWatcher periodically calls PollLister.List, diffs against a cached
// snapshot, and emits Added/Modified/Deleted events via PollPublisher.
// This gives full watch semantics to read-only backends that only
// implement List.
type PollWatcher struct {
	lister   PollLister
	pub      PollPublisher
	interval time.Duration

	mu       sync.Mutex
	snapshot map[string]pollEntry // keyed by object name
}

type pollEntry struct {
	obj  runtime.Object
	hash string
}

// NewPollWatcher creates a PollWatcher. Call Run to start polling.
func NewPollWatcher(lister PollLister, pub PollPublisher, interval time.Duration) *PollWatcher {
	return &PollWatcher{
		lister:   lister,
		pub:      pub,
		interval: interval,
		snapshot: make(map[string]pollEntry),
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
		name := pollObjectName(obj)
		if name == "" {
			continue
		}
		seen[name] = true
		h := pollHashObject(obj)
		prev, existed := pw.snapshot[name]
		if !existed {
			pw.snapshot[name] = pollEntry{obj: obj, hash: h}
			pw.pub.PublishAdded(obj)
		} else if prev.hash != h {
			pw.snapshot[name] = pollEntry{obj: obj, hash: h}
			pw.pub.PublishModified(obj)
		}
	}

	// Detect deletions.
	for name, e := range pw.snapshot {
		if !seen[name] {
			delete(pw.snapshot, name)
			pw.pub.PublishDeleted(e.obj)
		}
	}
}

func pollObjectName(obj runtime.Object) string {
	acc, err := meta.Accessor(obj)
	if err != nil {
		return ""
	}
	return acc.GetName()
}

func pollHashObject(obj runtime.Object) string {
	b, _ := json.Marshal(obj)
	return string(b)
}
