package multihost

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/klog/v2"
)

// WatchStitcher builds a served object for a (ref, body) under a caller
// identity, stamping the metadata CR's RV. The REST adapter supplies
// it.
type WatchStitcher interface {
	// StitchFor fetches the body via the BodyStore using the WATCHER's
	// identity (applies per-user authz), overlays the metadata CR's RV.
	// Returns (nil,false) if the caller may not see the object or it is
	// absent.
	StitchFor(u user.Info, ns, name string) (runtime.Object, bool)
	// NewBookmark returns an empty served object suitable as the
	// carrier for an initial-events-end BOOKMARK. It must be a
	// scheme-registered served type, NOT PartialObjectMetadata, or the
	// watch encoder rejects it (0044).
	NewBookmark() runtime.Object
}

// HubCounters tracks the per-event Get dedup cache effectiveness and
// the live watcher count.
type HubCounters struct {
	GetCacheHit  atomic.Int64
	GetCacheMiss atomic.Int64
	Watchers     atomic.Int64
	FanoutEvents atomic.Int64
}

// Snapshot returns a copy for logging.
func (c *HubCounters) Snapshot() map[string]int64 {
	return map[string]int64{
		"getCacheHit":  c.GetCacheHit.Load(),
		"getCacheMiss": c.GetCacheMiss.Load(),
		"watchers":     c.Watchers.Load(),
		"fanoutEvents": c.FanoutEvents.Load(),
	}
}

// Hub owns the shared metadata-CR informer side (via RawSink) and the
// registry of active per-watcher pipelines. One per replica. It is the
// 0044 inversion of the single-global-watch shape: each client Watch
// gets its own identity-carrying pipeline, while the shared metadata
// informer remains the single RV authority and cross-replica trigger.
type Hub struct {
	backend  *BodyStore
	stitcher WatchStitcher
	gate     IdentityGate

	sharedPoll   bool
	pollInterval time.Duration
	bufferSize   int

	Counters HubCounters

	mu       sync.RWMutex
	watchers map[int64]*PerWatcher
	nextID   int64
	curRV    string

	// lastSig is the 0043 emission filter RE-HOMED into the per-watcher
	// path (0048): keyed by metadata-CR record name → last emitted
	// VisibleSignature. A MODIFIED whose signature is unchanged (lock
	// acquire/release/renewal churn) is suppressed BEFORE fan-out, so
	// lock churn never reaches any watcher.
	lastSig map[string]string

	sharedSeen map[string]string
}

// HubOptions configures a Hub.
type HubOptions struct {
	Backend      *BodyStore
	Stitcher     WatchStitcher
	IdentityGate IdentityGate
	SharedPoll   bool
	PollInterval time.Duration
	BufferSize   int
}

// NewHub constructs a Hub.
func NewHub(o HubOptions) *Hub {
	if o.PollInterval <= 0 {
		o.PollInterval = defaultPollInterval
	}
	if o.BufferSize <= 0 {
		o.BufferSize = defaultWatchBuffer
	}
	gate := o.IdentityGate
	if gate == nil {
		gate = DefaultIdentityGate
	}
	return &Hub{
		backend:      o.Backend,
		stitcher:     o.Stitcher,
		gate:         gate,
		sharedPoll:   o.SharedPoll,
		pollInterval: o.PollInterval,
		bufferSize:   o.BufferSize,
		watchers:     map[int64]*PerWatcher{},
		lastSig:      map[string]string{},
		sharedSeen:   map[string]string{},
	}
}

// SharedPoll reports whether the hub runs in shared-poll mode.
func (h *Hub) SharedPoll() bool { return h.sharedPoll }

// CurrentResourceVersion returns the last observed metadata-CR RV.
func (h *Hub) CurrentResourceVersion() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.curRV
}

// SetCurrentResourceVersion records a new high-water RV.
func (h *Hub) SetCurrentResourceVersion(rv string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if rvLess(h.curRV, rv) {
		h.curRV = rv
	}
}

// OnMetadataEvent is called by the MetaStore informer (via RawSink) for
// every metadata-CR change. It is the single RV authority and
// cross-replica trigger. Before fanning out it applies the re-homed
// emission filter (suppressing lock-only / renewal-only transitions),
// then does ONE BodyStore.GetFor per distinct (identity, ns, name) via
// a per-event dedup cache.
func (h *Hub) OnMetadataEvent(et watch.EventType, ref ResourceRef, rec *Record, rv string) {
	h.SetCurrentResourceVersion(rv)

	name := RecordName(ref)
	switch et {
	case watch.Modified:
		sig := VisibleSignature(rec)
		h.mu.Lock()
		prev, seen := h.lastSig[name]
		if seen && prev == sig {
			h.mu.Unlock()
			klog.V(2).InfoS("emission-filter-suppress", "ref", ref.String(), "rv", rv, "reason", "lock-or-renewal-only")
			return
		}
		h.lastSig[name] = sig
		h.mu.Unlock()
	case watch.Added:
		h.mu.Lock()
		h.lastSig[name] = VisibleSignature(rec)
		h.mu.Unlock()
	case watch.Deleted:
		h.mu.Lock()
		delete(h.lastSig, name)
		h.mu.Unlock()
	}

	h.Counters.FanoutEvents.Add(1)

	h.mu.RLock()
	subs := make([]*PerWatcher, 0, len(h.watchers))
	for _, w := range h.watchers {
		subs = append(subs, w)
	}
	h.mu.RUnlock()
	if len(subs) == 0 {
		return
	}

	cache := newGetCache(h)
	for _, w := range subs {
		if w.namespace != "" && w.namespace != ref.Namespace {
			continue
		}
		obj, ok := cache.get(w.identity, ref.Namespace, ref.Name)
		if et == watch.Deleted {
			if !ok {
				continue
			}
			w.dedupEmit(watch.Deleted, obj)
			continue
		}
		if !ok {
			continue // per-user authz gate on the cross-replica path.
		}
		if !w.matches(obj) {
			continue
		}
		w.dedupEmit(et, obj)
	}
}

// getCache is the per-event (identity,ns,name) BodyStore.GetFor dedup.
type getCache struct {
	hub   *Hub
	cache map[string]cachedObj
}

type cachedObj struct {
	obj runtime.Object
	ok  bool
}

func newGetCache(h *Hub) *getCache {
	return &getCache{hub: h, cache: map[string]cachedObj{}}
}

func (c *getCache) get(id user.Info, ns, name string) (runtime.Object, bool) {
	key := identityKey(id) + "\x00" + ns + "\x00" + name
	if v, hit := c.cache[key]; hit {
		c.hub.Counters.GetCacheHit.Add(1)
		return v.obj, v.ok
	}
	c.hub.Counters.GetCacheMiss.Add(1)
	obj, ok := c.hub.stitcher.StitchFor(id, ns, name)
	c.cache[key] = cachedObj{obj: obj, ok: ok}
	return obj, ok
}

func identityKey(u user.Info) string {
	if u == nil {
		return "<nil>"
	}
	return u.GetName()
}

// ---- per-watcher pipeline ----

// PerWatcher is one client watch subscription. It implements
// watch.Interface so the apiserver streams it directly.
type PerWatcher struct {
	hub       *Hub
	id        int64
	identity  user.Info
	namespace string
	selector  labels.Selector

	result chan watch.Event
	stop   chan struct{}
	once   sync.Once

	// resultMu guards result-channel sends against the close in Stop.
	resultMu sync.Mutex
	closed   bool

	emu     sync.Mutex
	emitted map[string]string // ns/name -> last emitted RV

	subMu sync.Mutex
	sub   *BodySubscription
}

// ResultChan implements watch.Interface.
func (w *PerWatcher) ResultChan() <-chan watch.Event { return w.result }

// Stop implements watch.Interface.
func (w *PerWatcher) Stop() {
	w.once.Do(func() {
		close(w.stop)
		w.subMu.Lock()
		sub := w.sub
		w.subMu.Unlock()
		if sub != nil {
			sub.Close()
		}
		w.hub.mu.Lock()
		delete(w.hub.watchers, w.id)
		w.hub.mu.Unlock()
		w.hub.Counters.Watchers.Add(-1)
		w.resultMu.Lock()
		w.closed = true
		close(w.result)
		w.resultMu.Unlock()
	})
}

func (w *PerWatcher) emit(ev watch.Event) {
	w.resultMu.Lock()
	defer w.resultMu.Unlock()
	if w.closed {
		return
	}
	select {
	case <-w.stop:
	case w.result <- ev:
	default:
		klog.V(3).InfoS("perwatcher-drop", "id", w.id, "user", identityKey(w.identity))
	}
}

// dedupEmit emits an object event only if its RV is newer than the last
// RV emitted for that (ns/name) on this watcher. This collapses the two
// live sources (per-watcher backend channel/loop and the shared
// metadata informer cross-replica path). Deletes always emit.
func (w *PerWatcher) dedupEmit(et watch.EventType, obj runtime.Object) {
	acc, err := meta.Accessor(obj)
	if err != nil {
		w.emit(watch.Event{Type: et, Object: obj})
		return
	}
	key := acc.GetNamespace() + "/" + acc.GetName()
	rv := acc.GetResourceVersion()
	if et == watch.Deleted {
		w.emu.Lock()
		delete(w.emitted, key)
		w.emu.Unlock()
		w.emit(watch.Event{Type: et, Object: obj})
		return
	}
	w.emu.Lock()
	prev, seen := w.emitted[key]
	if seen && !rvLess(prev, rv) {
		w.emu.Unlock()
		return
	}
	w.emitted[key] = rv
	w.emu.Unlock()
	w.emit(watch.Event{Type: et, Object: obj})
}

func (w *PerWatcher) matches(obj runtime.Object) bool {
	if (w.selector == nil || w.selector.Empty()) && w.namespace == "" {
		return true
	}
	acc, err := meta.Accessor(obj)
	if err != nil {
		return true
	}
	if w.namespace != "" && acc.GetNamespace() != w.namespace {
		return false
	}
	if w.selector != nil && !w.selector.Empty() && !w.selector.Matches(labels.Set(acc.GetLabels())) {
		return false
	}
	return true
}

// NewWatch registers a new per-watcher subscription. initial is the
// owner-filtered, RV-stamped replay slice (ADDED events). The pipeline
// emits those, then an initial-events-end BOOKMARK, then live events.
func (h *Hub) NewWatch(ctx context.Context, u user.Info, namespace string, selector labels.Selector, mode WatchMode, initial []runtime.Object) *PerWatcher {
	h.mu.Lock()
	h.nextID++
	id := h.nextID
	bookmarkRV := h.curRV
	w := &PerWatcher{
		hub:       h,
		id:        id,
		identity:  u,
		namespace: namespace,
		selector:  selector,
		result:    make(chan watch.Event, h.bufferSize),
		stop:      make(chan struct{}),
		emitted:   map[string]string{},
	}
	h.watchers[id] = w
	h.mu.Unlock()
	h.Counters.Watchers.Add(1)

	go func() {
		for _, o := range initial {
			if !w.matches(o) {
				continue
			}
			if acc, err := meta.Accessor(o); err == nil {
				w.emu.Lock()
				w.emitted[acc.GetNamespace()+"/"+acc.GetName()] = acc.GetResourceVersion()
				w.emu.Unlock()
			}
			w.emit(watch.Event{Type: watch.Added, Object: o})
		}
		w.emit(bookmarkEvent(h.stitcher.NewBookmark(), bookmarkRV))

		if !h.sharedPoll {
			switch mode {
			case WatchPush:
				h.runPush(ctx, w)
			case WatchPoll:
				h.runPoll(ctx, w)
			}
		}
	}()

	return w
}

func (h *Hub) runPush(ctx context.Context, w *PerWatcher) {
	sub, ok := h.backend.WatchFor(w.identity, h.bufferSize)
	if !ok {
		// Budget exhausted: fall back to per-watcher poll.
		h.runPoll(ctx, w)
		return
	}
	w.subMu.Lock()
	w.sub = sub
	w.subMu.Unlock()
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stop:
			return
		case c, open := <-sub.C:
			if !open {
				return
			}
			h.emitBackendChange(w, c)
		}
	}
}

func (h *Hub) runPoll(ctx context.Context, w *PerWatcher) {
	snap := map[string]string{}
	tick := time.NewTicker(h.pollInterval)
	defer tick.Stop()
	for {
		refs := h.backend.ListFor(w.identity, w.namespace)
		seen := map[string]bool{}
		for _, r := range refs {
			key := r.Namespace + "/" + r.Name
			seen[key] = true
			hsh := HashBody(r.Body)
			prev, existed := snap[key]
			if !existed {
				snap[key] = hsh
				h.emitBackendChange(w, BodyChange{Type: BodyAdded, Namespace: r.Namespace, Name: r.Name, Body: r.Body})
			} else if prev != hsh {
				snap[key] = hsh
				h.emitBackendChange(w, BodyChange{Type: BodyModified, Namespace: r.Namespace, Name: r.Name, Body: r.Body})
			}
		}
		for key := range snap {
			if !seen[key] {
				delete(snap, key)
				ns, name := splitKey(key)
				h.emitBackendChange(w, BodyChange{Type: BodyDeleted, Namespace: ns, Name: name})
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-w.stop:
			return
		case <-tick.C:
		}
	}
}

func (h *Hub) emitBackendChange(w *PerWatcher, c BodyChange) {
	if w.namespace != "" && w.namespace != c.Namespace {
		return
	}
	obj, ok := h.stitcher.StitchFor(w.identity, c.Namespace, c.Name)
	if !ok {
		return
	}
	if !w.matches(obj) {
		return
	}
	et := watch.Added
	switch c.Type {
	case BodyModified:
		et = watch.Modified
	case BodyDeleted:
		et = watch.Deleted
	}
	w.dedupEmit(et, obj)
}

// RunSharedPoll runs the single system-identity poll loop that fans out
// to ALL watchers (SharedPoll mode). It does NOT enforce per-user authz
// on the live watch path — every watcher sees every object the selector
// admits. Recovers the single-global-watch cost (one List per interval
// regardless of watcher count). The consumer starts this goroutine when
// SharedPoll is enabled.
func (h *Hub) RunSharedPoll(ctx context.Context) {
	if !h.sharedPoll {
		return
	}
	tick := time.NewTicker(h.pollInterval)
	defer tick.Stop()
	system := &user.DefaultInfo{Name: "system:masters", Groups: []string{"system:masters"}}
	for {
		refs := h.backend.ListFor(system, "")
		seen := map[string]bool{}
		for _, r := range refs {
			key := r.Namespace + "/" + r.Name
			seen[key] = true
			hsh := HashBody(r.Body)
			prev, existed := h.sharedSeen[key]
			if !existed {
				h.sharedSeen[key] = hsh
				h.sharedFanout(BodyAdded, r.Namespace, r.Name)
			} else if prev != hsh {
				h.sharedSeen[key] = hsh
				h.sharedFanout(BodyModified, r.Namespace, r.Name)
			}
		}
		for key := range h.sharedSeen {
			if !seen[key] {
				delete(h.sharedSeen, key)
				ns, name := splitKey(key)
				h.sharedFanout(BodyDeleted, ns, name)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (h *Hub) sharedFanout(ct BodyChangeType, ns, name string) {
	system := &user.DefaultInfo{Name: "system:masters", Groups: []string{"system:masters"}}
	obj, ok := h.stitcher.StitchFor(system, ns, name)
	if !ok && ct != BodyDeleted {
		return
	}
	h.mu.RLock()
	subs := make([]*PerWatcher, 0, len(h.watchers))
	for _, w := range h.watchers {
		subs = append(subs, w)
	}
	h.mu.RUnlock()
	et := watch.Added
	switch ct {
	case BodyModified:
		et = watch.Modified
	case BodyDeleted:
		et = watch.Deleted
	}
	for _, w := range subs {
		if obj == nil {
			continue
		}
		if w.matches(obj) {
			w.emit(watch.Event{Type: et, Object: obj})
		}
	}
}

// ---- helpers ----

func bookmarkEvent(obj runtime.Object, rv string) watch.Event {
	if setter, ok := obj.(metav1.ObjectMetaAccessor); ok {
		om := setter.GetObjectMeta()
		om.SetResourceVersion(rv)
		om.SetAnnotations(map[string]string{"k8s.io/initial-events-end": "true"})
	}
	return watch.Event{Type: watch.Bookmark, Object: obj}
}

func splitKey(key string) (string, string) {
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			return key[:i], key[i+1:]
		}
	}
	return "", key
}
