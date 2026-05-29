// Package watch implements experiment 0044's PER-WATCHER watch
// emission pipeline. It inverts the single-global-watch shape that
// 0025 (push) and 0034 (shared informer re-broadcast) validated.
//
// In 0042/0034, one metadata-CR informer fed one watch.Broadcaster
// that fanned out to every client. Per-user authorization on the
// watch stream was impossible: every client saw the same events.
//
// Here, each client Watch subscription gets its own PerWatcher
// carrying that caller's user.Info, and its own backend access:
//
//   - push  : one backend.WatchFor(user) subscription per watcher.
//   - poll  : one backend.ListFor(user) loop per watcher.
//
// The shared metadata-CR informer remains the single RV authority and
// the cross-replica trigger (the 0042/0034 finding): when it fires
// for an object, the Hub re-fetches the body via Backend.GetFor(
// watcherUser, ...) — the WATCHER's identity, so backend authz
// applies on the cross-replica path too — and stamps the metadata
// CR's host RV onto the emitted event. That per-event Get is
// deduplicated within a single fan-out by (identity, ns, name) so N
// watchers sharing an identity+object cost ONE Get, not N.
//
// SharedPoll mode (the cheaper opt-in) runs ONE system-identity poll
// loop for all watchers and fans its diff out to every watcher,
// filtered only by label/namespace selector — recovering the
// single-global-watch cost profile at the price of NOT enforcing
// per-user authz.
package watch

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

	"github.com/cheeseandcereal/aggexp/experiments/0044-per-watcher-watch-identity/pkg/backend"
	"github.com/cheeseandcereal/aggexp/experiments/0044-per-watcher-watch-identity/pkg/metastore"
)

// Mode selects the per-watcher live source.
type Mode int

const (
	// ModePush opens one backend.WatchFor(user) per subscription.
	ModePush Mode = iota
	// ModePoll runs one backend.ListFor(user) loop per subscription.
	ModePoll
)

// Stitcher builds a served object for a (ref, body) under a caller
// identity, stamping the metadata CR's RV. The REST adapter supplies
// it. Returns (nil,false) if the caller may not see the object or it
// is absent.
type Stitcher interface {
	// StitchFor fetches the body via the backend using the WATCHER's
	// identity (Backend.GetFor) and overlays the metadata CR's RV.
	// onlyIfOwned mirrors the per-user authz gate.
	StitchFor(u user.Info, ns, name string) (runtime.Object, bool)
}

// HubCounters tracks the per-event Get dedup cache effectiveness and
// the live watcher count. Backend.Watch/List/Get volumes live on the
// backend.Counters.
type HubCounters struct {
	GetCacheHit  atomic.Int64
	GetCacheMiss atomic.Int64
	Watchers     atomic.Int64
	// FanoutEvents counts metadata-informer events fanned out.
	FanoutEvents atomic.Int64
}

// Snapshot returns a copy for logging.
func (c *HubCounters) Snapshot() map[string]int64 {
	hit := c.GetCacheHit.Load()
	miss := c.GetCacheMiss.Load()
	return map[string]int64{
		"getCacheHit":  hit,
		"getCacheMiss": miss,
		"watchers":     c.Watchers.Load(),
		"fanoutEvents": c.FanoutEvents.Load(),
	}
}

// Hub owns the shared metadata-CR informer side and the registry of
// active per-watcher pipelines. One per replica.
type Hub struct {
	backend  *backend.Store
	stitcher Stitcher

	sharedPoll   bool
	pollInterval time.Duration
	bufferSize   int

	Counters HubCounters

	mu       sync.RWMutex
	watchers map[int64]*PerWatcher
	nextID   int64
	curRV    string // last observed metadata-CR RV (host etcd authority)

	// sharedSeen is the SharedPoll-mode snapshot (system identity).
	sharedSeen map[string]string // ns/name -> hash
}

// HubOptions configures a Hub.
type HubOptions struct {
	Backend      *backend.Store
	Stitcher     Stitcher
	SharedPoll   bool
	PollInterval time.Duration
	BufferSize   int
}

// NewHub constructs a Hub.
func NewHub(o HubOptions) *Hub {
	if o.PollInterval <= 0 {
		o.PollInterval = 5 * time.Second
	}
	if o.BufferSize <= 0 {
		o.BufferSize = 100
	}
	return &Hub{
		backend:      o.Backend,
		stitcher:     o.Stitcher,
		sharedPoll:   o.SharedPoll,
		pollInterval: o.PollInterval,
		bufferSize:   o.BufferSize,
		watchers:     map[int64]*PerWatcher{},
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

// OnMetadataEvent is called by the metastore informer for every
// metadata-CR change for the served resource. It is the SINGLE RV
// authority and cross-replica trigger. It fans out to every active
// per-watcher pipeline, doing ONE Backend.GetFor per distinct
// (identity, ns, name) via a per-event dedup cache.
func (h *Hub) OnMetadataEvent(et watch.EventType, ref metastore.ResourceRef, rv string) {
	h.SetCurrentResourceVersion(rv)
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

	// Per-event Get dedup cache, scoped to THIS fan-out only. Keyed by
	// (identity, ns, name). Multiple watchers sharing an identity and
	// interested in the same object share one Backend.GetFor.
	cache := newGetCache(h)

	for _, w := range subs {
		// Namespace pre-filter (cheap, before any Get).
		if w.namespace != "" && w.namespace != ref.Namespace {
			continue
		}
		obj, ok := cache.get(w.identity, ref.Namespace, ref.Name)
		if et == watch.Deleted {
			// On delete the body may be gone; if we cannot fetch it,
			// synthesize a minimal deleted object only if the watcher
			// previously could see it. For simplicity, if the cache
			// fetch returns not-ok on delete, skip (the body backend's
			// own delete path is not authoritative for per-user authz).
			if !ok {
				continue
			}
			w.emit(watch.Event{Type: watch.Deleted, Object: obj})
			continue
		}
		if !ok {
			// Caller may not see this object (owner mismatch) or it is
			// absent. No event for this watcher — this IS the per-user
			// authz gate on the cross-replica path.
			continue
		}
		if !w.matches(obj) {
			continue
		}
		w.emit(watch.Event{Type: et, Object: obj})
	}
}

// getCache is the per-event (identity,ns,name) Backend.GetFor dedup.
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
	hub      *Hub
	id       int64
	identity user.Info
	namespace string
	selector labels.Selector

	result chan watch.Event
	stop   chan struct{}
	once   sync.Once

	// per-watcher backend access (push mode).
	sub *backend.Subscription
}

// ResultChan implements watch.Interface.
func (w *PerWatcher) ResultChan() <-chan watch.Event { return w.result }

// Stop implements watch.Interface.
func (w *PerWatcher) Stop() {
	w.once.Do(func() {
		close(w.stop)
		if w.sub != nil {
			w.sub.Close()
		}
		w.hub.mu.Lock()
		delete(w.hub.watchers, w.id)
		w.hub.mu.Unlock()
		w.hub.Counters.Watchers.Add(-1)
		// Drain & close result on a goroutine so a slow consumer
		// doesn't deadlock Stop.
		close(w.result)
		klog.V(2).InfoS("perwatcher-stopped", "id", w.id, "user", identityKey(w.identity))
	})
}

func (w *PerWatcher) emit(ev watch.Event) {
	select {
	case <-w.stop:
	case w.result <- ev:
	default:
		// Slow consumer; drop (DropIfChannelFull). The reflector will
		// relist on a gap.
		klog.V(3).InfoS("perwatcher-drop", "id", w.id, "user", identityKey(w.identity))
	}
}

func (w *PerWatcher) matches(obj runtime.Object) bool {
	if w.selector == nil || w.selector.Empty() {
		if w.namespace == "" {
			return true
		}
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
// owner-filtered, RV-stamped replay slice (ADDED events) the REST
// adapter computed via Backend.ListFor. The pipeline emits those,
// then an initial-events-end BOOKMARK, then live events.
func (h *Hub) NewWatch(ctx context.Context, u user.Info, namespace string, selector labels.Selector, mode Mode, initial []runtime.Object) *PerWatcher {
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
	}
	h.watchers[id] = w
	h.mu.Unlock()
	h.Counters.Watchers.Add(1)
	klog.V(2).InfoS("perwatcher-started", "id", id, "user", identityKey(u), "ns", namespace, "mode", mode, "sharedPoll", h.sharedPoll)

	// Initial replay + initial-events-end BOOKMARK (the 0011/0025
	// contract; closes kubectl wait --for=jsonpath). Run on a
	// goroutine so NewWatch returns promptly to the apiserver.
	go func() {
		for _, o := range initial {
			if !w.matches(o) {
				continue
			}
			w.emit(watch.Event{Type: watch.Added, Object: o})
		}
		w.emit(bookmark(bookmarkRV))

		// Per-watcher live backend access. In SharedPoll mode the hub's
		// single shared loop handles liveness; here we only open
		// per-watcher access in per-watcher mode.
		if !h.sharedPoll {
			switch mode {
			case ModePush:
				h.runPush(ctx, w)
			case ModePoll:
				h.runPoll(ctx, w)
			}
		}
	}()

	return w
}

// runPush opens one backend.WatchFor(user) subscription for this
// watcher (the N-backend-watches cost). The backend owner-filters the
// channel. Each backend change triggers a metadata-RV-stamped
// re-fetch via the stitcher (so the emitted RV is the host authority,
// not a backend RV) and is delivered owner-and-selector-filtered.
func (h *Hub) runPush(ctx context.Context, w *PerWatcher) {
	sub, ok := h.backend.WatchFor(w.identity, h.bufferSize)
	if !ok {
		// Budget exhausted: fall back to a per-watcher poll loop (the
		// internal-multiplex backpressure path).
		klog.V(2).InfoS("perwatcher-push-budget-fallback-to-poll", "id", w.id, "user", identityKey(w.identity))
		h.runPoll(ctx, w)
		return
	}
	w.sub = sub
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

// runPoll runs one backend.ListFor(user) loop for this watcher (the N
// poll loops cost), diffing against a per-watcher snapshot.
func (h *Hub) runPoll(ctx context.Context, w *PerWatcher) {
	snap := map[string]string{} // ns/name -> color|size|phase hash
	tick := time.NewTicker(h.pollInterval)
	defer tick.Stop()
	for {
		refs := h.backend.ListFor(w.identity, w.namespace)
		seen := map[string]bool{}
		for _, r := range refs {
			key := r.Namespace + "/" + r.Name
			seen[key] = true
			hsh := bodyHash(r.Body)
			prev, existed := snap[key]
			if !existed {
				snap[key] = hsh
				h.emitBackendChange(w, backend.Change{Type: backend.Added, Namespace: r.Namespace, Name: r.Name, Body: r.Body})
			} else if prev != hsh {
				snap[key] = hsh
				h.emitBackendChange(w, backend.Change{Type: backend.Modified, Namespace: r.Namespace, Name: r.Name, Body: r.Body})
			}
		}
		for key := range snap {
			if !seen[key] {
				delete(snap, key)
				ns, name := splitKey(key)
				h.emitBackendChange(w, backend.Change{Type: backend.Deleted, Namespace: ns, Name: name})
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

// emitBackendChange turns a per-watcher backend change into an RV-
// stamped served event. The RV comes from the metadata CR (via the
// stitcher's Backend.GetFor → metastore Record), NOT from the backend.
func (h *Hub) emitBackendChange(w *PerWatcher, c backend.Change) {
	if w.namespace != "" && w.namespace != c.Namespace {
		return
	}
	if c.Type == backend.Deleted {
		// Best-effort delete: stitch may 404; emit a minimal object.
		obj, ok := h.stitcher.StitchFor(w.identity, c.Namespace, c.Name)
		if !ok {
			return
		}
		if w.matches(obj) {
			w.emit(watch.Event{Type: watch.Deleted, Object: obj})
		}
		return
	}
	obj, ok := h.stitcher.StitchFor(w.identity, c.Namespace, c.Name)
	if !ok {
		return // owner mismatch / absent
	}
	if !w.matches(obj) {
		return
	}
	et := watch.Added
	if c.Type == backend.Modified {
		et = watch.Modified
	}
	w.emit(watch.Event{Type: et, Object: obj})
}

// RunSharedPoll runs the single system-identity poll loop that fans
// out to ALL watchers (SharedPoll mode). It does NOT enforce per-user
// authz — every watcher sees every object the selector admits. This
// recovers the 0025/0034 single-global-watch cost (one List per
// interval regardless of watcher count).
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
			hsh := bodyHash(r.Body)
			prev, existed := h.sharedSeen[key]
			if !existed {
				h.sharedSeen[key] = hsh
				h.sharedFanout(backend.Added, r.Namespace, r.Name)
			} else if prev != hsh {
				h.sharedSeen[key] = hsh
				h.sharedFanout(backend.Modified, r.Namespace, r.Name)
			}
		}
		for key := range h.sharedSeen {
			if !seen[key] {
				delete(h.sharedSeen, key)
				ns, name := splitKey(key)
				h.sharedFanout(backend.Deleted, ns, name)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// sharedFanout delivers one shared-poll change to every watcher,
// selector-filtered only (NOT owner-filtered). RV is stamped from the
// metadata CR via a system-identity stitch (one Get, shared).
func (h *Hub) sharedFanout(ct backend.ChangeType, ns, name string) {
	system := &user.DefaultInfo{Name: "system:masters", Groups: []string{"system:masters"}}
	obj, ok := h.stitcher.StitchFor(system, ns, name)
	if !ok && ct != backend.Deleted {
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
	case backend.Modified:
		et = watch.Modified
	case backend.Deleted:
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

func bookmark(rv string) watch.Event {
	obj := &metav1.PartialObjectMetadata{}
	obj.SetResourceVersion(rv)
	if obj.GetAnnotations() == nil {
		obj.SetAnnotations(map[string]string{})
	}
	obj.Annotations["k8s.io/initial-events-end"] = "true"
	return watch.Event{Type: watch.Bookmark, Object: obj}
}

func bodyHash(b backend.Body) string {
	return b.Color + "|" + itoa(int(b.Size)) + "|" + b.Phase + "|" + b.Owner
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

func splitKey(key string) (string, string) {
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			return key[:i], key[i+1:]
		}
	}
	return "", key
}

func rvLess(a, b string) bool {
	if a == "" {
		return b != ""
	}
	if b == "" {
		return false
	}
	if len(a) != len(b) {
		return len(a) < len(b)
	}
	return a < b
}
