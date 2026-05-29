// Package backend is experiment 0044's Widget body store. It is the
// 0042 shared-body-CRD store (cross-replica readable via an informer)
// EXTENDED with caller-identity awareness so that per-user watch
// authorization is observable.
//
// The body lives on a SHARED host CRD (widgetbodies.widgetbody.
// aggexp.io). Every replica reads it via an informer, so a write that
// lands on replica 0 is visible to replicas 1/2 (the 0042 finding).
// Each body carries an `owner` field, set server-side from the
// creating caller's user.Info. The identity-aware reads —
// GetFor(user,...), ListFor(user,...) and the per-watcher WatchFor(
// user) — filter to bodies the caller owns (system identities see
// everything). That filtering is what makes per-user authz on watch
// streams observable in experiment 0044.
//
// Two backend-access shapes are exposed, mirroring the 0025/0034 push
// vs poll axis but PER WATCHER rather than single-global:
//
//   - WatchFor(user, budget): a push Watcher. Returns a channel of
//     owner-filtered body change events. Internally it draws from one
//     shared informer (the single upstream "stream"); the optional
//     subscription budget caps how many concurrent push subscriptions
//     the backend will admit (the internal-multiplex pattern from the
//     0044 README — one upstream serving many per-watcher channels).
//
//   - ListFor(user, ns): the poll-mode read. A per-watcher poll loop
//     in pkg/watch calls this on an interval.
//
// The body store remains RV-BLIND: the body CR's own resourceVersion
// is read but never surfaced. Only the metadata CR's host RV (see
// pkg/metastore) is the authority.
package backend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0047-host-etcd-write-ceiling/pkg/apis/aggexp"
)

// BodyGroup is the dedicated group hosting the body CRD.
const BodyGroup = "widgetbody.aggexp.io"

// GVR of the body CRD on the host cluster.
var GVR = schema.GroupVersionResource{
	Group:    BodyGroup,
	Version:  "v1",
	Resource: "widgetbodies",
}

const (
	bodyKind       = "WidgetBody"
	bodyAPIVersion = BodyGroup + "/v1"
)

// Body is the spec+status of one Widget plus its owner identity. No
// KRM metadata, no RV.
type Body struct {
	Color string
	Size  int32
	Phase string
	Owner string
}

// ChangeType classifies a per-watcher body event.
type ChangeType string

const (
	Added    ChangeType = "ADDED"
	Modified ChangeType = "MODIFIED"
	Deleted  ChangeType = "DELETED"
)

// Change is a single owner-filtered body event delivered to a
// per-watcher push subscription.
type Change struct {
	Type      ChangeType
	Namespace string
	Name      string
	Body      Body
}

// Counters is the backend-call instrumentation the 0044 scenarios
// read. All fields are accessed atomically.
type Counters struct {
	// WatchOpened counts per-watcher push subscriptions opened
	// (backend Watch calls).
	WatchOpened atomic.Int64
	// WatchRejected counts push subscriptions refused because the
	// upstream-subscription budget was exhausted (internal-multiplex
	// pressure signal).
	WatchRejected atomic.Int64
	// ListCalls counts ListFor invocations (poll-loop reads + the
	// REST List path).
	ListCalls atomic.Int64
	// GetCalls counts GetFor invocations that actually hit the
	// store (cache misses on the per-event Get dedup cache).
	GetCalls atomic.Int64
	// ActiveWatches is the current number of open push subscriptions.
	ActiveWatches atomic.Int64
}

// Snapshot returns a plain copy of the counters for logging.
func (c *Counters) Snapshot() map[string]int64 {
	return map[string]int64{
		"watchOpened":   c.WatchOpened.Load(),
		"watchRejected": c.WatchRejected.Load(),
		"listCalls":     c.ListCalls.Load(),
		"getCalls":      c.GetCalls.Load(),
		"activeWatches": c.ActiveWatches.Load(),
	}
}

// Store is a CRD-backed, informer-fed, RV-blind, identity-aware body
// store.
type Store struct {
	dyn       dynamic.Interface
	fieldMgr  string
	replicaID string

	factory  dynamicinformer.DynamicSharedInformerFactory
	informer cache.SharedIndexInformer
	lister   cache.GenericLister

	// upstreamBudget caps concurrent push subscriptions. 0 = unlimited.
	// This models a backend with limited upstream streaming capacity;
	// the internal-multiplex pattern is what keeps N watchers from
	// each needing their own upstream subscription.
	upstreamBudget int

	// writeDelay artificially delays Put to force the embedded lock's
	// renewal heartbeats (0047 scenario 2 "slow backend"). 0 = no
	// delay.
	writeDelay time.Duration

	Counters Counters

	mu      sync.RWMutex
	started bool

	// subscribers is the per-watcher push fan-out. The single shared
	// informer (one upstream stream) feeds every subscriber channel
	// by filtering on owner — the internal-multiplex pattern.
	subscribers map[int64]*subscriber
	nextSubID   int64
}

type subscriber struct {
	id    int64
	user  string // owner to filter on; "" means system (sees all)
	all   bool   // system identity: deliver everything
	ch    chan Change
	once  sync.Once
	store *Store
}

// Options configures a Store.
type Options struct {
	Dynamic        dynamic.Interface
	FieldManager   string
	ReplicaID      string
	ResyncPeriod   time.Duration
	UpstreamBudget int // 0 = unlimited push subscriptions
	WriteDelay     time.Duration // slow-backend toggle (0047 scenario 2)
}

// New constructs a Store.
func New(opts Options) *Store {
	if opts.Dynamic == nil {
		panic("backend.New: Dynamic client is required")
	}
	return &Store{
		dyn:            opts.Dynamic,
		fieldMgr:       opts.FieldManager,
		replicaID:      opts.ReplicaID,
		upstreamBudget: opts.UpstreamBudget,
		writeDelay:     opts.WriteDelay,
		subscribers:    map[int64]*subscriber{},
		factory: dynamicinformer.NewFilteredDynamicSharedInformerFactory(
			opts.Dynamic, opts.ResyncPeriod, metav1.NamespaceAll, nil,
		),
	}
}

// Start spins up the shared informer on the body CRD. Blocks until
// the initial cache sync completes. The informer's event handler is
// the single upstream stream that the internal multiplex fans out to
// every per-watcher push subscriber (filtered by owner).
func (s *Store) Start(ctx context.Context) error {
	inf := s.factory.ForResource(GVR)
	s.informer = inf.Informer()
	s.lister = inf.Lister()

	_, err := s.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { s.fanout(Added, obj) },
		UpdateFunc: func(_, obj interface{}) { s.fanout(Modified, obj) },
		DeleteFunc: func(obj interface{}) { s.fanoutDelete(obj) },
	})
	if err != nil {
		return fmt.Errorf("backend: add event handler: %w", err)
	}

	s.factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), s.informer.HasSynced) {
		return fmt.Errorf("backend: body informer cache sync failed")
	}
	s.mu.Lock()
	s.started = true
	s.mu.Unlock()
	klog.InfoS("body-informer-synced", "replica", s.replicaID, "upstreamBudget", s.upstreamBudget)
	return nil
}

// fanout delivers one informer event to every per-watcher subscriber
// whose owner filter matches. This is the internal-multiplex core:
// ONE upstream informer event becomes N (owner-filtered) per-watcher
// channel sends — the backend never opens N upstream streams.
func (s *Store) fanout(ct ChangeType, obj interface{}) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	ns, nm := refOf(u)
	b := bodyFromUnstructured(u)
	s.deliver(Change{Type: ct, Namespace: ns, Name: nm, Body: b})
}

func (s *Store) fanoutDelete(obj interface{}) {
	if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	ns, nm := refOf(u)
	b := bodyFromUnstructured(u)
	s.deliver(Change{Type: Deleted, Namespace: ns, Name: nm, Body: b})
}

func (s *Store) deliver(c Change) {
	s.mu.RLock()
	subs := make([]*subscriber, 0, len(s.subscribers))
	for _, sub := range s.subscribers {
		subs = append(subs, sub)
	}
	s.mu.RUnlock()
	for _, sub := range subs {
		if !sub.all && c.Body.Owner != sub.user {
			continue // owner filter: per-user authz on the watch stream
		}
		select {
		case sub.ch <- c:
		default:
			// Slow subscriber: drop. The watcher reconnects and
			// replays from List (0025/0034 DropIfChannelFull policy).
			klog.V(3).InfoS("backend-subscriber-drop", "sub", sub.id, "owner", sub.user)
		}
	}
}

// Subscription is a per-watcher push handle.
type Subscription struct {
	C     <-chan Change
	close func()
}

// Close tears down the subscription and releases its budget slot.
func (sub *Subscription) Close() { sub.close() }

// WatchFor opens a per-watcher push subscription carrying the caller's
// identity. Owner-filtering is applied on every delivered event, so
// the caller only sees changes to bodies it owns (system identities
// see all). Returns (nil, false) if the upstream-subscription budget
// is exhausted — the caller is expected to fall back to poll, the
// internal-multiplex backpressure signal.
func (s *Store) WatchFor(u user.Info, bufferSize int) (*Subscription, bool) {
	owner, all := ownerOf(u)
	s.mu.Lock()
	if s.upstreamBudget > 0 && len(s.subscribers) >= s.upstreamBudget {
		s.mu.Unlock()
		s.Counters.WatchRejected.Add(1)
		klog.V(2).InfoS("backend-watch-budget-exhausted", "replica", s.replicaID, "budget", s.upstreamBudget, "owner", owner)
		return nil, false
	}
	s.nextSubID++
	id := s.nextSubID
	if bufferSize <= 0 {
		bufferSize = 100
	}
	sub := &subscriber{id: id, user: owner, all: all, ch: make(chan Change, bufferSize), store: s}
	s.subscribers[id] = sub
	s.mu.Unlock()

	s.Counters.WatchOpened.Add(1)
	s.Counters.ActiveWatches.Add(1)
	klog.V(2).InfoS("backend-watch-opened", "replica", s.replicaID, "sub", id, "owner", owner, "all", all)

	closeFn := func() {
		sub.once.Do(func() {
			s.mu.Lock()
			delete(s.subscribers, id)
			s.mu.Unlock()
			close(sub.ch)
			s.Counters.ActiveWatches.Add(-1)
			klog.V(2).InfoS("backend-watch-closed", "replica", s.replicaID, "sub", id, "owner", owner)
		})
	}
	return &Subscription{C: sub.ch, close: closeFn}, true
}

// GetFor returns the body for (namespace, name) IF the caller may see
// it (owner match, or system identity). Returns (Body{}, false) if
// absent or not owned by the caller.
func (s *Store) GetFor(u user.Info, namespace, name string) (Body, bool) {
	s.Counters.GetCalls.Add(1)
	if s.lister == nil {
		return Body{}, false
	}
	obj, err := s.lister.Get(bodyName(namespace, name))
	if err != nil {
		return Body{}, false
	}
	uu, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return Body{}, false
	}
	b := bodyFromUnstructured(uu)
	if !s.maySee(u, b) {
		return Body{}, false
	}
	return b, true
}

// Get is the identity-blind read used by the writer's own roundtrip
// and by the stitch path (where ownership is enforced elsewhere or
// not at all). It does NOT count against the dedup-cache Get counter.
func (s *Store) Get(namespace, name string) (Body, bool) {
	if s.lister == nil {
		return Body{}, false
	}
	obj, err := s.lister.Get(bodyName(namespace, name))
	if err != nil {
		return Body{}, false
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return Body{}, false
	}
	return bodyFromUnstructured(u), true
}

// Ref pairs a body with its identity for listing.
type Ref struct {
	Namespace string
	Name      string
	Body      Body
}

// ListFor returns all bodies the caller may see, optionally filtered
// to a namespace. This is the poll-mode read.
func (s *Store) ListFor(u user.Info, namespace string) []Ref {
	s.Counters.ListCalls.Add(1)
	if s.lister == nil {
		return nil
	}
	objs, err := s.lister.List(labels.Everything())
	if err != nil {
		return nil
	}
	out := make([]Ref, 0, len(objs))
	for _, o := range objs {
		uu, ok := o.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		ns, nm := refOf(uu)
		if namespace != "" && ns != namespace {
			continue
		}
		b := bodyFromUnstructured(uu)
		if !s.maySee(u, b) {
			continue // owner filter
		}
		out = append(out, Ref{Namespace: ns, Name: nm, Body: b})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// List is the identity-blind list used by the REST List path's stitch
// (the REST layer applies its own per-user filter on top).
func (s *Store) List(namespace string) []Ref {
	if s.lister == nil {
		return nil
	}
	objs, err := s.lister.List(labels.Everything())
	if err != nil {
		return nil
	}
	out := make([]Ref, 0, len(objs))
	for _, o := range objs {
		u, ok := o.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		ns, nm := refOf(u)
		if namespace != "" && ns != namespace {
			continue
		}
		out = append(out, Ref{Namespace: ns, Name: nm, Body: bodyFromUnstructured(u)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// maySee implements the per-user authz predicate: a caller sees a
// body if it owns it, or if the caller is a system identity.
func (s *Store) maySee(u user.Info, b Body) bool {
	owner, all := ownerOf(u)
	if all {
		return true
	}
	return b.Owner == owner
}

// Put creates-or-updates the body CR via the dynamic client. The
// returned body CR's RV is read but DISCARDED — never surfaced.
func (s *Store) Put(ctx context.Context, namespace, name string, b Body) error {
	if s.writeDelay > 0 {
		// Slow-backend toggle: a Put that spans multiple lock-renewal
		// intervals forces the embedded lock's renewal heartbeats to
		// fire (0047 scenario 2). The renewal goroutine keeps the
		// lease alive while we sleep here.
		select {
		case <-time.After(s.writeDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	cn := bodyName(namespace, name)
	u := encodeBody(namespace, name, b)
	u.SetName(cn)

	existing, err := s.dyn.Resource(GVR).Get(ctx, cn, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("backend: get for put %s: %w", cn, err)
	}
	if err != nil {
		u.SetResourceVersion("")
		if _, cerr := s.dyn.Resource(GVR).Create(ctx, u, metav1.CreateOptions{FieldManager: s.fieldMgr}); cerr != nil {
			return fmt.Errorf("backend: create %s: %w", cn, cerr)
		}
		return nil
	}
	u.SetResourceVersion(existing.GetResourceVersion())
	u.SetUID(existing.GetUID())
	if _, uerr := s.dyn.Resource(GVR).Update(ctx, u, metav1.UpdateOptions{FieldManager: s.fieldMgr}); uerr != nil {
		return fmt.Errorf("backend: update %s: %w", cn, uerr)
	}
	return nil
}

// Delete removes the body CR. Idempotent.
func (s *Store) Delete(ctx context.Context, namespace, name string) error {
	cn := bodyName(namespace, name)
	err := s.dyn.Resource(GVR).Delete(ctx, cn, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("backend: delete %s: %w", cn, err)
	}
	return nil
}

// GetDirect reads the body straight from the host kube-apiserver,
// bypassing the informer cache (used right after a write so the
// writer replica sees its own write without informer lag).
func (s *Store) GetDirect(ctx context.Context, namespace, name string) (Body, bool) {
	u, err := s.dyn.Resource(GVR).Get(ctx, bodyName(namespace, name), metav1.GetOptions{})
	if err != nil {
		return Body{}, false
	}
	return bodyFromUnstructured(u), true
}

// ---- identity helpers ----

// ownerOf maps a user.Info to (owner string, isSystem). System /
// control-plane identities (masters group, the AA's own SA, the
// kube-aggregator) see everything; ordinary users are scoped to the
// objects whose owner equals their username.
func ownerOf(u user.Info) (string, bool) {
	if u == nil {
		return "", true // nil user only on internal paths; see all
	}
	name := u.GetName()
	for _, g := range u.GetGroups() {
		if g == "system:masters" {
			return name, true
		}
	}
	switch {
	case name == "system:kube-aggregator",
		name == "system:apiserver",
		hasPrefix(name, "system:node:"),
		hasPrefix(name, "system:serviceaccount:"):
		return name, true
	}
	return name, false
}

func hasPrefix(s, p string) bool {
	return len(s) >= len(p) && s[:len(p)] == p
}

// ---- encode / decode ----

func refOf(u *unstructured.Unstructured) (string, string) {
	ns, _, _ := unstructured.NestedString(u.Object, "spec", "resourceRef", "namespace")
	nm, _, _ := unstructured.NestedString(u.Object, "spec", "resourceRef", "name")
	return ns, nm
}

func bodyName(namespace, name string) string {
	ns := namespace
	if ns == "" {
		ns = "cluster"
	}
	candidate := fmt.Sprintf("body.%s.%s", ns, name)
	if len(candidate) <= 253 && dns1123.MatchString(candidate) {
		return candidate
	}
	h := sha256.New()
	h.Write([]byte(candidate))
	return "wbody-" + hex.EncodeToString(h.Sum(nil))[:24]
}

var dns1123 = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

func encodeBody(namespace, name string, b Body) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{Group: GVR.Group, Version: GVR.Version, Kind: bodyKind})
	u.Object["spec"] = map[string]any{
		"resourceRef": map[string]any{
			"namespace": namespace,
			"name":      name,
		},
		"body": map[string]any{
			"color": b.Color,
			"size":  int64(b.Size),
			"phase": b.Phase,
			"owner": b.Owner,
		},
	}
	return u
}

func bodyFromUnstructured(u *unstructured.Unstructured) Body {
	color, _, _ := unstructured.NestedString(u.Object, "spec", "body", "color")
	phase, _, _ := unstructured.NestedString(u.Object, "spec", "body", "phase")
	owner, _, _ := unstructured.NestedString(u.Object, "spec", "body", "owner")
	size, _, _ := unstructured.NestedInt64(u.Object, "spec", "body", "size")
	return Body{Color: color, Size: int32(size), Phase: phase, Owner: owner}
}

// ---- conversions between Body and Widget ----

// BodyFromWidget extracts the body from a Widget.
func BodyFromWidget(w *aggexp.Widget) Body {
	return Body{Color: w.Spec.Color, Size: w.Spec.Size, Phase: w.Status.Phase, Owner: w.Spec.Owner}
}

// HashBody returns a sha256 hex digest of the body's watcher-visible
// fields. The metastore Record stores this as spec.observed.bodyHash;
// the emission filter keys on it (composed in from 0043).
func HashBody(b Body) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%d|%s|%s", b.Color, b.Size, b.Phase, b.Owner)
	return hex.EncodeToString(h.Sum(nil))
}

// ApplyBody overlays a Body onto a Widget's spec/status.
func ApplyBody(w *aggexp.Widget, b Body) {
	w.Spec.Color = b.Color
	w.Spec.Size = b.Size
	w.Spec.Owner = b.Owner
	w.Status.Phase = b.Phase
}
