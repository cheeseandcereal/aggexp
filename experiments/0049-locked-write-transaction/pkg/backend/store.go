// Package backend is experiment 0048's Widget body store. It is the
// COMPOSITION of three arc mechanisms on one body backend:
//
//   - 0042/0044 shared body CRD (widgetbodies.widgetbody.aggexp.io),
//     cross-replica readable via an informer, RV-BLIND (the body CR's
//     own resourceVersion is read but never surfaced — only the
//     metadata CR's host RV is the authority).
//   - 0044 caller-identity awareness: every body carries a server-
//     stamped `owner`, and the identity-aware reads (GetFor, ListFor,
//     WatchFor) filter to the bodies a caller owns (system identities
//     see all), making per-user watch authz observable.
//   - 0045 backend-as-source-of-truth-for-existence: GetAuthoritative
//     and ListAuthoritative read the host apiserver DIRECTLY (never the
//     informer cache, never a store-miss short-circuit), so the read
//     path can reconcile (adopt/collect) inline. An optional negative-
//     existence cache (flag, default off) bounds amplification.
//
// Owner is purely an authz tag living on the body CR; it is NOT
// surfaced on the served Widget (the 0046-generated Widget type has no
// owner field — the capstone intentionally keeps the generated type
// pristine and carries owner out-of-band).
//
// Code is duplicated from 0042/0043/0044/0045 per the lab ethos.
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

	widgetsv1 "github.com/cheeseandcereal/aggexp/experiments/0049-locked-write-transaction/pkg/apis/widgets/v1"
	"github.com/cheeseandcereal/aggexp/experiments/0049-locked-write-transaction/pkg/metrics"
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

// DelaySeconds is a debug knob: when >0, Put sleeps this long before
// writing the body. Used by the lock-renewal scenario to force a
// backend write to outlast the lease duration. Set via the
// WIDGET_BACKEND_DELAY_SECONDS env var at startup (see pkg/server).
var DelaySeconds int

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

// Counters is the per-watcher backend-call instrumentation (0044). The
// 0045 read-path counters live separately in pkg/metrics.
type Counters struct {
	WatchOpened   atomic.Int64
	WatchRejected atomic.Int64
	ListCalls     atomic.Int64
	GetCalls      atomic.Int64
	ActiveWatches atomic.Int64
}

// Snapshot returns a plain copy of the per-watcher counters.
func (c *Counters) Snapshot() map[string]int64 {
	return map[string]int64{
		"watchOpened":   c.WatchOpened.Load(),
		"watchRejected": c.WatchRejected.Load(),
		"listCalls":     c.ListCalls.Load(),
		"getCalls":      c.GetCalls.Load(),
		"activeWatches": c.ActiveWatches.Load(),
	}
}

// negEntry is a short-TTL negative-existence cache entry (0045).
type negEntry struct {
	until time.Time
}

// Store is a CRD-backed, informer-fed, RV-blind, identity-aware body
// store with authoritative direct-read existence queries.
type Store struct {
	dyn       dynamic.Interface
	fieldMgr  string
	replicaID string

	factory  dynamicinformer.DynamicSharedInformerFactory
	informer cache.SharedIndexInformer
	lister   cache.GenericLister

	// upstreamBudget caps concurrent push subscriptions. 0 = unlimited.
	upstreamBudget int

	// Counters is the 0044 per-watcher instrumentation.
	Counters Counters
	// metrics is the 0045 read-path instrumentation.
	metrics *metrics.Counters

	// 0045 negative-existence cache.
	negEnabled bool
	negTTL     time.Duration
	negMu      sync.Mutex
	neg        map[string]negEntry

	mu      sync.RWMutex
	started bool

	// subscribers is the per-watcher push fan-out (0044 internal
	// multiplex): one shared informer feeds every subscriber channel,
	// owner-filtered.
	subscribers map[int64]*subscriber
	nextSubID   int64
}

type subscriber struct {
	id    int64
	user  string
	all   bool
	ch    chan Change
	once  sync.Once
	store *Store
}

// Options configures a Store.
type Options struct {
	Dynamic         dynamic.Interface
	FieldManager    string
	ReplicaID       string
	ResyncPeriod    time.Duration
	UpstreamBudget  int
	Metrics         *metrics.Counters
	NegCacheEnabled bool
	NegCacheTTL     time.Duration
}

// New constructs a Store.
func New(opts Options) *Store {
	if opts.Dynamic == nil {
		panic("backend.New: Dynamic client is required")
	}
	if opts.Metrics == nil {
		opts.Metrics = &metrics.Counters{}
	}
	ttl := opts.NegCacheTTL
	if ttl <= 0 {
		ttl = 2 * time.Second
	}
	return &Store{
		dyn:            opts.Dynamic,
		fieldMgr:       opts.FieldManager,
		replicaID:      opts.ReplicaID,
		upstreamBudget: opts.UpstreamBudget,
		metrics:        opts.Metrics,
		subscribers:    map[int64]*subscriber{},
		factory: dynamicinformer.NewFilteredDynamicSharedInformerFactory(
			opts.Dynamic, opts.ResyncPeriod, metav1.NamespaceAll, nil,
		),
		negEnabled: opts.NegCacheEnabled,
		negTTL:     ttl,
		neg:        map[string]negEntry{},
	}
}

// Start spins up the shared informer on the body CRD. Blocks until the
// initial cache sync completes. The informer's event handler is the
// single upstream stream fanned out to every per-watcher push
// subscriber (filtered by owner).
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

// ---- 0045 negative cache toggles ----

// SetNegCache toggles the negative-existence cache at runtime.
func (s *Store) SetNegCache(enabled bool) {
	s.negMu.Lock()
	defer s.negMu.Unlock()
	s.negEnabled = enabled
	s.neg = map[string]negEntry{}
}

// NegCacheEnabled reports the current state.
func (s *Store) NegCacheEnabled() bool {
	s.negMu.Lock()
	defer s.negMu.Unlock()
	return s.negEnabled
}

// ---- 0044 push fan-out ----

func (s *Store) fanout(ct ChangeType, obj interface{}) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	ns, nm := refOf(u)
	s.deliver(Change{Type: ct, Namespace: ns, Name: nm, Body: bodyFromUnstructured(u)})
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
	s.deliver(Change{Type: Deleted, Namespace: ns, Name: nm, Body: bodyFromUnstructured(u)})
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
			continue
		}
		select {
		case sub.ch <- c:
		default:
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
// identity. Owner-filtered on every event (system identities see all).
// Returns (nil, false) if the upstream-subscription budget is
// exhausted — caller falls back to poll (internal-multiplex
// backpressure).
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

// ---- identity-aware reads (0044) ----

// GetFor returns the body for (namespace, name) IF the caller may see
// it (owner match, or system identity). Reads the informer cache.
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

// Get is the identity-blind cache read used by the stitch hot path.
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

// ListFor returns all bodies the caller may see (owner-filtered),
// optionally scoped to a namespace. Reads the informer cache.
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
			continue
		}
		out = append(out, Ref{Namespace: ns, Name: nm, Body: b})
	}
	sortRefs(out)
	return out
}

// List is the identity-blind cache list used by the stitch hot path.
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
	sortRefs(out)
	return out
}

// ---- authoritative existence queries (0045) ----

// GetAuthoritative is the existence query the read path uses. It
// always reaches the host apiserver directly — never a store-miss
// short-circuit, never the possibly-stale informer cache — so the
// backend is genuinely the source of truth for existence. Counts a
// BackendGet on every round-trip and consults the negative cache.
//
// It is identity-aware on the authz side: the body it returns still
// carries the owner, so the caller can apply maySee. But the EXISTENCE
// decision is the backend's, independent of identity.
func (s *Store) GetAuthoritative(ctx context.Context, namespace, name string) (Body, bool) {
	key := namespace + "/" + name

	if s.negEnabled {
		s.negMu.Lock()
		if e, ok := s.neg[key]; ok {
			if time.Now().Before(e.until) {
				s.negMu.Unlock()
				s.metrics.NegCacheHit.Add(1)
				return Body{}, false
			}
			delete(s.neg, key)
		}
		s.negMu.Unlock()
		s.metrics.NegCacheMiss.Add(1)
	}

	s.metrics.BackendGet.Add(1)
	u, err := s.dyn.Resource(GVR).Get(ctx, bodyName(namespace, name), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) && s.negEnabled {
			s.negMu.Lock()
			s.neg[key] = negEntry{until: time.Now().Add(s.negTTL)}
			s.negMu.Unlock()
		}
		return Body{}, false
	}
	if s.negEnabled {
		s.negMu.Lock()
		delete(s.neg, key)
		s.negMu.Unlock()
	}
	return bodyFromUnstructured(u), true
}

// ListAuthoritative lists bodies straight from the host apiserver
// (counts a BackendList). The authoritative set the read path's List
// reconcile diffs against.
func (s *Store) ListAuthoritative(ctx context.Context, namespace string) ([]Ref, error) {
	s.metrics.BackendList.Add(1)
	ul, err := s.dyn.Resource(GVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]Ref, 0, len(ul.Items))
	for i := range ul.Items {
		u := &ul.Items[i]
		ns, nm := refOf(u)
		if namespace != "" && ns != namespace {
			continue
		}
		out = append(out, Ref{Namespace: ns, Name: nm, Body: bodyFromUnstructured(u)})
	}
	sortRefs(out)
	return out, nil
}

// maySee implements the per-user authz predicate.
func (s *Store) maySee(u user.Info, b Body) bool {
	_, all := ownerOf(u)
	if all {
		return true
	}
	owner, _ := ownerOf(u)
	return b.Owner == owner
}

// ---- writes ----

// Put creates-or-updates the body CR via the dynamic client. The body
// CR's RV is read but DISCARDED — never surfaced.
func (s *Store) Put(ctx context.Context, namespace, name string, b Body) error {
	if DelaySeconds > 0 {
		time.Sleep(time.Duration(DelaySeconds) * time.Second)
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
		s.invalidateNeg(namespace, name)
		return nil
	}
	u.SetResourceVersion(existing.GetResourceVersion())
	u.SetUID(existing.GetUID())
	if _, uerr := s.dyn.Resource(GVR).Update(ctx, u, metav1.UpdateOptions{FieldManager: s.fieldMgr}); uerr != nil {
		return fmt.Errorf("backend: update %s: %w", cn, uerr)
	}
	s.invalidateNeg(namespace, name)
	return nil
}

func (s *Store) invalidateNeg(namespace, name string) {
	if !s.negEnabled {
		return
	}
	s.negMu.Lock()
	delete(s.neg, namespace+"/"+name)
	s.negMu.Unlock()
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

// GetDirect reads the body straight from the host kube-apiserver
// (bypassing the informer cache), identity-blind.
func (s *Store) GetDirect(ctx context.Context, namespace, name string) (Body, bool) {
	u, err := s.dyn.Resource(GVR).Get(ctx, bodyName(namespace, name), metav1.GetOptions{})
	if err != nil {
		return Body{}, false
	}
	return bodyFromUnstructured(u), true
}

// ---- identity helpers ----

func ownerOf(u user.Info) (string, bool) {
	if u == nil {
		return "", true
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

func hasPrefix(s, p string) bool { return len(s) >= len(p) && s[:len(p)] == p }

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
		"resourceRef": map[string]any{"namespace": namespace, "name": name},
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

func sortRefs(out []Ref) {
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
}

// ---- conversions between Body and the 0046-generated Widget ----

// BodyFromWidget extracts the body from a generated Widget. Owner is
// carried separately (server-stamped from identity), not from the
// Widget — the generated type has no owner field.
func BodyFromWidget(w *widgetsv1.Widget) Body {
	return Body{
		Color: string(w.Spec.Color),
		Size:  w.Spec.Size,
		Phase: string(w.Status.Phase),
	}
}

// ApplyBody overlays a Body onto a generated Widget's spec/status.
// Owner is intentionally NOT applied to the Widget (authz-only tag).
func ApplyBody(w *widgetsv1.Widget, b Body) {
	w.Spec.Color = widgetsv1.WidgetSpecColor(b.Color)
	w.Spec.Size = b.Size
	w.Status.Phase = widgetsv1.WidgetStatusPhase(b.Phase)
}

// HashBody returns a stable sha256 of a body's fields. The AA records
// this on the metadata CR (spec.observed.bodyHash) so the emission
// filter can tell a real body change from lock churn (0043). Owner is
// included so an owner change (rare; server-stamped) is a visible
// change.
func HashBody(b Body) string {
	h := sha256.New()
	fmt.Fprintf(h, "color=%s;size=%d;phase=%s;owner=%s", b.Color, b.Size, b.Phase, b.Owner)
	return hex.EncodeToString(h.Sum(nil))
}
