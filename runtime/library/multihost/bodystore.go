package multihost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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
)

// BodyChangeType classifies a per-watcher body event.
type BodyChangeType string

const (
	BodyAdded    BodyChangeType = "ADDED"
	BodyModified BodyChangeType = "MODIFIED"
	BodyDeleted  BodyChangeType = "DELETED"
)

// BodyChange is one owner-filtered body event delivered to a
// per-watcher push subscription.
type BodyChange struct {
	Type      BodyChangeType
	Namespace string
	Name      string
	Body      Body
}

// BodyRef pairs a body with its identity for listing.
type BodyRef struct {
	Namespace string
	Name      string
	Body      Body
}

// BodyCounters is the per-watcher backend-call instrumentation (0044).
type BodyCounters struct {
	WatchOpened   atomic.Int64
	WatchRejected atomic.Int64
	ListCalls     atomic.Int64
	GetCalls      atomic.Int64
	ActiveWatches atomic.Int64
}

// Snapshot returns a plain copy of the counters.
func (c *BodyCounters) Snapshot() map[string]int64 {
	return map[string]int64{
		"watchOpened":   c.WatchOpened.Load(),
		"watchRejected": c.WatchRejected.Load(),
		"listCalls":     c.ListCalls.Load(),
		"getCalls":      c.GetCalls.Load(),
		"activeWatches": c.ActiveWatches.Load(),
	}
}

type negEntry struct {
	until time.Time
}

// BodyStore is the shared body CRD store (0042): the business body of
// each served object lives on a cluster-scoped CRD readable by every
// replica via an informer, RV-BLIND (the body CR's own resourceVersion
// is read but never surfaced — only the metadata CR's host RV is the
// authority). It is identity-aware (0044): every body carries a
// server-stamped owner, and GetFor/ListFor/WatchFor filter to the
// bodies a caller owns. It exposes authoritative direct-read existence
// queries (0045) for the opt-in read-path reconcile.
type BodyStore struct {
	dyn       dynamic.Interface
	gvr       schema.GroupVersionResource
	kind      string
	fieldMgr  string
	replicaID string
	gate      IdentityGate

	factory  dynamicinformer.DynamicSharedInformerFactory
	informer cache.SharedIndexInformer
	lister   cache.GenericLister
	resync   time.Duration

	upstreamBudget int
	Counters       BodyCounters
	metrics        *Counters

	negEnabled bool
	negTTL     time.Duration
	negMu      sync.Mutex
	neg        map[string]negEntry

	mu          sync.RWMutex
	subscribers map[int64]*subscriber
	nextSubID   int64
}

type subscriber struct {
	id   int64
	user string
	all  bool
	ch   chan BodyChange
	once sync.Once
}

// BodyStoreOptions configures a BodyStore.
type BodyStoreOptions struct {
	Dynamic         dynamic.Interface
	GVR             schema.GroupVersionResource
	Kind            string
	FieldManager    string
	ReplicaID       string
	ResyncPeriod    time.Duration
	UpstreamBudget  int
	Metrics         *Counters
	IdentityGate    IdentityGate
	NegCacheEnabled bool
	NegCacheTTL     time.Duration
}

// NewBodyStore constructs a BodyStore.
func NewBodyStore(opts BodyStoreOptions) *BodyStore {
	if opts.Dynamic == nil {
		panic("multihost.NewBodyStore: Dynamic client is required")
	}
	if opts.Metrics == nil {
		opts.Metrics = &Counters{}
	}
	gate := opts.IdentityGate
	if gate == nil {
		gate = DefaultIdentityGate
	}
	ttl := opts.NegCacheTTL
	if ttl <= 0 {
		ttl = 2 * time.Second
	}
	return &BodyStore{
		dyn:            opts.Dynamic,
		gvr:            opts.GVR,
		kind:           opts.Kind,
		fieldMgr:       opts.FieldManager,
		replicaID:      opts.ReplicaID,
		gate:           gate,
		resync:         opts.ResyncPeriod,
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
// initial cache sync completes. The informer is the single upstream
// stream fanned out to every per-watcher push subscriber, owner-
// filtered (the internal multiplex, 0044).
func (s *BodyStore) Start(ctx context.Context) error {
	inf := s.factory.ForResource(s.gvr)
	s.informer = inf.Informer()
	s.lister = inf.Lister()

	_, err := s.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { s.fanout(BodyAdded, obj) },
		UpdateFunc: func(_, obj interface{}) { s.fanout(BodyModified, obj) },
		DeleteFunc: func(obj interface{}) { s.fanoutDelete(obj) },
	})
	if err != nil {
		return fmt.Errorf("bodystore: add event handler: %w", err)
	}

	s.factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), s.informer.HasSynced) {
		return fmt.Errorf("bodystore: informer cache sync failed")
	}
	klog.InfoS("body-informer-synced", "replica", s.replicaID, "upstreamBudget", s.upstreamBudget)
	return nil
}

// ---- negative cache toggles (0045) ----

// SetNegCache toggles the negative-existence cache at runtime.
func (s *BodyStore) SetNegCache(enabled bool) {
	s.negMu.Lock()
	defer s.negMu.Unlock()
	s.negEnabled = enabled
	s.neg = map[string]negEntry{}
}

// NegCacheEnabled reports the current state.
func (s *BodyStore) NegCacheEnabled() bool {
	s.negMu.Lock()
	defer s.negMu.Unlock()
	return s.negEnabled
}

// ---- push fan-out (0044) ----

func (s *BodyStore) fanout(ct BodyChangeType, obj interface{}) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	ns, nm := bodyRefOf(u)
	s.deliver(BodyChange{Type: ct, Namespace: ns, Name: nm, Body: bodyFromUnstructured(u)})
}

func (s *BodyStore) fanoutDelete(obj interface{}) {
	if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	ns, nm := bodyRefOf(u)
	s.deliver(BodyChange{Type: BodyDeleted, Namespace: ns, Name: nm, Body: bodyFromUnstructured(u)})
}

func (s *BodyStore) deliver(c BodyChange) {
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
			klog.V(3).InfoS("body-subscriber-drop", "sub", sub.id, "owner", sub.user)
		}
	}
}

// BodySubscription is a per-watcher push handle.
type BodySubscription struct {
	C     <-chan BodyChange
	close func()
}

// Close tears down the subscription and releases its budget slot.
func (sub *BodySubscription) Close() { sub.close() }

// WatchFor opens a per-watcher push subscription carrying the caller's
// identity. Owner-filtered on every event (system identities see all).
// Returns (nil, false) if the upstream-subscription budget is
// exhausted — caller falls back to poll (internal-multiplex
// backpressure, 0044).
func (s *BodyStore) WatchFor(u user.Info, bufferSize int) (*BodySubscription, bool) {
	owner, all := s.ownerOf(u)
	s.mu.Lock()
	if s.upstreamBudget > 0 && len(s.subscribers) >= s.upstreamBudget {
		s.mu.Unlock()
		s.Counters.WatchRejected.Add(1)
		return nil, false
	}
	s.nextSubID++
	id := s.nextSubID
	if bufferSize <= 0 {
		bufferSize = defaultWatchBuffer
	}
	sub := &subscriber{id: id, user: owner, all: all, ch: make(chan BodyChange, bufferSize)}
	s.subscribers[id] = sub
	s.mu.Unlock()

	s.Counters.WatchOpened.Add(1)
	s.Counters.ActiveWatches.Add(1)

	closeFn := func() {
		sub.once.Do(func() {
			s.mu.Lock()
			delete(s.subscribers, id)
			s.mu.Unlock()
			close(sub.ch)
			s.Counters.ActiveWatches.Add(-1)
		})
	}
	return &BodySubscription{C: sub.ch, close: closeFn}, true
}

// ---- identity-aware reads (0044) ----

// GetFor returns the body for (namespace, name) IF the caller may see
// it (owner match, or system identity). Reads the informer cache.
func (s *BodyStore) GetFor(u user.Info, namespace, name string) (Body, bool) {
	s.Counters.GetCalls.Add(1)
	b, ok := s.cacheGet(namespace, name)
	if !ok {
		return Body{}, false
	}
	if !s.maySee(u, b) {
		return Body{}, false
	}
	return b, true
}

// Get is the identity-blind cache read used by the stitch hot path.
func (s *BodyStore) Get(namespace, name string) (Body, bool) {
	return s.cacheGet(namespace, name)
}

func (s *BodyStore) cacheGet(namespace, name string) (Body, bool) {
	if s.lister == nil {
		return Body{}, false
	}
	obj, err := s.lister.Get(BodyName(namespace, name))
	if err != nil {
		return Body{}, false
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return Body{}, false
	}
	return bodyFromUnstructured(u), true
}

// ListFor returns all bodies the caller may see (owner-filtered),
// optionally scoped to a namespace. Reads the informer cache.
func (s *BodyStore) ListFor(u user.Info, namespace string) []BodyRef {
	s.Counters.ListCalls.Add(1)
	return s.cacheList(namespace, func(b Body) bool { return s.maySee(u, b) })
}

// List is the identity-blind cache list used by the stitch hot path.
func (s *BodyStore) List(namespace string) []BodyRef {
	return s.cacheList(namespace, nil)
}

func (s *BodyStore) cacheList(namespace string, keep func(Body) bool) []BodyRef {
	if s.lister == nil {
		return nil
	}
	objs, err := s.lister.List(labels.Everything())
	if err != nil {
		return nil
	}
	out := make([]BodyRef, 0, len(objs))
	for _, o := range objs {
		u, ok := o.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		ns, nm := bodyRefOf(u)
		if namespace != "" && ns != namespace {
			continue
		}
		b := bodyFromUnstructured(u)
		if keep != nil && !keep(b) {
			continue
		}
		out = append(out, BodyRef{Namespace: ns, Name: nm, Body: b})
	}
	sortBodyRefs(out)
	return out
}

// ---- authoritative existence queries (0045) ----

// GetAuthoritative is the existence query the read path uses. It always
// reaches the host apiserver directly — never the informer cache,
// never a store-miss short-circuit — so the backend is genuinely the
// source of truth for existence. Consults the negative cache.
func (s *BodyStore) GetAuthoritative(ctx context.Context, namespace, name string) (Body, bool) {
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
	u, err := s.dyn.Resource(s.gvr).Get(ctx, BodyName(namespace, name), metav1.GetOptions{})
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

// ListAuthoritative lists bodies straight from the host apiserver (the
// authoritative set the read-path List reconcile diffs against).
func (s *BodyStore) ListAuthoritative(ctx context.Context, namespace string) ([]BodyRef, error) {
	s.metrics.BackendList.Add(1)
	ul, err := s.dyn.Resource(s.gvr).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]BodyRef, 0, len(ul.Items))
	for i := range ul.Items {
		u := &ul.Items[i]
		ns, nm := bodyRefOf(u)
		if namespace != "" && ns != namespace {
			continue
		}
		out = append(out, BodyRef{Namespace: ns, Name: nm, Body: bodyFromUnstructured(u)})
	}
	sortBodyRefs(out)
	return out, nil
}

// ---- writes ----

// Put creates-or-updates the body CR via the dynamic client. The body
// CR's RV is read but DISCARDED — never surfaced. A CAS conflict on
// Update is returned as an apierrors.IsConflict error so the 0049
// transaction can retry it.
func (s *BodyStore) Put(ctx context.Context, namespace, name string, b Body) error {
	cn := BodyName(namespace, name)
	u := encodeBody(namespace, name, b, s.gvr, s.kind)
	u.SetName(cn)

	existing, err := s.dyn.Resource(s.gvr).Get(ctx, cn, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("bodystore: get for put %s: %w", cn, err)
	}
	if err != nil {
		u.SetResourceVersion("")
		if _, cerr := s.dyn.Resource(s.gvr).Create(ctx, u, metav1.CreateOptions{FieldManager: s.fieldMgr}); cerr != nil {
			return cerr
		}
		s.invalidateNeg(namespace, name)
		return nil
	}
	u.SetResourceVersion(existing.GetResourceVersion())
	u.SetUID(existing.GetUID())
	if _, uerr := s.dyn.Resource(s.gvr).Update(ctx, u, metav1.UpdateOptions{FieldManager: s.fieldMgr}); uerr != nil {
		return uerr
	}
	s.invalidateNeg(namespace, name)
	return nil
}

func (s *BodyStore) invalidateNeg(namespace, name string) {
	if !s.negEnabled {
		return
	}
	s.negMu.Lock()
	delete(s.neg, namespace+"/"+name)
	s.negMu.Unlock()
}

// Delete removes the body CR. Idempotent.
func (s *BodyStore) Delete(ctx context.Context, namespace, name string) error {
	err := s.dyn.Resource(s.gvr).Delete(ctx, BodyName(namespace, name), metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("bodystore: delete %s: %w", BodyName(namespace, name), err)
	}
	return nil
}

// GetDirect reads the body straight from the host kube-apiserver
// (bypassing the informer cache), identity-blind.
func (s *BodyStore) GetDirect(ctx context.Context, namespace, name string) (Body, bool) {
	u, err := s.dyn.Resource(s.gvr).Get(ctx, BodyName(namespace, name), metav1.GetOptions{})
	if err != nil {
		return Body{}, false
	}
	return bodyFromUnstructured(u), true
}

// ---- identity helpers ----

func (s *BodyStore) maySee(u user.Info, b Body) bool {
	return s.gate(u, b.Owner)
}

func (s *BodyStore) ownerOf(u user.Info) (string, bool) {
	if u == nil {
		return "", true
	}
	// "all" is true when the gate would admit any owner — probe with a
	// sentinel that no real owner equals.
	all := s.gate(u, "\x00multihost-all-probe")
	return u.GetName(), all
}

// DefaultIdentityGate grants system identities (system:masters group,
// kube-aggregator, apiserver, node, serviceaccount) full visibility and
// otherwise requires the object's owner to equal the caller's name.
func DefaultIdentityGate(u user.Info, owner string) bool {
	if u == nil {
		return true
	}
	for _, g := range u.GetGroups() {
		if g == "system:masters" {
			return true
		}
	}
	n := u.GetName()
	switch {
	case n == "system:kube-aggregator", n == "system:apiserver",
		hasPrefix(n, "system:node:"), hasPrefix(n, "system:serviceaccount:"):
		return true
	}
	return owner == n
}

func hasPrefix(s, p string) bool { return len(s) >= len(p) && s[:len(p)] == p }

// ---- encode / decode ----

func bodyRefOf(u *unstructured.Unstructured) (string, string) {
	ns, _, _ := unstructured.NestedString(u.Object, "spec", "resourceRef", "namespace")
	nm, _, _ := unstructured.NestedString(u.Object, "spec", "resourceRef", "name")
	return ns, nm
}

func encodeBody(namespace, name string, b Body, gvr schema.GroupVersionResource, kind string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{Group: gvr.Group, Version: gvr.Version, Kind: kind})
	body := map[string]any{"owner": b.Owner}
	for k, v := range b.Fields {
		body[k] = v
	}
	u.Object["spec"] = map[string]any{
		"resourceRef": map[string]any{"namespace": namespace, "name": name},
		"body":        body,
	}
	return u
}

func bodyFromUnstructured(u *unstructured.Unstructured) Body {
	owner, _, _ := unstructured.NestedString(u.Object, "spec", "body", "owner")
	fields := map[string]interface{}{}
	if m, found, _ := unstructured.NestedMap(u.Object, "spec", "body"); found {
		for k, v := range m {
			if k == "owner" {
				continue
			}
			fields[k] = v
		}
	}
	return Body{Owner: owner, Fields: fields}
}

func sortBodyRefs(out []BodyRef) {
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
}

// HashBody returns a stable sha256 of a body's fields plus owner. The
// AA records this on the metadata CR (spec.observed.bodyHash) so the
// emission filter can tell a real body change from lock churn (0043).
func HashBody(b Body) string {
	keys := make([]string, 0, len(b.Fields))
	for k := range b.Fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	fmt.Fprintf(h, "owner=%s", b.Owner)
	for _, k := range keys {
		fmt.Fprintf(h, ";%s=%v", k, b.Fields[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}
