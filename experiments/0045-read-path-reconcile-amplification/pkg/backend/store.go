// Package backend is experiment 0045's Widget body store. It holds
// ONLY the business body (spec + status) for each Widget, keyed by
// namespace/name, and is deliberately RV-BLIND: it never surfaces a
// resourceVersion. The body lives on a shared cluster-scoped CRD
// (widgetbodies.widgetbody.aggexp.io), inherited from 0042.
//
// 0045 treats the backend as the SOURCE OF TRUTH FOR OBJECT
// EXISTENCE. The read path (pkg/widgetrest) reconciles the metadata
// store against the backend inline on every Get and List: a backend
// object with no metadata record is ADOPTED (a record is
// synthesized); a metadata record whose backend object is gone is
// COLLECTED (subject to a minAge grace window). There is no
// "tolerant-Get": a backend 404 is a 404 regardless of finalizers.
//
// Because the backend is a shared CRD, it can be mutated OUT OF BAND
// (kubectl apply/delete the WidgetBody CR directly), which is exactly
// how the experiment's scenarios make adoption and collection on read
// observable. To keep existence authoritative the read path must NOT
// short-circuit existence from the metadata store and must NOT serve
// a stale informer cache for the existence decision; it queries the
// host apiserver directly. The cache-based Get/List remain for the
// watch/stitch hot path only.
package backend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0045-read-path-reconcile-amplification/pkg/apis/aggexp"
	"github.com/cheeseandcereal/aggexp/experiments/0045-read-path-reconcile-amplification/pkg/metrics"
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

// Body is the spec+status of one Widget. No metadata, no RV.
type Body struct {
	Color string
	Size  int32
	Phase string
}

// negEntry is a short-TTL negative-existence cache entry.
type negEntry struct {
	until time.Time
}

// Store is a CRD-backed, informer-fed, RV-blind body store with
// authoritative direct-read existence queries and instrumentation.
type Store struct {
	dyn       dynamic.Interface
	fieldMgr  string
	replicaID string

	factory  dynamicinformer.DynamicSharedInformerFactory
	informer cache.SharedIndexInformer
	lister   cache.GenericLister

	counters *metrics.Counters

	// Negative-existence cache, behind a flag (default off). When on,
	// an authoritative Get that 404s remembers the miss for negTTL so
	// a flood of Gets for the same non-existent name does not hammer
	// the backend. Default-off so the un-cached amplification is
	// measured first.
	negEnabled bool
	negTTL     time.Duration
	negMu      sync.Mutex
	neg        map[string]negEntry

	mu      sync.RWMutex
	started bool
}

// Options configures a Store.
type Options struct {
	Dynamic         dynamic.Interface
	FieldManager    string
	ReplicaID       string
	ResyncPeriod    time.Duration
	Counters        *metrics.Counters
	NegCacheEnabled bool
	NegCacheTTL     time.Duration
}

// New constructs a Store.
func New(opts Options) *Store {
	if opts.Dynamic == nil {
		panic("backend.New: Dynamic client is required")
	}
	if opts.Counters == nil {
		opts.Counters = &metrics.Counters{}
	}
	ttl := opts.NegCacheTTL
	if ttl <= 0 {
		ttl = 2 * time.Second
	}
	return &Store{
		dyn:       opts.Dynamic,
		fieldMgr:  opts.FieldManager,
		replicaID: opts.ReplicaID,
		counters:  opts.Counters,
		factory: dynamicinformer.NewFilteredDynamicSharedInformerFactory(
			opts.Dynamic, opts.ResyncPeriod, metav1.NamespaceAll, nil,
		),
		negEnabled: opts.NegCacheEnabled,
		negTTL:     ttl,
		neg:        map[string]negEntry{},
	}
}

// Start spins up the shared informer on the body CRD. Blocks until
// the initial cache sync completes. The informer exists for the
// watch/stitch hot path; existence decisions go through the
// authoritative direct-read methods.
func (s *Store) Start(ctx context.Context) error {
	inf := s.factory.ForResource(GVR)
	s.informer = inf.Informer()
	s.lister = inf.Lister()
	s.factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), s.informer.HasSynced) {
		return fmt.Errorf("backend: body informer cache sync failed")
	}
	s.mu.Lock()
	s.started = true
	s.mu.Unlock()
	klog.InfoS("body-informer-synced", "replica", s.replicaID)
	return nil
}

// SetNegCache toggles the negative-existence cache at runtime (used
// by the debug endpoint between amplification runs).
func (s *Store) SetNegCache(enabled bool) {
	s.negMu.Lock()
	defer s.negMu.Unlock()
	s.negEnabled = enabled
	s.neg = map[string]negEntry{} // flush on toggle
}

// NegCacheEnabled reports the current state.
func (s *Store) NegCacheEnabled() bool {
	s.negMu.Lock()
	defer s.negMu.Unlock()
	return s.negEnabled
}

// GetAuthoritative is the EXISTENCE query the read path uses. It
// always reaches the backend (the host apiserver) directly — never a
// store-miss short-circuit, never the possibly-stale informer cache —
// so the backend is genuinely the source of truth for existence. It
// counts a BackendGet on every backend round-trip and consults the
// negative cache when enabled.
func (s *Store) GetAuthoritative(ctx context.Context, namespace, name string) (Body, bool) {
	key := namespace + "/" + name

	if s.negEnabled {
		s.negMu.Lock()
		if e, ok := s.neg[key]; ok {
			if time.Now().Before(e.until) {
				s.negMu.Unlock()
				s.counters.NegCacheHit.Add(1)
				return Body{}, false
			}
			delete(s.neg, key)
		}
		s.negMu.Unlock()
		s.counters.NegCacheMiss.Add(1)
	}

	s.counters.BackendGet.Add(1)
	u, err := s.dyn.Resource(GVR).Get(ctx, bodyName(namespace, name), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) && s.negEnabled {
			s.negMu.Lock()
			s.neg[key] = negEntry{until: time.Now().Add(s.negTTL)}
			s.negMu.Unlock()
		}
		return Body{}, false
	}
	// A positive result invalidates any stale negative entry.
	if s.negEnabled {
		s.negMu.Lock()
		delete(s.neg, key)
		s.negMu.Unlock()
	}
	return bodyFromUnstructured(u), true
}

// Get returns the body from the informer cache (cross-replica
// consistent, used by the stitch hot path only — NOT for the
// existence decision).
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

// ListAuthoritative lists bodies straight from the host apiserver
// (counts a BackendList). This is the authoritative set the read
// path's List reconcile diffs against.
func (s *Store) ListAuthoritative(ctx context.Context, namespace string) ([]Ref, error) {
	s.counters.BackendList.Add(1)
	ul, err := s.dyn.Resource(GVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]Ref, 0, len(ul.Items))
	for i := range ul.Items {
		u := &ul.Items[i]
		ns, _, _ := unstructured.NestedString(u.Object, "spec", "resourceRef", "namespace")
		nm, _, _ := unstructured.NestedString(u.Object, "spec", "resourceRef", "name")
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
	return out, nil
}

// List returns all bodies from the informer cache (stitch hot path).
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
		ns, _, _ := unstructured.NestedString(u.Object, "spec", "resourceRef", "namespace")
		nm, _, _ := unstructured.NestedString(u.Object, "spec", "resourceRef", "name")
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

// Put creates-or-updates the body CR via the dynamic client. The
// returned body CR's RV is read but DISCARDED — never surfaced.
func (s *Store) Put(ctx context.Context, namespace, name string, b Body) error {
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
// bypassing the informer cache. Unlike GetAuthoritative it does NOT
// count against the amplification instrument or consult the negative
// cache — it's the stitch-time body fetch used right after a write.
func (s *Store) GetDirect(ctx context.Context, namespace, name string) (Body, bool) {
	u, err := s.dyn.Resource(GVR).Get(ctx, bodyName(namespace, name), metav1.GetOptions{})
	if err != nil {
		return Body{}, false
	}
	return bodyFromUnstructured(u), true
}

// ---- encode / decode ----

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
		},
	}
	return u
}

func bodyFromUnstructured(u *unstructured.Unstructured) Body {
	color, _, _ := unstructured.NestedString(u.Object, "spec", "body", "color")
	phase, _, _ := unstructured.NestedString(u.Object, "spec", "body", "phase")
	size, _, _ := unstructured.NestedInt64(u.Object, "spec", "body", "size")
	return Body{Color: color, Size: int32(size), Phase: phase}
}

// ---- conversions between Body and Widget ----

// BodyFromWidget extracts the body from a Widget.
func BodyFromWidget(w *aggexp.Widget) Body {
	return Body{Color: w.Spec.Color, Size: w.Spec.Size, Phase: w.Status.Phase}
}

// ApplyBody overlays a Body onto a Widget's spec/status.
func ApplyBody(w *aggexp.Widget, b Body) {
	w.Spec.Color = b.Color
	w.Spec.Size = b.Size
	w.Status.Phase = b.Phase
}
