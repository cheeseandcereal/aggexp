// Package backend is experiment 0042's Widget body store. It holds
// ONLY the business body (spec + status) for each Widget, keyed by
// namespace/name, and is deliberately RV-BLIND: it never surfaces a
// resourceVersion. The authoritative RV is the metadata CR's host RV
// (see pkg/metastore).
//
// The body lives on a SEPARATE host CRD (widgetbodies.widgetbody.
// aggexp.io) — distinct from both the served group (aggexp.io) and
// the metadata group (widgetmeta.aggexp.io). This keeps the 0024
// split intact (metadata in one CR, body in another, stitched on
// read) while making the body CROSS-REPLICA readable: every replica
// reads the body from an informer on the shared body CRD, so a write
// that lands on replica 0 is immediately visible to replicas 1/2.
//
// A per-replica in-memory map (the original sketch) was insufficient:
// the metadata CR propagates cross-replica via its informer, but a
// per-replica body map does not, so Get on a non-writer replica would
// 404. A shared body CRD resolves that. The body CR's own RV is read
// but DISCARDED — only the metadata CR's RV is ever surfaced.
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

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0044-per-watcher-watch-identity/pkg/apis/aggexp"
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

// Store is a CRD-backed, informer-fed, RV-blind body store.
type Store struct {
	dyn       dynamic.Interface
	fieldMgr  string
	replicaID string

	factory  dynamicinformer.DynamicSharedInformerFactory
	informer cache.SharedIndexInformer
	lister   cache.GenericLister

	mu      sync.RWMutex
	started bool
}

// Options configures a Store.
type Options struct {
	Dynamic      dynamic.Interface
	FieldManager string
	ReplicaID    string
	ResyncPeriod time.Duration
}

// New constructs a Store.
func New(opts Options) *Store {
	if opts.Dynamic == nil {
		panic("backend.New: Dynamic client is required")
	}
	return &Store{
		dyn:       opts.Dynamic,
		fieldMgr:  opts.FieldManager,
		replicaID: opts.ReplicaID,
		factory: dynamicinformer.NewFilteredDynamicSharedInformerFactory(
			opts.Dynamic, opts.ResyncPeriod, metav1.NamespaceAll, nil,
		),
	}
}

// Start spins up the shared informer on the body CRD. Blocks until
// the initial cache sync completes. The body informer has NO event
// handler that drives the served Watch — watch events are driven by
// the metadata-CR informer only (that's where the RV authority is).
// The body informer exists purely to make Get/List cross-replica
// consistent.
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

// Get returns the body for (namespace, name) and whether it exists,
// read from the informer cache (cross-replica consistent).
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

// List returns all bodies, optionally filtered to a namespace.
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
// bypassing the informer cache (used right after a write so the
// writer replica sees its own write without informer lag).
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
