// Package metastore implements experiment 0043's stitched metadata-CR
// store. It is the 0042 metadata-CR store extended with an EMBEDDED
// per-object lock (spec.lock) and an observed body hash
// (spec.observed.bodyHash). It is a synthesis of two prior
// experiments:
//
//   - 0024 (metadata-crd-store): KRM metadata (uid, labels,
//     annotations, finalizers, ownerReferences, deletionTimestamp,
//     creationTimestamp) for each served Widget lives on a
//     cluster-scoped CRD on the host kube-apiserver
//     (resourcemetadatas.widgetmeta.aggexp.io/v1). The business body
//     (spec + status) lives elsewhere — here, in an in-memory backend
//     that never sees metadata or RV.
//
//   - 0034 (shared-watch-cross-replica): each replica runs its own
//     dynamic informer on the metadata CRD. The host etcd RV of the
//     metadata CR is the single RV authority. All replicas observe
//     the same monotonic RV stream, so Get/List/Watch agree across
//     replicas and cross-replica resume-by-RV holds.
//
// The load-bearing decision (0042 README): every served Widget's
// metadata.resourceVersion is the host etcd RV of its metadata CR.
// Never a backend-supplied RV, never a per-replica counter. This
// store stamps that RV on Get/List and drives Watch events carrying
// that RV via the informer.
//
// This code is DUPLICATED from 0024/0034 rather than imported (per
// the lab ethos). The stitched, metadata-only Record shape diverges
// from 0034's whole-object converter and from runtime/library's
// crdstore whole-object converter; that divergence is the point.
package metastore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// MetaGroup is the dedicated group hosting the metadata CRD. It is
// DISTINCT from the served group (aggexp.io): an APIService claims an
// entire group/version, so the metadata CRD cannot live in the served
// group. Same constraint 0024/0034/0010 hit.
const MetaGroup = "widgetmeta.aggexp.io"

// GVR of the metadata CRD on the host cluster.
var GVR = schema.GroupVersionResource{
	Group:    MetaGroup,
	Version:  "v1",
	Resource: "resourcemetadatas",
}

// MetaKind / MetaAPIVersion identify the CRD's unstructured shape.
const (
	MetaKind       = "ResourceMetadata"
	MetaAPIVersion = MetaGroup + "/v1"
)

// ResourceRef identifies the served resource instance whose metadata
// a Record carries.
type ResourceRef struct {
	Group     string
	Resource  string
	Namespace string
	Name      string
}

// LockState is the embedded per-object lock carried on the metadata
// CR's spec.lock (0043). It is CAS'd on the CR's own RV. An empty
// HolderIdentity means the lock is free.
type LockState struct {
	HolderIdentity       string
	AcquiredAt           *metav1.Time
	RenewedAt            *metav1.Time
	LeaseDurationSeconds int32
}

// Held reports whether the lock has a holder.
func (l *LockState) Held() bool { return l != nil && l.HolderIdentity != "" }

// Record is the metadata overlay persisted on the metadata CR plus
// the CR's own etcd RV/UID (the authoritative RV of the stitched
// object).
type Record struct {
	Ref ResourceRef

	// The metadata CR's own etcd-assigned metadata. RecordRV is the
	// single RV authority for the stitched object.
	RecordUID string
	RecordRV  string

	// KRM payload stitched onto the served object.
	UID               string
	CreationTimestamp metav1.Time
	DeletionTimestamp *metav1.Time
	Labels            map[string]string
	Annotations       map[string]string
	Finalizers        []string
	ManagedFields     []byte // JSON []metav1.ManagedFieldsEntry
	OwnerReferences   []byte // JSON []metav1.OwnerReference

	// 0043: embedded lock + observed body hash. Lock may be nil
	// (lock free). BodyHash is the sha256 of the body the AA last
	// committed; it is the watcher-visible signal the emission filter
	// keys on (along with the KRM metadata above).
	Lock     *LockState
	BodyHash string
}

// EventSink fans Watch events out to local watch clients. The
// object's RV (the metadata CR's host RV) is preserved unchanged —
// this is the 0034 EventSink contract, not the substrate Publisher
// (which would stamp a per-replica counter).
type EventSink interface {
	Action(et watch.EventType, obj runtime.Object)
	CurrentResourceVersion() string
	SetCurrentResourceVersion(rv string)
}

// Stitcher turns a (Record, body) pair into the served object. The
// metastore is body-agnostic: the REST adapter supplies a Stitcher
// that knows how to fetch the body for a ref and overlay metadata.
type Stitcher interface {
	// StitchForRef builds the served object for ref using the given
	// Record (may be nil if no metadata CR exists). Returns
	// (nil, false) if the body is absent (e.g. a metadata CR exists
	// but the backend has no body — treat as not-present for watch).
	StitchForRef(ref ResourceRef, rec *Record) (runtime.Object, bool)
}

// Store is the metadata-CR store with a shared informer.
type Store struct {
	dyn      dynamic.Interface
	fieldMgr string

	// Served-resource identity used to filter the informer's
	// cluster-wide CRD stream down to this resource's records.
	group    string
	resource string

	replicaID string

	factory  dynamicinformer.DynamicSharedInformerFactory
	informer cache.SharedIndexInformer
	lister   cache.GenericLister

	mu       sync.RWMutex
	sink     EventSink
	stitcher Stitcher

	// 0043 emission filter: per-record signature of the last
	// watcher-visible state we emitted. A CR transition whose
	// signature is unchanged (lock acquire/release/renewal only) is
	// suppressed before emission. Keyed by RecordName.
	lastEmitted map[string]string
}

// Options configures a Store.
type Options struct {
	Dynamic      dynamic.Interface
	FieldManager string
	Group        string // served group, e.g. "aggexp.io"
	Resource     string // served resource, e.g. "widgets"
	ReplicaID    string
	ResyncPeriod time.Duration
}

// New constructs a Store.
func New(opts Options) *Store {
	if opts.Dynamic == nil {
		panic("metastore.New: Dynamic client is required")
	}
	return &Store{
		dyn:       opts.Dynamic,
		fieldMgr:  opts.FieldManager,
		group:     opts.Group,
		resource:  opts.Resource,
		replicaID: opts.ReplicaID,
		factory: dynamicinformer.NewFilteredDynamicSharedInformerFactory(
			opts.Dynamic, opts.ResyncPeriod, metav1.NamespaceAll, nil,
		),
		lastEmitted: map[string]string{},
	}
}

// SetSink wires the broadcaster the informer fans events through.
// Must be called before Start.
func (s *Store) SetSink(sink EventSink) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sink = sink
}

// SetStitcher wires the body+metadata stitcher used to build served
// objects for informer-driven watch events. Must be called before
// Start.
func (s *Store) SetStitcher(st Stitcher) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stitcher = st
}

// Start spins up the shared informer on the metadata CRD and begins
// forwarding events. Blocks until the initial cache sync completes.
func (s *Store) Start(ctx context.Context) error {
	inf := s.factory.ForResource(GVR)
	s.informer = inf.Informer()
	s.lister = inf.Lister()

	_, err := s.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { s.handle(watch.Added, obj) },
		UpdateFunc: func(_, obj interface{}) { s.handle(watch.Modified, obj) },
		DeleteFunc: func(obj interface{}) { s.handleDelete(obj) },
	})
	if err != nil {
		return fmt.Errorf("metastore: add event handler: %w", err)
	}

	s.factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), s.informer.HasSynced) {
		return fmt.Errorf("metastore: informer cache sync failed")
	}
	klog.InfoS("metastore-informer-synced", "replica", s.replicaID, "group", s.group, "resource", s.resource)
	return nil
}

func (s *Store) handle(et watch.EventType, obj interface{}) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		klog.V(2).InfoS("metastore-informer-non-unstructured", "type", fmt.Sprintf("%T", obj))
		return
	}
	ref := RefFromUnstructured(u)
	if ref.Group != s.group || ref.Resource != s.resource {
		return // a record for some other served resource
	}
	rv := u.GetResourceVersion()
	klog.V(2).InfoS("metastore-informer-event", "replica", s.replicaID, "type", et, "name", u.GetName(), "ref", refString(ref), "rv", rv)

	s.mu.RLock()
	sink := s.sink
	st := s.stitcher
	s.mu.RUnlock()
	if sink == nil || st == nil {
		return
	}
	sink.SetCurrentResourceVersion(rv)

	rec, derr := decode(u, ref)
	if derr != nil {
		klog.Warningf("metastore: decode on event failed name=%s: %v", u.GetName(), derr)
		return
	}
	served, present := st.StitchForRef(ref, rec)
	if !present {
		// metadata CR exists but body is gone (or vice versa). For a
		// DELETED event we still want to notify; synthesize from the
		// record alone is the stitcher's job, so if it says absent on
		// a delete we just skip — the body backend's own delete path
		// already published.
		if et == watch.Deleted {
			return
		}
		return
	}

	// 0043 EMISSION FILTER. A single served-object CR carries both the
	// watcher-visible state (body hash + KRM metadata) AND the
	// embedded lock. Lock acquire/release/renewal write the CR and
	// fire this MODIFIED, but they do NOT change anything a watcher
	// should see. We compute a signature of ONLY the visible state and
	// suppress the event when it is unchanged from the last emission.
	//
	// This is the key result of the experiment: without this filter,
	// one user write surfaces as up to three MODIFIEDs (acquire / body
	// / release) plus a steady drip of renewal MODIFIEDs over a long
	// op. With it, exactly one MODIFIED surfaces per visible change and
	// zero from lock churn.
	if et == watch.Modified {
		sig := visibleSignature(rec)
		name := u.GetName()
		s.mu.Lock()
		prev, seen := s.lastEmitted[name]
		if seen && prev == sig {
			s.mu.Unlock()
			klog.V(2).InfoS("emission-filter-suppress", "replica", s.replicaID, "name", name, "ref", refString(ref), "rv", rv, "reason", "lock-or-renewal-only")
			return
		}
		s.lastEmitted[name] = sig
		s.mu.Unlock()
		klog.V(2).InfoS("emission-filter-emit", "replica", s.replicaID, "name", name, "ref", refString(ref), "rv", rv, "type", "MODIFIED")
	} else if et == watch.Added {
		s.mu.Lock()
		s.lastEmitted[u.GetName()] = visibleSignature(rec)
		s.mu.Unlock()
	}
	sink.Action(et, served)
}

func (s *Store) handleDelete(obj interface{}) {
	if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	ref := RefFromUnstructured(u)
	if ref.Group != s.group || ref.Resource != s.resource {
		return
	}
	rv := u.GetResourceVersion()
	klog.V(2).InfoS("metastore-informer-event", "replica", s.replicaID, "type", watch.Deleted, "name", u.GetName(), "ref", refString(ref), "rv", rv)

	s.mu.RLock()
	sink := s.sink
	st := s.stitcher
	s.mu.RUnlock()
	if sink == nil || st == nil {
		return
	}
	sink.SetCurrentResourceVersion(rv)
	rec, _ := decode(u, ref)
	served, _ := st.StitchForRef(ref, rec)
	if served != nil {
		s.mu.Lock()
		delete(s.lastEmitted, u.GetName())
		s.mu.Unlock()
		sink.Action(watch.Deleted, served)
	}
}

// visibleSignature builds a stable string of the WATCHER-VISIBLE state
// of a stitched object: the observed body hash plus the served KRM
// metadata that surfaces to clients. It deliberately EXCLUDES the
// resourceVersion (which advances on every CR write, including lock
// churn) and the embedded spec.lock. Two CR transitions with the same
// signature differ only in lock/renewal state and must not surface as
// MODIFIED.
func visibleSignature(rec *Record) string {
	// Use the record's body hash (set by the AA on commit) as the
	// body-change signal, plus the KRM metadata fields a watcher sees.
	var b strings.Builder
	b.WriteString("bh=")
	b.WriteString(rec.BodyHash)
	b.WriteString(";uid=")
	b.WriteString(rec.UID)
	if rec.DeletionTimestamp != nil {
		b.WriteString(";del=")
		b.WriteString(rec.DeletionTimestamp.UTC().Format(time.RFC3339))
	}
	b.WriteString(";labels=")
	b.WriteString(stableMap(rec.Labels))
	b.WriteString(";anns=")
	b.WriteString(stableMap(rec.Annotations))
	b.WriteString(";fin=")
	fins := append([]string(nil), rec.Finalizers...)
	sort.Strings(fins)
	b.WriteString(strings.Join(fins, ","))
	if len(rec.OwnerReferences) > 0 {
		b.WriteString(";owners=")
		b.Write(rec.OwnerReferences)
	}
	return b.String()
}

func stableMap(m map[string]string) string {
	if len(m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(m[k])
		b.WriteString(",")
	}
	return b.String()
}

// HighWaterRV returns the highest metadata-CR RV the informer has
// observed for this served resource. Used to stamp List's
// ListMeta.resourceVersion. Falls back to the sink's recorded RV.
func (s *Store) HighWaterRV() string {
	s.mu.RLock()
	sink := s.sink
	s.mu.RUnlock()
	if sink != nil {
		if rv := sink.CurrentResourceVersion(); rv != "" {
			return rv
		}
	}
	// Compute from the cache if the sink hasn't recorded anything.
	max := ""
	if s.lister != nil {
		objs, err := s.lister.List(labels.Everything())
		if err == nil {
			for _, o := range objs {
				if u, ok := o.(*unstructured.Unstructured); ok {
					if rvLess(max, u.GetResourceVersion()) {
						max = u.GetResourceVersion()
					}
				}
			}
		}
	}
	return max
}

// GetFromCache reads a Record from the informer cache by ref. Returns
// (nil, nil) if absent. This is the hot-path read used by Get/List so
// every replica reads the same informer-cached host RV.
func (s *Store) GetFromCache(ref ResourceRef) (*Record, error) {
	if s.lister == nil {
		return nil, nil
	}
	name := RecordName(ref)
	obj, err := s.lister.Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("metastore: cache object %T not unstructured", obj)
	}
	return decode(u, ref)
}

// ListFromCache returns all Records for this served (group, resource)
// from the informer cache, plus the high-water RV across them.
func (s *Store) ListFromCache() ([]*Record, string, error) {
	if s.lister == nil {
		return nil, "", nil
	}
	objs, err := s.lister.List(labels.Everything())
	if err != nil {
		return nil, "", err
	}
	out := make([]*Record, 0, len(objs))
	max := ""
	for _, o := range objs {
		u, ok := o.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		ref := RefFromUnstructured(u)
		if ref.Group != s.group || ref.Resource != s.resource {
			continue
		}
		rec, derr := decode(u, ref)
		if derr != nil {
			klog.Warningf("metastore: skipping decode error on %s: %v", u.GetName(), derr)
			continue
		}
		out = append(out, rec)
		if rvLess(max, rec.RecordRV) {
			max = rec.RecordRV
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ref.Name < out[j].Ref.Name })
	return out, max, nil
}

// Put creates-or-updates a Record via the dynamic client (write
// path; goes straight to the host kube-apiserver). The returned
// Record carries the fresh host RV.
func (s *Store) Put(ctx context.Context, rec *Record) (*Record, error) {
	name := RecordName(rec.Ref)
	u := encode(rec)
	u.SetName(name)

	existing, err := s.dyn.Resource(GVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("metastore: get for put %s: %w", name, err)
	}
	if err != nil {
		u.SetResourceVersion("")
		created, cerr := s.dyn.Resource(GVR).Create(ctx, u, metav1.CreateOptions{FieldManager: s.fieldMgr})
		if cerr != nil {
			return nil, fmt.Errorf("metastore: create %s: %w", name, cerr)
		}
		klog.V(2).InfoS("metastore-create", "replica", s.replicaID, "ref", refString(rec.Ref), "name", name, "rv", created.GetResourceVersion())
		return decode(created, rec.Ref)
	}
	u.SetResourceVersion(existing.GetResourceVersion())
	u.SetUID(existing.GetUID())
	updated, uerr := s.dyn.Resource(GVR).Update(ctx, u, metav1.UpdateOptions{FieldManager: s.fieldMgr})
	if uerr != nil {
		return nil, fmt.Errorf("metastore: update %s: %w", name, uerr)
	}
	klog.V(2).InfoS("metastore-update", "replica", s.replicaID, "ref", refString(rec.Ref), "name", name, "rv", updated.GetResourceVersion())
	return decode(updated, rec.Ref)
}

// Delete removes a Record. Idempotent.
func (s *Store) Delete(ctx context.Context, ref ResourceRef) error {
	name := RecordName(ref)
	err := s.dyn.Resource(GVR).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("metastore: delete %s: %w", name, err)
	}
	klog.V(2).InfoS("metastore-delete", "replica", s.replicaID, "ref", refString(ref), "name", name)
	return nil
}

// GetDirect reads a Record straight from the host kube-apiserver
// (bypassing the informer cache). Used right after a write so the
// caller sees its own write's RV without waiting for informer
// propagation.
func (s *Store) GetDirect(ctx context.Context, ref ResourceRef) (*Record, error) {
	name := RecordName(ref)
	u, err := s.dyn.Resource(GVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return decode(u, ref)
}

// ---- 0043 CAS surface for the embedded lock ----
//
// The locking layer operates on the raw CR so a single Update both
// (a) compares-and-swaps on the CR's own resourceVersion and (b)
// mutates exactly the spec.lock subfield (and, on release, the body
// hash + metadata). These methods preserve all other spec fields by
// reading the raw object first.

// GetRawDirect reads the raw metadata CR straight from the host
// kube-apiserver. Returns (nil, nil) if absent. The locker needs the
// raw object to capture the exact RV and to mutate spec.lock in place
// while preserving every other field.
func (s *Store) GetRawDirect(ctx context.Context, ref ResourceRef) (*unstructured.Unstructured, error) {
	name := RecordName(ref)
	u, err := s.dyn.Resource(GVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return u, nil
}

// SetLockOn mutates the spec.lock subfield of a raw CR in place. A nil
// or empty-holder LockState clears the lock.
func SetLockOn(u *unstructured.Unstructured, ls *LockState) {
	if ls == nil || ls.HolderIdentity == "" {
		unstructured.RemoveNestedField(u.Object, "spec", "lock")
		return
	}
	lock := map[string]any{"holderIdentity": ls.HolderIdentity}
	if ls.AcquiredAt != nil && !ls.AcquiredAt.IsZero() {
		lock["acquiredAt"] = ls.AcquiredAt.UTC().Format(time.RFC3339)
	}
	if ls.RenewedAt != nil && !ls.RenewedAt.IsZero() {
		lock["renewedAt"] = ls.RenewedAt.UTC().Format(time.RFC3339)
	}
	if ls.LeaseDurationSeconds > 0 {
		lock["leaseDurationSeconds"] = int64(ls.LeaseDurationSeconds)
	}
	_ = unstructured.SetNestedMap(u.Object, lock, "spec", "lock")
}

// LockFrom extracts the LockState from a raw CR (nil if free).
func LockFrom(u *unstructured.Unstructured) *LockState {
	lockMap, found, _ := unstructured.NestedMap(u.Object, "spec", "lock")
	if !found {
		return nil
	}
	ls := &LockState{}
	if v, ok := lockMap["holderIdentity"].(string); ok {
		ls.HolderIdentity = v
	}
	if ls.HolderIdentity == "" {
		return nil
	}
	if v, ok := lockMap["acquiredAt"].(string); ok && v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			mt := metav1.NewTime(t)
			ls.AcquiredAt = &mt
		}
	}
	if v, ok := lockMap["renewedAt"].(string); ok && v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			mt := metav1.NewTime(t)
			ls.RenewedAt = &mt
		}
	}
	switch v := lockMap["leaseDurationSeconds"].(type) {
	case int64:
		ls.LeaseDurationSeconds = int32(v)
	case float64:
		ls.LeaseDurationSeconds = int32(v)
	}
	return ls
}

// CreateRawWithLock creates a fresh metadata CR carrying only the
// resourceRef and the given lock (no metadata/body yet). Used when a
// writer acquires the lock for an object whose metadata CR does not
// yet exist (Create path). A CAS-loss (AlreadyExists) is surfaced so
// the locker can retry by reading the now-existing CR.
func (s *Store) CreateRawWithLock(ctx context.Context, ref ResourceRef, ls *LockState) (*unstructured.Unstructured, error) {
	name := RecordName(ref)
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{Group: GVR.Group, Version: GVR.Version, Kind: MetaKind})
	u.SetName(name)
	refMap := map[string]any{"group": ref.Group, "resource": ref.Resource, "name": ref.Name}
	if ref.Namespace != "" {
		refMap["namespace"] = ref.Namespace
	}
	u.Object["spec"] = map[string]any{"resourceRef": refMap}
	SetLockOn(u, ls)
	created, err := s.dyn.Resource(GVR).Create(ctx, u, metav1.CreateOptions{FieldManager: s.fieldMgr})
	if err != nil {
		return nil, err
	}
	klog.V(2).InfoS("metastore-lock-create", "replica", s.replicaID, "ref", refString(ref), "name", name, "holder", ls.HolderIdentity, "rv", created.GetResourceVersion())
	return created, nil
}

// UpdateRaw writes the raw CR back with its current resourceVersion
// (CAS). A 409 Conflict means another writer mutated the CR first —
// the locker treats that as a CAS loss and retries. Returns the
// updated CR (with its fresh RV).
func (s *Store) UpdateRaw(ctx context.Context, u *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	updated, err := s.dyn.Resource(GVR).Update(ctx, u, metav1.UpdateOptions{FieldManager: s.fieldMgr})
	if err != nil {
		return nil, err
	}
	klog.V(4).InfoS("metastore-raw-update", "replica", s.replicaID, "name", u.GetName(), "rv", updated.GetResourceVersion())
	return updated, nil
}

// PutBodyHashAndMeta overlays the body hash + KRM metadata onto a raw
// CR (mutating in place) and CAS-updates it, clearing the lock. This
// is the single "commit + release" write of the embedded-lock design:
// the body-change observation and the lock release land in one CR
// write, advancing the RV exactly once. The raw CR passed in must
// carry the RV the caller wants to CAS on.
func (s *Store) PutBodyHashAndMeta(ctx context.Context, u *unstructured.Unstructured, rec *Record) (*Record, error) {
	// Rebuild spec.metadata + spec.observed from the record, preserve
	// resourceRef, and clear the lock (release).
	enc := encode(rec)
	spec, _, _ := unstructured.NestedMap(enc.Object, "spec")
	// Drop any lock the encode produced (release on commit).
	delete(spec, "lock")
	u.Object["spec"] = spec
	updated, err := s.dyn.Resource(GVR).Update(ctx, u, metav1.UpdateOptions{FieldManager: s.fieldMgr})
	if err != nil {
		return nil, err
	}
	klog.V(2).InfoS("metastore-commit-release", "replica", s.replicaID, "ref", refString(rec.Ref), "name", u.GetName(), "rv", updated.GetResourceVersion(), "bodyHash", rec.BodyHash)
	return decode(updated, rec.Ref)
}

// ---- name / ref encoding ----

// RecordName computes a deterministic metadata-CR name for a ref.
func RecordName(ref ResourceRef) string {
	ns := ref.Namespace
	if ns == "" {
		ns = "cluster"
	}
	grp := strings.ReplaceAll(ref.Group, ".", "-")
	candidate := fmt.Sprintf("%s.%s.%s.%s", grp, ref.Resource, ns, ref.Name)
	if len(candidate) <= 253 && isDNS1123Subdomain(candidate) {
		return candidate
	}
	h := sha256.New()
	h.Write([]byte(candidate))
	sum := hex.EncodeToString(h.Sum(nil))
	return "rmeta-" + sum[:24]
}

var dns1123 = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

func isDNS1123Subdomain(s string) bool {
	if len(s) == 0 || len(s) > 253 {
		return false
	}
	return dns1123.MatchString(s)
}

// RefFromUnstructured extracts the ResourceRef from a ResourceMetadata.
func RefFromUnstructured(u *unstructured.Unstructured) ResourceRef {
	g, _, _ := unstructured.NestedString(u.Object, "spec", "resourceRef", "group")
	r, _, _ := unstructured.NestedString(u.Object, "spec", "resourceRef", "resource")
	ns, _, _ := unstructured.NestedString(u.Object, "spec", "resourceRef", "namespace")
	nm, _, _ := unstructured.NestedString(u.Object, "spec", "resourceRef", "name")
	return ResourceRef{Group: g, Resource: r, Namespace: ns, Name: nm}
}

func encode(rec *Record) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{Group: GVR.Group, Version: GVR.Version, Kind: MetaKind})
	ref := map[string]any{
		"group":    rec.Ref.Group,
		"resource": rec.Ref.Resource,
		"name":     rec.Ref.Name,
	}
	if rec.Ref.Namespace != "" {
		ref["namespace"] = rec.Ref.Namespace
	}
	meta := map[string]any{}
	if rec.UID != "" {
		meta["uid"] = rec.UID
	}
	if !rec.CreationTimestamp.IsZero() {
		meta["creationTimestamp"] = rec.CreationTimestamp.UTC().Format(time.RFC3339)
	}
	if rec.DeletionTimestamp != nil && !rec.DeletionTimestamp.IsZero() {
		meta["deletionTimestamp"] = rec.DeletionTimestamp.UTC().Format(time.RFC3339)
	}
	if len(rec.Labels) > 0 {
		l := map[string]any{}
		for k, v := range rec.Labels {
			l[k] = v
		}
		meta["labels"] = l
	}
	if len(rec.Annotations) > 0 {
		a := map[string]any{}
		for k, v := range rec.Annotations {
			a[k] = v
		}
		meta["annotations"] = a
	}
	if len(rec.Finalizers) > 0 {
		fins := make([]any, len(rec.Finalizers))
		for i, v := range rec.Finalizers {
			fins[i] = v
		}
		meta["finalizers"] = fins
	}
	if len(rec.ManagedFields) > 0 {
		meta["managedFields"] = string(rec.ManagedFields)
	}
	if len(rec.OwnerReferences) > 0 {
		meta["ownerReferences"] = string(rec.OwnerReferences)
	}
	spec := map[string]any{"resourceRef": ref, "metadata": meta}
	if rec.BodyHash != "" {
		spec["observed"] = map[string]any{"bodyHash": rec.BodyHash}
	}
	if rec.Lock != nil && rec.Lock.HolderIdentity != "" {
		lock := map[string]any{"holderIdentity": rec.Lock.HolderIdentity}
		if rec.Lock.AcquiredAt != nil && !rec.Lock.AcquiredAt.IsZero() {
			lock["acquiredAt"] = rec.Lock.AcquiredAt.UTC().Format(time.RFC3339)
		}
		if rec.Lock.RenewedAt != nil && !rec.Lock.RenewedAt.IsZero() {
			lock["renewedAt"] = rec.Lock.RenewedAt.UTC().Format(time.RFC3339)
		}
		if rec.Lock.LeaseDurationSeconds > 0 {
			lock["leaseDurationSeconds"] = int64(rec.Lock.LeaseDurationSeconds)
		}
		spec["lock"] = lock
	}
	u.Object["spec"] = spec
	return u
}

func decode(u *unstructured.Unstructured, fallback ResourceRef) (*Record, error) {
	ref := RefFromUnstructured(u)
	if ref.Group == "" {
		ref = fallback
	}
	rec := &Record{
		Ref:       ref,
		RecordUID: string(u.GetUID()),
		RecordRV:  u.GetResourceVersion(),
	}
	meta, found, err := unstructured.NestedMap(u.Object, "spec", "metadata")
	if err != nil {
		return nil, err
	}
	if found {
		if err := decodeMeta(rec, meta); err != nil {
			return nil, err
		}
	}
	// 0043: observed body hash.
	if bh, _, _ := unstructured.NestedString(u.Object, "spec", "observed", "bodyHash"); bh != "" {
		rec.BodyHash = bh
	}
	// 0043: embedded lock.
	if lockMap, found, _ := unstructured.NestedMap(u.Object, "spec", "lock"); found {
		ls := &LockState{}
		if v, ok := lockMap["holderIdentity"].(string); ok {
			ls.HolderIdentity = v
		}
		if v, ok := lockMap["acquiredAt"].(string); ok && v != "" {
			if t, err := time.Parse(time.RFC3339, v); err == nil {
				mt := metav1.NewTime(t)
				ls.AcquiredAt = &mt
			}
		}
		if v, ok := lockMap["renewedAt"].(string); ok && v != "" {
			if t, err := time.Parse(time.RFC3339, v); err == nil {
				mt := metav1.NewTime(t)
				ls.RenewedAt = &mt
			}
		}
		switch v := lockMap["leaseDurationSeconds"].(type) {
		case int64:
			ls.LeaseDurationSeconds = int32(v)
		case float64:
			ls.LeaseDurationSeconds = int32(v)
		}
		if ls.HolderIdentity != "" {
			rec.Lock = ls
		}
	}
	return rec, nil
}

// decodeMeta fills the KRM metadata fields of rec from the decoded
// spec.metadata map.
func decodeMeta(rec *Record, meta map[string]any) error {
	if v, ok := meta["uid"].(string); ok {
		rec.UID = v
	}
	if v, ok := meta["creationTimestamp"].(string); ok && v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			rec.CreationTimestamp = metav1.NewTime(t)
		}
	}
	if v, ok := meta["deletionTimestamp"].(string); ok && v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			mt := metav1.NewTime(t)
			rec.DeletionTimestamp = &mt
		}
	}
	if m, ok := meta["labels"].(map[string]any); ok {
		rec.Labels = map[string]string{}
		for k, v := range m {
			if vs, ok := v.(string); ok {
				rec.Labels[k] = vs
			}
		}
	}
	if m, ok := meta["annotations"].(map[string]any); ok {
		rec.Annotations = map[string]string{}
		for k, v := range m {
			if vs, ok := v.(string); ok {
				rec.Annotations[k] = vs
			}
		}
	}
	if arr, ok := meta["finalizers"].([]any); ok {
		fins := make([]string, 0, len(arr))
		for _, v := range arr {
			if vs, ok := v.(string); ok {
				fins = append(fins, vs)
			}
		}
		rec.Finalizers = fins
	}
	if v, ok := meta["managedFields"].(string); ok && v != "" {
		var tmp []metav1.ManagedFieldsEntry
		if err := json.Unmarshal([]byte(v), &tmp); err != nil {
			return fmt.Errorf("managedFields not valid JSON: %w", err)
		}
		rec.ManagedFields = []byte(v)
	}
	if v, ok := meta["ownerReferences"].(string); ok && v != "" {
		var tmp []metav1.OwnerReference
		if err := json.Unmarshal([]byte(v), &tmp); err != nil {
			return fmt.Errorf("ownerReferences not valid JSON: %w", err)
		}
		rec.OwnerReferences = []byte(v)
	}
	return nil
}

// ---- small helpers ----

// rvLess returns true when a < b numerically (treats empty as min).
func rvLess(a, b string) bool {
	if a == "" {
		return b != ""
	}
	if b == "" {
		return false
	}
	an, aerr := strconv.ParseUint(a, 10, 64)
	bn, berr := strconv.ParseUint(b, 10, 64)
	if aerr != nil || berr != nil {
		return a < b
	}
	return an < bn
}

func refString(r ResourceRef) string {
	ns := r.Namespace
	if ns == "" {
		ns = "cluster"
	}
	return fmt.Sprintf("%s/%s/%s/%s", r.Group, r.Resource, ns, r.Name)
}
