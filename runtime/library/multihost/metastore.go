package multihost

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// RawSink receives metadata-CR informer events as (type, ref, record,
// rv) WITHOUT a pre-stitched object. The per-watcher Hub (watch.go)
// implements it: it owns the fan-out and the re-homed emission filter,
// so the metastore forwards the decoded Record (carrying the lock +
// observed body hash) and lets the Hub decide what reaches each
// watcher. This is the 0048 re-homing: the emission filter lives in
// whichever component owns the fan-out.
type RawSink interface {
	OnMetadataEvent(et watch.EventType, ref ResourceRef, rec *Record, rv string)
}

// MetaStore is the stitched metadata-CR store with a shared informer.
// The host etcd resourceVersion of each metadata CR is the single RV
// authority for the corresponding stitched object (0042). The store
// also exposes the raw CAS surface the embedded lock writes against
// (0043) and the transactional commit write (0049).
type MetaStore struct {
	dyn       dynamic.Interface
	gvr       schema.GroupVersionResource
	kind      string
	fieldMgr  string
	group     string
	resource  string
	replicaID string

	factory  dynamicinformer.DynamicSharedInformerFactory
	informer cache.SharedIndexInformer
	lister   cache.GenericLister

	resync time.Duration

	rawSink RawSink
	curRV   string
}

// MetaStoreOptions configures a MetaStore.
type MetaStoreOptions struct {
	Dynamic      dynamic.Interface
	GVR          schema.GroupVersionResource
	Kind         string
	FieldManager string
	Group        string // served group, e.g. "example.io"
	Resource     string // served resource, e.g. "widgets"
	ReplicaID    string
	ResyncPeriod time.Duration
}

// NewMetaStore constructs a MetaStore.
func NewMetaStore(opts MetaStoreOptions) *MetaStore {
	if opts.Dynamic == nil {
		panic("multihost.NewMetaStore: Dynamic client is required")
	}
	return &MetaStore{
		dyn:       opts.Dynamic,
		gvr:       opts.GVR,
		kind:      opts.Kind,
		fieldMgr:  opts.FieldManager,
		group:     opts.Group,
		resource:  opts.Resource,
		replicaID: opts.ReplicaID,
		resync:    opts.ResyncPeriod,
		factory: dynamicinformer.NewFilteredDynamicSharedInformerFactory(
			opts.Dynamic, opts.ResyncPeriod, metav1.NamespaceAll, nil,
		),
	}
}

// SetRawSink wires the per-watcher Hub. Must be called before Start.
func (s *MetaStore) SetRawSink(rs RawSink) { s.rawSink = rs }

// Start spins up the shared informer on the metadata CRD and begins
// forwarding events to the RawSink. Blocks until the initial cache
// sync completes.
func (s *MetaStore) Start(ctx context.Context) error {
	inf := s.factory.ForResource(s.gvr)
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

func (s *MetaStore) handle(et watch.EventType, obj interface{}) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	ref := refFromUnstructured(u)
	if ref.Group != s.group || ref.Resource != s.resource {
		return // a record for some other served resource
	}
	rv := u.GetResourceVersion()
	if rvLess(s.curRV, rv) {
		s.curRV = rv
	}
	rec, err := decodeRecord(u, ref)
	if err != nil {
		klog.Warningf("metastore: decode on event failed name=%s: %v", u.GetName(), err)
		return
	}
	if s.rawSink != nil {
		s.rawSink.OnMetadataEvent(et, ref, rec, rv)
	}
}

func (s *MetaStore) handleDelete(obj interface{}) {
	if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	ref := refFromUnstructured(u)
	if ref.Group != s.group || ref.Resource != s.resource {
		return
	}
	rv := u.GetResourceVersion()
	rec, _ := decodeRecord(u, ref)
	if s.rawSink != nil {
		s.rawSink.OnMetadataEvent(watch.Deleted, ref, rec, rv)
	}
}

// CurrentResourceVersion returns the highest metadata-CR RV observed.
func (s *MetaStore) CurrentResourceVersion() string { return s.curRV }

// HighWaterRV returns the highest metadata-CR RV observed for this
// served resource (for stamping List's ListMeta.resourceVersion).
func (s *MetaStore) HighWaterRV() string {
	if s.curRV != "" {
		return s.curRV
	}
	max := ""
	if s.lister != nil {
		objs, err := s.lister.List(labels.Everything())
		if err == nil {
			for _, o := range objs {
				if u, ok := o.(*unstructured.Unstructured); ok && rvLess(max, u.GetResourceVersion()) {
					max = u.GetResourceVersion()
				}
			}
		}
	}
	return max
}

// GetFromCache reads a Record from the informer cache by ref. Returns
// (nil, nil) if absent. Every replica reads the same informer-cached
// host RV, so cross-replica Get agrees.
func (s *MetaStore) GetFromCache(ref ResourceRef) (*Record, error) {
	if s.lister == nil {
		return nil, nil
	}
	obj, err := s.lister.Get(RecordName(ref))
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
	return decodeRecord(u, ref)
}

// ListFromCache returns all Records for this served (group, resource)
// from the informer cache, sorted by name, plus the high-water RV.
func (s *MetaStore) ListFromCache() ([]*Record, string, error) {
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
		ref := refFromUnstructured(u)
		if ref.Group != s.group || ref.Resource != s.resource {
			continue
		}
		rec, derr := decodeRecord(u, ref)
		if derr != nil {
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

// GetDirect reads a Record straight from the host kube-apiserver
// (bypassing the informer cache) so a caller sees its own write's RV
// without waiting for informer propagation.
func (s *MetaStore) GetDirect(ctx context.Context, ref ResourceRef) (*Record, error) {
	u, err := s.dyn.Resource(s.gvr).Get(ctx, RecordName(ref), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return decodeRecord(u, ref)
}

// Put creates-or-updates a Record via the dynamic client. Returns the
// Record carrying the fresh host RV. Used by adopt (no lock held).
func (s *MetaStore) Put(ctx context.Context, rec *Record) (*Record, error) {
	name := RecordName(rec.Ref)
	u := encodeRecord(rec, s.gvr, s.kind)
	u.SetName(name)

	existing, err := s.dyn.Resource(s.gvr).Get(ctx, name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("metastore: get for put %s: %w", name, err)
	}
	if err != nil {
		u.SetResourceVersion("")
		created, cerr := s.dyn.Resource(s.gvr).Create(ctx, u, metav1.CreateOptions{FieldManager: s.fieldMgr})
		if cerr != nil {
			return nil, fmt.Errorf("metastore: create %s: %w", name, cerr)
		}
		return decodeRecord(created, rec.Ref)
	}
	u.SetResourceVersion(existing.GetResourceVersion())
	u.SetUID(existing.GetUID())
	updated, uerr := s.dyn.Resource(s.gvr).Update(ctx, u, metav1.UpdateOptions{FieldManager: s.fieldMgr})
	if uerr != nil {
		return nil, fmt.Errorf("metastore: update %s: %w", name, uerr)
	}
	return decodeRecord(updated, rec.Ref)
}

// Delete removes a Record. Idempotent. Deleting the metadata CR
// releases any embedded lock atomically (0043).
func (s *MetaStore) Delete(ctx context.Context, ref ResourceRef) error {
	err := s.dyn.Resource(s.gvr).Delete(ctx, RecordName(ref), metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("metastore: delete %s: %w", RecordName(ref), err)
	}
	return nil
}

// ---- 0043 raw CAS surface for the embedded lock ----

// GetRawDirect reads the raw metadata CR from the host kube-apiserver.
// Returns (nil, nil) if absent. The locker needs the raw object to
// capture the exact RV and mutate spec.lock in place while preserving
// every other field.
func (s *MetaStore) GetRawDirect(ctx context.Context, ref ResourceRef) (*unstructured.Unstructured, error) {
	u, err := s.dyn.Resource(s.gvr).Get(ctx, RecordName(ref), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return u, nil
}

// CreateRawWithLock creates a fresh metadata CR carrying only the
// resourceRef and the given lock (no metadata/body yet). Used when a
// writer acquires the lock for an object whose CR does not yet exist
// (Create path). An AlreadyExists is surfaced so the locker can retry.
func (s *MetaStore) CreateRawWithLock(ctx context.Context, ref ResourceRef, ls *LockState) (*unstructured.Unstructured, error) {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{Group: s.gvr.Group, Version: s.gvr.Version, Kind: s.kind})
	u.SetName(RecordName(ref))
	refMap := map[string]any{"group": ref.Group, "resource": ref.Resource, "name": ref.Name}
	if ref.Namespace != "" {
		refMap["namespace"] = ref.Namespace
	}
	u.Object["spec"] = map[string]any{"resourceRef": refMap}
	setLockOn(u, ls)
	created, err := s.dyn.Resource(s.gvr).Create(ctx, u, metav1.CreateOptions{FieldManager: s.fieldMgr})
	if err != nil {
		return nil, err
	}
	return created, nil
}

// UpdateRaw writes the raw CR back with its current resourceVersion
// (CAS). A 409 Conflict means another writer mutated it first.
func (s *MetaStore) UpdateRaw(ctx context.Context, u *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	return s.dyn.Resource(s.gvr).Update(ctx, u, metav1.UpdateOptions{FieldManager: s.fieldMgr})
}

// CommitBodyHashAndMeta overlays the body hash + KRM metadata onto the
// held raw CR and CAS-updates it, clearing the lock. This is the
// single "commit + release" write of the embedded-lock design: the
// body-change observation and the lock release land in one CR write,
// advancing the RV exactly once. The raw CR passed in must carry the
// RV the caller wants to CAS on. A 409 here is the second CAS surface
// the 0049 transaction retries against.
func (s *MetaStore) CommitBodyHashAndMeta(ctx context.Context, u *unstructured.Unstructured, rec *Record) (*Record, error) {
	enc := encodeRecord(rec, s.gvr, s.kind)
	spec, _, _ := unstructured.NestedMap(enc.Object, "spec")
	delete(spec, "lock") // release on commit
	u.Object["spec"] = spec
	updated, err := s.dyn.Resource(s.gvr).Update(ctx, u, metav1.UpdateOptions{FieldManager: s.fieldMgr})
	if err != nil {
		return nil, err
	}
	return decodeRecord(updated, rec.Ref)
}

// ---- emission filter signature ----

// VisibleSignature builds a stable string of the WATCHER-VISIBLE state
// of a stitched object: the observed body hash plus the served KRM
// metadata. It deliberately EXCLUDES the resourceVersion (which
// advances on every CR write, including lock churn) and the embedded
// spec.lock. Two CR transitions with the same signature differ only in
// lock/renewal state and must NOT surface as MODIFIED (0043). The Hub
// keys the re-homed emission filter on this.
func VisibleSignature(rec *Record) string {
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

// ---- lock helpers on raw CRs ----

func setLockOn(u *unstructured.Unstructured, ls *LockState) {
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

func lockFromRaw(u *unstructured.Unstructured) *LockState {
	lockMap, found, _ := unstructured.NestedMap(u.Object, "spec", "lock")
	if !found {
		return nil
	}
	return lockStateFromMap(lockMap)
}

func lockStateFromMap(lockMap map[string]any) *LockState {
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

// ---- encode / decode ----

func refFromUnstructured(u *unstructured.Unstructured) ResourceRef {
	g, _, _ := unstructured.NestedString(u.Object, "spec", "resourceRef", "group")
	r, _, _ := unstructured.NestedString(u.Object, "spec", "resourceRef", "resource")
	ns, _, _ := unstructured.NestedString(u.Object, "spec", "resourceRef", "namespace")
	nm, _, _ := unstructured.NestedString(u.Object, "spec", "resourceRef", "name")
	return ResourceRef{Group: g, Resource: r, Namespace: ns, Name: nm}
}

func encodeRecord(rec *Record, gvr schema.GroupVersionResource, kind string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{Group: gvr.Group, Version: gvr.Version, Kind: kind})
	ref := map[string]any{"group": rec.Ref.Group, "resource": rec.Ref.Resource, "name": rec.Ref.Name}
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
		setLockOn(u, rec.Lock)
		if l, _, _ := unstructured.NestedMap(u.Object, "spec", "lock"); l != nil {
			spec["lock"] = l
		}
	}
	u.Object["spec"] = spec
	return u
}

func decodeRecord(u *unstructured.Unstructured, fallback ResourceRef) (*Record, error) {
	ref := refFromUnstructured(u)
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
	if bh, _, _ := unstructured.NestedString(u.Object, "spec", "observed", "bodyHash"); bh != "" {
		rec.BodyHash = bh
	}
	if lockMap, found, _ := unstructured.NestedMap(u.Object, "spec", "lock"); found {
		rec.Lock = lockStateFromMap(lockMap)
	}
	return rec, nil
}

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
