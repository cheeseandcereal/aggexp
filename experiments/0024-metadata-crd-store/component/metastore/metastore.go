// Package metastore implements the 0024 MetadataStore backed by a
// cluster-scoped CRD (resourcemetadatas.aggexpmeta.aggexp.io/v1) on
// the host cluster. Every Get/List/Create/Update/Delete by the
// middleware's REST adapter consults this store. Writes to this
// store go through the host kube-apiserver's dynamic client — the
// component AA holds no state of its own.
//
// Record / ResourceRef mirror thesis.MetadataStore / thesis.Record
// but expanded with the concrete JSON fields the CRD persists.
package metastore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"
)

// GVR of the ResourceMetadata CRD.
var GVR = schema.GroupVersionResource{
	Group:    "aggexpmeta.aggexp.io",
	Version:  "v1",
	Resource: "resourcemetadatas",
}

// ResourceRef identifies the exposed resource instance.
type ResourceRef struct {
	Group     string
	Resource  string
	Namespace string
	Name      string
}

// Record is the metadata overlay the middleware persists.
type Record struct {
	Ref ResourceRef

	// Raw ResourceMetadata object's own metadata — used by the
	// middleware as the authoritative resourceVersion of the
	// stitched response.
	RecordUID             string
	RecordResourceVersion string

	// KRM payload.
	UID               string
	CreationTimestamp metav1.Time
	DeletionTimestamp *metav1.Time
	Labels            map[string]string
	Annotations       map[string]string
	Finalizers        []string
	// ManagedFields raw JSON (as []metav1.ManagedFieldsEntry).
	// Empty means no prior SSA state.
	ManagedFields []byte
	// OwnerReferences raw JSON (as []metav1.OwnerReference).
	OwnerReferences []byte
}

// Store is the CRD-backed MetadataStore.
type Store struct {
	dyn       dynamic.Interface
	fieldMgr  string
	component string // log tag
}

// New constructs a Store around a dynamic client.
func New(dyn dynamic.Interface, fieldManager string) *Store {
	return &Store{dyn: dyn, fieldMgr: fieldManager, component: "metastore"}
}

// RecordName computes the ResourceMetadata.metadata.name for a
// given ResourceRef. Uses the deterministic form:
//
//	<group-with-dashes>.<resource>.<namespace-or-_cluster_>.<name>
//
// If the result exceeds 253 chars (DNS-1123 subdomain limit) or
// contains characters invalid for a Kubernetes name, a sha256-based
// fallback is used: "rmeta-<hex12>".
func RecordName(ref ResourceRef) string {
	ns := ref.Namespace
	if ns == "" {
		ns = "_cluster_"
	}
	// Kubernetes name regex: DNS-1123 subdomain; `.` is allowed as
	// a label separator but `/` and ':' are not. We replace group's
	// '.' with '-' to keep label boundaries predictable.
	grp := strings.ReplaceAll(ref.Group, ".", "-")
	candidate := fmt.Sprintf("%s.%s.%s.%s", grp, ref.Resource, ns, ref.Name)
	if len(candidate) <= 253 && isDNS1123Subdomain(candidate) {
		return candidate
	}
	// Hash fallback: stable for a given ref.
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

// Get fetches the Record for ref, or (nil, nil) if not found.
func (s *Store) Get(ctx context.Context, ref ResourceRef) (*Record, error) {
	name := RecordName(ref)
	u, err := s.dyn.Resource(GVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("%s: get %s: %w", s.component, name, err)
	}
	return decode(u, ref)
}

// List returns all Records matching the (group, resource) filter.
// Namespace-level filtering is the caller's responsibility.
func (s *Store) List(ctx context.Context, group, resource string) ([]*Record, error) {
	ul, err := s.dyn.Resource(GVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("%s: list: %w", s.component, err)
	}
	out := make([]*Record, 0, len(ul.Items))
	for i := range ul.Items {
		u := &ul.Items[i]
		g, r := refPathFromUnstructured(u)
		if g != group || r != resource {
			continue
		}
		rec, err := decode(u, ResourceRef{})
		if err != nil {
			klog.Warningf("%s: skipping decode error on %s: %v", s.component, u.GetName(), err)
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

// Put creates-or-updates a Record. Uses optimistic concurrency when
// the Record carries a RecordResourceVersion; creates if the
// record is absent.
func (s *Store) Put(ctx context.Context, rec *Record) (*Record, error) {
	name := RecordName(rec.Ref)
	u := encode(rec)
	u.SetName(name)

	existing, err := s.dyn.Resource(GVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("%s: get for put %s: %w", s.component, name, err)
	}
	if err != nil {
		// Create.
		u.SetResourceVersion("")
		created, cerr := s.dyn.Resource(GVR).Create(ctx, u, metav1.CreateOptions{FieldManager: s.fieldMgr})
		if cerr != nil {
			return nil, fmt.Errorf("%s: create %s: %w", s.component, name, cerr)
		}
		klog.V(2).Infof("metastore:create ref=%s name=%s rv=%s uid=%s", refString(rec.Ref), name, created.GetResourceVersion(), rec.UID)
		return decode(created, rec.Ref)
	}
	// Update. Preserve the existing RV (the dynamic client will
	// complain if we write with a stale one). We do not let the
	// caller override the RecordResourceVersion on purpose: the
	// middleware re-reads before every write in practice.
	u.SetResourceVersion(existing.GetResourceVersion())
	u.SetUID(existing.GetUID())
	updated, uerr := s.dyn.Resource(GVR).Update(ctx, u, metav1.UpdateOptions{FieldManager: s.fieldMgr})
	if uerr != nil {
		return nil, fmt.Errorf("%s: update %s: %w", s.component, name, uerr)
	}
	klog.V(2).Infof("metastore:update ref=%s name=%s rv=%s", refString(rec.Ref), name, updated.GetResourceVersion())
	return decode(updated, rec.Ref)
}

// Delete removes a Record. Idempotent.
func (s *Store) Delete(ctx context.Context, ref ResourceRef) error {
	name := RecordName(ref)
	err := s.dyn.Resource(GVR).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("%s: delete %s: %w", s.component, name, err)
	}
	klog.V(2).Infof("metastore:delete ref=%s name=%s", refString(ref), name)
	return nil
}

// Watch opens a dynamic watch on all ResourceMetadata objects. The
// caller is responsible for filtering by group+resource.
func (s *Store) Watch(ctx context.Context, resourceVersion string) (watch.Interface, error) {
	return s.dyn.Resource(GVR).Watch(ctx, metav1.ListOptions{
		ResourceVersion: resourceVersion,
	})
}

// RefFromUnstructured extracts the ResourceRef from a
// ResourceMetadata's unstructured representation.
func RefFromUnstructured(u *unstructured.Unstructured) ResourceRef {
	g, r := refPathFromUnstructured(u)
	ns, _, _ := unstructured.NestedString(u.Object, "spec", "resourceRef", "namespace")
	nm, _, _ := unstructured.NestedString(u.Object, "spec", "resourceRef", "name")
	return ResourceRef{Group: g, Resource: r, Namespace: ns, Name: nm}
}

func refPathFromUnstructured(u *unstructured.Unstructured) (group, resource string) {
	g, _, _ := unstructured.NestedString(u.Object, "spec", "resourceRef", "group")
	r, _, _ := unstructured.NestedString(u.Object, "spec", "resourceRef", "resource")
	return g, r
}

// -------------------- encode / decode --------------------

// encode converts a Record to the *unstructured.Unstructured shape
// the CRD expects.
func encode(rec *Record) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   GVR.Group,
		Version: GVR.Version,
		Kind:    "ResourceMetadata",
	})
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
		for i, s := range rec.Finalizers {
			fins[i] = s
		}
		meta["finalizers"] = fins
	}
	if len(rec.ManagedFields) > 0 {
		meta["managedFields"] = string(rec.ManagedFields)
	}
	if len(rec.OwnerReferences) > 0 {
		meta["ownerReferences"] = string(rec.OwnerReferences)
	}
	u.Object["spec"] = map[string]any{
		"resourceRef": ref,
		"metadata":    meta,
	}
	return u
}

// decode pulls a Record out of an *unstructured.Unstructured. If
// fallback is non-zero it's used as a hint for the ref when the
// payload is missing (shouldn't happen but we defend).
func decode(u *unstructured.Unstructured, fallback ResourceRef) (*Record, error) {
	ref := RefFromUnstructured(u)
	if ref.Group == "" {
		ref = fallback
	}
	rec := &Record{
		Ref:                   ref,
		RecordUID:             string(u.GetUID()),
		RecordResourceVersion: u.GetResourceVersion(),
	}
	meta, found, err := unstructured.NestedMap(u.Object, "spec", "metadata")
	if err != nil {
		return nil, err
	}
	if !found {
		return rec, nil
	}
	if s, ok := meta["uid"].(string); ok {
		rec.UID = s
	}
	if s, ok := meta["creationTimestamp"].(string); ok && s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			rec.CreationTimestamp = metav1.NewTime(t)
		}
	}
	if s, ok := meta["deletionTimestamp"].(string); ok && s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			mt := metav1.NewTime(t)
			rec.DeletionTimestamp = &mt
		}
	}
	if m, ok := meta["labels"].(map[string]any); ok {
		rec.Labels = map[string]string{}
		for k, v := range m {
			if s, ok := v.(string); ok {
				rec.Labels[k] = s
			}
		}
	}
	if m, ok := meta["annotations"].(map[string]any); ok {
		rec.Annotations = map[string]string{}
		for k, v := range m {
			if s, ok := v.(string); ok {
				rec.Annotations[k] = s
			}
		}
	}
	if arr, ok := meta["finalizers"].([]any); ok {
		fins := make([]string, 0, len(arr))
		for _, v := range arr {
			if s, ok := v.(string); ok {
				fins = append(fins, s)
			}
		}
		rec.Finalizers = fins
	}
	if s, ok := meta["managedFields"].(string); ok && s != "" {
		// Validate quickly to reject garbage early.
		var tmp []metav1.ManagedFieldsEntry
		if err := json.Unmarshal([]byte(s), &tmp); err != nil {
			return nil, fmt.Errorf("managedFields not valid JSON: %w", err)
		}
		rec.ManagedFields = []byte(s)
	}
	if s, ok := meta["ownerReferences"].(string); ok && s != "" {
		var tmp []metav1.OwnerReference
		if err := json.Unmarshal([]byte(s), &tmp); err != nil {
			return nil, fmt.Errorf("ownerReferences not valid JSON: %w", err)
		}
		rec.OwnerReferences = []byte(s)
	}
	return rec, nil
}

// ErrRefInvalid signals a bad ResourceRef.
var ErrRefInvalid = errors.New("metastore: ResourceRef missing required fields")

func refString(r ResourceRef) string {
	ns := r.Namespace
	if ns == "" {
		ns = "_cluster_"
	}
	return fmt.Sprintf("%s/%s/%s/%s", r.Group, r.Resource, ns, r.Name)
}
