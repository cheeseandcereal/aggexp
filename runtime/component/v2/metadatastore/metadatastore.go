// Package metadatastore implements the v2 MetadataStore: the
// middleware's authoritative home for KRM metadata (uid,
// resourceVersion, creationTimestamp, labels, annotations,
// managedFields, finalizers, ownerReferences, deletionTimestamp).
//
// Backed by a shared cluster-scoped CRD
// `resourcemetadatas.aggexpmeta.aggexp.io/v1` on the host cluster,
// stitched onto backend business data on every request path. This
// is the "fifth storage axis" from FINDINGS/0024: business data on
// the backend, KRM metadata on the host. The key property 0024
// validates is that ArgoCD's tracker does NOT double-track
// Record-backed metadata because the tracking annotations the
// ecosystem uses stay inside the stitched exposed resource
// (resourcemetadata.spec.metadata.annotations), not the
// ResourceMetadata CR's own metadata.annotations.
//
// Naming: Records are named <group-with-dashes>.<resource>.<ns|cluster>.<name>
// with an sha256 fallback when the composed name busts DNS-1123.
//
// # Schema evolution
//
// Single-version CRD. When a field is added to Record (e.g. a new
// top-level KRM field), the stored shape grows; older rows decode
// with the missing field zero-valued. A true cross-version
// migration path (v1 → v1alpha2) is not provided — recorded as a
// scope cut in FINDINGS/0030; the migration story is "snapshot,
// apply new CRD, restore" or a dedicated conversion webhook, both
// out of the v2 substrate's scope.
//
// # Encryption at rest
//
// Records land in host etcd. Operators persisting secrets-adjacent
// annotations (OIDC token hints, credential IDs) need
// EncryptionConfiguration enabled for
// resourcemetadatas.aggexpmeta.aggexp.io on their host cluster.
// Recorded again for visibility; not enforced by the substrate.
package metadatastore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	_ "embed"
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

// CRDYAML is the ResourceMetadata CRD manifest. Embedded so
// consumers can `kubectl apply -f <(code)` via a helper.
//
//go:embed resourcemetadata-crd.yaml
var CRDYAML []byte

// ResourceRef identifies the exposed resource instance a Record
// describes.
type ResourceRef struct {
	Group     string
	Resource  string
	Namespace string // empty for cluster-scoped resources
	Name      string
}

// Record is the KRM overlay persisted for one ResourceRef.
type Record struct {
	Ref ResourceRef

	// RecordUID / RecordResourceVersion are the ResourceMetadata
	// CR's OWN ObjectMeta fields (from the host apiserver). Used by
	// the middleware to drive the stitched response's RV — unified
	// RV authority, per FINDINGS/0025.
	RecordUID             string
	RecordResourceVersion string

	UID               string
	CreationTimestamp metav1.Time
	DeletionTimestamp *metav1.Time
	Labels            map[string]string
	Annotations       map[string]string
	Finalizers        []string
	// ManagedFields is raw JSON of []metav1.ManagedFieldsEntry.
	ManagedFields []byte
	// OwnerReferences is raw JSON of []metav1.OwnerReference.
	OwnerReferences []byte
}

// Store is the CRD-backed MetadataStore.
type Store struct {
	dyn       dynamic.Interface
	fieldMgr  string
	component string // log tag
}

// New builds a Store around a dynamic client.
func New(d dynamic.Interface, fieldManager string) *Store {
	return &Store{dyn: d, fieldMgr: fieldManager, component: "metastore"}
}

// RecordName deterministically maps a ResourceRef to a CRD name.
// See package doc for the composition rule.
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

// List returns Records matching the (group, resource) filter.
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

// Put creates-or-updates a Record.
func (s *Store) Put(ctx context.Context, rec *Record) (*Record, error) {
	name := RecordName(rec.Ref)
	u := encode(rec)
	u.SetName(name)

	existing, err := s.dyn.Resource(GVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("%s: get for put %s: %w", s.component, name, err)
	}
	if err != nil {
		u.SetResourceVersion("")
		created, cerr := s.dyn.Resource(GVR).Create(ctx, u, metav1.CreateOptions{FieldManager: s.fieldMgr})
		if cerr != nil {
			return nil, fmt.Errorf("%s: create %s: %w", s.component, name, cerr)
		}
		klog.V(2).Infof("metastore:create ref=%s name=%s rv=%s uid=%s",
			refString(rec.Ref), name, created.GetResourceVersion(), rec.UID)
		return decode(created, rec.Ref)
	}
	u.SetResourceVersion(existing.GetResourceVersion())
	u.SetUID(existing.GetUID())
	updated, uerr := s.dyn.Resource(GVR).Update(ctx, u, metav1.UpdateOptions{FieldManager: s.fieldMgr})
	if uerr != nil {
		return nil, fmt.Errorf("%s: update %s: %w", s.component, name, uerr)
	}
	klog.V(2).Infof("metastore:update ref=%s name=%s rv=%s",
		refString(rec.Ref), name, updated.GetResourceVersion())
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

// Watch opens a dynamic watch on the CRD.
func (s *Store) Watch(ctx context.Context, resourceVersion string) (watch.Interface, error) {
	return s.dyn.Resource(GVR).Watch(ctx, metav1.ListOptions{ResourceVersion: resourceVersion})
}

// RefFromUnstructured extracts a ResourceRef from a ResourceMetadata
// unstructured.
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

func encode(rec *Record) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{
		Group: GVR.Group, Version: GVR.Version, Kind: "ResourceMetadata",
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

func refString(r ResourceRef) string {
	ns := r.Namespace
	if ns == "" {
		ns = "cluster"
	}
	return fmt.Sprintf("%s/%s/%s/%s", r.Group, r.Resource, ns, r.Name)
}
