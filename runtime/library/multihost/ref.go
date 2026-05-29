package multihost

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ResourceRef identifies a single served resource instance whose
// metadata a Record carries and whose body a BodyStore holds. It is
// the join key between the metadata CR, the body CR, and the served
// object.
type ResourceRef struct {
	Group     string
	Resource  string
	Namespace string
	Name      string
}

// String renders a ref for logging. Cluster-scoped objects render
// their namespace as "cluster".
func (r ResourceRef) String() string {
	ns := r.Namespace
	if ns == "" {
		ns = "cluster"
	}
	return fmt.Sprintf("%s/%s/%s/%s", r.Group, r.Resource, ns, r.Name)
}

// LockState is the embedded per-object write lock carried on the
// metadata CR's spec.lock subfield (0043). It is CAS'd on the CR's own
// resourceVersion. An empty HolderIdentity means the lock is free.
type LockState struct {
	HolderIdentity       string
	AcquiredAt           *metav1.Time
	RenewedAt            *metav1.Time
	LeaseDurationSeconds int32
}

// Held reports whether the lock currently has a holder.
func (l *LockState) Held() bool { return l != nil && l.HolderIdentity != "" }

// Record is the metadata overlay persisted on the metadata CR plus the
// CR's own etcd-assigned RV/UID. RecordRV is the single resourceVersion
// authority for the stitched object (0042): it is the host etcd RV of
// the metadata CR, never a backend RV, never a per-replica counter.
type Record struct {
	Ref ResourceRef

	// The metadata CR's own etcd-assigned identity. RecordRV is the
	// authoritative resourceVersion of the stitched object.
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

	// 0043 embedded lock + observed body hash. Lock is nil when free.
	// BodyHash is the hash of the body the AA last committed; it is
	// the watcher-visible body-change signal the emission filter keys
	// on (see VisibleSignature).
	Lock     *LockState
	BodyHash string
}

// RecordName computes a deterministic, DNS-1123-subdomain-safe name
// for the metadata CR backing ref. Long or non-conforming refs fall
// back to a sha256-derived name.
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
	h := sha256.Sum256([]byte(candidate))
	return "rmeta-" + hex.EncodeToString(h[:])[:24]
}

// BodyName computes a deterministic name for the body CR backing
// (namespace, name).
func BodyName(namespace, name string) string {
	ns := namespace
	if ns == "" {
		ns = "cluster"
	}
	candidate := fmt.Sprintf("body.%s.%s", ns, name)
	if len(candidate) <= 253 && isDNS1123Subdomain(candidate) {
		return candidate
	}
	h := sha256.Sum256([]byte(candidate))
	return "wbody-" + hex.EncodeToString(h[:])[:24]
}

var dns1123 = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

func isDNS1123Subdomain(s string) bool {
	if len(s) == 0 || len(s) > 253 {
		return false
	}
	return dns1123.MatchString(s)
}

// rvLess returns true when a < b, comparing numerically when both are
// integer RVs (etcd RVs are) and lexically otherwise. Empty is the
// minimum.
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
