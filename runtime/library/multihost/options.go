package multihost

import (
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/dynamic"
)

// Body is the opaque business payload of a served object as the
// multi-host machinery sees it. It carries no KRM metadata and no
// resourceVersion (those live on the metadata CR, the RV authority).
// Owner is a server-stamped authorization tag used by the identity
// gate; it is NOT surfaced on the served object.
//
// Fields holds the body's mutable state as a flat map. The consumer's
// Converter encodes/decodes Fields to and from the body CR and the
// served object. Keeping Body opaque is what lets the substrate be
// generic over the served type while the body CRD schema and the
// served type stay the consumer's concern.
type Body struct {
	// Owner is the identity that created the object (server-stamped).
	// The identity gate compares it against the caller's identity.
	Owner string
	// Fields is the consumer-defined body payload. The Converter is
	// the only thing that interprets it; the substrate treats it as
	// opaque except for hashing (HashBody).
	Fields map[string]interface{}
}

// Converter is the consumer-supplied bridge between the multi-host
// stores' generic shapes (Record, Body) and the consumer's typed
// served object. It is the multi-host analogue of the single-replica
// library's CRDStoreConverter: the substrate does not try to be
// magically generic over arbitrary types; the consumer owns the
// mapping for its own resource.
type Converter interface {
	// New returns a new empty served object (for BOOKMARK carriers and
	// list-item construction). Must be a scheme-registered served type
	// (NOT PartialObjectMetadata), or the watch encoder rejects it.
	New() runtime.Object
	// NewList returns a new empty served list object.
	NewList() runtime.Object

	// BodyFromObject extracts the opaque Body from a served object.
	// Owner is set separately by the adapter (from identity), so the
	// Converter need not populate Body.Owner.
	BodyFromObject(obj runtime.Object) Body
	// Stitch builds the served object from (ref, body, rec). rec may
	// be nil (an adopted/synthetic object); the adapter stamps the RV
	// and synthetic UID in that case. The Converter overlays the body
	// fields and the KRM metadata from rec onto a fresh object.
	Stitch(ref ResourceRef, body Body, rec *Record) runtime.Object

	// RecordFromObject extracts the KRM metadata Record fields from a
	// served object. The adapter fills RecordUID/RecordRV/BodyHash; the
	// Converter fills UID, labels, annotations, finalizers, etc.
	RecordFromObject(obj runtime.Object, ref ResourceRef) *Record
}

// IdentityGate decides whether a caller may see an object with the
// given owner. The default gate (see DefaultIdentityGate) grants
// system identities full visibility and otherwise requires an owner
// match. Consumers may supply their own.
type IdentityGate func(u user.Info, ownerOfObject string) bool

// WatchMode selects the per-watcher live source.
type WatchMode int

const (
	// WatchPush opens one BodyStore push subscription per watcher.
	WatchPush WatchMode = iota
	// WatchPoll runs one identity-scoped poll loop per watcher.
	WatchPoll
)

// Options configures the multi-host REST adapter. Each capability is
// independently opt-in, matching the single-replica runtime/library
// ethos. A consumer enables only what it needs.
type Options struct {
	// --- required ---

	// Dynamic is a dynamic client against the host kube-apiserver,
	// used for the metadata and body CRD informers and writes.
	Dynamic dynamic.Interface
	// Converter bridges the generic Record/Body shapes and the
	// consumer's served type. Required.
	Converter Converter
	// GroupResource identifies the served resource (used in error
	// messages and ref construction). Required.
	GroupResource schema.GroupResource

	// MetaGVR is the GroupVersionResource of the metadata CRD on the
	// host cluster (the RV authority). Required.
	MetaGVR schema.GroupVersionResource
	// MetaKind is the Kind of the metadata CR. Defaults to
	// "ResourceMetadata" (the arc convention) when empty.
	MetaKind string
	// BodyGVR is the GroupVersionResource of the shared body CRD.
	// Required: multi-replica read consistency requires the body to
	// be host-reachable from every replica (0042). A node-local body
	// backend breaks cross-replica reads.
	BodyGVR schema.GroupVersionResource
	// BodyKind is the Kind of the body CR (for encoding writes).
	BodyKind string

	// ReplicaID identifies this replica in logs and as the lock holder
	// identity. Defaults to POD_NAME / hostname when empty.
	ReplicaID string
	// FieldManager is stamped on host-CR writes. Defaults to the
	// resource name when empty.
	FieldManager string
	// ResyncPeriod for the shared informers. 0 disables resync.
	ResyncPeriod time.Duration

	// --- lock (0043 + 0049) ---

	// Lock enables the embedded per-object write lock on the metadata
	// CR (spec.lock), CAS'd on the CR RV. When false, writes go
	// straight through without coordination (single-replica safe; not
	// safe for concurrent multi-replica writes to the same object).
	Lock bool
	// LeaseDuration is the embedded lock's lease. Defaults to 15s.
	LeaseDuration time.Duration
	// LockRenew enables the renewal heartbeat (re-stamps every
	// LeaseDuration/3) so a slow backend op does not lose its lease.
	// Defaults true when Lock is set.
	LockRenew bool
	// DisableLockRenew forces the renewal heartbeat off even when Lock
	// is set (the zero value of LockRenew cannot express "explicitly
	// off", so this is the override).
	DisableLockRenew bool

	// TransactionalWrite enables the VALIDATED 0049 locked-write
	// transaction: the post-acquire body+commit writes run inside a
	// retry loop covering BOTH CAS surfaces (body CR and metadata
	// commit), surfacing budget exhaustion as 409 (never 500). This is
	// the DEFAULT correct write path and should be left on whenever
	// Lock is on. Setting Lock without TransactionalWrite reproduces
	// the 0048 admission-gate-only behavior (a CAS race surfaces as a
	// 500) and exists only for the regression baseline.
	TransactionalWrite bool
	// TransactionAttempts is the transaction-retry budget. Defaults to
	// 5, the 0049-validated value (observed max depth 4).
	TransactionAttempts int

	// --- watch (0044) ---

	// Watch enables the per-watcher identity-carrying watch. When
	// false the adapter still serves Get/List but Watch returns an
	// empty stream's worth of nothing (consumers that do not need
	// watch can leave it off, but most will want it on).
	Watch bool
	// WatchModeSelect picks the per-watcher live source (push or poll).
	WatchModeSelect WatchMode
	// SharedPoll runs a single system-identity poll loop fanned out to
	// all watchers instead of per-watcher access. It does NOT enforce
	// per-user authz on the live watch path (the cheap opt-in). The
	// initial replay and unary List still owner-filter.
	SharedPoll bool
	// PollInterval is the per-watcher / shared poll interval. Defaults
	// to 5s.
	PollInterval time.Duration
	// WatchBufferSize is the per-watcher channel buffer. Defaults 100.
	WatchBufferSize int
	// UpstreamBudget caps concurrent push subscriptions; over budget a
	// watcher falls back to per-watcher poll. 0 = unlimited.
	UpstreamBudget int

	// IdentityGate overrides the default owner-visibility predicate.
	IdentityGate IdentityGate

	// --- read-path reconcile (0045) — OPT-IN, DEFAULT OFF ---

	// ReadPathReconcile makes the backend the source of truth for
	// existence: Get/List reach the body CR directly (no informer-cache
	// short-circuit), adopting unknown backend objects and collecting
	// orphan records inline. This removes the tolerant-Get sharp edge
	// at the cost of 1:1 read amplification against the backend (0045).
	// DEFAULT OFF: when off, the adapter reads the informer cache and
	// relies on the periodic sweep for GC.
	ReadPathReconcile bool
	// Adopt enables adopting backend objects that have no metadata
	// record (only meaningful with ReadPathReconcile or the sweep).
	Adopt bool
	// GCEnabled enables collecting orphan records (only meaningful with
	// ReadPathReconcile or the sweep).
	GCEnabled bool
	// CollectMinAge is the grace window before an orphan record is
	// collected. Defaults to 30s.
	CollectMinAge time.Duration
}

const (
	defaultLeaseDuration = 15 * time.Second
	defaultTxnAttempts   = 5
	defaultPollInterval  = 5 * time.Second
	defaultWatchBuffer   = 100
	defaultCollectMinAge = 30 * time.Second
)
