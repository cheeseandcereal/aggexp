// Package multihost provides multi-replica (multi-host) enhancements
// for the v1 runtime/storage path. It is the multi-replica analogue of
// the single-replica runtime/library enhanced adapter: where
// runtime/library adds pagination, OCC, field selectors, and BOOKMARK
// on top of a single-process backend, multihost adds the machinery a
// multi-replica aggregated apiserver needs so that kubectl, client-go,
// and controller-runtime treat it as a built-in regardless of which
// replica serves a request.
//
// It consolidates the 0042-0049 multi-replica library-composition arc.
// Each capability below is independently opt-in via Options, matching
// the runtime/library ethos: a consumer enables only what it needs. The
// runtime/storage.Backend/WritableBackend interfaces are unchanged;
// this package provides multi-host stores plus a multi-host REST
// adapter on top.
//
// # When to use this vs. single-replica runtime/library
//
// Use runtime/library when the apiserver runs as a single replica (or
// when replicas do not need to agree on resourceVersion or coordinate
// writes). Use multihost when you run multiple replicas behind a
// load-balanced Service and need: a single resourceVersion authority
// across replicas, cross-replica read/list/watch consistency, safe
// concurrent writes to the same object, and per-user authorization on
// watch streams.
//
// # The host-RV authority model (0042)
//
// In multi-host mode the KRM metadata of each served object lives on a
// cluster-scoped metadata CR on the host kube-apiserver, and the
// business body lives on a SEPARATE shared body CRD. The host etcd
// resourceVersion of the METADATA CR is the single RV authority for the
// stitched object: it is stamped uniformly on Get, List, and Watch,
// never a backend RV and never a per-replica counter. Every replica
// runs an informer on the metadata CR and observes the same monotonic
// etcd RV stream, so cross-replica resume-by-RV holds.
//
// The body CR is RV-BLIND: its own resourceVersion is read and
// discarded. Conflating the two RV streams reintroduces the RV-authority
// split. Crucially, both halves (metadata and body) must be
// host-reachable from every replica — a node-local body backend
// (in-memory, local disk) breaks cross-replica reads (0042). That is
// why BodyGVR is required.
//
// # Shared metadata + body CRD requirement
//
// The consumer must install two CRDs on the host cluster: the metadata
// CRD (MetaGVR) and the body CRD (BodyGVR). The metadata CRD's
// structural schema must permit spec.lock and spec.observed (the
// embedded lock and observed body hash), and must NOT require
// spec.metadata (the embedded-lock acquire creates a CR carrying only
// resourceRef + lock before metadata is known — 0043).
//
// # Lock as admission gate + transactional write path (0049)
//
// When Lock is enabled, each write acquires an embedded CAS lock on the
// metadata CR's spec.lock, CAS'd on the CR RV. The lock is an ADMISSION
// GATE that reduces contention — it is NOT, by itself, sufficient for
// correctness. The validated 0049 insight: the post-acquire body-CR Put
// and the metadata commit are two INDEPENDENT CAS surfaces, and the
// held lock does not make them one (it is re-entrant per replica, and a
// lease can be taken over mid-write). Correctness comes from wrapping
// acquire -> body -> commit in a re-read-and-retry transaction that
// CASes BOTH surfaces and surfaces budget exhaustion as a clean 409
// Conflict, never a 500. This is the DEFAULT write path
// (TransactionalWrite) and adds zero writes on the uncontended fast
// path; it adds writes only proportional to actual contention. The
// retry budget defaults to 5, the 0049-validated value (observed max
// depth 4).
//
// Setting Lock without TransactionalWrite reproduces the buggy 0048
// admission-gate-only behavior (a cross-replica CAS race surfaces as a
// 500) and exists only as a regression baseline.
//
// # Emission filtering (0043) and pre-acquire OCC
//
// Because the lock lives on the served object's RV-authority CR,
// acquire/release/renewal writes advance the CR's RV and fire the
// metadata informer ("lock churn"). The per-watcher Hub applies an
// emission filter keyed on a VisibleSignature (observed body hash +
// served KRM metadata, excluding RV and spec.lock): a MODIFIED whose
// signature is unchanged from the last emission is suppressed, so lock
// churn never reaches a watcher. The emission filter is REQUIRED, not
// polish — without it every write and every renewal surfaces. The OCC
// precondition is checked against the PRE-acquire RV (captured before
// the lock write bumps it), so lock churn never produces a false 409.
//
// # Per-watcher identity-carrying watch (0044)
//
// Each client Watch subscription gets its own pipeline carrying the
// caller's user.Info, so a backend can scope a watch stream per user.
// The shared metadata informer remains the single RV authority and
// cross-replica trigger; on each metadata event the Hub does ONE
// BodyStore.GetFor per distinct (identity, ns, name) via a per-event
// dedup cache (so N watchers sharing an identity cost one Get, not N).
// SharedPoll is an opt-in single system-identity loop fanned out to all
// watchers — cheaper, but it does NOT enforce per-user authz on the
// live watch path (the initial replay and unary List still owner-filter).
//
// # Read-path reconcile (0045) — OPT-IN, DEFAULT OFF
//
// With ReadPathReconcile, the backend is the source of truth for
// existence: Get/List reach the body CR directly (no informer-cache
// short-circuit), adopting unknown backend objects and collecting
// orphan records inline. This removes the tolerant-Get sharp edge (a
// 404 is a 404; there is nothing to hand-edit) at the cost of 1:1 read
// amplification against the backend — a workload-dependent choice. It
// is OFF by default; when off, the adapter reads the informer cache and
// relies on the periodic sweep (ReconcileList with fromSweep=true) for
// GC.
//
// # The Converter obligation
//
// The substrate does not try to be magically generic over the served
// type. The consumer supplies a Converter (the multi-host analogue of
// runtime/library's CRDStoreConverter) that bridges the generic
// Record/Body shapes and the consumer's typed served object: New,
// NewList, BodyFromObject, Stitch, and RecordFromObject. The served
// type, its scheme, and its CRD schemas remain the consumer's concern.
//
// # Usage
//
//	store := multihost.New(multihost.Options{
//	    Dynamic:            dynamicClient,
//	    Converter:          myConverter,
//	    GroupResource:      schema.GroupResource{Group: "example.io", Resource: "widgets"},
//	    MetaGVR:            metaGVR,
//	    BodyGVR:            bodyGVR,
//	    BodyKind:           "WidgetBody",
//	    Lock:               true,
//	    TransactionalWrite: true, // the validated 0049 write path
//	    Watch:              true,
//	    // ReadPathReconcile left false (default): cache reads + sweep GC.
//	})
//	if err := store.Start(ctx); err != nil { ... }
//
// # Known scaling characteristics (0047)
//
// The embedded-lock-on-RV-authority-CR design carries an inherent ~2x
// host-write amplification per served write (acquire + commit), plus a
// tunable renewal term on slow ops, plus one body-CR write. On
// single-node etcd the first binding constraint is lock-contention
// fail-fast on hot objects, not etcd write bandwidth. Per-watcher watch
// is read-only against the RV authority and does not contribute to the
// write rate: watcher scale and write scale are independent axes.
package multihost
