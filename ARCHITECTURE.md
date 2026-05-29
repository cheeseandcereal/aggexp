# Architecture

This file describes the current architectural state of the
**substrate** — the disciplined code under `runtime/` and
`drivers/`. It is silent about individual experiments (those live
under `experiments/` and are documented in their own READMEs) and
silent about the broader problem space (that's `SYNTHESIS.md`'s
job).

This file is rewritten (not appended to) when the substrate's
architecture actually shifts.

## Current state

Five substrate promotions have landed.

- **Promotion 1** (`runtime/{server, authz, storage, group}`) gave
  experiments the library-mode path: a Go consumer links against
  the substrate, implements `runtime/storage.Backend`, and runs an
  etcd-less aggregated apiserver. Evidence: 0002, 0007, 0009, 0010,
  0011.
- **Promotion 2** (`runtime/component/{proto, scheme, openapi,
  grpcbackend}` plus the top-level `runtime/component.Run`) gave
  experiments the component-mode path: a generic component-server
  binary handles the Kubernetes wire contract for any resource, and
  the resource-specific code lives in a separate backend process
  (possibly a different language) behind a gRPC service. Evidence:
  0013, 0017, 0018, 0021.
- **Promotion 3** (`runtime/component/v2/`) consolidates the
  stateful-middleware-refinement arc (experiments 0022-0029). v2
  lives alongside v1, not as a replacement; v1 is frozen for its
  existing consumers. Evidence: 0022-0029, 0031.
- **Promotion 4** (`runtime/library/`) consolidates the
  production-library-readiness arc (experiments 0032-0040). Provides
  composable enhancements on top of the v1 `runtime/storage` path:
  deterministic UIDs, pagination, field selectors, optimistic
  concurrency, WatchList BOOKMARK, poll-mode consumer watch, status
  subresource helpers, CRD-backed shared storage, and Lease-based
  locking. Evidence: 0032-0040.
- **Promotion 5** (`runtime/library/multihost/`) consolidates the
  multi-replica library-composition arc (experiments 0042-0049).
  Provides the multi-replica (multi-host) analogue of the
  library-enhanced adapter: host-CR resourceVersion authority over a
  stitched metadata/body CRD split, an embedded per-object lock with
  the validated transactional write path, emission filtering,
  pre-acquire OCC, per-watcher identity-carrying watch, and an
  opt-in inline read-path reconcile. Each capability is independently
  opt-in via an Options struct; the `runtime/storage.Backend`
  interface is unchanged. Evidence: 0042, 0043, 0044, 0045, 0047,
  0048, 0049.

`drivers/` remains empty; no driver has met the two-consumer bar.

### `runtime/` package tree

```
runtime/
├── README.md                 — layout guide
├── server/                   — etcd-less Options + Config + Run
├── authz/                    — external-HTTP-policy Authorizer
├── storage/                  — Backend interface + rest.Storage adapter
│                               (in-process library consumer path, v1)
├── group/                    — API-group installer
├── library/                  — production-grade enhancements (Promotion 4)
│   ├── doc.go                — package overview
│   ├── adapter.go            — enhanced REST adapter (composable)
│   ├── options.go            — Options struct
│   ├── uid.go                — deterministic UID generation
│   ├── pagination.go         — limit+continue token logic
│   ├── fieldsel.go           — field selector support
│   ├── occ.go                — optimistic concurrency (RV-conflict)
│   ├── bookmark.go           — WatchList BOOKMARK emission
│   ├── pollwatch.go          — poll-mode consumer watch
│   ├── subresource.go        — status subresource helpers
│   ├── crdstore.go           — CRD-backed shared storage (multi-replica)
│   ├── locker.go             — Lease-based per-object locking
│   ├── helpers.go            — shared utilities
│   ├── adapter_test.go       — unit tests
│   │
│   └── multihost/            — multi-replica library composition (Promotion 5)
│       ├── doc.go            — overview + when-to-use + correctness model
│       ├── ref.go            — ResourceRef, Record, LockState, CR naming
│       ├── options.go        — Options + Converter/IdentityGate interfaces
│       ├── metastore.go      — stitched metadata-CR store (host-RV authority);
│       │                        raw CAS surface + transactional commit;
│       │                        VisibleSignature (emission filter key)
│       ├── bodystore.go      — shared body-CR store: RV-blind, identity-aware,
│       │                        authoritative direct reads, push fan-out
│       ├── lock.go           — embedded lock + the 0049 WriteTxn (both CAS
│       │                        surfaces retried under the held lock; 409-not-500)
│       ├── watch.go          — per-watcher Hub: identity-carrying pipelines,
│       │                        re-homed emission filter, per-event dedup cache,
│       │                        SharedPoll
│       ├── adapter.go        — composed multi-host REST (read-path reconcile
│       │                        opt-in, default off)
│       ├── metrics.go        — counters (amplification + txn profile)
│       └── multihost_test.go — unit tests (fake dynamic client + etcd sim)
└── component/                — deployable component-server path
    ├── doc.go                — when to use component vs library
    ├── api.go                — v1 Options, NewOptions, AddFlags, Run
    ├── proto/                — v1 gRPC protocol + committed bindings
    ├── scheme/               — v1 dynamic Scheme builder + typed Object
    ├── openapi/              — v1 defs-map helpers + openapi-gen output
    ├── grpcbackend/          — v1 rest.Storage adapter
    │
    └── v2/                   — v2 substrate (0030 promotion)
        ├── doc.go            — overview + migration guidance
        ├── proto/            — v2 gRPC protocol
        ├── scheme/           — v2 typed wrapper + Scheme builder
        ├── openapi/          — Compose returns a LIVE closure;
        │                        Synthesize lifts plain JSON Schema
        ├── grpcbackend/      — v2 rest.Storage adapter
        ├── httpbackend/      — HTTP/JSON + SSE client
        ├── metadatastore/    — CRD-backed KRM metadata store
        ├── gc/               — GC reconciler for orphaned Records
        ├── admission/        — CEL validation + JSONPath mutation
        ├── multiplex/        — dynamic APIDefinition-CRD reconciler
        └── watch/            — transport-neutral watch helpers
```

Approximate LOC for the substrate:

- v1 (`runtime/storage` + server + group + authz): ~1,600 lines Go
  + ~900 test lines.
- v1 generated: ~2,000 lines proto bindings + ~2,700 lines
  openapi-gen output.
- v2 (`runtime/component/v2`): ~4,565 lines Go + ~1,620 test lines.
- v2 generated: ~2,500 lines proto bindings.
- library (`runtime/library`): ~1,100 lines Go + ~450 test lines.
- multihost (`runtime/library/multihost`): ~3,700 lines Go + ~840
  test lines.

### Five consumer shapes

An experiment that wants an aggregated API picks one of:

1. **Library mode (v1)** — link against `runtime/server` +
   `runtime/group` + `runtime/storage`, implement
   `runtime/storage.Backend`. Best for Go-native backends with
   typed codegen. Used by 0002, 0007, 0009, 0010, 0011. Wire
   parity + SSA + explain all work; scheme and types live in the
   experiment.
2. **Library mode (enhanced)** — same as (1) but use
   `runtime/library.New` instead of `runtime/storage.New`. Gains
   pagination, deterministic UIDs, OCC, field selectors, BOOKMARK,
   and optional multi-replica features (CRD store, locking). No
   additional code generation needed. Used by experiments
   0032-0040 individually; now consolidated.
3. **Library mode (multi-host)** — the multi-replica variant of (2).
   Use `runtime/library/multihost.New` with a consumer-supplied
   `Converter`, a metadata CRD (the RV authority) and a shared body
   CRD. Gains host-CR RV authority across replicas, an embedded
   per-object lock with the transactional write path, per-watcher
   identity-scoped watch, and optional inline read-path reconcile.
   Best for a multi-replica StatefulSet behind a load-balanced
   Service that needs cross-replica read/list/watch consistency and
   safe concurrent writes. Validated by experiments 0042-0049.
4. **Component mode, v1** — use `runtime/component.Run` in a tiny
   main, implement the v1 `runtime/component/proto.Backend` gRPC
   service in a separate process. Used by 0013, 0017, 0018, 0021.
   The backend can be any language.
5. **Component mode, v2** — use `runtime/component/v2` packages.
   Same core contract but with: state split (metadata on host CRD,
   business data on backend), unified RV authority,
   initial-events-end BOOKMARK, declarative admission, dual
   transport. Two sub-shapes: single-AA (full parity) and multiplex
   (CRUD works, SSA+explain degrade for dynamically-installed groups).

### `runtime/library` design principles

- **Composable, not monolithic.** Each feature is independently
  opt-in via the Options struct. A consumer that only needs
  pagination + OCC enables those two flags. No all-or-nothing.
- **Same Backend interface.** `runtime/storage.Backend` and
  `runtime/storage.WritableBackend` are unchanged. The library
  package provides an enhanced REST adapter on top.
- **Multi-replica features are opt-in.** CRDStore and
  LockedBackend require `k8s.io/client-go` for dynamic informers
  and Lease objects. A single-replica consumer doesn't need them.
- **PollWatcher is independent.** It takes a PollLister + Publisher
  and runs as a goroutine. It is not coupled to the REST adapter.

### Per-request flow (library mode, enhanced)

```
kubectl
   │  HTTPS (client cert validated by kube-apiserver)
   ▼
kube-apiserver (aggregation layer)
   │  mTLS + X-Remote-{User,Group,Extra-*}
   ▼
extension apiserver (built on runtime/server)
   │
   ├─ DelegatingAuthenticator        (library)
   ├─ runtime/authz.Authorizer       (optional)
   │
   └─ handler chain → endpoint filter → library.REST
          │
          │  (optional) FieldSelector validation + filtering
          │  (optional) Pagination (limit + continue)
          │  (optional) OCC check on Update
          │  (optional) DeterministicUID stamp on Create
          │
          │  Backend.Get / Backend.List / WritableBackend.Create/Update/Delete
          │
          │  (optional) LockedBackend wraps WritableBackend with Lease acquire/release
          │  (optional) CRDStore replaces in-memory Backend with informer cache + dynamic writes
          │
          └─ watch.Broadcaster
                 (optional) initial-events-end BOOKMARK at tail of prefix
                 (optional) PollWatcher: List + diff → PublishAdded/Modified/Deleted
```

### Deployment shape (unchanged)

```
┌─────────────────────────────────────────────────────────────┐
│  kind cluster                                               │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  kube-apiserver                                       │  │
│  │     │ aggregation (mTLS)                              │  │
│  │     ▼                                                 │  │
│  │  extension apiserver pod                              │  │
│  │     runtime/server + runtime/library.REST             │  │
│  │     (optional: CRDStore informer → host CRD)         │  │
│  │     (optional: LockedBackend → Leases)               │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### `runtime/library/multihost` design principles

Multihost is the multi-replica analogue of library-enhanced mode. It
keeps the dependency-light `runtime/library` core intact and lives in
a subpackage because, like `crdstore.go`/`locker.go`, its features
need `k8s.io/client-go` dynamic informers and host CRDs.

- **Composable, opt-in via an Options struct.** The multi-host store,
  the embedded lock, the per-watcher watch, and the read-path
  reconcile are independently selectable. A consumer that wants only
  host-RV authority + cross-replica reads turns off Lock and Watch's
  fancier modes.
- **Same Backend interface; new stores on top.**
  `runtime/storage.Backend`/`WritableBackend` are unchanged. Multihost
  provides a `MetaStore`, a `BodyStore`, and a composed `REST` adapter.
- **Consumer provides a Converter.** As with `crdstore.go`'s
  `CRDStoreConverter`, the consumer supplies the unstructured↔typed
  mapping for the metadata and body CRs (`Converter`: New, NewList,
  BodyFromObject, Stitch, RecordFromObject). The substrate is not
  magically generic over the served type.
- **One RV authority.** The metadata CR's host etcd resourceVersion is
  stamped uniformly on Get/List/Watch. The body CR is RV-blind. Both
  halves must be host-reachable from every replica (a node-local body
  backend breaks cross-replica reads — 0042).
- **The lock is an admission gate; the transactional write path is
  correctness.** The validated 0049 write path (`WriteTxn`) is the
  default: acquire → body Put → metadata commit are retried as a unit
  on a CAS conflict at *either* the body or the commit surface, so a
  cross-replica race surfaces as a clean 409, never a 500. The
  uncontended fast path adds zero writes.
- **Read-path reconcile is opt-in, default off.** It trades the
  tolerant-Get sharp edge for 1:1 read amplification (0045) — a
  workload-dependent choice. Off by default; the periodic-sweep GC
  shape (`ReconcileList` with fromSweep=true) is the fallback.

### Per-request flow (library mode, multi-host)

```
kubectl  ─HTTPS→  kube-apiserver (aggregation, mTLS, X-Remote-*)
                      │
                      ▼
  extension apiserver replica N  (runtime/server + multihost.REST)
      │
      ├─ Get/List:
      │     (reconcile off, default) BodyStore informer cache read,
      │        stitch with MetaStore informer-cached Record (host RV)
      │     (reconcile on, opt-in)  BodyStore.GetAuthoritative direct
      │        read → adopt/collect inline (1:1 amplification)
      │     identity gate (owner vs caller) on every served object
      │
      ├─ Create/Update:  locker.WriteTxn
      │     pre-acquire OCC (client RV vs pre-acquire host RV)
      │     acquire embedded lock (CAS on metadata-CR RV)
      │     BodyStore.Put  (CAS surface #1, RV-blind body CR)
      │     metadata commit + lock release (CAS surface #2, RV authority)
      │     retry the pair on either CAS conflict; 409 on budget exhaust
      │
      └─ Watch:  per-watcher pipeline carrying caller user.Info
            initial owner-filtered RV-stamped replay + BOOKMARK
            live source: per-watcher push/poll, or shared system poll
            shared MetaStore informer = single RV authority + trigger
               → re-homed emission filter (suppress lock/renewal churn)
               → per-event (identity,ns,name) GetFor dedup cache
```

All replicas observe the same metadata-CR etcd RV stream, so
cross-replica resume-by-RV and read/list consistency hold regardless
of which replica the aggregation layer routes to.

### Deployment shape (multi-host)

```
┌──────────────────────────────────────────────────────────────┐
│  kind cluster                                                 │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  kube-apiserver  ── aggregation (mTLS) ──┐               │  │
│  │                                          ▼               │  │
│  │  load-balanced Service → StatefulSet (N replicas)        │  │
│  │     each: runtime/server + multihost.REST                │  │
│  │       MetaStore informer ─┐                              │  │
│  │       BodyStore informer ─┤  (host CRDs, every replica)  │  │
│  │                           ▼                              │  │
│  │   resourcemetadatas.<meta-group>   (RV authority + lock) │  │
│  │   widgetbodies.<body-group>        (RV-blind shared body)│  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

### Deployment shape (single-replica, unchanged)

```
┌─────────────────────────────────────────────────────────────┐
│  kind cluster                                               │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  kube-apiserver                                       │  │
│  │     │ aggregation (mTLS)                              │  │
│  │     ▼                                                 │  │
│  │  extension apiserver pod                              │  │
│  │     runtime/server + runtime/library.REST             │  │
│  │     (optional: CRDStore informer → host CRD)         │  │
│  │     (optional: LockedBackend → Leases)               │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## What is *not* in runtime/

- Per-group Schemes, types, install packages. Those stay with the
  experiment that owns the type.
- Generated OpenAPI. Experiments run `openapi-gen` themselves.
- Concrete drivers (filesystem, github, http). Those live with
  experiments until two demand the same shape.
- A CLI entry point. Each experiment has its own main.
- ConvertToTable fixes from 0036 (the library handles
  PartialObjectMetadata population on rows, but the multi-object-
  per-row edge case is experiment-specific).

## Promotion history

- **2026-04-29** — first promotion. `runtime/{server, authz,
  storage, group}` from 0002+0004.
- **2026-04-29** — second promotion. `runtime/component/{proto,
  scheme, openapi, grpcbackend}` from 0013, 0017, 0018.
- **2026-04-30** — third promotion. `runtime/component/v2/`
  consolidates 0022-0029.
- **2026-05-28** — fourth promotion. `runtime/library/`
  consolidates 0032-0040 production-library-readiness arc.
- **2026-05-29** — fifth promotion. `runtime/library/multihost/`
  consolidates 0042-0049 multi-replica library-composition arc.

## Known library gaps

- **CRDStore requires consumer-provided Converter.** The mapping
  between unstructured CRD objects and typed consumer objects is
  not generic; consumers must implement CRDStoreConverter.
- **LockedBackend extracts namespace from obj metadata.** For
  Delete (which has no object body), the lock key uses empty
  namespace. A production consumer doing namespace-scoped deletes
  should extract namespace from request context.
- **Field selector validation is adapter-layer only.** The
  `AddFieldLabelConversionFunc` registration on the scheme (needed
  for kube-apiserver's built-in field selector parsing) is the
  consumer's obligation. The library validates at the REST layer.
- **PollWatcher uses JSON marshal for diff.** This is correct but
  not optimal at scale. A content-hash or generation-counter
  approach would be more efficient for large objects.
- **No integrated ConvertToTable fix for pagination metadata
  propagation on table responses generated by non-list objects.**
  The library propagates Continue/RemainingItemCount on list-type
  ConvertToTable; edge cases with mixed table sources are
  experiment-specific.

## Known multihost gaps

These are limits of Promotion 5 specifically, separate from the
single-replica library gaps above.

- **Generated bare-`$ref` SSA gap is out of scope here.** The 0048
  capstone surfaced that an OpenAPI-first generator emitting a bare
  `$ref` nested object trips the SSA typed-converter / strict decoder
  (`spec.X.true` phantom). That is a property of the *generator*
  (an 0046 concern), not of multi-host machinery — the multihost
  stores carry whatever served type the consumer supplies and never
  inspect its schema. It remains an open generator-side follow-on and
  is explicitly NOT addressed by this promotion. A consumer whose
  generated schema has this shape will hit it on `apply --server-side`
  regardless of multihost.
- **Read amplification when reconcile is enabled.** With
  `ReadPathReconcile` on, every Get/List is a direct (authoritative)
  backend round-trip — 1:1 amplification by construction (0045). The
  optional negative cache reduces it but reintroduces a bounded
  staleness window against out-of-band backend mutation. Default off;
  enabling it is a workload-dependent trade.
- **Lock-contention write ceiling (0047).** The embedded
  lock-on-RV-authority-CR design carries an inherent ~2× host-write
  amplification per served write (acquire + commit), plus a tunable
  renewal term on slow ops, plus one body-CR write. On single-node
  etcd the first binding constraint is lock-contention fail-fast on
  hot objects (writers ≈ 2× the hot-object count collapse aggregate
  throughput on 409s), not etcd write bandwidth. Per-watcher watch is
  read-only against the RV authority, so watcher scale and write scale
  are independent axes.
- **No bundled periodic sweep goroutine.** `ReconcileList(fromSweep:
  true)` is provided as the GC primitive, but the consumer owns
  scheduling it (interval, debug endpoint). The substrate does not
  spawn the sweep loop.
- **First-watch-event stitch race (mild, inherited from 0048).** The
  per-watcher live source can fire before the metadata informer has
  the record cached, so the very first event for a freshly created
  object can carry a synthetic UID until the next event. Ecosystem
  clients tolerate it (the real UID arrives on the follow-up).

## Anticipated next substrate work (not commitments)

- First post-library-promotion consumer experiment to validate the
  API surface under a real use case.
- `drivers/` opens when a second polling-based external-backend
  driver shows up with a shape identical to 0004's github/.
- Library-level dynamic-install fix (V3 endpoint refresh + SSA
  typed-converter rebuild) — touches apiserver internals.
