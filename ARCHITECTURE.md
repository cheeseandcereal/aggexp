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

Four substrate promotions have landed.

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
│   └── adapter_test.go       — unit tests
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

### Four consumer shapes

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
3. **Component mode, v1** — use `runtime/component.Run` in a tiny
   main, implement the v1 `runtime/component/proto.Backend` gRPC
   service in a separate process. Used by 0013, 0017, 0018, 0021.
   The backend can be any language.
4. **Component mode, v2** — use `runtime/component/v2` packages.
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

## Anticipated next substrate work (not commitments)

- First post-library-promotion consumer experiment to validate the
  API surface under a real use case.
- `drivers/` opens when a second polling-based external-backend
  driver shows up with a shape identical to 0004's github/.
- Library-level dynamic-install fix (V3 endpoint refresh + SSA
  typed-converter rebuild) — touches apiserver internals.
