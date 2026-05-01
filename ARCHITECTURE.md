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

Three substrate promotions have landed.

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
  existing consumers. Evidence: 0022 (thesis), 0023 (schema source),
  0024 (metadata CRD), 0025 (push watch), 0026 (HTTP transport),
  0027 (multiplex), 0028 (GC), 0029 (declarative admission).

`drivers/` remains empty; no driver has met the two-consumer bar.

### `runtime/` package tree

```
runtime/
├── README.md                 — layout guide
├── server/                   — etcd-less Options + Config + Run
├── authz/                    — external-HTTP-policy Authorizer
├── storage/                  — Backend interface + rest.Storage adapter
│                               (in-process library consumer path)
├── group/                    — API-group installer
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
        ├── proto/            — v2 gRPC protocol (adds Validate/Mutate,
        │                        watch_capability, schema_is_openapi,
        │                        AdmissionCause[])
        ├── scheme/           — v2 typed wrapper + Scheme builder
        │                        (new canonical names so v1 and v2 can
        │                         coexist in one binary)
        ├── openapi/          — Compose returns a LIVE closure;
        │                        Synthesize lifts plain JSON Schema
        │                        (Track B); refs use "#/definitions/"
        │                        (0024 fix); re-exports v1's
        │                        generated meta/v1 defs.
        ├── grpcbackend/      — v2 rest.Storage adapter. Optional
        │                        MetadataStore + admission.Engine;
        │                        unified RV authority; initial-events-end
        │                        BOOKMARK; push/poll watch mode.
        ├── httpbackend/      — HTTP/JSON + SSE client implementing the
        │                        same Backend interface. Transport is
        │                        a flag, not a rebuild (0026).
        ├── metadatastore/    — CRD-backed KRM metadata store. Ships
        │                        the ResourceMetadata CRD YAML embedded.
        ├── gc/               — GC reconciler for orphaned Records.
        │                        HTTP /gc/run + /gc/last debug endpoints.
        ├── admission/        — CEL validation + JSONPath mutation
        │                        engine. Composes additively with
        │                        backend-RPC admission.
        ├── multiplex/        — dynamic APIDefinition-CRD reconciler.
        │                        Typed APIDefinition runtime.Object;
        │                        APIDefinition CRD YAML embedded.
        └── watch/            — transport-neutral watch helpers:
                                 BookmarkObject builder; RV Authority.
```

Approximate LOC for the substrate:

- v1 hand-written: ~1,600 lines Go + ~900 test lines.
- v1 generated:   ~2,000 lines proto bindings + ~2,700 lines
  openapi-gen output (meta/v1 etc.).
- v2 hand-written: ~4,565 lines Go + ~1,620 test lines.
- v2 generated:   ~2,500 lines proto bindings.

v2 is larger than v1's hand-written budget because v1 only covers
proto + scheme + openapi + rest. v2 adds: metadatastore (~380),
gc (~260), admission (~320), httpbackend (~470), multiplex (~800),
watch (~120). The v2 rest adapter itself (grpcbackend) is ~965 —
larger than v1's 690 because it integrates metastore, admission,
unified RV, and initial-events-end BOOKMARK into one seam.

### Three consumer shapes

An experiment that wants an aggregated API picks one of:

1. **Library mode** — link against `runtime/server` +
   `runtime/group` + `runtime/storage`, implement
   `runtime/storage.Backend`. Best for Go-native backends with
   typed codegen. Used by 0002, 0007, 0009, 0010, 0011. Wire
   parity + SSA + explain all work; scheme and types live in the
   experiment.
2. **Component mode, v1** — use `runtime/component.Run` in a tiny
   main, implement the v1 `runtime/component/proto.Backend` gRPC
   service in a separate process. Used by 0013, 0017, 0018, 0021.
   The backend can be any language; the component server is
   resource-agnostic. No state in the middleware (backend owns
   everything).
3. **Component mode, v2** — use `runtime/component/v2` packages.
   Same core contract (JSON-bytes proto, resource-agnostic
   middleware) but with the arc's five commitments folded in:
   state split (metadata on host CRD, business data on backend),
   unified RV authority, initial-events-end BOOKMARK, declarative
   admission, dual transport. Two sub-shapes:
   - **Single-AA.** Full wire parity — explain, SSA, unified RV,
     BOOKMARK all work. Closest to v1's shape.
   - **Multiplex.** One middleware process hosts many AAs declared
     as `APIDefinition` CRDs. CRUD + list + watch + table render
     work. `kubectl explain` and SSA degrade on
     dynamically-installed groups (known gap; see
     `runtime/component/v2/multiplex` package doc).

### Per-request request flow (v2)

```
kubectl
   │  HTTPS (client cert validated by kube-apiserver)
   ▼
kube-apiserver (aggregation layer)
   │  mTLS + X-Remote-{User,Group,Extra-*}
   ▼
extension apiserver (built on runtime/server)
   │
   ├─ DelegatingAuthenticator         (library)
   ├─ runtime/authz.Authorizer        (optional, union.New first)
   │
   └─ handler chain → endpoint filter → v2 rest.Storage
          │
          │  (declarative) admission.Engine         (optional)
          │     Mutate → Validate
          │     → 422 with field.ErrorList on deny
          │
          │  (backend-RPC) Mutate / Validate        (opt-in)
          │     → same 422 wire shape on deny
          │
          │  transport: gRPC (grpcbackend.Dial) OR HTTP (httpbackend.New)
          │     Get / List / Create / Update / Apply / Delete / Watch
          │
          │  (optional) metadatastore.Store.{Get,Put,Delete,List,Watch}
          │     stitches KRM metadata onto backend spec/status
          │
          │  (optional) gc.Reconciler.Start
          │     periodic sweep deletes orphaned Records
          │
          └─ utilwatch.Broadcaster
                 emits initial-events-end BOOKMARK unconditionally
                 at tail of Watch prefix; push or poll upstream
```

### State split (v2)

```
┌───────────────────────────────────────────────────────────────┐
│ host-cluster etcd                                             │
│   ┌──────────────────────────────────────────────────────┐    │
│   │ aggexpmeta.aggexp.io/v1 ResourceMetadata (cluster)   │    │
│   │   one CR per (group, resource, ns, name) combo      │    │
│   │   carries uid, labels, annotations, finalizers,     │    │
│   │   ownerReferences, managedFields, deletionTimestamp │    │
│   └──────────────────────────────────────────────────────┘    │
└───────────────────────────────────────────────────────────────┘
                           ▲
         metadatastore.Store │ (dynamic client)
                           │
┌───────────────────────────┴───────────────────────────────────┐
│ middleware pod (runtime/component/v2)                         │
│   REST stitches on every Get/List/Watch.                      │
│   gc.Reconciler reconciles the two stores periodically.       │
└───────────────────────────┬───────────────────────────────────┘
                           │ gRPC / HTTP+SSE
                           ▼
┌───────────────────────────────────────────────────────────────┐
│ backend process (any language)                                │
│   serves spec + status, namespace + name only.                │
│   knows zero Kubernetes-specific concepts.                    │
└───────────────────────────────────────────────────────────────┘
```

### Deployment shape

```
┌─────────────────────────────────────────────────────────────┐
│  kind cluster                                               │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  namespace: default                                   │  │
│  │  ┌───────────────┐    APIService v1.<group>           │  │
│  │  │ kube-apiserver│ ─────────────┐                     │  │
│  │  └───────┬───────┘              │                     │  │
│  │          │ aggregation (mTLS)   ▼                     │  │
│  │  ┌───────────────────────────────────────────────────┐ │
│  │  │  CRDs:                                            │ │
│  │  │    - resourcemetadatas.aggexpmeta.aggexp.io (v2)  │ │
│  │  │    - apidefinitions.aggexp.io         (v2 mx)     │ │
│  │  └───────────────────────────────────────────────────┘ │
│  └──────────┼──────────────────────────────────────────────┘
│             ▼                                              │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  namespace: aggexp-system                             │  │
│  │    Service: aggexp:443 ──► Pod: aggexp (8443/HTTPS)   │  │
│  │       runtime/component/v2 binary                     │  │
│  │    Secret: aggexp-serving-cert                        │  │
│  │    ServiceAccount + RBAC                              │  │
│  │                                                       │  │
│  │    one or more backend Deployments (gRPC or HTTP)     │  │
│  │                                                       │  │
│  │    optional: policy-service Deployment                │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## What is *not* in runtime/

- Per-group Schemes, types, install packages. Those stay with the
  experiment that owns the type; the substrate is generic over
  schemes.
- Generated OpenAPI (v1 path). Experiments run `openapi-gen` (or
  hand-copy from a neighboring experiment) themselves; the
  substrate only accepts the resulting `GetOpenAPIDefinitions`
  function. v2's Track B synthesis path replaces this for new
  consumers: ship plain JSON Schema, the middleware lifts.
- Concrete drivers (filesystem, github, http). Those live with
  experiments until two demand the same concrete shape.
- A CLI entry point. Each experiment has its own
  `cmd/aggexp-<name>/main.go`.
- Push-vs-poll runtime capability probing in v2; we take the
  backend's declared `watchCapability` as truth for now. A probe
  could land in a future v2 sub-version if the declared-vs-actual
  gap matters.

## Promotion history

- **2026-04-29** — first promotion. Extracted `runtime/{server,
  authz, storage, group}` from 0002+0004's shared shape. See
  `FINDINGS/0007-runtime-fs-driver.md` for the first post-promotion
  consumer.
- **2026-04-29** — second promotion. Extracted
  `runtime/component/{proto, scheme, openapi, grpcbackend}` from
  0013, 0017, 0018. See
  `FINDINGS/0021-runtime-component-parity.md` for the first
  post-promotion consumer.
- **2026-04-30** — third promotion. `runtime/component/v2/`
  consolidates the stateful-middleware-refinement arc (0022-0029).
  See `FINDINGS/0030-runtime-component-v2-promotion.md`. A follow-on
  consumer experiment (0031) is queued.

## Known v2 gaps

These are known and intentional. Upstream fixes either require
library-level changes beyond this arc's scope or have no evidence
demanding attention yet:

- **Dynamic-install SSA + explain.** Groups installed after
  PrepareRun (multiplex mode) do not participate in the library's
  `/openapi/v3` per-group endpoint map or in
  `managedfields.NewTypeConverter`. CRUD + list + watch + table
  work; `kubectl explain` and `kubectl apply --server-side` degrade.
  Single-AA v2 consumers have full parity.
- **MetadataStore schema evolution.** Single-version CRD. Migration
  across CRD versions would need a conversion webhook or an
  offline snapshot+restore. Not provided.
- **Backend-to-middleware identity / mTLS.** The middleware trusts
  the backend; the backend trusts the middleware. Multi-tenant
  deployments would want SPIFFE or mTLS on this leg. Not provided.
- **Encryption at rest for ResourceMetadata.** Records land in host
  etcd. Operators persisting secrets-adjacent annotations need
  `EncryptionConfiguration` enabled for
  `resourcemetadatas.aggexpmeta.aggexp.io` on the host cluster.
- **Push-capability runtime probe.** v2 honors the declared
  `watchCapability` in APIDefinition. A probe (try Watch once,
  fall back to poll on codes.Unimplemented) is a future addition.

## Anticipated next substrate work (not commitments)

- First post-v2-promotion consumer (`0031-runtime-component-v2-parity`
  or similar) to shake out the gaps a parity consumer exposes.
- Library-level dynamic-install fix (V3 endpoint refresh + SSA
  typed-converter rebuild on `InstallAPIGroup`). This touches
  apiserver internals and may require an upstream patch rather than
  substrate-side work.
- `drivers/` opens when a second polling-based external-backend
  driver shows up with a shape identical to 0004's github/.
