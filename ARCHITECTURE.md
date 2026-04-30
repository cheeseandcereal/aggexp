# Architecture

This file describes the current architectural state of the
**substrate** — the disciplined code under `runtime/` and `drivers/`.
It is silent about individual experiments (those live under
`experiments/` and are documented in their own READMEs) and silent
about the broader problem space (that's `SYNTHESIS.md`'s job).

This file is rewritten (not appended to) when the substrate's
architecture actually shifts.

## Current state

Two substrate promotions have landed. `runtime/` holds the original
four packages plus a `runtime/component/` family extracted from the
KRM component-server arc (experiments 0013 + 0017 + 0018). `drivers/`
is still empty; no driver has been promoted because no two
experiments have yet demanded an identically-shaped concrete
backend.

### `runtime/` package tree

```
runtime/
├── README.md                 — layout guide
├── server/                   — etcd-less Options + Config + Run
│   ├── doc.go
│   ├── options.go
│   └── options_test.go
├── authz/                    — external-HTTP-policy Authorizer
│   ├── doc.go
│   ├── authorizer.go
│   └── authorizer_test.go
├── storage/                  — Backend interface + rest.Storage adapter
│   │                           (in-process library consumer path)
│   ├── doc.go
│   ├── backend.go
│   ├── adapter.go
│   ├── helpers.go
│   └── adapter_test.go
├── group/                    — API-group installer
│   ├── doc.go
│   ├── group.go
│   └── group_test.go
└── component/                — deployable component-server path
                                (gRPC-backed rest.Storage; polyglot
                                backends; resource-shape discovered
                                at startup via the Backend service)
    ├── doc.go                — when to use component vs library
    ├── api.go                — Options, NewOptions, AddFlags, Run
    ├── api_test.go
    ├── proto/                — gRPC protocol + committed bindings
    │   ├── doc.go
    │   ├── backend.proto
    │   ├── backend.pb.go
    │   └── backend_grpc.pb.go
    ├── scheme/               — dyn.Object typed wrapper + dynamic
    │   │                       Scheme builder (internal+external GV)
    │   ├── doc.go
    │   ├── object.go
    │   ├── scheme.go
    │   └── scheme_test.go
    ├── openapi/              — backend-OpenAPI-into-defs-map helpers
    │   │                       and committed openapi-gen output for
    │   │                       meta/v1 + runtime + unstructured
    │   ├── doc.go
    │   ├── compose.go
    │   ├── generated.go
    │   └── compose_test.go
    └── grpcbackend/          — rest.Storage adapter proxying to the
        │                       Backend gRPC service
        ├── doc.go
        ├── rest.go
        └── rest_test.go
```

Substrate totals as of the second promotion: approximately 2,200
lines of Go + ~900 lines of tests, plus ~2,700 lines of committed
openapi-gen output and ~2,000 lines of committed proto bindings
(both amortized across every component-mode consumer).

### Two consumer shapes

An experiment that wants an aggregated API picks one of:

1. **Library mode** — link against `runtime/server` +
   `runtime/group` + `runtime/storage`, implement
   `runtime/storage.Backend` for the resource. Used by 0002, 0007,
   0009, 0010, 0011. Best for Go-native backends, typed resource
   models with codegen'd deepcopy, and experiments where the
   backend is a library-level consumer.

2. **Component mode** — use `runtime/component.Run` in a tiny
   `main.go`; implement the `runtime/component/proto.Backend`
   gRPC service in a separate process (possibly a different
   language). Used by 0013, 0017, 0018, 0021. Best for polyglot
   backends, for amortizing the apiserver wiring cost across many
   backends, or for cases where the backend must run in a
   different security/trust domain.

Both modes share `runtime/server` (the Options + generic-apiserver
Config + Run), `runtime/group` (the API-group installer), and
`runtime/authz` (the external-HTTP-policy Authorizer). The choice
is in how `rest.Storage` is implemented, not in the apiserver
plumbing.

### Per-request request flow

```
kubectl
   │  HTTPS (client cert validated by kube-apiserver)
   ▼
kube-apiserver (aggregation layer)
   │  mTLS w/ aggregator client cert
   │  X-Remote-User, X-Remote-Group, X-Remote-Extra-*
   ▼
extension apiserver (built on runtime/server)
   │
   ├─ DelegatingAuthenticator  (library)
   │  │  user.Info in context
   │  ▼
   ├─ runtime/authz.Authorizer (first in union chain)
   │  │  POST JSON to policy service
   │  │  Allow / Deny / NoOpinion
   │  ▼
   └─ handler chain → endpoint filter → rest.Storage
                                           │
                                           ▼
                                    runtime/storage.REST (adapter)
                                           │
                                           ├─ Get / List / Watch  → Backend
                                           │
                                           ├─ Create / Update /   → WritableBackend
                                           │  Delete / Patch       (if backend implements it)
                                           │
                                           ├─ ConvertToTable       → Backend.Table*
                                           │
                                           └─ PublishAdded/
                                              Modified/Deleted    (called by backend
                                                                   polling loops; stamps
                                                                   RV; fans out via
                                                                   watch.Broadcaster)
```

### Backend → adapter contract

```
┌─────────────────────────┐         ┌───────────────────────────────┐
│ experiment-owned        │         │ runtime/storage.REST          │
│ Backend implementation  │ ──────► │   (rest.Storage + all the     │
│  • New / NewList        │ wraps   │    rest.* interfaces the      │
│  • Kind / SingularName  │         │    library demands)           │
│  • NamespaceScoped      │         │                               │
│  • Get / List           │         │   • owns watch.Broadcaster    │
│  • TableColumns         │         │   • owns atomic.Uint64 RV     │
│  • RowsFor              │         │   • stale-RV → 410 Gone       │
│                         │         │   • label selector filter     │
│  optionally:            │         │   • rest.Patcher (if Writable)│
│  • Create / Update /    │ ◄────── │                               │
│    Delete               │ gives   │   Publisher interface for     │
│                         │ back    │   backends to push watch      │
│                         │         │   events.                     │
└─────────────────────────┘         └───────────────────────────────┘
```

### Deployment shape (unchanged)

```
┌─────────────────────────────────────────────────────────────┐
│  kind cluster                                               │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  namespace: default                                   │  │
│  │  ┌───────────────┐    APIService v1.aggexp.io         │  │
│  │  │ kube-apiserver│ ─────────────┐                     │  │
│  │  └───────┬───────┘              │                     │  │
│  │          │ aggregation (mTLS)   ▼                     │  │
│  └──────────┼──────────────────────────────────────────  │  │
│             ▼                                              │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  namespace: aggexp-system                             │  │
│  │    Service: aggexp:443 ──► Pod: aggexp (8443/HTTPS)   │  │
│  │       binary: aggexp-<experiment>:dev                 │  │
│  │       linked against:                                 │  │
│  │         runtime/server  runtime/storage               │  │
│  │         runtime/authz   runtime/group                 │  │
│  │         + per-experiment scheme/types/backend         │  │
│  │                                                       │  │
│  │    Secret: aggexp-serving-cert                        │  │
│  │    ServiceAccount + RBAC                              │  │
│  │                                                       │  │
│  │    optional: policy-service Deployment                │  │
│  │              (speaks runtime/authz's JSON protocol)   │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

The Deployment image and the per-experiment manifests vary;
`deploy/manifests/` defines the namespace, SA, RBAC, Service, and
APIService common to all experiments.

## What is *not* in runtime/

- Per-group Schemes, types, install packages. Those stay with the
  experiment that owns the type; the substrate is generic over
  schemes.
- Generated OpenAPI. Experiments run `openapi-gen` (or hand-copy from
  a neighboring experiment) themselves; the substrate only accepts
  the resulting `GetOpenAPIDefinitions` function.
- Concrete drivers (filesystem, github, http). Those live with
  experiments until two demand the same concrete shape.
- A CLI entry point. Each experiment has its own `cmd/aggexp-<name>/
  main.go`; the substrate exposes `Options.Run(ctx, serverName,
  Input, installers, postStartHooks)`.

## Promotion history

- **2026-04-29** — first promotion. Extracted `runtime/server`,
  `runtime/authz`, `runtime/storage`, `runtime/group` from
  0002+0004's shared shape. See
  `FINDINGS/0007-runtime-fs-driver.md` for how the extracted
  substrate behaved when driven by a new (filesystem) backend.
- **2026-04-29** — second promotion. Extracted
  `runtime/component/` (proto, scheme, openapi, grpcbackend + the
  top-level api.go) from the shared shape between 0013, 0017, and
  0018. See `FINDINGS/0021-runtime-component-parity.md` for how
  the extracted substrate behaved when driven by a fresh consumer.

## Anticipated next substrate work (not commitments)

- `runtime/openapi/common.go` pre-generated meta/v1 + runtime +
  version schemas so experiments carry only their own type
  schemas. Waits until a second or third experiment feels the
  pain of re-running openapi-gen.
- `drivers/` opens when a second polling-based external-backend
  driver shows up with a shape identical to 0004's github/. E.g.
  if `http-driver` and `github-webhook-watch` both land, they may
  share enough that a `drivers/polling/` helper becomes warranted.
- `runtime/admission/` for validating/mutating admission when the
  first experiment needs it (the 0003 finding flagged "name-based
  CREATE policy is an admission concern, not authz").
