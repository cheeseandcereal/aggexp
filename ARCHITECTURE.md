# Architecture

This file describes the current architectural state of the
**substrate** — the disciplined code under `runtime/` and `drivers/`.
It is silent about individual experiments (those live under
`experiments/` and are documented in their own READMEs) and silent
about the broader problem space (that's `SYNTHESIS.md`'s job).

This file is rewritten (not appended to) when the substrate's
architecture actually shifts.

## Current state

The first substrate promotion has landed. `runtime/` holds four
packages, extracted from the shared shape between 0002-hello-
aggregated (in-memory) and 0004-github-driver-static-pat (polling
external). `drivers/` is still empty; no driver has been promoted
because no two experiments have yet demanded an identically-shaped
concrete backend.

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
│   ├── doc.go
│   ├── backend.go
│   ├── adapter.go
│   ├── helpers.go
│   └── adapter_test.go
└── group/                    — API-group installer
    ├── doc.go
    ├── group.go
    └── group_test.go
```

Total substrate: ~1,030 lines of code + ~600 lines of tests.

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
