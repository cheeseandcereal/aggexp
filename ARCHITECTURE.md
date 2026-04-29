# Architecture

This file describes the current architectural state of the
**substrate** — the disciplined code under `runtime/` and `drivers/`.
It is silent about individual experiments (those live under
`experiments/` and are documented in their own READMEs) and silent
about the broader problem space (that's `SYNTHESIS.md`'s job).

This file is rewritten (not appended to) when the substrate's
architecture actually shifts.

## Current state

**There is no substrate yet.**

`runtime/` and `drivers/` exist as empty directories. No code has been
promoted from experiments. This is expected and correct per the ethos:
promotion happens only when two or more experiments have demanded the
same abstraction, and at this point only one experiment exists
(`0001-raw-http-aggregation`, a deliberately minimal probe).

## Deployment shape (current)

While the substrate is empty, the lab's deployment shape is:

```
┌─────────────────────────────────────────────────────────────┐
│  kind cluster (name: aggexp)                                │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  namespace: default (kube-apiserver, etc.)            │  │
│  │                                                       │  │
│  │  ┌───────────────┐    APIService                      │  │
│  │  │ kube-apiserver│ ─────────────┐   v1.aggexp.io      │  │
│  │  └───────┬───────┘              │                     │  │
│  │          │ aggregation layer    │                     │  │
│  │          │ (mTLS w/ aggregator  │                     │  │
│  │          │  client cert;        ▼                     │  │
│  │          │  adds X-Remote-*)                          │  │
│  └──────────┼──────────────────────────────────────────  │  │
│             │                                           │   │
│             ▼                                           │   │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  namespace: aggexp-system                             │  │
│  │                                                       │  │
│  │    Service: aggexp:443  ────►  Pod: aggexp            │  │
│  │                                   (port 8443/HTTPS)   │  │
│  │                                   serving the         │  │
│  │                                   current experiment  │  │
│  │                                                       │  │
│  │    Secret: aggexp-serving-cert                        │  │
│  │    ServiceAccount: aggexp                             │  │
│  │    ClusterRoleBinding: aggexp:system:auth-delegator   │  │
│  │    RoleBinding (kube-system):                         │  │
│  │      aggexp-auth-reader                               │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

The Deployment's image is what varies per experiment. The shared
manifests in `deploy/manifests/` define the namespace, SA, RBAC,
Service, and APIService; each experiment provides its own Deployment
overlay in `experiments/NNNN-*/manifests/deployment-override.yaml`.

## Anticipated substrate shape (not yet built)

Documented here as a *hypothesis*, not a commitment. The Driver
interface sketched in the plan may prove wrong under experimental
pressure; that's the point.

- `runtime/server/` — genericapiserver wiring, TLS, options, health.
- `runtime/auth/` — delegating authenticator helper; Authorizer
  interface for per-request identity-based authz.
- `runtime/storage/` — rest.Storage adapter over a driver.Driver,
  watch broadcaster, synthetic resourceVersion scheme.
- `runtime/driver/` — the Driver interface: what it means to be
  "anything as a Kubernetes resource."
- `drivers/fs/`, `drivers/github/`, `drivers/http/` — concrete
  adapters.

These will exist only when experimentation demands them.
