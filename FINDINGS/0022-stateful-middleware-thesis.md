# Findings — 0022 stateful-middleware-thesis

## What we were trying to learn

This experiment kicks off the stateful-middleware-refinement arc.
Its deliverable is the **design commitments** that the remaining
nine experiments of the arc must honor. The design hash-out
happened in planning; this experiment writes down the commitments
as (a) prose here and (b) Go interfaces in `thesis/thesis.go` that
0024-0029 and the 0030 substrate implement.

Not an experiment in the usual sense — no cluster deployed, no
kubectl scenarios. The commitments themselves are the finding.

## What was decided

### State is required

SYNTHESIS has drawn this line across multiple experiments; 0022
now makes it an arc-level axiom. Specifically: ownership tracking
(managedFields), lifecycle hooks (finalizers, ownerReferences),
identity (stable uid), concurrency control (monotonic
resourceVersion), and user-facing metadata (labels, annotations)
are all library-layer features that depend on persisted state. A
stateless AA cannot simulate them well. `0009` proved the losses;
`0010` proved a CRD store recovers them.

### Metadata state ≠ business data

0010 used a CRD as a facade: exposed `Widget` backed 1:1 by
`WidgetStorage` on the host. Both contained spec+status+metadata.
0015 discovered the cost: ArgoCD's cluster cache saw both
resources and double-tracked.

0022 commits a cleaner split. The **backend** serves business data
(spec + status) without Kubernetes knowledge. The **middleware**
serves the full Kubernetes API and persists KRM metadata into a
shared CRD (`aggexpmetadata.aggexp.io/v1 ResourceMetadata`) that
is invisible to discovery, invisible to exposed-resource
controllers. Overlay on Get.

Why shared CRD, one kind across all AAs: the metadata payload is
identical regardless of the AA. No schema variability. A single
`ResourceMetadata` CRD with a `resourceRef` field discriminating
by (group, resource, namespace, name) is sufficient. No reason to
proliferate.

### No generic backend state API

If a backend wants state, it runs its own. The middleware
doesn't offer a key/value surface. Decided in planning; recorded
here. Rationale: coupling backend lifecycle to middleware state
creates scope sprawl; backends that need state can use any K8s
storage (CRD, ConfigMap, external DB) directly.

### OpenAPI source is genuinely open

0023 probes three paths:

- (a) backend ships OpenAPI (0017 status quo)
- (b) middleware synthesizes OpenAPI from a simpler JSON Schema
  the backend ships
- (c) full OpenAPI lives in `APIDefinition` CRD on the host cluster,
  middleware reads without talking to the backend

Tooling ergonomics per language (Go, Python, Rust, Node) is part
of the probe. The winning track becomes the default; the others
stay as pluggable `SchemaSource` values in `APIDefinition`. The
`thesis` package enumerates all three in its interface.

### Full dynamic multiplex reconciler

0027 builds a single middleware process that watches
`APIDefinition` CRDs and dynamically registers / deregisters
APIServices. Full reconciler with graceful shutdown. Per-config
status written back into the CRD. No metrics for this arc.
mTLS backend-to-middleware deferred.

### Admission declared in config, additive to RPC admission

0029 adds `APIDefinition.spec.admission` with CEL validations +
JSONPath mutations. Middleware evaluates these locally — no
backend round-trip. Backends may still implement Validate/Mutate
RPCs (0020 pattern) for cases CEL can't express. The two
compose.

### Transport is swappable

0026 adds an HTTP/JSON + SSE backend transport alongside gRPC.
The protocol shape stays the same; transport is a
`BackendRef.Transport` field. Polyglot backends pick whichever
has less tooling cost in their ecosystem.

### Four watch capability levels

0025 formalizes. Backends declare their capability:

- **poll** — middleware polls List periodically; backend
  implements no Watch RPC.
- **push** — backend streams events; middleware forwards.
- **both** — backend prefers push, poll as fallback.

Backend declares this at schema registration. Middleware honors.

### `runtime/component/v2/` is a new package

0030 promotes. `v1` stays for frozen experiments 0013-0021. `v2`
embodies these commitments.

## The `thesis` Go package

`experiments/0022-stateful-middleware-thesis/thesis/thesis.go`
is ~300 lines of pure interface definitions plus enum types. It
compiles (no implementation) and commits concrete signatures for:

- `APIDefinition` (the config CRD spec)
- `SchemaSource` enum (three values per above)
- `WatchCapability` enum (three values per above)
- `Backend` interface (what a backend author in any language
  implements)
- `MetadataStore` interface (middleware's persistence)
- `Multiplex` interface (0027's surface)
- `Record` + `ResourceRef` (the shared metadata CRD shape,
  expressed as Go)

These are commitments, not implementations. 0024 implements
`MetadataStore` with a CRD backend. 0027 implements `Multiplex`.
0030 consolidates both under `runtime/component/v2`.

## Fundamentals touched

No experiment here, so nothing is observed. The design carries
across every fundamental:

- **Wire protocol fidelity** — by making wire protocol the
  middleware's job entirely.
- **Storage independence** — by separating metadata state from
  business data and backend state.
- **Per-request authorization** — unchanged; authz remains the
  middleware's concern per prior synthesis.
- **Resource modeling freedom** — the `Backend` interface
  reduces the Kubernetes surface a backend must learn.
- **Watch and consistency semantics** — the WatchCapability
  enum makes the polling-vs-push axis a first-class concern.

The sixth fundamental (identity handoff) is unchanged in this
arc's design: the middleware continues to forward `UserInfo` to
the backend on every request via the protocol.

## Consequents

- **Schema evolution for the shared metadata CRD** is an open
  problem. When the `ResourceMetadata` spec grows a field, old
  records must migrate. 0030 will record the migration story as
  a substrate-level consequent.
- **Backend-to-middleware identity** is not scoped to this arc.
  The backend trusts the middleware; the middleware trusts the
  aggregation layer. In multi-tenant deployments this would
  need mTLS or SPIFFE. Recorded as a 0027 scope cut.
- **Encryption-at-rest of metadata** is a cluster concern. The
  metadata CRD lands in host etcd. Operators running
  secrets-adjacent metadata (e.g. an annotation carrying a
  bearer token) need cluster-level encryption. 0024 findings
  will surface this for the specific payloads it persists.

## What this changes for SYNTHESIS and EXPERIMENTS

SYNTHESIS does not need a rewrite from 0022 alone — no new
findings. When 0023 merges with its empirical schema-source
recommendation, that's when SYNTHESIS shifts.

EXPERIMENTS.md gains entries for the 10 experiments of this
arc, all marked as "in-progress" or candidate status.

## Open questions raised

None beyond what's already queued as 0023-0031.

## Status

Complete. Interfaces committed. Arc proceeds to 0023.
