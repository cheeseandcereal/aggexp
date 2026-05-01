# Experiment 0022: stateful-middleware-thesis

Kickoff for the stateful-middleware-refinement arc. Captures the
design decisions reached for the arc and writes a first-pass Go
interface sketch of what the substrate's `runtime/component/v2/`
is going to expose.

Not "just a design doc" — the Go package `thesis/` compiles and
commits the arc's interface commitments so subsequent experiments
can read concrete signatures rather than prose. The package does
not run as a server; it is scaffolding.

## Hypothesis

The current `runtime/component/` conflates three axes: wire
protocol, KRM metadata state, and business data. Separating them
unlocks:

- **Stateful middleware**: KRM metadata (managedFields, finalizers,
  ownerReferences, labels, annotations, uid, resourceVersion) lives
  in a shared CRD on the host cluster (`aggexpmetadata.aggexp.io/v1
  ResourceMetadata`), not in the backend and not mirrored to the
  exposed resource's own CRD. The metadata CRD is an implementation
  detail invisible to ecosystem controllers.
- **Multiplex-capable server**: one middleware process can serve
  many AAs, each defined by an `aggexpapidefinition.aggexp.io/v1
  APIDefinition` CRD on the host cluster. Reconciler reacts to
  config changes.
- **Backends don't know Kubernetes**: business-data CRUD + watch is
  all a backend needs to implement. OpenAPI schema source, SSA
  field management, finalizer semantics, resourceVersion
  monotonicity, bookmarks, admission — all live in the middleware.

## Design decisions (arc-level commitments)

These are locked for the arc as of 0022. Subsequent experiments
may challenge them in FINDINGS; doing so requires an explicit
reference back to this thesis.

1. **State is required for Kubernetes-like behavior.** SSA
   managedFields, finalizers, ownerReferences, labels, annotations,
   stable UIDs, monotonic resourceVersions — all library-layer
   concerns that need persistence. `0009` proved a stateless AA
   loses them; `0010` proved a CRD store recovers them.

2. **Metadata state lives in a single shared CRD kind.** Shape:
   `aggexpmetadata.aggexp.io/v1 ResourceMetadata`, cluster-scoped,
   spec fields `resourceRef {group, resource, namespace, name}` +
   explicit nested `metadata {managedFields, finalizers,
   ownerReferences, labels, annotations, uid, resourceVersion}`.
   One CRD kind serves every AA. Because all AAs store identical
   KRM metadata shape, the schema can be shared.

3. **No generic state store exposed to backends.** If a backend
   wants state, it runs its own. The middleware only stores KRM
   metadata for its managed resources.

4. **Metadata CRD is invisible to exposed-resource clients.** The
   `0015` double-tracking finding motivates this. The exposed AA
   has its APIService visible via discovery; the metadata CRD lives
   under a different APIGroup (`aggexpmetadata.aggexp.io`) and is
   not advertised as a user-facing resource. Ecosystem controllers
   scanning the discovery for resources-they-manage should ignore it.

5. **OpenAPI source is open.** `0023-schema-source-exploration`
   probes three candidates: (a) backend ships OpenAPI (status quo
   from 0017), (b) backend ships plain JSON Schema and middleware
   synthesizes full Kubernetes OpenAPI, (c) `APIDefinition` CRD
   carries the OpenAPI and middleware reads it from host. The
   winning track propagates into the rest of the arc. Tooling
   ergonomics across Go/Python/Rust/Node is part of the 0023 probe.

6. **Full dynamic reconciler in the multiplex server.** Watches
   `APIDefinition` CRDs; add / update / remove APIServices at
   runtime; per-config status written back. Status (Ready /
   Provisioning / Failed + Conditions) — no metrics in this arc.
   mTLS backend-to-middleware deferred (consequent).

7. **Declarative admission in config.** `APIDefinition.spec.admission`
   carries CEL rules that the middleware evaluates without a
   backend round-trip. Backends can still implement the Validate /
   Mutate RPCs from `0020`; config-driven admission is additive.

8. **Four transport capabilities for backends.**
   - gRPC (from `0017/0018/0019/0021`).
   - HTTP/JSON + SSE for watch (new in `0026`).
   - Push-backed watch (new in `0025`): backend streams events
     instead of middleware polling.
   - Polling fallback (status quo): middleware polls List.
   Backend advertises capability in schema; middleware picks
   cheapest available.

9. **`runtime/component/v2/` is a new package.** The v1 package
   stays for frozen experiments 0013-0021. The v2 package embodies
   these decisions. Migration is additive.

## Deliverable

`thesis/` — Go package with stub types + interfaces that the
remaining experiments and the 0030 substrate will implement. No
runtime behavior; compiles.

## Status

complete

<!-- See FINDINGS/0022-stateful-middleware-thesis.md for the full
design writeup. -->

## Decisions made

- **Don't build a server, just interfaces.** This experiment's
  value is the commitment — the types below are what the rest of
  the arc has to honor.
- **Package name `thesis`** rather than `v2` or `runtime` to
  clarify that it's scaffolding, not substrate. Substrate lands
  in 0030 under `runtime/component/v2/`.
- **No tests.** Experiments don't require them; the interfaces
  are exercised by subsequent experiments' consumption.
- **Put interfaces in a single file** (`thesis/thesis.go`) so the
  whole arc commitment reads top-to-bottom without jumping.

## Prerequisites

None. Self-contained. Commits against the root module.
