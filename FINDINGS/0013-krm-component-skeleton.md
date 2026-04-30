# Findings — 0013 krm-component-skeleton

## What we were trying to learn

This is the first experiment in a new arc. The previous nine made the
`runtime/` substrate a Go library that experiments linked against.
This experiment inverts that: the substrate becomes a **deployable
generic component server**, and an experiment's resource-specific
logic lives in a **thin backend service** behind a simple RPC. If the
shape works, a backend in any language can expose a Kubernetes API
without ever importing `k8s.io/apiserver`.

Three hypotheses going in:

1. The component server can register a resource type at startup by
   asking its backend over gRPC for the schema (GVR, Kind,
   OpenAPI, table columns, writable-or-not).
2. Core kubectl surface (`get`, `apply`, `delete`, `watch`,
   `explain`) works end-to-end when all CRUD is delegated over gRPC
   to an unstructured-JSON backend.
3. Where the unstructured-types path breaks — SSA specifically is
   the obvious risk — is itself a valuable finding. Experiment 0017
   will refine.

## What we did

Built three pieces under `experiments/0013-krm-component-skeleton/`:

- `proto/backend.proto` — the gRPC contract. A `Backend` service
  with `GetSchema`, `Get`, `List`, `Create`, `Update`, `Delete`, and
  a server-streaming `Watch`. Objects are passed as JSON bytes
  inside `bytes` fields, so the proto does not have to change per
  user resource type. Caller identity (name, groups, uid, extras)
  is a sibling message on every request.
- `gen/` — generated Go bindings. Committed so consumers of the
  repo don't need `protoc`.
- `component/` — the generic component server. Uses the substrate's
  `runtime/server` and `runtime/group` for the standard
  apiserver/auth plumbing; implements its own `rest.Storage`
  adapter (`pkg/grpcbackend`) that proxies to the gRPC backend; and
  a dynamic `pkg/scheme` that builds a `runtime.Scheme` around
  whatever GVR+Kind the backend ships at startup.
- `backend-note/` — a reference Go backend that serves a `notes.aggexp.io/v1`
  resource. In-memory. No `k8s.io/apiserver` import (verified by
  reading its go.mod and source). `Note` is a plain Go struct with
  JSON tags.

Deployed both to kind cluster `aggexp-krm`. Component server talks
to the backend at `note-backend.aggexp-system.svc:9090` in
plaintext gRPC. Ran kubectl CRUD + watch + explain + SSA.

## What we observed

### The dynamic registration path works

The component server starts, dials the backend, calls `GetSchema`,
receives GVR + Kind + columns + `namespaced=true` + `writable=true`.
It then builds a scheme around that and registers the resource with
the generic apiserver. The APIService goes `Available=True` within
a few seconds of pod-ready. No compile-time knowledge of the `Note`
type exists anywhere in the component-server binary.

This is the headline success. The substrate-as-deployable-component
shape is viable.

### CRUD + watch + discovery all work

- `kubectl api-resources | grep notes` → the group/resource appears.
- `kubectl apply -f sample-note.yaml` → create succeeds; a `Note`
  appears in `kubectl get notes`.
- `kubectl get note hello -o yaml` → full object returned with spec,
  status, and library-assigned `uid`, `creationTimestamp`. Passes
  through JSON serialization twice (component → gRPC → backend →
  gRPC → component → kubectl) without issue.
- `kubectl delete note hello` → delete succeeds; a DELETED event
  fires on any active watch.
- `kubectl get notes -w` → streams events end-to-end. The
  server-streaming gRPC Watch RPC from backend to component works;
  the component fans events through its own `watch.Broadcaster`
  to kubectl clients.

Compat scoreboard for this experiment (with Hello-apply probes
skipped, as in all non-`hellos` experiments): 4 PASS, 1 FAIL, 2
SKIP. The FAIL is on `kubectl get notes -w` **only because** the
compat script's Hello-apply SKIPs left the resource empty and the
watcher had no objects to stream during its 5s window. A direct
test with a pre-existing Note produced clean output.

### `kubectl explain` works, but degrades as predicted

`kubectl explain note` returns:

```
GROUP:      aggexp.io
KIND:       Note
VERSION:    v1

DESCRIPTION:
    Dynamic resource served by the 0013 KRM component skeleton.
```

Just the description — no fields. The component server registers
unstructured schemas with `x-kubernetes-preserve-unknown-fields: true`;
kubectl's explain prints what's there, which is minimal. The
library needs a faithful per-type schema to produce rich
per-field docs, and our unstructured path does not provide one.

This is exactly the degradation anticipated in the hypothesis and
the target for experiment 0017.

### SSA fails at typed-converter construction

`kubectl apply --server-side -f note.yaml` returned:

```
Error from server: failed to create manager for existing fields:
failed to convert new object (/; /, Kind=) to proper version
(aggexp.io/v1): Object 'Kind' is missing in 'unstructured object
has no kind'
```

The library's SSA path (`managedfields.NewTypeConverter`) wants to
walk the OpenAPI schema for our GVK to build a field-ownership
model. Under the unstructured path, there's no schema rich enough,
and the converter tries to ingest an object without a typed Kind
registered. SSA is unusable in this skeleton.

0009 observed that SSA "appears to work but loses managedFields on
persistence"; 0013 regresses further — SSA doesn't even start.
Resolving this cleanly requires either:

1. Building typed Go structs at component-server startup from the
   backend's schema (reflection-based type generation; heavy).
2. Teaching the generic apiserver's SSA path to operate on truly
   unstructured objects (library work; upstream or via our own
   adapter).
3. Shipping the backend-provided OpenAPI through the existing
   typed-converter machinery in a way that the converter can key
   on. The library's converter is generic over schemas, but the
   `NewDefinitionNamer(Scheme)` path requires Go types in the
   scheme to map to schema entries.

All three are out of scope for 0013. Experiment 0017 will probe
option 3 at minimum.

### First workable protocol shape

The proto file is ~130 lines. Messages:

- `GetSchemaResponse { Group, Version, Resource, Kind, Singular,
  Namespaced, Writable, Columns [{Name, Type, Format, Description,
  Priority}], RowFields []string, OpenAPISchemaJSON bytes }`.
- `UserInfo { Name, Uid, Groups []string, Extras map<string, StringList> }`.
- `GetRequest { Namespace, Name, User }`; `GetResponse { ObjectJson bytes }`.
- `ListRequest { Namespace, LabelSelector, User }`; `ListResponse { ItemsJson [][]bytes, ResourceVersion string }`.
- `CreateRequest { ObjectJson, User }`; `CreateResponse { ObjectJson }`.
- `UpdateRequest { Name, ObjectJson, ForceAllowCreate, User }`;
  `UpdateResponse { ObjectJson, Created }`.
- `DeleteRequest { Namespace, Name, User }`;
  `DeleteResponse { ObjectJson }`.
- `WatchRequest { Namespace, LabelSelector, User }`;
  `WatchEvent { Type (ADDED|MODIFIED|DELETED), ObjectJson }` streamed.

The `RowFields` entry is a pragmatic addition: the component server
uses `[]string` field paths to extract table-row cells from each
object when kubectl asks for table format. Alternative would be a
table-rendering RPC; we chose the simpler path for the skeleton.

OpenAPI JSON is present in the proto but **unused** by the
component server today — it registers unstructured types with a
preserve-unknown schema and ignores the backend's spec. 0017 is
expected to start using it.

### What was hard

1. **Dynamic type registration.** `Scheme.AddKnownTypeWithName` with
   `*unstructured.Unstructured` accepts the GVK but does not map
   that GVK to a Go type distinct from any other unstructured GVK.
   You cannot distinguish two Note-vs-Other-Kind objects at the
   reflect level — both are `*unstructured.Unstructured`. Watch
   serialization had to stamp `apiVersion` and `kind` explicitly
   on each emitted object for kubectl to accept them. Took an
   afternoon to find.

2. **OpenAPI v3 is library-mandatory.** `InstallAPIGroup` calls
   `OpenAPIV3Config.GetDefinitions`; if that returns an empty map
   the library `klog.Fatal`s with "cannot find model definition
   for k8s.io/apimachinery/pkg/version.Info". The fix: compose the
   generated `GetOpenAPIDefinitions` (for the library's own
   internal types like `version.Info`, `meta/v1.*`, `runtime.*`)
   with our unstructured-Note shim. Not subtle, but the library's
   error message points at the wrong file. Logged as a consequent.

3. **gRPC watch → broadcaster fan-out.** The backend emits one
   WatchEvent stream at startup; every kubectl client gets its own
   broadcaster channel. Wiring the single upstream stream into
   many downstream watchers was straightforward with the pattern
   from 0009/0010 (gRPC side just happens to be a stream rather
   than a Go channel).

### Seams to fix in 0017

- **SSA**: get it working. Probably by shipping the OpenAPI from
  the backend into a real typed-converter rather than unstructured.
- **Per-kind explain**: the backend's OpenAPI should actually drive
  `kubectl explain`'s per-field output.
- **Multi-resource backend**: this skeleton has one backend = one
  resource. A real component server should accept N backends
  (one per resource) at startup, or a single backend exposing N
  resources.
- **mTLS + identity** between component and backend. Today's
  component trusts the backend's network address; an attacker who
  could reach the backend's port could speak to it directly.
- **Admission hook**: 0003 named the authz-vs-admission boundary.
  The proto has a natural place for a `Validate`/`Mutate` RPC.

## Fundamentals touched

**Wire protocol fidelity** (primary). The component server sits
exactly at the Kubernetes wire boundary; its job is to honor the
wire contract. Headline: **a generic, schema-dynamic server can
fully honor the read/write/watch/table paths**. Where it breaks is
at paths that assume typed Go models of the resource (SSA,
per-field explain), not at the wire protocol itself.

**Resource modeling freedom** (secondary). The unstructured path
dodges per-type code at the component server — which is exactly
what lets the backend be in any language. The cost is on the
Kubernetes-idioms side: clients that depend on SSA or rich
per-field explain see degraded behavior against our resources. The
tradeoff is real and is the whole reason for the arc.

**Storage independence** (tertiary). Storage now lives in two
completely separated layers: the component server (stateless by
design; caches only what the substrate's broadcaster needs) and
the backend (implements whatever persistence it wants). The
skeleton uses in-memory; future backends can use anything.

## Consequents

- **The library's OpenAPI-fatal error message is misleading.** It
  says "If you added a new type, you may need to add
  +k8s:openapi-gen=true to the package or type and run code-gen
  again" — useless advice if you're supplying a schema dynamically.
  Our fix was to compose the generated definitions into the
  dynamic definitions. Worth remembering.
- **Unstructured watch events need apiVersion+kind stamped** on
  every emitted object or kubectl refuses them. The library
  doesn't do this for you when the Go type doesn't carry it.
- **Go 1.24 pin** — same as every other experiment; `go mod tidy`
  auto-bumped to whatever the host has. Re-pinned.
- **gnostic-models@v0.6.8 pin** — same yaml-conflict consequent as
  0007/0009.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**:

- Wire protocol fidelity section: adds a datapoint that **the wire
  contract can be honored with no per-resource Go types**, via
  unstructured storage and a dynamic scheme. The library's other
  features (SSA, rich explain) have a typed-model dependency that
  this path cannot satisfy.
- Storage independence section: the "component + thin backend"
  shape is a third axis of storage options alongside (in-memory
  direct), (external-API-as-truth), (facade-over-CRD). Where 0010
  used a CRD on the host cluster, 0013 uses a gRPC backend
  that can be literally anywhere; storage is whatever the backend
  chooses.
- Resource modeling freedom section: the unstructured tradeoff
  should be named explicitly — dynamic schema at the cost of rich
  field-level behaviors.

For **EXPERIMENTS.md**:

- 0013 complete under Wire protocol fidelity (primary) with cross-
  references under Resource modeling freedom and Storage
  independence.
- Experiment 0017 (KRM protocol refinement) now has concrete work:
  fix SSA, fix per-kind explain, probably ship a typed-converter
  path driven by the backend's OpenAPI.
- Experiment 0018 (KRM parity with 0009 S3 backend) is clarified:
  the S3 backend can remain in Go, just refactored to run as a
  gRPC service alongside the component rather than a library.
- Experiment 0019 (polyglot backend) now has a clear target:
  write the Note backend (or equivalent) in python or rust.

## Open questions raised

- What's the smallest OpenAPI schema shape the backend must ship
  to make SSA work? Is it more or less than what `openapi-gen`
  produces for a typed Go struct?
- The gRPC protocol leaks `runtime.Scheme` concepts (GroupKind,
  ResourceName) into the proto. Is that right, or should the
  backend not even know about Kubernetes GVK semantics?
- Per-caller authz today is entirely the component server's
  concern (via `runtime/authz` substrate). Should the backend
  ever see authz decisions, or always receive pre-authorized
  requests? Probably the latter, but worth formalizing.
- Performance: we pay for JSON round-trips on every request. A
  concrete benchmark comparing 0013 (gRPC+JSON) vs 0009 (library
  direct) would quantify the cost.
