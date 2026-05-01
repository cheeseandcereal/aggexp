# Findings — 0030 runtime-component-v2-promotion

## What we were trying to learn

The stateful-middleware-refinement arc (0022 thesis + 0023-0029
empirical experiments) accumulated design commitments that cannot
live in the existing `runtime/component` without breaking its
current consumers (0013, 0017, 0018, 0021 class). 0030 is the
substrate promotion that consolidates those commitments into
`runtime/component/v2/` — a fresh package tree alongside v1, not a
replacement.

Not an experiment: the deliverable is the substrate package and its
tests. The "hypothesis" in the README is whether the arc's design
survives one coherent substrate, answered by whether a follow-on
consumer experiment (0031) can use v2 without per-consumer patches.
That's for a later task; this one ships v2 and writes down what it
took.

The five substrate-fixes SYNTHESIS flagged for v2 (plus a sixth,
state split, that 0024/0028 demanded):

1. `WrapAsList` must emit `#/definitions/` refs (0024).
2. `initial-events-end` BOOKMARK emitted unconditionally (0025).
3. RV authority unified across Get/List/Watch (0025).
4. Dynamic-install-friendly OpenAPI (0027) — two sub-fixes:
   cache-defeat on `DefaultOpenAPIV3Config.Definitions` + V3
   endpoint refresh on `InstallAPIGroup`.
5. State split: business data on backend + KRM metadata on host
   CRD, stitched by middleware (0024, 0028).
6. Declarative admission composes with backend-RPC admission (0029).

## What we built

`runtime/component/v2/` with sub-packages:

- `proto/` — new `.proto` with Validate, Mutate, watch_capability
  declaration, schema_is_openapi flag, AdmissionCause list for
  multi-cause 422 denial.
- `scheme/` — typed Object wrapper + dynamic Scheme. Canonical
  names use the v2 import path so v1 and v2 can coexist.
- `openapi/` — Compose returns a live closure (no eager
  materialization); Synthesize lifts plain JSON Schema to
  Kubernetes OpenAPI (Track B, 0023); refs use `#/definitions/`
  (0024 fix); baseline meta/v1 definitions re-exported from v1's
  generated file.
- `grpcbackend/` — the main REST adapter. Integrates MetadataStore,
  admission.Engine, unified RV authority, push/poll watch mode,
  and the unconditional initial-events-end BOOKMARK. Exports
  `Dial(ctx, addr)` as a convenience gRPC client constructor.
- `httpbackend/` — HTTP/JSON + SSE client implementing the same
  `componentv2pb.BackendClient` interface. Swappable with gRPC by
  flag.
- `metadatastore/` — CRD-backed KRM metadata store. Ships the
  `resourcemetadatas.aggexpmeta.aggexp.io/v1` CRD YAML as an
  embedded file for programmatic apply.
- `gc/` — reconciler for orphaned Records, with HTTP `/gc/run` and
  `/gc/last` debug endpoints. Policy: skip on finalizer,
  ownerReferences, deletionTimestamp-set, or within grace window.
- `admission/` — CEL validation + JSONPath mutation engine.
  `LoadFromFile` for static YAML; `New(cfg)` compiles CEL at
  startup.
- `multiplex/` — APIDefinition-CRD-driven reconciler. APIDefinition
  is a typed runtime.Object (not unstructured). The
  `apidefinitions.aggexp.io/v1` CRD YAML is embedded. `ShutdownSweep`
  deletes managed APIServices on SIGTERM.
- `watch/` — transport-neutral watch helpers: BookmarkObject
  builder, RV Authority primitive.

Hand-written LOC: 4,565. Test LOC: 1,623. v1 kept: 1,579 hand-written.

## What we re-derived vs lifted

**Re-derived** (fresh code, not copied from v1 or experiments):

- The REST adapter (`v2/grpcbackend/rest.go`). v1's
  `grpcbackend.REST` was the template conceptually, but v2's has a
  different shape: metastore-stitching baked in, unified RV, the
  BOOKMARK emission, admission composition. Copying and patching
  v1 would have produced a mess. Rewriting was cleaner. ~965 lines.
- The OpenAPI Compose closure. v1's was a flat function that
  materialized defs once. v2's is a live getter; `Compose` takes a
  `func() map[...]OpenAPIDefinition` and the runtime reads it on
  every openapi-builder pass.
- The multiplex reconciler. 0027's main.go was 800+ lines of
  bootstrap + reconcile + status-write + APIService CRUD. v2's is
  similar size but split into a Multiplex type with clean surface
  (`New`, `AttachServer`, `OpenAPIClosure`, `Run`, `ShutdownSweep`)
  plus typed APIDefinition. The 0027 version was experiment-grade;
  v2's is what a caller imports.

**Lifted with minimal rework** (same shape, import paths updated,
docs sharpened):

- The admission engine from 0029 (`v2/admission/`).
- The metastore from 0024 (`v2/metadatastore/`).
- The GC reconciler from 0028 (`v2/gc/`).
- The HTTP transport client from 0026/0027 (`v2/httpbackend/`).
- The Track B synthesis from 0023 — folded into `v2/openapi/Synthesize`.
- The typed Object wrapper from v1/scheme — folded into v2/scheme,
  canonical name changed, otherwise identical.

**Not lifted**:

- The 0022 `thesis/thesis.go` interface enum types. v2 uses its own
  concrete types (`APIDefinition`, `BackendSpec`, etc.) rather than
  re-importing the thesis sketch. The thesis was a design artifact;
  its interfaces live on in v2 as concrete shapes.

## Scope cuts

Recorded here per the task's "record each in the FINDINGS file"
instruction:

- **Single-version MetadataStore CRD.** Migration from v1 to future
  versions is not provided. Operators would need a conversion
  webhook or an offline snapshot+restore. Called out in the
  package doc. The arc acknowledged this in 0022.

- **V3 endpoint refresh + SSA typed-converter rebuild for
  dynamically-installed groups.** The **cache-defeat** half of the
  0027 fix LANDS in v2: `openapi.Compose` returns a live closure
  that re-reads defs on every openapi-builder pass, so dynamic
  Install sees new schemas. What DOES NOT land is refreshing the
  per-group `/openapi/v3/apis/<group>/<version>` endpoint and the
  managedfields TypeConverter that the library builds once at
  PrepareRun. Dynamic-install in multiplex mode CRUD/list/watch/table
  works; `kubectl explain <kind>` and `kubectl apply --server-side`
  degrade on dynamically-installed groups. Statically-installed
  single-AA consumers have full parity. Explicitly marked as a
  **known gap in v2 alpha**; the task-level guidance was "the user
  would rather have v2 without that fix than no v2." Closing it
  likely needs library-level work (`genericapiserver`
  internals) beyond the substrate's reach.

- **Push-vs-poll runtime probe.** 0025 suggested that the declared
  `watchCapability` should be cross-checked against a runtime probe
  (open Watch once, observe Unimplemented → downgrade to poll).
  v2 honors the declared capability as truth; no probe. A backend
  that lies (declares push, errors on Watch) produces an observable
  disconnect/retry loop — loud-enough failure mode that the probe
  is deferred to a future v2 sub-version if the declared-vs-actual
  gap proves load-bearing.

- **Unifying RV authority across all storage axes.** v2 unifies RV
  authority across the component-server path (the REST's atomic
  counter + Record's host-etcd RV) but does NOT touch
  `runtime/server`'s RV path, which is used by the library-mode
  consumers (0002, 0007, 0009, 0010, 0011). Those consumers manage
  their own RV in `runtime/storage.REST`'s `atomic.Uint64`. Scoping
  to the component-server path avoided entangling the promotion
  with library-mode consumers that weren't part of the arc.

- **cel-go v0.22.0 pin.** Already on the module graph from 0029.
  Not changed. Recorded for future consequent.

- **Package layout deviation.** The task suggested a standalone
  `watch/` package for RV authority + BOOKMARK emission. I folded
  the actual watch-handler integration into `grpcbackend.REST`
  (where the broadcaster lives) and kept `watch/` as a thin
  package exporting the BOOKMARK builder and an RV Authority
  primitive — consumers that want to coordinate RV across
  multiple paths (e.g. a custom REST layered on v2) can import it
  without pulling the whole REST adapter. Minor deviation.

## Known gaps remaining in v2 alpha

- Dynamic-install SSA + explain (see scope cuts).
- MetadataStore schema-evolution migration (see scope cuts).
- Backend-to-middleware mTLS / SPIFFE.
- Encryption-at-rest for ResourceMetadata (operator config, not
  substrate code).
- Runtime probe of watch capability (see scope cuts).

All five surface in the v2 `doc.go` and/or the multiplex package
doc so consumers don't discover them post-deployment.

## What surprised me

- **The REST adapter grew larger, not smaller.** v1's
  `grpcbackend/rest.go` is 693 lines. v2's is ~965. I expected
  v2 to be smaller because it factors out the metastore and
  admission into sub-packages. It isn't: the new integration
  seams (admission composition, metastore stitching on every
  path, unified RV, BOOKMARK emission, push/poll mode dispatch)
  each add a handful of lines at the REST layer, and they add up.
  Rewriting from scratch saved copying 693 lines and patching
  them; it didn't avoid the ~250 new lines integration requires.

- **The `#/definitions/` vs `#/components/schemas/` split is
  deeper than I read in 0024.** kubectl's /openapi/v3 client
  warns when it sees `#/definitions/` in what should be a v3
  document. v2 emits `#/definitions/` to stay compatible with
  strict v2 consumers like ArgoCD's cluster cache. kubectl
  still succeeds; it just prints a warning on apply. Properly
  fixing requires serving format-appropriate refs per endpoint,
  which is a library-level concern. Recorded as a continued
  consequent; the v2 choice is right for the common case.

- **APIDefinition as a typed runtime.Object was load-bearing.**
  0027 used unstructured access throughout its reconciler.
  `parseAPIDefSpec` was 50 lines of stringly-typed dives. In v2,
  `APIDefinition` is a real Go struct and `parseAPIDef` is 15
  lines of `json.Unmarshal + validate`. The admission Config
  unmarshals cleanly because it's already typed in
  `v2/admission/`. Half the experiment-level reconciler
  complexity was unstructured-access bookkeeping, not actual
  reconcile logic.

- **Fake dynamic client needs custom list kinds registered.**
  `dynamic/fake.NewSimpleDynamicClient` panics on `.List()` unless
  you use `NewSimpleDynamicClientWithCustomListKinds` with a
  GVR→list-kind map. First test run discovered this; trivial
  once known. Recorded here because the next agent writing
  similar tests will hit it.

- **cel-go's non-bool detection happens at compile time.**
  `ast.OutputType().IsExactType(cel.BoolType)` rejects
  `"not bool"` at `NewEngine` time. 0029 already exploited
  this; v2 keeps the behavior. Consumers get a startup error,
  not a runtime surprise.

## Fundamentals touched

**Storage independence** (primary). The fifth axis 0024 named —
metadata on host CRD + business data on backend — is now a
first-class substrate primitive (`v2/metadatastore/`). Any
component-mode consumer can opt in with `rest.WithMetadataStore(s)`.
The GC obligation (0028) is similarly substrate (`v2/gc/`). This
closes the "you must write the CRD store yourself" gap that made
0024 and 0028 experiment-grade. Fundamental: a stateless middleware
with a typed-metadata overlay is a viable storage pattern, and the
substrate supports it out of the box.

**Wire protocol fidelity** (primary). Two concrete fidelity bugs
from the arc close as substrate-level defaults:

- `initial-events-end` BOOKMARK emitted unconditionally at the
  Watch tail. `kubectl wait --for=jsonpath` works; WatchList-aware
  clients get the initial-events-list-blueprint augmentation for
  free. Fundamental: the BOOKMARK is the library's native seam,
  our job is just to emit it.

- `#/definitions/` refs (0024). v2-style refs are what strict v2
  consumers accept inside a v2-aggregated document. Consequent
  (tied to kube-openapi's current behavior) but the v2 substrate
  always emits them.

What does NOT close: V3 endpoint refresh for dynamic groups (SSA
and explain degrade in multiplex mode). Known gap.

**Watch and consistency semantics** (primary). RV authority is now
unified within the component-server path. Middleware-counter is
primary; Record.RecordResourceVersion (from host etcd) is
authoritative when a Record exists. Get/List/Watch see the same
sequence. Push-backed watch (0025) is a first-class mode; poll is
the safe default. Fundamental: a stateless middleware can own RV
end-to-end when metadata state lives in a trustworthy store.

**Per-request authorization** (secondary). Declarative admission
(CEL + JSONPath) composes additively with backend-RPC admission;
v2 exposes both paths on the same REST. The 422 wire shape is
unified regardless of which layer denied. Fundamental: the
authz-vs-admission boundary from 0003 is now closed for the
component-server architecture by two complementary mechanisms.

**Resource modeling freedom** (secondary). Transport is swappable
(gRPC or HTTP+SSE) by descriptor field, not rebuild. Track B
schema synthesis means backends ship plain JSON Schema with
zero Kubernetes concepts. Fundamental: "language-agnostic" extends
to "transport-agnostic."

**Identity handoff** (tertiary). Unchanged from v1 in structure;
the identity still rides the request context into the backend
RPC as UserInfo. Consequent: HTTP transport's extra-key escaping
(`authentication.kubernetes.io%2Fcredential-id`) is mirrored from
the aggregation layer's convention.

## What this changes for SYNTHESIS and EXPERIMENTS

**SYNTHESIS.** Add to the "Current state" section: `runtime/component/v2`
is landed. Update the five-substrate-fix list to note which landed
fully (BOOKMARK, unified RV, #/definitions/ refs) and which landed
with known gaps (dynamic-install OpenAPI: cache-defeat yes, V3
endpoint refresh no). Storage independence section's fifth axis
becomes a substrate primitive, not an experiment-specific pattern.

**EXPERIMENTS.** Mark `0030-runtime-component-v2-promotion` complete.
`0031-runtime-component-v2-parity` remains queued as the natural
first post-promotion consumer. New candidates added by the gaps:
`dynamic-install-openapi-refresh` (library-level fix for
V3+SSA on post-PrepareRun groups) and `v2-backend-mtls` (if/when a
multi-tenant deployment demands it).

## Open questions raised

- **Does a fresh consumer experiment surface seams v2 got wrong?**
  This is the 0031 question. A parity consumer (replay 0024's S3
  Bucket scenario or 0026's Note against v2) tests whether the
  integration seams are as clean as they look without an external
  consumer exercising them. Expected to surface small rough
  edges; the substrate budget allows patching.

- **How does multiplex-with-metastore behave at three concurrent
  AAs under write load?** 0024 proved one AA stitching works;
  0027 proved three AAs register dynamically. v2 wires metadatastore
  into the multiplex reconciler but has not exercised the
  concurrency cross-product. The metastore uses host-etcd optimistic
  concurrency, so in principle it's correct; empirical confirmation
  is still open.

- **Is the `watch/` sub-package load-bearing, or did I over-factor?**
  The BOOKMARK builder is used by the REST; the Authority primitive
  is unused by the substrate itself. Kept as a pre-commitment to
  consumers that layer their own REST on top. If 0031 doesn't
  import it, I'd fold it back into grpcbackend.

- **Is the descriptor-based watch mode selection (ModePoll vs
  ModePush) too static?** A backend that can serve both could
  benefit from the middleware probing at startup and preferring
  push. Today the descriptor field is truth. Runtime probe lands
  in a future v2 sub-version if it matters.

- **Does the initial-events-end BOOKMARK's
  `k8s.io/initial-events-end=true` annotation collide with an
  `allowWatchBookmarks=false` client?** 0025's findings flagged
  this. v2 emits the BOOKMARK unconditionally. A client that
  explicitly opts out with `allowWatchBookmarks=false` should
  probably not receive it. Not tested; recorded as a sharp-edge
  to remember.

## Status

Complete. v2 landed alongside v1; all tests pass
(`go test ./runtime/...`); `hack/verify-spine.sh` exits 0;
ARCHITECTURE.md rewritten. First consumer (0031) queued.
