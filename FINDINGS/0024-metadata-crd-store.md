# Findings — 0024 metadata-crd-store

## What we were trying to learn

`FINDINGS/0022-stateful-middleware-thesis` committed to the
following split for the rest of the stateful-middleware arc:
**business data** (spec + status) lives on the backend; **KRM
metadata** (uid, resourceVersion, creationTimestamp, labels,
annotations, managedFields, finalizers, ownerReferences,
deletionTimestamp) lives on a shared host-cluster CRD that the
middleware stitches onto every response. 0010 demonstrated that
a CRD facade recovers the library features a stateless AA loses;
0015 demonstrated a specific downside of that design — the
backing CRD is discoverable by ecosystem controllers, and when a
facade passes tracking annotations through verbatim, ArgoCD
double-tracks. The thesis of 0024: put the metadata store under
a **different APIGroup**, as a single shared
`resourcemetadatas.aggexpmeta.aggexp.io/v1 ResourceMetadata`
cluster-scoped kind, so the backing store is no longer a
per-exposed-resource mirror.

0024 is a working implementation of that split, over the 0018 S3
Bucket scenario (backend-s3 talking to s3-mock, schema via 0023
Track B middleware-synthesis, middleware = a custom REST adapter
layered on the runtime/component proto client plus a dynamic
client to the host's ResourceMetadata CRD). Six scenarios end to
end; ArgoCD installed alongside; the visibility question probed.

## What we did

- A `metastore` package encoding a ResourceRef
  (group/resource/namespace/name) + a Record (the KRM overlay)
  against a dynamic client. Deterministic naming of the backing
  ResourceMetadata CRs: `<group-with-dashes>.<resource>.<namespace-or-cluster>.<name>`,
  with a sha256 fallback when the composed name violates
  DNS-1123.
- A `stitchedrest` package that forks the shape of
  `runtime/component/grpcbackend.REST` but interposes the
  metastore on every path:
  - **Get**: backend.Get + metastore.Get; stitch.
  - **List**: backend.List + one metastore.List filtered by
    (group, resource); stitch each item.
  - **Create**: metastore.Put with fresh UID + initial
    managedFields; then backend.Create; stitch response.
  - **Update / SSA**: library hands us the merged object;
    extract metadata → metastore.Put; forward spec/status to
    backend.Update; stitch response.
  - **Delete**: if finalizers non-empty, set deletionTimestamp
    on Record and return still-present; otherwise backend.Delete
    + metastore.Delete.
  - **Watch**: two upstream goroutines (backend gRPC stream and
    metastore dynamic watch); every event is re-stitched before
    republishing.
- A synthesis helper that lifts a plain JSON Schema to K8s
  OpenAPI v3 (Track B; copied from 0023), with one change: the
  metadata `$ref` points at `#/definitions/...` rather than
  `#/components/schemas/...`. Same rationale for a local
  `WrapAsListV2Refs` helper that overrides the substrate's
  `runtime/component/openapi.WrapAsList`. The motivation for
  this change is described under Consequents and is the
  load-bearing fix for scenario 6.
- A CRD YAML with a rich schema for ResourceMetadata (typed
  scalar fields for uid/labels/annotations/finalizers/deletion
  timestamp; `managedFields` and `ownerReferences` persisted as
  embedded JSON strings to keep the CRD free of metav1 version
  coupling).
- backend-s3 copied from 0018 and **simplified**: the on-wire
  `Meta` shrinks to `{name}`; no UID fabrication, no
  creationTimestamp, no resourceVersion. Middleware owns all of
  it.
- s3-mock verbatim from 0009/0018.
- Permissive RBAC: `buckets.aggexp.io` open to
  `system:authenticated`; `resourcemetadatas.aggexpmeta.aggexp.io`
  open to the AA's SA. ArgoCD installed via the standard
  `argoproj/argo-cd/stable@v3.0.12` install manifest with its
  default cluster-wide `*` RBAC.
- kind cluster `aggexp-meta-crd`, dedicated kubeconfig, six
  scenarios driven from the worktree.

## What we observed

### Per-scenario outcomes

1. **Create + list** — PASS. `kubectl apply` creates the S3
   bucket on the backend **and** a `ResourceMetadata` on the
   host with the stitched metadata. Both kubectl get buckets
   (table) and get bucket <name> -o yaml surface a fully-shaped
   object. The backing ResourceMetadata is visible as
   `aggexp-io.buckets.cluster.sc1` — human-readable, derived
   from the ref.

2. **Server-Side Apply** — PASS including conflict detection.
   Alice applies with `--field-manager=user-a` (owner:alice).
   `managedFields` persists on subsequent GETs, with one entry
   under `user-a` covering `spec.region` and `spec.tags.owner`.
   Bob's competing apply with `--field-manager=user-b`
   (owner:bob, region: us-west-2) returns HTTP 409 with
   `conflicts with "user-a"` on both overlapping fields. No
   silent drop (unlike 0009), no synthetic-scheme failure (0018
   couldn't do SSA at all). The round trip is identical to what
   kubectl would see against a plain CRD.

3. **Finalizer round trip** — PASS. kubectl patch adds
   `lab.aggexp.io/test`. kubectl delete returns success but the
   object remains with `deletionTimestamp` set and the
   finalizer still in the list. A follow-up kubectl patch
   clearing finalizers triggers the backend DELETE + metastore
   DELETE path (the middleware's Update detects the
   deletionTimestamp-set + finalizers-cleared transition). On
   the next GET the bucket is gone. This matches Kubernetes
   finalizer conventions exactly.

4. **OwnerReferences** — PASS for round-trip, limitation
   confirmed for GC. Patching an ownerRef that points at a
   v1/ConfigMap round-trips through the middleware: subsequent
   GETs show the `ownerReferences` array verbatim. Deleting the
   ConfigMap does NOT cascade-GC the Bucket, as anticipated —
   Kubernetes' native GC controller does not follow
   cross-group ownerReferences through aggregated APIs,
   consistent with the limitation noted at the end of
   FINDINGS/0010.

5. **Labels / annotations round-trip** — PASS. kubectl label
   and kubectl annotate patch the Bucket; subsequent GETs
   reflect the changes. Both end up in
   `ResourceMetadata.spec.metadata.{labels,annotations}` on
   the host side — the middleware owns these, the backend
   never sees them.

6. **ArgoCD visibility** — PASS (the key one). ArgoCD was
   installed via the standard upstream manifest; its
   application-controller discovered the full API surface,
   including both `buckets.aggexp.io` and
   `resourcemetadatas.aggexpmeta.aggexp.io`. After a Track B
   schema-ref fix (see Consequents) its cluster cache synced
   without SchemaErrors. A test Application targeting a Bucket
   with an `argocd.argoproj.io/tracking-id` annotation showed
   that ArgoCD did **not** treat the corresponding
   ResourceMetadata as a second managed resource. This is the
   load-bearing result: 0015's double-tracking issue does not
   reappear under the 0024 design.

   The reason, verified at the data layer: the tracking
   annotation on the exposed Bucket lands under
   `ResourceMetadata.spec.metadata.annotations` (the nested
   payload) and NOT under
   `ResourceMetadata.metadata.annotations` (the host-apiserver
   top-level). ArgoCD's ResourceTracker reads tracking
   annotations from `.metadata.annotations` only; it does not
   walk into arbitrary-CRD `spec.*` payloads. Because the
   middleware treats the tracking annotation as part of the
   stitched Bucket's metadata — one abstraction layer deep on
   the host — the annotation never appears at the top-level
   metadata of any ResourceMetadata CR. One cluster-scoped
   CRD, many instances, zero tracking visibility to ecosystem
   controllers.

### Performance

Stitched `kubectl get bucket`: 10-call mean ~71 ms (69–74 ms
range).
Direct `kubectl get resourcemetadata`: 10-call mean ~68 ms
(65–76 ms range).

Stitch overhead: ~3–5 ms per Get. Matches 0010's finding that
"an extra kube-apiserver hop per request is invisible at lab
scale" and stays well under the ~65 ms aggregation-layer floor
0006 / 0008 named. Cumulative for Create the cost is one gRPC
roundtrip to the backend + two host-apiserver roundtrips
(metastore.Put + client dial check); still well within
single-caller interactive budgets. At sustained high write
rates the host apiserver sees 2× load per user write (one for
the authz check the AA delegates to it, one for the metastore
write) — same pattern 0010 observed for a facade, plus the
second hop is now to a different GVR so there's no RBAC
duplication.

### The synthesis ref-format fix (consequent, load-bearing for
scenario 6)

0023's Track B emits the metadata `$ref` as
`#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta`.
kubectl accepts this in the aggregated `/openapi/v2` endpoint
without complaint. ArgoCD's gitops-engine cluster cache does
not: the OpenAPI parser in `kube-openapi/pkg/validation/spec`
refuses a `#/components/schemas/...` ref inside a v2 document
with `SchemaError(...): unallowed reference to non-definition
"#/components/schemas/..."`. Kube-apiserver's OpenAPI
aggregator does not rewrite the ref between v2 and v3 formats —
it passes it through verbatim.

The fix is small and confined to the synthesis boundary:

- `synthesis.LiftJSONSchemaToOpenAPI` emits the metadata ref as
  `#/definitions/...` (v2 style). /openapi/v2 consumers accept
  it, and /openapi/v3 local references are tolerant of the
  same form.
- `WrapAsListV2Refs` (a local replacement for
  `runtime/component/openapi.WrapAsList`) does the same for the
  list schema's item ref and its ListMeta ref.

After the fix, ArgoCD's cluster cache syncs cleanly. Before
the fix, **any** test Application in the cluster sat in a
"ComparisonError: error synchronizing cache state" state —
Scenario 6 could not be probed at all. This is the specific
kind of sharp edge the "compatibility with existing tooling"
thread in the VISION is meant to surface: the synthesis path
worked for kubectl because kubectl's OpenAPI client is
lenient; it broke for ArgoCD because ArgoCD's parser is
strict.

This is a **consequent** (tied to current kube-openapi
behavior; could change upstream) but a **load-bearing** one:
0023 recommends Track B as the default schema source for the
arc, and the default must produce an OpenAPI that survives
strict consumers. SYNTHESIS should annotate 0023's Track B
recommendation with "use `#/definitions/` refs", and the
substrate's `runtime/component/openapi.WrapAsList` is now a
known snag. A substrate fix is queued for 0030.

One remaining wrinkle: `kubectl apply` on an existing object
emits a warning `unable to resolve
#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
in OpenAPI V3` — kubectl's v3 client prefers
`#/components/schemas/` style. The apply still succeeds; the
object round-trips correctly. Fixing this properly means
serving different refs to /v2 and /v3 endpoints, which is
library-level work.

### UID handling edge

First draft persisted a synthetic UID (`synthetic-<uuid>`) when
the middleware synthesized a Record-less stitch on first read.
On a follow-up Update the synthetic UID leaked into the
persisted Record. Fix: on Update, if the incoming object carries
a `synthetic-` UID or an empty one, mint a real UUID at
persistence time. The synthetic prefix is the in-memory marker
that the Record didn't exist; it must never land on disk. No
data loss under the bug, but the persisted UID was unstable
across out-of-band-write + update flows.

### Watch fan-out is uneventful

The two goroutines — one on the backend gRPC Watch stream, one
on the metastore dynamic Watch — feed a single broadcaster.
Every event is re-stitched (fetch the counterpart, stitch,
publish). In testing, both event streams delivered as expected;
no races observed. The design relies on metastore writes being
the authoritative RV source (the Record's own
resourceVersion is the response's RV); the atomic counter is a
fallback for rare backend-only events.

## What surprised me

- **ArgoCD's schema strictness broke kubectl-passing
  components.** I expected this experiment to break at the
  metadata-stitching layer, or at the RBAC layer, or in the
  Watch loop. It broke in OpenAPI aggregation, on an issue that
  was latent in 0023 but unexposed because 0023 ran no strict
  OpenAPI consumer. That one category of failure — lenient
  kubectl passes, stricter consumers fail — deserves a named
  habit: **after any schema-source change, run the cluster
  cache through a strict consumer**. The most obvious strict
  consumer is ArgoCD; a cheaper proxy would be running
  `go run k8s.io/kube-openapi/pkg/validation/spec` or similar
  over the /openapi/v2 output, but that's not a thing we have.
  Adding ArgoCD-cluster-cache-synced as a compat scoreboard
  probe would catch this going forward.
- **The tracking-annotation isolation was cleaner than I
  expected.** The 0015 failure mode was specifically that
  annotations on an exposed Widget "leaked through the facade"
  into the backing WidgetStorage's `.metadata.annotations`, and
  ArgoCD then tracked both. In 0024, annotations on a Bucket
  land inside `resourcemetadata.spec.metadata.annotations` —
  not on the RM's own `.metadata.annotations`. Because the
  ResourceMetadata's annotations field is semantically *a
  field within the payload* rather than "metadata of the CR
  itself", ArgoCD's tracker can't see it. This is an
  unintended but load-bearing consequence of the
  Record-in-spec design.
- **Finalizer-clear-triggers-delete was not free**. The naive
  first draft honored the deletionTimestamp-set-on-delete but
  didn't observe finalizer-clearing in Update. Kubernetes'
  genericregistry.Store handles this on the library side; our
  stateless middleware doesn't ride that path. Adding the
  check was five lines; identifying the gap was 20 seconds of
  surprise on scenario 3.
- **Backend simplifies visibly.** 0018's backend-s3 persisted
  UIDs across poll cycles to keep identity stable; 0024's
  doesn't bother because the middleware owns UID. ~10 lines of
  deleted code on the backend; the real win is conceptual —
  the backend author has one fewer Kubernetes concept to
  care about.

## Fundamentals touched

**Storage independence** (primary). A new storage-axis variant
is now documented: business data on the backend **plus**
metadata-only in host etcd. It composes cleanly with the
fourth axis from 0013 (component + thin-backend). The cost is
roughly the same as 0010's facade in absolute latency but
trades 0010's "doubled kube-apiserver load at the same GVR"
for "distributed load across two GVRs under different groups".
The discoverability wrinkle 0015 surfaced is absent: the
metadata CRD is a single shared kind, not a per-exposed-
resource mirror, and the annotations ecosystem controllers use
for tracking stay on the stitched exposed resource.

This is the first positive existence proof that **metadata
state ≠ business data** is implementable. 0022 committed to the
shape; 0024 shows it works end-to-end with six scenarios
including a write-side gitops consumer.

**Wire protocol fidelity** (secondary). Nothing new in the
abstract: kubectl + SSA + watch work. The consequent-level
finding is strict-OpenAPI-consumers vs. Track B lifted schemas
— documented above. SYNTHESIS should note that the choice of
ref format is a live concern, not a cosmetic one.

**Resource modeling freedom** (tertiary). The backend's
business-data shape is genuinely Kubernetes-unaware — `Meta`
has only `name`. All the KRM concerns live in middleware code
that never touches the backend protocol. A polyglot backend
following this pattern would write zero Kubernetes concepts.
Consistent with 0019's finding that JSON bytes in the proto
make language portability free.

## Consequents (implementation-dependent; do not generalize)

- **kube-openapi ref format**: `#/components/schemas/...` in a
  `/openapi/v2` document is rejected by strict OpenAPI
  parsers; `#/definitions/...` works across both v2 and v3
  local references. The 0024 synthesis and WrapAsListV2Refs
  use `#/definitions/`. The substrate's
  `runtime/component/openapi.WrapAsList` still emits v3 refs
  and will break the same way for anything serving a strict
  consumer; fixing it substrate-side is queued for 0030.
- **kubectl warning on v3 refs**: kubectl's `/openapi/v3`
  client prefers `#/components/schemas/`. With our v2-style
  refs it prints a warning on `apply` (`unable to resolve
  #/definitions/...ObjectMeta in OpenAPI V3`). The apply
  succeeds. Properly fixing requires serving different refs
  to the two endpoints.
- **Synthetic UID marker**: a `synthetic-` prefix on a UID is
  the middleware's in-memory marker for
  stitch-without-persisted-metadata. Update must replace it
  before writing, or the prefix will leak into the Record.
- **Rmeta name format collision**: if two exposed resources
  across different AAs (e.g. `widgets.aggexp.io` and
  `widgets.aggexp.io/v2`) happened to have the same
  (group, resource, name) pair, they'd collide on
  ResourceMetadata name. The name encodes
  `group-with-dashes.resource.namespace.name`; the collision
  surface is narrow but real. The hash fallback mitigates for
  oversized inputs but not for collisions on short inputs.
- **Encryption at rest**: ResourceMetadata lands in host etcd.
  Operators storing secrets-adjacent data in annotations (OIDC
  tokens, credential hints) need cluster-level
  encryption-at-rest turned on for the `aggexpmeta.aggexp.io`
  resources. This was called out as a 0024-scoped open item in
  FINDINGS/0022 and is hereby surfaced: **any operator
  deploying this pattern in production needs to configure
  `EncryptionConfiguration` for `resourcemetadatas.aggexpmeta.aggexp.io`.**
  For the lab this is moot.
- **Dynamic client + InClusterConfig**: the middleware uses
  `clientgorest.InClusterConfig()` to build its dynamic client
  for the metastore. This requires the pod to have a projected
  ServiceAccount token. Our base `aggexp` SA has one. Not
  configurable from a flag in this experiment.
- **gnostic-models v0.6.8 pin**: recurring arc consequent.
  Honored at go.mod.
- **ArgoCD v3.0.12 is the `stable` tag at experiment time.**
  Specific behavior (SchemaError text, cluster-cache-sync
  mechanism) is tied to this version; future ArgoCD versions
  may be more permissive about ref formats or may introduce
  new checks.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**:

- **Storage independence.** Add the fifth axis: **business data
  on an external backend + KRM metadata in a shared
  host-cluster CRD stitched by middleware**. It composes with
  the fourth axis (component + thin-backend), 0024 is the
  reference implementation. The double-tracking failure mode
  0015 named for the per-exposed-resource CRD facade (0010) is
  absent from this axis because the metadata CRD is a single
  shared kind and ecosystem-controller tracking annotations
  stay inside the stitched exposed resource.
- **Wire protocol fidelity.** Annotate 0023's Track B
  recommendation with the ref-format consequent: emit
  `#/definitions/...` refs, not `#/components/schemas/...`,
  for any schema path that participates in `/openapi/v2`
  aggregation. Strict OpenAPI consumers (ArgoCD's cluster
  cache) reject the v3-style form inside a v2 document, and
  kube-apiserver does not rewrite it.
- **Resource modeling freedom.** Add the claim: a Kubernetes
  resource can be modeled with the backend entirely unaware of
  KRM metadata; `Meta` shrinks to `{name}` on the wire, and
  the middleware owns UID/RV/managedFields/finalizers/
  ownerRefs/labels/annotations/deletionTimestamp.

For **EXPERIMENTS.md**: mark `0024-metadata-crd-store` complete
under Storage independence with cross-references to 0022
(thesis), 0023 (schema source), and 0015 (the double-tracking
failure mode this experiment avoids). No candidate is retired by
this experiment; the arc continues to 0025 (push-backed watch).

## Open questions raised

- **Multi-exposed-resource collision in the ResourceMetadata
  name-space.** The name format encodes group/resource/
  namespace/name, but two AAs under different group-versions
  could still produce the same ResourceMetadata name for
  coincidentally identical (group, resource, name) triples.
  A generation-counter or hash fallback applied uniformly
  might be safer than the human-readable form.
- **Multi-AA deployment.** If two AAs under the same
  middleware write to the same ResourceMetadata kind, they
  share write semantics. Concurrent writes from two processes
  on the same Record would fight on resourceVersion. Out of
  scope here; 0027's multiplex reconciler is the natural
  venue.
- **Metadata GC.** If a backend object disappears out of band
  (S3 bucket deleted from the AWS console), the
  ResourceMetadata CR stays. A reconciler sweep (candidate
  0028) is needed to clean these up. In the current
  implementation the orphaned Record merely takes up a CRD
  row; it doesn't cause errors on subsequent operations, but
  it is visible in `kubectl get rmeta`.
- **Schema-fix in the substrate.** The substrate's
  `runtime/component/openapi.WrapAsList` emits v3-style refs.
  0024 worked around by overriding; 0030 should correct the
  substrate. Until then, **every new arc experiment that
  installs ArgoCD alongside must use the v2-ref form** or
  reproduce 0024's SchemaError.
- **`/openapi/v3` kubectl-apply warning.** A small one, but
  worth tracking: the `#/definitions/` fix for v2 produces a
  kubectl v3-client warning on apply. A production-quality
  synthesis would serve format-appropriate refs to each
  endpoint — probably by consulting the request path inside
  the library's OpenAPI handler. Out of scope here.
- **Scenario-6 negative counterexample.** The experiment
  shows the shared-CRD design doesn't double-track. It does
  NOT attempt an exhaustive adversarial probe — e.g., ArgoCD
  v3.x with the `argocd.argoproj.io/compare-options:
  IncludeAll` directive, or an operator who explicitly creates
  an Application targeting the `aggexpmeta.aggexp.io` group.
  A follow-up "adversarial ArgoCD" probe would strengthen the
  claim.
