# Findings — 0031 runtime-component-v2-parity

## What we were trying to learn

Whether `runtime/component/v2/` — the third substrate promotion,
consolidating the 0022-0029 stateful-middleware arc — holds under
its first post-promotion consumer. 0021 was the equivalent probe
for the v1 promotion, and it was small and boring (a ~40-line
main.go replayed 0017's behavior verbatim). 0031 is deliberately
larger: the v2 promotion absorbed five separate commitments
(metastore, GC, declarative admission, dual transport, multiplex
with dynamic install) plus a handful of wire-fidelity fixes
(unified RV, #/definitions/ refs, initial-events-end BOOKMARK).
The question was whether all of that composes into one consumer
binary without per-consumer substrate patches.

This is also the natural closer for the stateful-middleware-
refinement arc (0022 → 0031), so it doubles as a phase-boundary
marker. The compat scoreboard was run for both serving groups.

## What I did

Wrote `experiments/0031-runtime-component-v2-parity/`:

- `cmd/v2-parity-aa/main.go` + `yaml.go` — 277 lines that wire
  `runtime/server.Options` + `v2/multiplex.Multiplex` +
  `v2/metadatastore.Store` + `v2/gc.Reconciler` +
  `v2/httpbackend.New` (for GC's backend probe) into one generic
  apiserver. Post-start hooks run the multiplex reconciler and
  the GC loop; a pre-shutdown hook sweeps the APIServices.
- `backend-widget-http/main.go` — 452-line stdlib-only HTTP/SSE
  backend for one kind (`widgets.aggexp.io/v1`), derived from
  0027's multi-kind backend but trimmed to a single hard-coded
  kind. Advertises `watchCapability: push`.
- `backend-gadget-grpc/main.go` — 279-line gRPC backend for one
  kind (`gadgets.aggexp.io/v1`), implementing
  `componentv2pb.BackendServer` with the usual `Unimplemented*`
  embed. Advertises `watchCapability: poll` — intentional
  contrast, so the multiplex process exercises both
  `grpcbackend.ModePush` and `ModePoll` at the same time.
- Manifests + RBAC + Dockerfiles + two `APIDefinition` samples
  + a `run-scenarios.sh` that captures a 16-scenario sweep.

Deployed into kind cluster `aggexp-0031`. Ran the scenario sweep
twice (once with the admission-path bug, once after the fix) and
the compat scoreboard once per group.

## What happened

Every scenario except the two 0030-known-gap cases passed. Summary:

- **Discovery**: both groups listed cleanly.
- **APIDefinition reconcile**: both CRs reached
  `Ready=True, Available=True` within ~20s of `kubectl apply`;
  matching `APIService/v1.widgets.aggexp.io` and
  `.../v1.gadgets.aggexp.io` created and `Available=True`.
- **CRUD**: `kubectl apply`, `kubectl get` (table), `kubectl get
  -o yaml`, `kubectl delete` all worked on both APIs.
- **Stitched Get** (0024 axis): `-o yaml` shows `uid`,
  `resourceVersion`, `creationTimestamp` that come from the host
  `ResourceMetadata` CR, not the backend. Visible by the
  annotation-pass-through: `kubectl annotate` lands cleanly on
  the exposed object, and `ResourceMetadata.spec.metadata.
  annotations` tracks it (not the CR's own
  `metadata.annotations` — 0024 contract still holds).
- **Watch**: both `kubectl get widgets -w` and `kubectl get
  gadgets -w` fired ADDED + MODIFIED events in the 3s window
  after `kubectl annotate`. Widget uses push (SSE stream from the
  backend, forwarded by v2/grpcbackend's `ModePush` consumer);
  gadget uses poll (v2/grpcbackend's `ModePoll` list-loop).
- **Declarative admission denial**: creating a widget with
  `spec.color=black` returns the expected HTTP 422
  `{reason: Invalid, field: spec.color, message: "..."}`.
- **Declarative admission mutation**: creating a widget with no
  `spec.title` returns an object whose `spec.title=Untitled` (the
  default value from the APIDefinition's mutation rule).
- **GC**: the 30s-interval, 10s-grace-window reconciler wiped
  orphan `ResourceMetadata` records after the widgets were
  deleted. Pre-sweep: 3 records; post-sweep: 0.
- **Graceful SIGTERM**: `kubectl delete deploy aggexp` removes
  both APIService objects within the 20s shutdown grace, so
  `kubectl api-resources` stops listing the groups immediately
  after the pod terminates.
- **Cross-API isolation**: with widgets fully deleted and
  re-installed, the gadgets APIService + data remained
  continuously `Available=True`.

The two 0030 known-gap cases confirmed as expected:

- `kubectl explain widget` returns *client-side*
  "couldn't find resource for" — kubectl's explain path asks for
  `/openapi/v3/apis/widgets.aggexp.io/v1` and the library
  doesn't register that endpoint for post-PrepareRun-installed
  groups.
- `kubectl apply --server-side` returns the exact error the 0030
  finding predicted: "failed to convert new object ... to smd
  typed: .metadata: schema error: no type found matching:
  io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta". The
  `managedfields.NewTypeConverter` schema map has no
  ObjectMeta entry for dynamically-installed groups.

Both gaps are library-substrate issues (kube-apiserver internals
freeze V3 endpoints + typed-converter at PrepareRun), flagged in
FINDINGS/0030's scope cuts. The substrate didn't introduce
anything new; it inherits exactly the 0027 boundary.

No substrate patches were required. The consumer compiled against
the v2 packages as-published, the multiplex package's documented
"nil the OpenAPIConfig.Definitions" step worked as described, and
the `AttachServer` / `Run` / `ShutdownSweep` surface was enough
to build a working middleware without dropping into
`genericapiserver` internals.

## Consumer-facing rough edges

The substrate held, but landed a few friction points worth feeding
back into a v2.1 design pass:

1. **`splitPath` leading-dot silent no-op (admission).** My first
   `APIDefinition` used `jsonPath: .spec.title`. The substrate's
   `admission.splitPath` does `strings.Split(p, ".")`, yielding
   `["", "spec", "title"]`. The walker then wrote the default
   value at `obj[""]["spec"]["title"]` — no error, no warning, no
   log. The object that made it to the backend had no title. This
   is a **consequent**: it's tied to the current `splitPath`
   impl, but it's particularly painful because (a) `kubectl
   explain`-style output uses leading dots as a display
   convention, (b) the internal `field.ErrorList` the REST emits
   for denials contains `spec.color`, not `.spec.color`, so
   consumers copy-pasting fieldPaths will inconsistently need
   leading dots or not depending on which field they're writing.
   The v2 admission package should either normalize (strip a
   leading `.`) or loudly reject it at
   `admission.New(cfg)` compile time.

2. **`OpenAPIV3Config.Definitions = nil` is a consumer obligation,
   not a substrate default.** The multiplex package doc
   correctly flags this ("callers MUST nil
   OpenAPIV3Config.Definitions + OpenAPIConfig.Definitions on the
   RecommendedConfig after runtime/server.Config() returns"),
   but it's load-bearing: if you don't do it, the OpenAPI closure
   the consumer passes in goes un-consulted and no resources get
   schemas at all. A 0031-new-consumer who reads the multiplex
   doc but not carefully enough will hit this as a mystifying
   "schemas are empty" failure. The substrate could either (a)
   expose a helper like `runtimeserver.Options.ConfigForMultiplex()`
   that does the nil-out, or (b) refactor `runtime/server.Config`
   to accept a "dynamic-install-friendly" option that pre-nils
   the caches. Minor but the kind of thing that eats 15 minutes
   the first time.

3. **GC is per-(group, resource), but multiplex is many-(group,
   resource).** The `gc.Reconciler` config takes a single
   `Group` + `Resource` pair, but the multiplex reconciler
   doesn't automatically spawn a GC per APIDefinition. My
   consumer spawns one GC for `widgets.aggexp.io/widgets` only;
   gadgets are unmonitored. This is fine for an experiment but
   gives me a rough edge feeling: the `multiplex.Options`
   struct has a field for `MetadataStore` (attached to every
   per-AA REST) but nothing analogous for GC. A natural v2.1
   would have `multiplex.Options.GC *gc.Config` that the
   reconciler honors by spawning one Reconciler per install and
   cancelling it on reconcileDelete.

4. **Embedded CRD YAMLs (`metadatastore.CRDYAML`,
   `multiplex.APIDefinitionCRDYAML`) are `[]byte`, but the
   substrate offers no apply helper.** My consumer wrote a
   ~20-line `ensureCRD` that unmarshals the YAML with
   `sigs.k8s.io/yaml`, constructs an `Unstructured`, and does
   `dyn.Resource(customresourcedefinitions).Get/Create`. This is
   rote; every v2 consumer will re-derive it. A substrate-level
   `metadatastore.EnsureCRD(ctx, dyn)` + equivalent in
   `multiplex` would be one tiny addition.

5. **APIService deployment manifest** in the base
   `deploy/manifests/50-apiservice.yaml` declares
   `v1.aggexp.io`, which is wrong for a multiplex consumer. The
   consumer (and 0027 before it) has to deploy only a subset of
   the base manifests, or accept a stale APIService being
   created and then ignored. Not a substrate issue per se, but
   the repo's shared `hack/deploy.sh` + `deploy/manifests/`
   pattern doesn't cleanly fit the multiplex consumer; 0031
   worked around by deploying four specific files individually
   and skipping 50-apiservice.yaml. A v2-aware
   `deploy/manifests/multiplex/` base would make the consumer
   setup tidier.

6. **Unused imports if you don't use every transport.** My
   first pass imported both `grpcbackend` and `httpbackend` in
   main.go "just in case" — compilation rejected the unused
   `grpcbackend` import. Since the multiplex package dials the
   right transport internally (per APIDefinition), the consumer
   usually needs neither import directly. Only the GC probe
   needed one (I used httpbackend because GC targets widgets).
   Minor; resolved by deleting the unused import. Not a v2
   issue, just a note on what a minimal consumer's import list
   looks like.

7. **ResourceMetadata CRD fallback vs programmatic apply**.
   The metastore ships `CRDYAML` for embedding, but doesn't apply
   it itself. If the consumer forgets, the first `Create` 500s
   with "ResourceMetadata is forbidden". My consumer has an
   `ensureCRD` hook for robustness, but the scenarios-driven
   testing in this experiment used the static manifest in
   `manifests/00-crds.yaml` (with the embedded YAML copied in as
   a fallback). Both paths worked. A v2.1 could pick one as the
   documented happy path — probably programmatic-apply-at-startup
   is more in the spirit of "one binary, one command."

None of these are blockers; all are friction I'd have expected for
an alpha. The rediscovery failure mode was avoided by reading the
known gaps in 0030 carefully before starting.

## Line-count comparison

- 0021 v1 single-AA parity consumer: **38 LOC** `main.go`.
- 0027 v1 multiplex experiment: ~**800 LOC** reconciler +
  ~**500 LOC** scheme/openapi/http-client boilerplate, all
  in-experiment.
- 0031 v2 multiplex consumer: **277 LOC** consumer-side
  middleware wiring (`main.go` 258 + `yaml.go` 19) — most of
  0027's reconciler is now in `runtime/component/v2/multiplex`.
  The delta 38 → 277 is the multiplex-vs-single-AA cost (the
  consumer is wiring dynamic CRD reconciliation, a post-start
  hook, a PreShutdown hook, an optional GC loop, and an embedded-
  CRD apply step). The delta 0027's 1,300 → 277 is the
  substrate-absorption payoff.

The 0021 floor (40 LOC for the trivial case) is not reachable in
a multiplex consumer because the multiplex consumer must coordinate
three lifecycles (apiserver, reconciler, GC) with three lifetimes
(forever, informer-scoped, context-scoped). That's a genuinely
larger problem, and 277 is a reasonable bounded price for it.

Backends: two new hand-written backends, 452 + 279 = 731 LOC.
That's the resource-specific translation work no substrate can
absorb. Per-backend LOC is dominated by HTTP-mux boilerplate
(widget) and by the `UnimplementedBackendServer`-embed +
protobuf-field bookkeeping (gadget). Both numbers are inflated vs
a production distillation because each backend is self-contained
rather than sharing a library — deliberately, per the repo's
"don't premature-abstract across experiments" rule.

## Fundamentals touched

**Resource modeling freedom** (primary). One binary hosts two
aggregated APIs declared as config CRDs, talking two transports
(HTTP/SSE and gRPC), with two different watch modes (push and
poll) — all configurable per APIDefinition with zero code change
per-API. Fundamental: once the middleware is resource-agnostic
(it is) and transport-agnostic (it is, for the two transports
0026 covered), "one Kubernetes API" collapses to "one row in the
APIDefinition table." The cost an operator pays to add a third
API is one `APIDefinition` YAML + one backend Deployment.

**Storage independence** (primary). The fifth storage axis
(business data on backend + KRM metadata on host CRD) now sits
first-class in the substrate. Two backends with different internal
storage implementations (both in-memory here, but each chose
independently — gRPC backend uses raw JSON bytes keyed by ns/name,
HTTP backend uses struct-typed with separate status) both compose
with the same MetadataStore. The MetadataStore is agnostic to
which backend stored the business data; it only cares about
`(group, resource, namespace, name)`. This confirms the 0024
thesis holds when lifted to substrate: the split is not per-
experiment-custom, it's a substrate primitive.

**Wire protocol fidelity** (secondary). Every wire-level behavior
0024-0029 validated individually held *compositionally* in one
process: stitched Get, unified RV, push + poll watch side-by-side,
initial-events-end BOOKMARK (kubectl get -w fires its initial
ADDED within sub-second), 422 wire shape on admission denial. The
`#/definitions/` fix from 0024 is emitted by the substrate
automatically — not re-verified in this experiment because no
strict OpenAPI consumer (like ArgoCD's cluster cache) was wired
here.

**Watch and consistency semantics** (secondary). Two backends with
different watch capabilities in one middleware, driven by
descriptor-level selection (`WatchCapability: push | poll`). Both
produced client-observable events in the 3s window; neither showed
bookkeeping errors in the logs. The `ModePush` consumer stream
survives the backend's SSE keepalive cycle (20s interval) and the
`ModePoll` loop's 15s poll cycle doesn't block the process.
The substrate's per-descriptor dispatch is clean.

## Consequents

- **cel-go v0.22.0** pinned via `runtime/component/v2/admission`.
  Unchanged from 0029. If upstream CEL gets stricter about
  boolean-output type inference, the substrate's
  `cel.BoolType`-gate at compile time will rain warnings; this
  is the existing 0029 consequent and doesn't move here.
- **`.spec.title` vs `spec.title`**: the JSONPath leading-dot
  silent-no-op is a consequent of `strings.Split(p, ".")` as the
  tokenizer — changeable without any behavior redesign.
- **`ObjectMeta`-missing error message is scary**: the SSA
  failure surfaces as
  `".metadata: schema error: no type found matching:
  io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"`. Operators
  reading this cold will assume their deployment is broken.
  The substrate or the multiplex reconciler could translate this
  into a user-facing condition on the APIDefinition ("SSA not
  supported for dynamically-installed groups; see known-gap
  docs"). Consequent; aesthetic; v2.1-worthy.
- **Kind cluster `aggexp-0031`**: torn down at completion.
- **Go 1.24 pin, k8s.io/apiserver v0.32.3 pin**: identical to
  the root module. Zero version bumps were necessary.
- **`deploy/manifests/50-apiservice.yaml` is wrong for
  multiplex**: a consumer rolling out with the default base
  manifest gets a stale `v1.aggexp.io` APIService that
  shadow-aggregates nothing. Consequent of the repo's
  single-APIService-per-binary assumption, not of v2 itself.

## What this changes for SYNTHESIS and EXPERIMENTS

**SYNTHESIS.** Add one datapoint to the "Current state" listing:
first post-promotion consumer of v2 landed, no substrate patches
required. No fundamental shift — the substrate extraction held
cleanly, which is what 0021 showed for v1 and 0031 shows for v2.
The consumer-rough-edges above are substrate-polish candidates
and don't change the problem-space understanding.

**EXPERIMENTS.** Mark 0031 complete. New candidates the rough
edges surface (not urgent; queued):

- `v2-admission-jsonpath-leading-dot-reject`: trivial substrate
  change, trivial confirmation experiment.
- `v2-multiplex-gc-per-api`: wire a `gc.Config` into
  `multiplex.Options` and spawn one GC per APIDefinition.
  Moderate substrate change; meaningful for multi-API operators.
- `v2-ensure-crd-helpers`: add `metadatastore.EnsureCRD` and
  `multiplex.EnsureAPIDefinitionCRD`. Trivial.
- `dynamic-install-openapi-refresh`: the library-level fix for V3
  endpoint refresh + SSA typed-converter rebuild. This one was
  already queued by 0030; 0031 confirms the behavior boundary is
  where 0030 said it was.

The stateful-middleware-refinement arc (0022-0031) is complete.

## Open questions raised

- **Does this shape scale to many APIDefinitions?** 0031 has two.
  0027 had three. Nobody's tried, say, 30. Goroutine count
  per-AA would become a concern; so would the multiplex
  reconciler's sequential `InstallAPIGroup` path (no lock, but
  the library's internals may or may not be concurrent-install-
  safe). A stress experiment would clarify.
- **Cross-API declarative admission**: can CEL in one
  APIDefinition reference another resource? 0029 said no (no
  lookups); this experiment confirmed the single-resource
  boundary by not attempting cross-resource rules. The backend-RPC
  path can do cross-resource lookups — a consumer that needs them
  would fall back to `supports_validation: true` in the backend
  and write a gRPC Validator.
- **Polyglot didn't get exercised**: both backends are Go here.
  The transport swap is the interesting thing, and that worked,
  but a Python gRPC gadget backend would be a stronger 0019-
  reinforcement. Not on 0031's critical path.
- **Annotation-based tracking under multiplex**: 0024 proved
  ArgoCD's tracker doesn't double-track when metadata lives in
  `ResourceMetadata.spec.metadata.annotations`. Under multiplex
  with two APIs, does the tracker cope with two distinct
  APIServices registered dynamically? Untested here; should work
  architecturally but a probe would be low-cost.

## Status

Complete. The substrate held. Kind cluster `aggexp-0031` torn
down at the end of the task.
