# Findings — 0023 schema-source-exploration

## What we were trying to learn

This is the first empirical experiment in the
stateful-middleware-refinement arc. `FINDINGS/0022` committed the
arc's interface commitments; this one probes a specific open
question: where should the OpenAPI schema that drives
`kubectl explain` and SSA come from?

Three candidates, enumerated as `thesis.SchemaSource` values in
0022:

- **Track A (backend-ships-openapi).** Backend returns full
  Kubernetes-flavored OpenAPI v3 from `GetSchema`. Status quo
  from 0017/0021.
- **Track B (middleware-synthesizes).** Backend returns a plain
  JSON Schema of just spec+status. Middleware lifts it to full
  Kubernetes OpenAPI.
- **Track C (config-resident).** Full OpenAPI lives in an
  `APIDefinition` CRD on the host cluster. Middleware reads it
  at startup; backend serves only CRUD+watch.

All three must produce identical kubectl behavior. The question the
experiment is designed to answer is not "which works" (all three do)
but "which costs the backend author least and the operator least
for the same observable behavior".

## What we did

One kind cluster, `aggexp-schema-src`. Three deployments in sequence,
each torn down before the next. Same `notes.aggexp.io/v1 Note`
resource shape across all three:

- `spec.title`: string, required, minLength 3 / maxLength 64
- `spec.body`: string, optional
- `status.updatedAt`: string, format date-time

Per track, six kubectl scenarios: `api-resources`, `apply`,
`get -o yaml`, `explain note.spec`, `apply --server-side`, and
`get notes -w` with a live annotation injected at t+2s. Output
captured verbatim.

The three tracks share the same CRUD path in Go; they differ only
in how the middleware obtains the schema. Track B adds a
`synthesis` package with one function — `LiftJSONSchemaToOpenAPI(
gvk, jsonSchema)` — that stamps apiVersion/kind/metadata/GVK-ext
onto a plain JSON Schema. Track C adds an `APIDefinition` CRD and a
startup loader (dynamic client reads a single CR by name; no watch,
no reconciler — just a config-time read).

## What we observed

### All six scenarios passed identically across all three tracks

The same kubectl commands produced byte-identical user-facing
output (modulo uids and timestamps) on A, B, and C:

| Scenario                     | A   | B   | C   |
|------------------------------|-----|-----|-----|
| `api-resources` lists notes  | PASS | PASS | PASS |
| `apply` creates Note         | PASS | PASS | PASS |
| `get -o yaml` round-trips    | PASS | PASS | PASS |
| `explain note.spec`          | PASS | PASS | PASS |
| `apply --server-side`        | PASS | PASS | PASS |
| `get notes -w` streams events | PASS | PASS | PASS |

SSA `managedFields` population was identical on all three: a single
`alice` entry with `fieldsV1: {f:spec: {f:body: {}, f:title: {}}}`.
`explain note.spec` rendered the per-field descriptions each
backend (or CRD author) wrote into the spec sub-schema. Watch
delivered one ADDED + one MODIFIED per the probe-annotate sequence,
as expected.

**The core wire-protocol claim holds for all three schema sources.**
From the caller's perspective, where the OpenAPI originates is
invisible.

### Track B's lift is cheap and deterministic

The synthesis function is 127 lines of Go, most of which are
comments. The actual work is:

1. Unmarshal input JSON Schema as `map[string]any`.
2. Ensure `type: object`.
3. Add apiVersion / kind / metadata properties (fixed boilerplate).
4. Attach `x-kubernetes-group-version-kind` extension.
5. Marshal back.

Observed byte sizes on Track B's run: backend shipped 568-byte
plain JSON Schema; middleware produced 1031-byte lifted OpenAPI.
The overhead is metadata-ref (the biggest single string), the GVK
extension, and the apiVersion/kind pair.

The key observation is that the lift is **purely mechanical**.
There is no resource-specific logic in `synthesis.go`; the same 127
lines serve any resource. The only Kubernetes-specific knowledge
lives in one place (middleware), not in every backend.

### Track C's APIDefinition CRD works as a configuration seam

The component's `apiDefSpec` struct is 20 lines; the startup
loader (`loadAPIDefinition` + `extractSpec`) is 37 lines of Go.
Total overhead for the config-resident path on the component side:
~60 lines and one new cluster permission (`get/list/watch` on
`apidefinitions.aggexpapidefinition.aggexp.io`).

Startup races are benign: the loader retries the CR read every 2s
for up to 60s before giving up. In practice the APIDefinition is
applied before the Deployment, so the first Get succeeds in
~0.1s. The loader logs total OpenAPI byte count for operator
observability: `loaded APIDefinition notes-aggexp-io: ... openapi-bytes=1346`.

### The ergonomics table

For each track, enumerate what the backend author writes (the
primary audience — everything else is middleware / operator).

| Aspect                             | Track A                                    | Track B                                 | Track C                             |
|------------------------------------|--------------------------------------------|-----------------------------------------|-------------------------------------|
| Who writes OpenAPI                 | Backend author                             | Backend author (plain JSON Schema)      | Operator / API author (YAML)        |
| Backend's `GetSchema` LOC          | ~66 (full K8s-dialect schema builder)      | ~55 (plain JSON Schema only)            | ~12 (stub; never called)            |
| Kubernetes concepts backend needs  | 4: `x-kubernetes-group-version-kind`, `apiVersion`/`kind` fields, `$ref` to ObjectMeta, List wrapper | 0                                       | 0                                   |
| Middleware-side code               | 0 (uses `runtime/component.Run` as-is)     | +127 LOC synthesis + 196 LOC custom Run | +282 LOC custom Run w/ dynamic client |
| Cluster artifacts                  | None                                       | None                                    | APIDefinition CRD + one CR instance |
| RBAC on host cluster               | None                                       | None                                    | Component SA needs read on `apidefinitions` |
| Failure mode if schema wrong       | `kubectl explain` degrades; SSA can fail   | synthesis error at startup; component exits | parse error at startup; component exits |
| Schema-update flow                 | Rebuild & redeploy backend                 | Rebuild & redeploy backend              | `kubectl apply` on APIDefinition; restart component |
| Distance between backend code and schema | Same file                            | Same file (simpler schema)              | Separate YAML                       |
| Polyglot-friendliness              | High but each language reimplements K8s OpenAPI conventions | High; author writes plain JSON Schema | Highest — no schema code in any language |

#### Concrete tooling per language

**Go backend author**

- Track A: `map[string]any` literal with four Kubernetes-specific
  keys (`x-kubernetes-group-version-kind`, `apiVersion`, `kind`,
  `$ref: .../v1.ObjectMeta`). No external dependency, but the
  author has to know the four incantations. Alternative:
  `kube-openapi`'s `openapi-gen` from typed Go structs — requires
  codegen (+`+k8s:openapi-gen=true` markers, +`zz_generated_openapi.go`
  output, ~+300 LOC after generation). That's what 0002 uses; it
  wasn't chosen for 0017/0021 and isn't chosen here because it
  drags in far more Kubernetes machinery than the component pattern
  wants.
- Track B: same `map[string]any` literal, but stripped of all four
  Kubernetes-specific keys. Reads like any OpenAPI-describable
  object. Alternative: `kin-openapi`, `invopop/jsonschema` — both
  produce plain JSON Schema from Go structs.
- Track C: no schema code; a 27-line YAML block in `APIDefinition`
  spec. Written once. Alternative: any API-authoring tool
  (`oasdiff`, Stoplight, Swagger Editor).

**Python backend author** (evidence from 0019)

- Track A: hand-rolled dict with the same four K8s-dialect keys.
  `pydantic` + `pydantic.json_schema` produces plain JSON Schema
  automatically but does not add the Kubernetes extensions; the
  author still has to post-process. OSS bridge like
  `kubernetes-validate` exists but targets validation, not schema
  emission. ~60 LOC of hand work plus the dict literal.
- Track B: `pydantic.BaseModel.model_json_schema()` — one line.
  The author writes `class NoteSpec(BaseModel): title: str =
  Field(min_length=3, max_length=64); body: str | None = None`
  and hands the schema to the middleware. Zero Kubernetes concepts
  on the backend side.
- Track C: no code at all. Schema lives in cluster config.

**Rust backend author**

- Track A: schema as a `serde_json::json!{}` literal with the same
  four K8s-specific keys. No mainstream crate emits Kubernetes
  OpenAPI — `kube-rs` consumes it but doesn't generate it. Author
  hand-rolls ~40-60 LOC.
- Track B: `schemars` crate on a struct + `#[derive(JsonSchema)]`
  produces plain JSON Schema. 1 derive macro, 0 Kubernetes
  concepts.
- Track C: no code.

**Node / TypeScript backend author**

- Track A: dict literal with the four K8s-specific keys, or
  `zod-to-json-schema` plus manual post-processing to add the
  extensions.
- Track B: `zod-to-json-schema` on a `zod` schema. Single call,
  widely-used ecosystem tooling, no Kubernetes concepts.
- Track C: no code.

### Total concept count

Counting the distinct Kubernetes concepts a backend author must
learn before they can ship a correct schema:

| Track | Concepts |
|-------|----------|
| A     | 4 (GVK extension key, ObjectMeta $ref, apiVersion/kind properties, list wrapper) |
| B     | 0 |
| C     | 0 (backend); 4 for the API author writing the APIDefinition |

Track C is "Track A with the concepts moved from backend code to
cluster config". B is "Track A with the concepts abstracted into
middleware".

### What surprised us

**How cleanly the lift generalizes.** We expected Track B's
synthesis to need per-resource hooks — some way for the backend to
say "this list field is type=map with key=X" — but the Note
schema didn't exercise that. The lift is a fixed transformation
that adds apiVersion/kind/metadata/GVK-ext and does nothing else;
anything the backend puts inside spec/status passes through
verbatim. The 0017 finding that SSA works against
`map[string]interface{}` content under the typed `dyn.Object`
wrapper carries forward unchanged: SMD deduces field merge
strategy where no schema annotation exists.

**How low the operator overhead on Track C turned out to be.**
We expected the CRD install + instance apply + component-SA RBAC
to feel like three steps; in practice the CRD install is a
one-time thing per cluster and the instance is one YAML. The
startup-race we worried about in the README didn't materialize:
the loader's 2-second retry swallows any "CRD not yet available"
window, and in production the APIDefinition would be applied
declaratively by whatever provisioned the component Deployment
anyway.

**How similar the three GetSchema results look from the
middleware's perspective.** After passing through
`openapi.ParseBackendSchema`, all three tracks ended up with
essentially the same `spec.Schema` object in the defs map. The
stamping that `ParseBackendSchema` does (adding the GVK extension
if absent) acts as a defensive backstop for Track A, a
complementary pass after synthesis for Track B, and the
entrypoint for Track C. The substrate's existing OpenAPI handling
is source-agnostic.

### What was hard

Nothing substantive. Each track built and deployed cleanly. The
only rough edge was my own typo: in the first draft of Track C's
component I wrote a Go `import` declaration mid-file to disambiguate
`k8s.io/client-go/rest` from `k8s.io/apiserver/pkg/registry/rest`.
Fixed by moving to top-of-file aliased imports
(`clientgorest "k8s.io/client-go/rest"` + `apiserverrest
"k8s.io/apiserver/pkg/registry/rest"`). No signal for the
experiment.

## Recommendation

**The rest of the arc (0024-0027) should standardize on Track B —
middleware-synthesizes.**

Rationale:

1. **Lowest backend-author concept count** (0 Kubernetes concepts)
   with a **single artifact to update** when the schema evolves
   (the backend's own source code). Track C has the same zero-
   concepts property on the backend side, but splits schema
   evolution across two artifacts (backend + APIDefinition CR)
   with different update cadences; the first time they drift is
   a quiet runtime mismatch.

2. **Matches the arc's middleware-knows-Kubernetes, backend-doesn't
   partition cleanly.** 0022's commitment was that the middleware
   owns KRM concerns and the backend owns business data. A
   Kubernetes-dialect OpenAPI is a KRM concern; a plain JSON
   Schema of the business data is business data. Track B is the
   version of schema handling that honors this line; Track A
   breaks it (backend knows K8s); Track C breaks it in the
   opposite direction (schema lives nowhere near the backend).

3. **Cheapest polyglot story.** Every ecosystem has a mainstream
   plain-JSON-Schema generator: `pydantic` in Python, `schemars` in
   Rust, `zod-to-json-schema` in TypeScript. None of them emit
   Kubernetes-dialect OpenAPI. Track B is the one path where the
   backend author uses their language's default tooling and
   hands the output to the middleware unchanged. Track A forces
   either hand-crafted dicts or downstream post-processing in
   each language.

4. **Best failure mode.** Track A's failure mode is silent
   degradation: ship a slightly wrong OpenAPI and kubectl explain
   goes fuzzy, SSA silently corrupts managedFields. Track B's
   failure mode is a hard startup error in synthesis — the
   component refuses to come up if the input isn't valid JSON
   Schema, so the failure is caught once at deploy time.
   Track C has the same hard-error property but over a separate
   cluster artifact, which is operationally noisier.

5. **Smallest middleware code delta from the 0017/0021 substrate.**
   Track B adds 127 LOC of synthesis and 196 LOC of a custom
   startup (total 323). Track C adds 282 LOC of custom startup
   plus dynamic-client dependency plus an entire CRD definition.
   When 0030 promotes v2 of the runtime, Track B's synthesis
   fits as one optional transform step in the schema-loader
   pipeline; Track C's config-resident mode is a separate schema
   source that imports client-go/dynamic.

### Tradeoffs flagged for the rest of the arc

- **Track B depends on the spec.Schema-parser tolerating plain
  JSON Schema after the lift.** Today
  `componentopenapi.ParseBackendSchema` is a plain
  `json.Unmarshal` into `spec.Schema`; any future tightening of
  what the parser accepts needs to preserve this compatibility.
- **Track B's lift is Note-shape-neutral.** Resources with lists
  that need `x-kubernetes-list-type: map` + `x-kubernetes-list-map-keys`
  will require the backend to emit those extensions directly in
  its JSON Schema, because synthesis doesn't infer them. This is
  fine — those extensions are shallow additions to a JSON Schema
  and don't constitute "learning Kubernetes OpenAPI" — but
  document it in 0030's substrate doc.
- **Track C stays as a supported `SchemaSource` value.** The
  arc's multiplex reconciler (0027) needs an `APIDefinition` CRD
  for its own reasons (watching AA-definition CRs is how it
  registers/deregisters APIServices). Once that CRD exists, a
  config-resident schema becomes effectively free; a user who
  prefers it can continue to use it. Track B is the default; C
  is an escape hatch.
- **Track A is retired.** Keep the backend-ships-OpenAPI wire
  path supported (backends from 0017/0018/0019/0021 still use
  it) but deprecate it for new backends. New backends should ship
  plain JSON Schema.

## Fundamentals touched

**Wire protocol fidelity** (primary). Three observations add to
SYNTHESIS:

- **The wire contract for `kubectl explain` is source-agnostic.**
  Given the correct `spec.Schema` object with GVK extension keyed
  at the Go canonical name, the library produces identical
  explain output regardless of whether the schema arrived via
  backend RPC, middleware synthesis, or host-cluster config read.
- **The wire contract for SSA is source-agnostic.** Same
  managedFields emission + conflict detection across all three
  tracks, once a typed Scheme wrapper is registered (0017's
  finding, unchanged).
- **The Kubernetes-dialect OpenAPI is a small, mechanical
  transformation of plain JSON Schema.** The adapter is 127
  lines; the extensions it adds are fixed boilerplate. This is
  the sharpening 0017 left open — "what's the minimum OpenAPI to
  make the library happy" — answered concretely: a plain JSON
  Schema + GVK extension + apiVersion/kind/metadata boilerplate.
  Everything else is advisory.

**Resource modeling freedom** (secondary). The typed-vs-
unstructured line from 0013/0017 carries forward unchanged — the
typed wrapper (`runtime/component/scheme.Object`) is still
required for SSA, and the content remains an untyped bag. What's
new is that the backend author no longer needs to know this;
Track B's lift handles the Kubernetes-facing side end-to-end, and
the wrapper is middleware-internal.

## Consequents

- **kube-openapi's `$ref` format is `#/components/schemas/<reverse-dns-name>`.**
  The metadata property synthesis injects uses
  `#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta`.
  This is the format the composed defs map already uses; keying
  it any other way silently produces an unresolved ref and
  explain shows nothing under metadata. Not a synthesis-specific
  consequent; carries from 0017.
- **`kind delete cluster` after each track is not necessary** if
  you just want to redeploy. Deleting the APIService +
  Deployments + ClusterRole(Binding)s + Services + RBAC for the
  APIDefinition CRD is enough to fully reset. Noted because the
  task template suggests a per-experiment cluster and this one
  shares across tracks.
- **Track C's dynamic client refuses the in-cluster kubeconfig
  fallback when no ServiceAccount is bound.** The
  `clientgorest.InClusterConfig()` path needs the default
  ServiceAccount token; we use the `aggexp` SA from the base
  manifests which already has `/var/run/secrets/kubernetes.io/serviceaccount/`
  projected, so this was transparent — but a user stripping
  that projection would get a startup error.
- **Go 1.24 pin** and **gnostic-models@v0.6.8** — recurring arc
  consequents. Both honored via the root go.mod.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**: the Wire protocol fidelity section's
"backend-supplied OpenAPI threaded through the defs map" claim
from 0017 broadens — the same wire behavior holds whether the
OpenAPI is supplied by the backend, synthesized by the middleware
from a plain JSON Schema, or read from a host-cluster CRD. Which
of those is chosen is an authoring-ergonomics decision, not a
wire-fidelity one. Resource modeling freedom gains a note that
"backends don't need to know Kubernetes OpenAPI dialect" is now
achievable (Track B / Track C); the typed-wrapper-in-middleware
pattern from 0017 composes cleanly with a non-K8s-aware backend.

For **EXPERIMENTS**: 0023 is complete. 0024-0027 standardize on
Track B's synthesis pattern as the default schema source; Track C's
config-resident path is preserved as a `SchemaSourceConfig` value
in the APIDefinition enum (as the thesis already commits). Track
A's `SchemaSourceBackend` value stays supported for the existing
0017/0018/0019/0021 backends but is no longer the recommended
default for new backends.

## Open questions raised

- **Keyed-list merge behavior under the synthesis path.** 0017
  left this open for the `dyn.Object` / untyped-content path; it
  remains open under Track B. A synthetic Note-plus-containers
  schema with `x-kubernetes-list-type: map` (shipped by the
  backend verbatim through synthesis) would answer whether SMD
  honors those extensions when the Go field is
  `map[string]interface{}`. Not blocking the arc.
- **Schema drift between backend and APIDefinition in Track C.**
  A backend that grows a new optional field silently and an
  APIDefinition that isn't updated will accept the new field in
  CREATE (pass-through via unstructured content) but won't
  display it in explain or participate in SSA. Production
  tooling would want a `backend.GetSchema` → APIDefinition
  reconciler that flags drift. Out of arc scope.
- **Runtime schema updates in Track B.** Shipping a new
  `GetSchema` response from the backend without restarting the
  middleware is untested. The 0022 thesis commits to dynamic
  APIDefinition reconciliation (0027); for Track B that means
  re-calling `GetSchema` periodically or on some signal. Not in
  scope here.
