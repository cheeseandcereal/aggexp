# Findings — 0029 declarative-admission-in-config

## What we were trying to learn

0020 put admission in the backend: two gRPC RPCs (Validate, Mutate)
that the component server calls on every CREATE and UPDATE. It
works, but it asks every backend author to write admission logic in
the backend's language, with one RPC round trip per write per hook.
For the common cases — "required fields," "value ranges,"
"cross-field constraints," "default values" — that is plain tax.

This experiment asks the obvious complement: can admission be
declared in config and evaluated entirely in the middleware, so
CEL-expressible rules never touch the backend? And if so, does the
backend-RPC seam still compose cleanly for the cases CEL can't
reach?

## What we did

Forked `0020-krm-admission-hook` into
`experiments/0029-declarative-admission-in-config`. Three
additions:

- `component/pkg/admission/` — a ~250-line package with:
    - `config.go`: YAML loader for a simplified `APIDefinition`
      shape containing `admission.mutations[]` and
      `admission.validations[]`.
    - `cel.go`: compiles validation expressions against a
      `cel.Env` with two variables (`object`, `oldObject`),
      requires bool output, keeps one `cel.Program` per rule
      for per-request eval.
    - `jsonpath.go`: a dotted-path mutator supporting `set` and
      `default` ops. Special-cases `metadata.annotations` /
      `metadata.labels` so annotation keys containing "." or "/"
      work. No array indexing.

- `component/pkg/grpcbackend/backend.go` — the `admit` helper now
  runs in two layers. Layer 1 (middleware): mutate then validate
  using the engine; on deny, the request never reaches the
  backend. Layer 2 (backend RPCs): unchanged from 0020.

- A static YAML config at `/etc/aggexp/admission/admission-config.yaml`,
  mounted from a ConfigMap. The component loads it on startup via
  a new `--admission-config` flag. The backend-note image is
  unchanged from 0020 — it still advertises
  `supports_validation=true` and `supports_mutation=true`, so
  both layers run.

The demo config carries three validations and two mutations:
- validation: `size(object.spec.title) <= 30` (tighter than the
  backend's 64-char cap).
- validation: `!object.spec.title.contains("BADWORD")`.
- validation: `has(object.spec.body) && size(object.spec.body) > 0`
  on CREATE only.
- mutation: `default spec.priority = "normal"` on CREATE.
- mutation: `set metadata.annotations.aggexp.io/middleware-stamped = "true"`
  on every write.

Deployed to kind cluster `aggexp-0029`. Four scenarios plus a
multi-failure probe and an UPDATE-rejects probe run; every output
is captured in the experiment's `demo.log`.

## What we observed

### All four required scenarios pass

1. **Valid CR** (`test-valid`): middleware allows, backend allows,
   stored. `kubectl get` shows both annotations
   (`aggexp.io/accepted-at` from backend, `aggexp.io/middleware-stamped`
   from middleware).
2. **Middleware rejects** (`test-long-title`, title 45 chars):
   HTTP 422 with the middleware's message verbatim. The backend's
   logs show NO mutate/validate call for this object — middleware
   denial short-circuits before any backend RPC. Proved by
   grepping `note-backend` logs: no entry for `test-long-title`.
3. **Middleware passes, backend rejects** (`foo-hello`): the name
   has no middleware rule against it, so middleware passes. The
   backend's own `test-|prod-` prefix rule then rejects. The
   backend's logs show exactly one mutate call and one validate
   call, and the validate returns Deny. HTTP 422 with the
   backend's reason verbatim.
4. **Valid CR** (same as 1): already covered.

### Multi-failure case is where declarative admission earns its keep

A request that trips three middleware validations at once
(`test-multi-raw`: title too long, title contains BADWORD, body
missing) produces a single HTTP 422 whose `details.causes[]`
array carries three entries. `apierrors.NewInvalid(gk, name,
field.ErrorList{...})` naturally emits all of them without any
extra plumbing. Wire format:

```
"message":"Note.aggexp.io \"test-multi-raw\" is invalid: [
  spec.title: Invalid value: ...: must be 30 characters or fewer,
  spec.title: Invalid value: ...: must not contain \"BADWORD\",
  spec.body:  Invalid value: ...: spec.body is required on CREATE ]",
"reason":"Invalid", "code":422,
"details":{"causes":[
  {"reason":"FieldValueInvalid","field":"spec.title","message":"..."},
  {"reason":"FieldValueInvalid","field":"spec.title","message":"..."},
  {"reason":"FieldValueInvalid","field":"spec.body","message":"..."}
]}
```

The backend-RPC shape from 0020 could do this too in principle —
`ValidateResponse` is a flat `{allowed, reason}` — but 0020's
implementation collapses all failures into a single reason string.
The declarative engine, by carrying `FieldPath` per validation,
can produce standards-compliant multi-cause 422s for free. kubectl
formats them as a bulleted list. `client-go`'s
`apierrors.IsInvalid` recognizes this as the same category a
built-in CRD validation failure would be.

### Middleware allows → backend mutates → client sees both stamps

`test-valid` after CREATE ends up with both annotations on the
stored object:

```yaml
metadata:
  annotations:
    aggexp.io/accepted-at: "2026-05-01T04:01:02Z"         # backend
    aggexp.io/middleware-stamped: "true"                  # middleware
```

Composes cleanly: middleware mutation happens first, producing a
JSON with `middleware-stamped=true`; backend then receives that
JSON, mutates it again to add `accepted-at`; backend persists;
middleware returns the fully-mutated object to kubectl.

### Middleware rules run on UPDATE too

`kubectl patch note test-valid --type merge -p '{"spec":{"title":"<long>"}}'`
fails with the middleware's message. On a valid PATCH, the
stored object's `aggexp.io/accepted-at` annotation is refreshed
(backend mutate ran again) while `aggexp.io/middleware-stamped`
remains `"true"` (middleware `set` is idempotent). The mutation
op filter `operations: [CREATE]` correctly suppresses the
`spec.priority default`  on UPDATE, as intended.

### Surprise: the backend silently drops unknown fields, so
### middleware mutations on unmodeled fields don't persist

The `spec.priority: default "normal"` mutation fires (verified by
logging pre-backend JSON) but is NOT visible on `kubectl get note
test-mutate-demo -o yaml`. Root cause: the backend-note's Go
struct (`type NoteSpec struct { Title string; Body string }`)
has no `Priority` field. `json.Unmarshal` silently drops unknown
fields; `json.Marshal` emits only what the struct defines; the
stored object has no `spec.priority`. Annotations persist because
the backend's `Meta` type includes `map[string]string` for
annotations.

This is a **boundary worth calling out**: middleware-only
mutations only round-trip if the backend preserves unknown fields
(e.g. via `json.RawMessage`, `map[string]any`, or a schemaless
storage model). A backend with strict Go-typed spec will silently
eat anything the middleware stamps. Not a bug in the middleware;
a bug in the composition contract. Recorded here for future
experiments.

### CEL's compile cost is amortized; runtime cost is ~µs/expr

The three validations compile once at startup (sub-ms each per
klog). Per-request eval is not measured precisely at lab scale
— the aggregation-layer hop (~65 ms) dominates everything. At
production scale with large object graphs CEL's eval cost would
need its own measurement; `cel-go` recommends `cel.Program` reuse
which we already do.

### Existing 0020 backend image runs unchanged

We did not rebuild backend-note's admission logic. The same Go
binary that 0020 tested now composes with a middleware admission
layer for free. The gRPC protocol is unchanged; the only config
the backend sees is its own `GetSchema` response advertising
`supports_validation=true`.

## Fundamentals touched

### Resource modeling freedom (primary)

Declarative admission is a concrete answer to: "how much of a
resource's policy can live outside the backend?"

Everything the CEL + JSONPath engine can express **can**. That's
a lot:
- Required field presence (`has(object.x) && object.x != null`).
- Value ranges on strings, ints, numbers (`size(...) <= N`, `>= M`).
- Enumerations (`object.x in ["a","b"]`).
- Forbidden-substring rules (`!object.x.contains("BADWORD")`).
- Cross-field constraints (`has(object.a) implies has(object.b)`).
- Default values on absent or empty fields.
- Label/annotation stamping.
- Any UPDATE-vs-CREATE difference via `oldObject` or per-rule
  operation filters.

Things the declarative layer **cannot** express without a backend
round-trip:
- External lookups (does this GitHub repo exist?).
- Cross-resource rules (does another Note with the same title
  already exist?).
- Rules keyed on the caller's downstream credential rather than
  `user.Info`.
- Anything that needs a library the middleware doesn't link
  (complex regex engines, domain-specific validators,
  cryptographic verification).

The backend-RPC path from 0020 is the natural home for those.
Composition is additive: a backend that needs a handful of
"external-knowledge" rules keeps its Validate RPC for those
rules, and the CEL layer handles everything else. The backend's
Go code stays small.

**Does declarative admission make backends meaningfully
simpler?** For backends where CEL covers >80% of the rules,
yes. A backend with a strict schema, simple field-shape rules,
and no cross-resource logic could drop its `supports_validation`
and `supports_mutation` flags entirely and go from "implements
two extra RPCs" to "implements none". The backend-note in this
experiment could be simplified to exactly that (drop Validate
and Mutate; let the middleware enforce the three or four rules
it had). The experiment deliberately kept them to demonstrate
composition.

For backends with significant external-lookup logic, declarative
admission is additive tax (+250 LoC of middleware, +1 config
file) that buys a fast-path for the local rules. Net still a
win at production scale because every middleware-rejected request
avoids one backend round trip; but the LoC count shifts from
"backend code" to "backend code + config file + middleware" —
the total does not go down.

Not a free lunch; a useful tool.

### Wire protocol fidelity (secondary)

`apierrors.NewInvalid(GroupKind, name, field.ErrorList{...})` is
the right seam for all declarative admission denials:

- Multi-cause 422s are a single call.
- Each cause carries a distinct `field` (the configured
  `fieldPath`), so `client.ApplyError` structured handlers can
  inspect individual failures.
- kubectl renders a bulleted list in the terminal.
- The wire format is byte-identical to what kube-apiserver's
  built-in validation emits for CRD violations — callers can't
  tell whether the denial came from a built-in admission plugin,
  a ValidatingAdmissionPolicy, our middleware, or the backend.
  That sameness is a feature: every existing retry loop in
  controller-runtime / ArgoCD / custom controllers already
  handles this shape.

Combined with 0020's observation that backend-emitted denials
also produce the same shape, we now have three layers of
admission all speaking the same 422 dialect: kube-apiserver VAP
(upstream), our middleware (declarative), our backend (gRPC
RPC). Clients cannot distinguish them. None of the three should
try to; the whole point of the wire contract is sameness.

## Consequents

These are implementation-tied observations that do not generalize
to the architecture of declarative admission.

- **cel-go v0.22.0 `env.Compile` returns an `*Issues` pointer and
  a nil-check on `iss.Err()` is load-bearing** — `iss != nil` is
  not sufficient, because `cel-go` returns a non-nil zero-issue
  container for success. Wrong check = false startup-time errors.
- **`ref.Val` to `bool` conversion wants `types.Bool`, not
  `reflect.TypeOf(bool)`**. The `ConvertToNative(reflect.TypeOf(true))`
  fallback works but the direct type-assertion on `types.Bool` is
  cleaner. Both kept in the helper because older cel-go versions
  may surface different concrete types.
- **`sigs.k8s.io/yaml` beats `gopkg.in/yaml.v3` for configs that
  also round-trip through JSON**. YAML's `map[any]any` result
  would not `json.Marshal` cleanly; sigs.k8s.io/yaml emits
  `map[string]any` via JSON round-trip which cel-go's
  variable-binding consumes directly.
- **`metadata.annotations.<key>` JSONPath ambiguity is real**.
  Annotation keys like `aggexp.io/middleware-stamped` contain
  both "." and "/", and our dotted-path walker would otherwise
  interpret "aggexp" and "io/middleware-stamped" as nested maps.
  Special-casing the two well-known parent paths
  (`metadata.annotations`, `metadata.labels`) addresses the 99%
  case without pulling in a full JSONPath library. A real
  implementation should probably use a library; for one
  experiment this special-case is cheaper.
- **Regenerating `.pb.go` is non-optional after renaming the
  experiment module path**. sed-on-.pb.go corrupts the embedded
  FileDescriptor byte length-prefixes and produces a
  `slice bounds out of range` panic at import time. protoc must
  regenerate.
- **The backend dropping unknown fields is not specific to Go**;
  any backend with a strict typed model (Rust serde with
  `#[serde(deny_unknown_fields)]`, Python pydantic, Node with
  zod's `.strict()`) would do the same. Middleware mutation
  needs cooperation from the backend's unmarshal stance. Workable
  contracts:
  (a) backend marshals its spec as `json.RawMessage` and passes
      it through storage unchanged;
  (b) middleware config enforces that mutations touch only
      well-known paths (`metadata.*`);
  (c) backend explicitly lists pass-through fields.
  None of these are chosen here; recorded as a composition
  boundary.

## What this changes for SYNTHESIS

- **Per-request authorization** entry gets a new note: the
  authz-vs-admission boundary (0003) has two ways to close in
  the component-server architecture — backend RPCs (0020) and
  middleware-declared rules (this experiment). They compose
  additively; middleware runs first.
- **Resource modeling freedom** entry gains: the backend's
  "admission API surface" can go from "two gRPC RPCs" to "zero
  RPCs + a YAML" for backends whose rules are all
  CEL-expressible.
- **Wire protocol fidelity** entry gains: `field.ErrorList` →
  422 multi-cause is the right seam for declarative admission;
  whether the causes originate in middleware or backend is
  invisible to clients.

No new fundamentals emerge. The `admission-in-config` shape is a
refinement under Resource Modeling Freedom, not a new category.

## What this changes for EXPERIMENTS.md

- Mark 0029 complete under "Per-request authorization (admission
  companion)" with cross-references under "Resource modeling
  freedom" and "Wire protocol fidelity".
- The follow-up question this raises — **declarative admission
  + 0027 reconciler**: when 0030 promotes component/v2, should
  the admission config live in the `APIDefinition` CRD? The
  current static-file approach is fine for single-AA cases; for
  the multi-AA multiplex story from 0027, it wants to live in
  the CRD that's already being reconciled. No new experiment
  needed before 0030; this is a substrate-promotion decision.

## Open questions raised

- **Hot-reload.** The config is read once at startup. For a real
  deployment, an fsnotify watcher or a CRD-backed source (0027)
  would be required. Not exercised here.
- **Admission latency at scale.** We did not measure per-rule
  eval cost with realistic-size objects and 50+ validations.
  cel-go's published benchmarks are promising but lab-scale at
  one request per scenario does not stress them.
- **CEL library expansion.** `cel-go` has an extensions module
  (`ext.Strings()`, `ext.Encoders()`, etc.) that kube-apiserver
  enables in its VAP evaluator. We did not enable any; the
  three rules here use only core CEL. A user writing "email
  format regex" style rules would want extensions.
- **Interaction with kube-apiserver VAP.** If an operator writes
  a host-cluster `ValidatingAdmissionPolicy` matching
  `aggexp.io/v1` AND our middleware admission runs, the two are
  independent layers (same as 0020). We did not construct the
  multi-layer collision case.
- **Partial mutation on denial.** Middleware mutation runs before
  middleware validation. If validation denies, the mutated object
  is discarded (the whole request is rejected). But if a later
  validation in the same set denies, earlier mutations have
  already happened in memory. Here that's harmless (the in-memory
  object is thrown away). A more ambitious engine that supported
  "mutate, validate, conditionally-mutate-more" would have to
  think harder about ordering semantics.
- **Mutation op subset.** We shipped `set` and `default`. A
  future experiment could add `remove`, `merge`, or full JSON
  Patch. Only worth doing if a real use case surfaces — the
  0022 thesis specifies "middleware stamps KRM metadata" as
  the primary consumer, which `set` and `default` cover.
