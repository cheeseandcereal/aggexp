# FINDINGS: 0046 — openapi-first-codegen

## What we were trying to learn

Whether a code generator that consumes one hand-authored OpenAPI v3
document can emit *all* the Go artifacts an aggregated API needs —
typed structs, deepcopy, dual-version scheme registration with
conversions, openapi-gen-shaped definitions, and field-label
conversions — such that a tiny AA built on the generated types passes
`kubectl explain`, `kubectl apply --server-side`, and field-selector
listing with **zero hand-written scheme/types/openapi-gen
boilerplate**. And, secondarily: where does the OpenAPI-subset boundary
fall — which constructs resist clean Go codegen?

This is the generator that 0023's schema-source exploration pointed at
(its recommended Track B synthesizes a schema at request time for the
*middleware* path; 0046 is the *library/typed* path it never built).

## What we did

Built `cmd/oapigen`: a four-stage generator (parse → resolve
intra-document `$ref`s → identify spec/status components from a small
YAML config → synthesize the `<Kind>`/`<Kind>List` shape and emit Go).
Parsing is delegated to `github.com/getkin/kin-openapi` v0.131.0; the
type mapping and all emission are hand-rolled over a small intermediate
representation (`Model`/`GoStruct`/`GoField`) so the table-driven
mapping logic is isolated from both the parser and the emitters.

The generator emits seven files into `pkg/apis/widgets/v1/`: `doc.go`
(carrying the input OpenAPI SHA-256), `types.go`, `register.go`,
`conversion.go`, `fieldlabels.go`, `zz_generated.deepcopy.go`, and
`zz_generated.openapi.go`. A `cmd/verify-aa` wires that generated
package onto `runtime/library` + `runtime/group` + `runtime/server`
over an in-memory backend. We deployed it to kind cluster `aggexp-0046`
and ran the four required scenarios.

The input is a 130-line OpenAPI document exercising the full v1 type
table: string, string+enum, string+date-time, integer (int32 and int64
default), number (float and double default), boolean, array<scalar>,
array<$ref>, object+additionalProperties (map), inline object,
free-form object, `$ref`, and optional-vs-required.

## What we observed

**All four scenarios passed.**

1. **explain.** `kubectl explain widget.spec` renders the generated
   field descriptions, the `-required-` markers (driven by OpenAPI
   `required`), the nested `Coordinates` `$ref` (recursable via
   `explain widget.spec.coordinates`), the `[]WidgetCondition`
   array-of-ref under status, the `map[string]string`, and the
   free-form `<Object>`. This proves the GVK extension + defs-map
   wiring (0002/0017): the openapi-gen-shaped `GetOpenAPIDefinitions`,
   merged with the substrate's baseline meta/v1 defs, threads through
   the apiserver's explain path.

2. **server-side apply.** `kubectl apply --server-side` persists
   managedFields with per-leaf ownership (`f:spec.f:color`,
   `f:spec.f:size`). A second field-manager mutating a field owned by
   the first gets a real conflict (`conflict with "mgr-a":
   .spec.color`); `--force-conflicts` resolves it. This is the
   internal-version-registration + typed-converter path from
   0002/0017, generated entirely mechanically.

3. **field/label selectors.** `spec.color=red`, `status.phase=Ready`,
   the compound `spec.color=red,spec.size=5`, and label selectors all
   filter correctly; an unknown field (`spec.bogus`) returns the
   library's BadRequest. The generated `AddFieldLabelConversionFunc`
   registration (the hard gate from 0037) plus the generated
   `FieldAccessor` + `SelectableFields` slice are consumed directly by
   `runtime/library`'s `FieldSelectorOptions` — the consumer writes
   nothing.

4. **reproducibility.** Re-running the generator leaves a clean tree.
   `hack/gen.sh` regenerates and asserts `git diff` is empty on the
   generated package. Two back-to-back runs produced byte-identical
   output. Determinism comes from sorting struct/enum/field/dependency
   order and using no clock, hostname, or randomness.

From 130 lines of OpenAPI we generate 776 lines of Go (types 100,
deepcopy 195, openapi defs 334, the rest registration/conversion/
field-labels/doc). The generator itself is ~1,630 lines; verify-aa +
backend is ~300.

## What surprised us

**Identity conversions are enough; a separate internal package is a
trap for a generator.** The instinct (and 0034's hand-written shape) is
to emit a distinct internal "hub" package with field-by-field
conversions. We prototyped that and abandoned it: Go struct conversion
(`internalversion.WidgetSpec(in.Spec)`) requires *identical* field
types, but a nested struct field like `Coordinates` is a *different
type* in the internal package, so the conversion does not compile.
Generating recursive field-by-field converters for arbitrary nesting is
doable but is a lot of fragile code. The 0037 model — register the same
Go type under both the external and internal GVs with a deep-copy
"identity" conversion — sidesteps the whole problem and still satisfies
the SSA/PATCH internal-hub requirement. For a *generator* (as opposed
to hand-written code), the same-type-under-both-GVs shape is strictly
better.

**`kubectl explain` does not surface enum values.** `spec.color` is a
generated named-string type with `red`/`green`/`blue` constants and the
OpenAPI `enum` is present in the def, but explain renders it as a plain
`<string>` with no enumerated values listed. The enum constraint *is*
enforced where it matters (the field selector and the wire schema), but
the human-facing explain output doesn't show it. This is a kube-openapi
/ kubectl rendering behavior, not a generator gap.

**`go mod tidy` under a newer toolchain silently bumps the `go`
directive.** Running tidy with a go 1.26 toolchain rewrote the
experiment's `go 1.24` to `go 1.26.0`, which then broke the
`golang:1.24-alpine` Docker build with a cryptic "requires go >=
1.26.0". Pinning back to `go 1.24` and building with `GOTOOLCHAIN=local`
fixes it. Worth knowing for any experiment with its own `go.mod`.

## Fundamentals touched

### Wire protocol fidelity (primary)

Confirmed: an OpenAPI-first generator can produce the exact artifacts
0002/0017/0024/0037/0038 found load-bearing — typed structs, a
dual-version scheme, GVK-stamped defs with the ObjectMeta ref-callback
and `Dependencies` list, `#/definitions/...`-compatible meta refs (via
the substrate baseline), and field-label conversions — such that
explain + SSA + selectors all work with no hand-written boilerplate.
The OpenAPI document is the single source of truth; everything
Kubernetes-shaped (apiVersion/kind/metadata/TypeMeta/GVK/deepcopy/
conversions) is synthesized. The schema author writes plain OpenAPI and
needs to know nothing about the Kubernetes OpenAPI dialect — the same
"backend author doesn't speak Kubernetes" property 0023 Track B argued
for, but realized in the typed-library path.

### Resource modeling freedom (secondary)

The v1 type table covers the shapes a real resource needs: scalars with
formats, named enums, date-time, nested objects (inline and `$ref`),
arrays of scalars and of objects, string-keyed maps, free-form objects,
and optional-vs-required (driving Go pointers). The boundary is drawn
at *composition*: `oneOf`/`anyOf`/arbitrary `allOf`/`not` are rejected,
because each implies a Go union/embedding strategy with no single
obvious mapping. This is a deliberate, conservative subset — every
construct inside it has exactly one defensible Go shape, which is what
keeps the generator small and the output reviewable.

## Consequents (explicitly)

- **kin-openapi ergonomics.** Good fit. `Schema.Type` is a `*Types`
  slice (call `.Slice()`/`.Is()`), `AdditionalProperties` is a struct
  with `Schema`/`Has`, `$ref` info survives on `SchemaRef.Ref` even
  after resolution, and external refs are rejected by default. The cost
  is a heavy transitive dependency tree (gnostic-models, go-openapi/*,
  oasdiff/yaml) — exactly why this lives in its own `go.mod` and never
  touches the substrate. The default-form root go.mod was confirmed
  unchanged.
- **Version pinning is load-bearing for build parity.** kin-openapi's
  transitive deps pulled `k8s.io/apimachinery` to v0.36.1 and
  `google/gnostic-models` to v0.7.0; both had to be pinned back
  (v0.32.3 / v0.6.8) to match the substrate, or the build fails with a
  `gopkg.in/yaml.v3` vs `go.yaml.in/yaml/v3` type mismatch deep in
  kube-openapi. A generator that pulls an OpenAPI parser must pin its
  k8s deps to the substrate's versions.
- **Generated LOC: 776 from 130 lines of input** (a ~6x expansion);
  generator is ~1,630 lines. The openapi defs file (334 lines) is the
  largest single generated artifact — the GVK extension + Dependencies
  + per-property `spec.Schema` literals are verbose, as they are in
  real openapi-gen output.
- **explain enum rendering** (above) is a kubectl/kube-openapi
  consequent of k8s 1.32, not a fundamental.
- **`go mod tidy` toolchain bump** (above) is a go-tooling consequent.
- **The `Coordinates` description prints twice in explain** — once from
  the property's description (copied from the referenced struct) and
  once from the struct's own description. Cosmetic; a real generator
  might suppress the property-level copy when it equals the target's.

## What this changes for SYNTHESIS and EXPERIMENTS

For SYNTHESIS: the OpenAPI-first generator is a viable, lower-
boilerplate alternative to the Go-types-first (openapi-gen) path for
the library mode. It closes the open question 0023 left for the typed
path. The supported subset (scalars+formats, enums, nested objects,
arrays, maps, free-form, optionality) is sufficient for the kinds of
resources the lab has modeled so far; the composition keywords are the
boundary. Worth folding into the schema-source section of SYNTHESIS as
"Track B realized for typed library APIs."

For EXPERIMENTS: a follow-on could (a) relax the subset — `allOf` as
struct embedding is the most tractable next step; (b) generate the
`/status` subresource split (0038) directly from a config flag, since
the status component is already identified; (c) explore whether the
generator should emit a distinct internal package with real
field-by-field conversions once a resource actually needs lossy
multi-version conversion (this experiment's identity-conversion shortcut
only works while external == internal).

## Open questions

- Where exactly should the line be for `allOf`? Pure "merge sibling
  property sets" `allOf` has an obvious struct-embedding mapping; mixing
  `allOf` with a local `properties` block is murkier. v1 rejects all of
  it; v2 could admit the merge-only case.
- Multi-version groups: the identity-conversion shortcut collapses the
  moment two external versions must convert through a shared internal
  hub with differing shapes. That needs real generated conversions —
  and probably the separate-internal-package shape this experiment
  deliberately avoided.
- Should the generator own the `oapigen.yaml` → CRD-for-storage and
  manifest emission too, or stay strictly a types generator? Kept
  strictly types here; the manifests are hand-written.
- enum surfacing in explain: is there a kube-openapi knob, or is it a
  kubectl-side limitation? Not investigated.
