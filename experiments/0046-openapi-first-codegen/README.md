# Experiment 0046: openapi-first-codegen

Build the code generator that 0023's schema-source exploration pointed
at but never implemented: consume a hand-authored **OpenAPI v3**
document and emit the Go artifacts an aggregated API needs — typed
structs, deepcopy, dual-version scheme registration, openapi-gen-shaped
definitions, and field-label conversions — then confirm `kubectl
explain` and `kubectl apply --server-side` parity on the generated
types.

Independent of the rest of the arc; can run in parallel from day one.

## Status

in-progress

<!-- valid values: in-progress, complete, abandoned -->
<!-- Scaffolded brief: hypothesis + run plan written; implementation
     (the generator, sample input, generated output, verifier AA)
     pending. -->

## Prior findings this builds on

- `FINDINGS/0002-hello-aggregated.md` — generated OpenAPI with GVK
  extensions is sufficient for `explain`; SSA works given internal-
  version registration. The `x-kubernetes-group-version-kind`
  extension is the discriminator (vs the hand-rolled 0001 schema).
- `FINDINGS/0017-krm-protocol-refinement.md` — `explain` needs the
  backend's OpenAPI threaded into the defs map keyed at the canonical
  name; SSA needs a typed Go object registered under the GVK
  (`managedfields.NewTypeConverter` + `Scheme.New(gvk)`).
- `FINDINGS/0023-schema-source-exploration.md` — three schema-source
  tracks; recommended **Track B** (synthesize from plain schema, zero
  Kubernetes concepts for the backend author) but never built an
  OpenAPI→Go-types generator for the typed library path.
- `FINDINGS/0024-metadata-crd-store.md` — served OpenAPI must use
  `#/definitions/...` v2 refs (ArgoCD's cluster cache rejects
  `#/components/schemas/...`).
- `FINDINGS/0037-field-selectors.md` — `AddFieldLabelConversionFunc`
  registration on the scheme is the hard gate for field selectors.
- `FINDINGS/0038-subresources-status.md` — `/status` subresource
  registration.

## Hypothesis

- **Wire protocol fidelity (primary); resource modeling freedom
  (secondary).** A generator can consume one hand-authored OpenAPI v3
  document (spec + status component schemas) and emit:
  - typed Go structs (`<Kind>`, `<Kind>List`, sub-types) + deepcopy,
  - scheme registration under both the external GV and the internal GV
    (`runtime.APIVersionInternal`) with 1:1 conversions (the SSA/PATCH
    load-bearing piece from 0002/0017),
  - an openapi-gen-shaped `GetOpenAPIDefinitions` carrying the
    `x-kubernetes-group-version-kind` extension, the ObjectMeta
    reference-callback, and the `Dependencies` list (0017/0024/0038),
  - `AddFieldLabelConversionFunc` registration from a declared
    selectable-fields list (0037),

  such that a tiny AA built on the generated types passes `kubectl
  explain`, `kubectl apply --server-side` (with managedFields
  persistence), and label/field-selector listing — with no hand-written
  scheme/types/openapi-gen boilerplate.

## Hard load-bearing decision

Schema source is **OpenAPI v3**, hand-authored. The generator is the
single place that "speaks OpenAPI fluently." Output is reproducible
(same input + config + tool version → byte-identical output; no clock,
hostname, or randomness). The OpenAPI-parsing **tooling-of-record** is
chosen and recorded here at implementation start (candidate:
`github.com/getkin/kin-openapi`; alternatives: the
`google/gnostic-models` / `kube-openapi` already in the k8s dependency
tree, or a minimal in-house v3 parser limited to the supported subset).

## What this is (files to create)

- `cmd/oapigen/` — the generator. Stages: parse OpenAPI v3 → resolve
  intra-document `$ref`s (reject external refs in v1) → identify
  spec/status components from a small config → synthesize the
  `<Kind>`/`<Kind>List` shape → emit Go structs (type mapping per the
  table below) → emit deepcopy → emit register (external + internal) +
  conversions + field-label conversions → emit openapi-gen defs.
- `testdata/widget.openapi.yaml` + `oapigen.yaml` (config: group,
  version, kind, plural, namespaced, spec/status component names,
  `listSelectableFields`, output path).
- `pkg/apis/<group>/v1/` — the **generated** output, committed so the
  diff is reviewable and reproducibility is checkable.
- `cmd/verify-aa/` — a tiny AA on `runtime/server` + `runtime/group` +
  `runtime/storage` (or `runtime/library`) wiring the generated types
  over an in-memory backend, used only to verify wire parity on kind.
- `manifests/`, `go.mod`, `Dockerfile`, `hack/deploy.sh`,
  `hack/gen.sh` (runs `oapigen` and checks the tree is clean).

This experiment likely opts into its **own `go.mod`** (the generator
may pull an OpenAPI-parsing dependency that the substrate must not
inherit). Record the reason here per AGENTS.md "Go module layout".

### OpenAPI → Go type mapping (v1 subset)

`string`→`string`; `string,date-time`→`metav1.Time`; `string,enum`→
named string + consts; `integer,int32`→`int32`; `integer`(int64
default)→`int64`; `number,float`→`float32`; `number`(double default)→
`float64`; `boolean`→`bool`; `array<T>`→`[]T`;
`object,additionalProperties<T>`→`map[string]T`; `object` w/ props →
named struct; free-form `object`→`map[string]interface{}`; `$ref`→named
struct. Optional (not required, no default) → pointer. `oneOf`/`anyOf`/
arbitrary `allOf` → rejected with a clear error in v1.

## How to run

Generation + build (no cluster needed):

```
cd experiments/0046-openapi-first-codegen
go run ./cmd/oapigen --config testdata/oapigen.yaml --openapi testdata/widget.openapi.yaml
go build ./...        # generated code compiles
./hack/gen.sh         # re-run generator; confirm `git diff` is empty (reproducible)
```

Wire-parity verification (on kind):

```
./hack/gen-certs.sh   # repo root
kind create cluster --name aggexp-0046
kubectl --context kind-aggexp-0046 create namespace aggexp-system
kubectl config use-context kind-aggexp-0046
./experiments/0046-openapi-first-codegen/hack/deploy.sh
```

### Scenario 1 — explain

`kubectl explain widget.spec` renders the generated field
descriptions/enums (not a catch-all), proving the GVK extension +
defs-map wiring (0002/0017).

### Scenario 2 — server-side apply + managedFields

`kubectl apply --server-side -f widget.yaml`, mutate, re-apply; confirm
managedFields are tracked and conflict detection works (the internal-
version registration + typed converter path).

### Scenario 3 — field/label selectors

`kubectl get widgets --field-selector spec.color=red` works (the
generated `AddFieldLabelConversionFunc` registration passes the
upstream pre-handler gate, 0037).

### Scenario 4 — reproducibility

Re-run the generator; confirm byte-identical output (`hack/gen.sh`
leaves a clean tree).

### Cleanup

```
kind delete cluster --name aggexp-0046
```

## Decisions made

- Schema source: OpenAPI v3, single file (external `$ref`s rejected in
  v1).
- OpenAPI-parsing tooling-of-record: choose and record at start
  (candidate `getkin/kin-openapi`).
- `oneOf`/`anyOf`/arbitrary `allOf` rejected in v1 (conservative; easy
  to relax later).
- Generated files carry a `// Code generated ... DO NOT EDIT.` header
  and the input OpenAPI SHA-256 in `doc.go`.
- Own `go.mod` for dependency isolation (document the dependency).

## Prerequisites

- For generation/build: `go` only.
- For wire-parity: kind cluster `aggexp-0046` (not the default
  `aggexp`) + serving cert. No external secrets.

## What we're looking to learn

- **Wire protocol fidelity.** Can an OpenAPI-first generator produce
  the exact artifacts (typed structs, dual-version scheme, GVK-stamped
  defs, field-label conversions) that 0002/0017/0024/0037/0038 found
  load-bearing, such that explain + SSA + selectors all work with zero
  hand-written boilerplate? Which OpenAPI constructs resist clean Go
  codegen (the v1 subset boundary)?

## Expected FINDINGS shape

- **Fundamental:** whether OpenAPI-first generation is a viable, lower-
  boilerplate alternative to the Go-types-first (openapi-gen) path for
  the library mode — and where the supported-subset boundary falls.
- **Consequent:** the chosen parsing library's ergonomics, generated
  LOC, and any kube-openapi / kubectl version quirks.
