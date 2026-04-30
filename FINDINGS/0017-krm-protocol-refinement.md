# Findings — 0017 krm-protocol-refinement

## What we were trying to learn

0013 built a deployable generic component server that delegates all
CRUD to a thin gRPC backend, and surfaced two specific library-
feature gaps against that shape:

1. `kubectl explain` degraded to a catch-all description because
   the backend's OpenAPI wasn't threaded through the library's
   explain-rendering path.
2. `kubectl apply --server-side` broke at
   `managedfields.NewTypeConverter` with `"unstructured object has
   no kind"`.

0017 is the follow-up: refine the protocol and implementation so
both gaps close, and record what that costs architecturally.

Three explicit attempts, in order:

- Compose the backend's OpenAPI into the library's defs map so
  explain works.
- Try SSA with the real OpenAPI shipped through; see if
  `NewTypeConverter` is enough.
- If (2) is still blocked by a typed-Scheme assumption, find the
  exact blocker and, stretch-stretch, prototype a way past it.

## What we did

Forked 0013's structure into `experiments/0017-krm-protocol-refinement/`:

- `proto/backend.proto` — regenerated Go bindings are committed.
  Changes from 0013:
  - New `Apply` RPC with an explicit `field_manager` and `force`.
  - `field_manager` field added to `CreateRequest` / `UpdateRequest`.
  - `GetSchemaResponse.supports_server_side_apply` flag. Plus
    `short_names` and `categories`.
  - OpenAPI v3 contract tightened: the backend's top-level schema
    must carry the `x-kubernetes-group-version-kind` extension;
    the component server stamps it defensively if missing.
- `component/pkg/dyn` — a typed Go wrapper `dyn.Object` +
  `dyn.ObjectList`. Implements `runtime.Object` but deliberately
  NOT `runtime.Unstructured`. `Spec/Status/...` live in a
  `Content map[string]interface{}` bag; ObjectMeta + TypeMeta are
  first-class struct fields.
- `component/pkg/scheme` — registers the target GVK against
  either `*unstructured.Unstructured` or `*dyn.Object` depending
  on a `--use-typed-wrapper` flag. When using the wrapper, the
  same Go type is registered under both the external and
  internal group-versions.
- `component/pkg/server` — parses the backend's OpenAPI v3 JSON
  at startup, stamps the GVK extension, composes it into the
  defs map at the Go canonical name the
  `openapi.NewDefinitionNamer(Scheme)` path will agree on.
- `component/pkg/grpcbackend` — the REST storage now propagates
  `UpdateOptions.FieldManager` / `CreateOptions.FieldManager` to
  the backend on Create/Update; decodes incoming JSON into
  whichever object type the mode selects.
- `backend-note` — adds a `json.RawMessage` field for
  `metadata.managedFields` so SSA state round-trips. Adds an
  Apply RPC implementation (unused by the component today; kept
  to validate the wire path). Ships a real OpenAPI v3 schema
  with properties + descriptions + GVK extension.
- `manifests/30-aggexp-deployment-override.yaml` — passes
  `--use-typed-wrapper=true`.

Deployed against kind cluster `aggexp-krmp` and exercised the
scenarios below.

## What we observed

### `kubectl explain` works end-to-end

With the backend-provided OpenAPI composed into the defs map,
`kubectl explain note` and `kubectl explain note.spec` both
render per-field documentation. Example output:

```
KIND:    Note
GROUP:   aggexp.io
VERSION: v1

FIELD: spec <Object>

DESCRIPTION:
    NoteSpec carries the caller-supplied fields. Writable;
    participates in server-side apply.

FIELDS:
  body   <string>
    Free-form body text. Not rendered by kubectl get.
  title  <string>
    Short display title. Rendered in the Title column of
    `kubectl get notes`.
```

The mechanism that matters: `GetCanonicalTypeName(sampleObject)`
(from `kube-openapi/pkg/util`) returns the Go import path + type
name for the object the Scheme returns from `Scheme.New(gvk)`.
For our Scheme registration — either
`*unstructured.Unstructured` or `*dyn.Object` — that canonical
name is deterministic and stable. Keying our composed schema at
that name (plus an `x-kubernetes-group-version-kind` extension
inside the schema) is enough; `openapi.NewDefinitionNamer` fills
in the GVK index automatically from `Scheme.AllKnownTypes()`, so
no custom `GetDefinitionName` callback is required.

**Explain outcome: WORKS.** No degradation vs. 0002's typed
code-generated path.

### SSA on the unstructured path still fails — specifically where

With only the explain fix (keeping `*unstructured.Unstructured`
in the Scheme), a first `kubectl apply --server-side`:

```
failed to create manager for existing fields: failed to convert
new object (/; /, Kind=) to proper version (aggexp.io/v1):
Object 'Kind' is missing in 'unstructured object has no kind'
```

The `managedfields.NewTypeConverter` constructor now succeeds
(no `cannot find model definition` fatal) — that's the
observable progress from shipping a real OpenAPI. The new
failure point is in
`managedfields/internal/skipnonapplied.go`'s first-apply path:

```go
emptyObj, err := f.objectCreater.New(gvk)
// ... uses the emptyObj via fieldManager.Update(emptyObj, liveObj, ...)
```

`objectCreater` is `apiGroupInfo.Scheme`. `Scheme.New(gvk)`
does `reflect.New(t).Interface()` — it returns a zero-value
`*unstructured.Unstructured{}` with empty TypeMeta. For any
object registered as `*unstructured.Unstructured`, this zero
value has no declared GVK. `Scheme.ObjectKinds(u)` treats
`Unstructured` specially and reads the GVK off the instance;
empty GVK → `NewMissingKindErr("unstructured object has no
kind")`. That error bubbles out of `toVersioned(emptyObj)`
inside the field-manager's Update. The entire SSA pipeline is
blocked before any structured-merge-diff work happens.

This is the exact architectural boundary 0013 hit and 0017 was
asked to probe: SSA's typed-converter construction and SSA's
empty-object creation are two separate library checkpoints with
two different typed-Scheme assumptions. Shipping an OpenAPI
unblocks the first; it does not unblock the second.

### Typed wrapper unblocks SSA fully

With `--use-typed-wrapper=true`, the Scheme registers
`*dyn.Object` / `*dyn.ObjectList` under the target GVK instead
of the unstructured types. Because `*dyn.Object` is a typed Go
struct that does NOT satisfy `runtime.Unstructured`,
`Scheme.ObjectKinds(obj)` falls through the typed branch and
attributes the Kind via `typeToGVK[reflect.Type]`. That map is
populated by `AddKnownTypeWithName`, so
`Scheme.New(gvk) → reflect.New(*dyn.Object)` produces an empty
`*dyn.Object{}` whose `ObjectKinds` call succeeds with the
registered GVK. `toVersioned(emptyObj)` then succeeds.

Observed once the wrapper was wired:

- `kubectl apply --server-side --field-manager=alice -f
  sample-note.yaml` creates the Note and returns
  `serverside-applied`. Subsequent GET shows a populated
  `metadata.managedFields` block with `manager: alice,
  operation: Apply, fieldsV1: {f:spec: {f:title: {}, f:body:
  {}}}`.
- A second apply by `bob` targeting `spec.title` is rejected
  with a proper conflict: `conflict with "alice": .spec.title`.
  `--force-conflicts` reassigns ownership to `bob`; `alice`'s
  entry drops `spec.title` from its fieldsV1.
- A strategic merge patch via `kubectl patch` adds a
  `kubectl-patch` manager to the managedFields list; SSA and
  non-SSA writers co-exist cleanly.
- Re-apply by the same manager is idempotent (no duplicate
  managedFields entries).

**SSA outcome: WORKS with `--use-typed-wrapper=true`; partial
without (explain works, SSA doesn't).**

### What was hard

1. **Two separate checkpoints, not one.** We expected the SSA
   blocker to be a single checkpoint we could close by shipping
   a better OpenAPI. In reality there are two: typeConverter
   construction (closed by OpenAPI with GVK extension) and
   empty-object creation (closed only by registering a typed Go
   struct). We spent time re-reading
   `managedfields/internal/*` before understanding the
   `emptyObj := objectCreater.New(gvk)` path is load-bearing.

2. **Internal-version registration is load-bearing for SSA.**
   The first successful wrapper attempt failed with `no kind
   Note is registered for the internal version of group
   aggexp.io`. Source:
   `apiserver/pkg/endpoints/installer.go` sets
   `reqScope.HubGroupVersion = {Group: <g>, Version:
   runtime.APIVersionInternal}` and
   `NewDefaultFieldManager` passes that hub into the SMD
   manager, whose `toUnversioned(obj)` targets that internal
   GV. Fix: register the same `*dyn.Object` Go type under
   *both* the external and the internal GroupVersion. No real
   conversion func is needed because
   `reflect.TypeOf(obj)` matches both entries in `typeToGVK`.

3. **`OpenAPICanonicalTypeName` interface is not the escape
   hatch it looked like at first.** The builder's
   `GetCanonicalTypeName(sample)` honors it, but
   `openapi.NewDefinitionNamer(Scheme)` does NOT — it uses
   reflect-based naming keyed to `Scheme.AllKnownTypes()`. So
   overriding the canonical name via the interface causes the
   two producers to disagree. We avoided it entirely; keying
   the composed defs at the default reflect-derived name is
   simpler and works.

4. **Backend must persist `metadata.managedFields`.** A backend
   whose Go type doesn't model managedFields drops them on
   marshal-then-unmarshal. SSA silently appears to work but
   subsequent GETs show no managedFields; the second apply
   then fails in an unhelpful way because the library can't
   tell who owns what. Fix in the Note backend: a
   `json.RawMessage` field under Meta. No k8s deps needed on
   the backend.

### What the protocol looks like now

See `proto/backend.proto`. Deltas from 0013 (wire-compatible
except where noted):

- **New:** `rpc Apply(ApplyRequest) returns (ApplyResponse)`.
  Breaks wire compat only if the backend set
  `supports_server_side_apply=true` without implementing Apply;
  backends that leave the flag false are unaffected.
- **New fields on existing messages:** `CreateRequest.field_manager`,
  `UpdateRequest.field_manager`. Default empty; backward-compatible.
- **New fields on `GetSchemaResponse`:** `short_names`,
  `categories`, `supports_server_side_apply`. Defaults are safe.
- **Semantic tightening:** the backend's OpenAPI JSON must carry
  the `x-kubernetes-group-version-kind` extension. The
  component server stamps it defensively when missing, so an
  old backend still works; a new backend that wants its schema
  used verbatim should include it.

The proto file is ~250 lines (up from ~190). The component server's
non-generated Go is ~1050 lines (up from ~870), mostly from
`pkg/dyn` and the OpenAPI composition in `pkg/server`.

## Fundamentals touched

**Wire protocol fidelity** (primary). Two concrete closures:

- The wire-level contract for `kubectl explain` is: ship an
  OpenAPI v3 schema whose definitions map has an entry at the
  Go canonical name of the sample object, with the GVK extension
  inside. No custom `GetDefinitionName` callback needed. This
  sharpens the 0002 finding from "generated OpenAPI is enough"
  to "the specific key is the Go canonical name of the Scheme's
  sample object, and the specific extension is
  `x-kubernetes-group-version-kind` on the top-level schema".
- The wire-level contract for SSA against a stateless-AA-style
  component server is: a typed Go struct must be registered
  under the target GVK (and its internal counterpart), and the
  backend must round-trip `metadata.managedFields`. With both,
  SSA's ownership tracking, conflict detection, and force
  semantics work end-to-end exactly as with a typed code-
  generated AA.

**Resource modeling freedom** (secondary). The finding from 0013
stands, but with a sharper line: the component server still needs
no compile-time knowledge of the resource's spec/status, but it
does need a typed Go wrapper (`dyn.Object`) as the stand-in for
the kind inside its own Scheme. That is a single generic struct,
not per-resource code. The fields under spec/status remain a
`map[string]interface{}` bag walked by structured-merge-diff's
deduced path. The tradeoff: SMD's list-key merge strategies
(`listType: map` / `listMapKeys`) can't be expressed, so list
fields inside spec default to atomic/replace semantics. For
unkeyed lists and scalar leaves this is invisible; for keyed
lists (e.g. `containers`) this would be a quality-of-merge
regression we haven't exercised.

**Storage independence** (tertiary). The backend now models
`managedFields` as opaque JSON; it is storage-agnostic about
them. A backend that persists to etcd-via-CRD (0010-style) or
to a blob-store (0009-style) just round-trips the raw bytes. No
new dependency.

## Consequents

- The library's `skipNonAppliedManager.Apply` first-time path
  calls `objectCreater.New(gvk)` and passes the result through
  `toVersioned()`. For an unstructured-only Scheme this is the
  exact failure site; for a typed Scheme it is unused state.
  The specific call site: `managedfields/internal/skipnonapplied.go:79`.
- The `apiGroupInfo.Scheme` field is a concrete
  `*runtime.Scheme`, not an interface. This means a component
  server cannot plug in a custom `ObjectCreater` that stamps
  GVKs on zero-value unstructured. An alternative library API
  that let `Creater` be an interface here would allow an
  unstructured-only SSA path without the typed-wrapper detour.
  That is upstream work, not in scope for this repo.
- `NewDefaultFieldManager` (called from `installer.go:711`)
  takes the scheme as `ObjectCreater` directly via
  `a.group.Creater`. Swapping this out requires forking the
  installer, which is out of scope per this repo's "work within
  the library's public API" rule.
- Structured-merge-diff walks typed Go objects via
  `typed.ParseableType.FromStructured`, which uses
  `value.NewValueReflect`. For `*dyn.Object`, the reflect path
  sees `TypeMeta`, `ObjectMeta`, and `Content
  map[string]interface{}`. Under the current schema the Content
  bag is treated by SMD as untyped when no matching parseable
  type exists, which is what we rely on for per-field merging
  inside spec to work. We have NOT stress-tested the behavior
  against spec schemas with `listType: map` / `listMapKeys`.
  See Open questions.
- The kind cluster is `aggexp-krmp` (0017's convention).
- Same gnostic-models / yaml-conflict pin as 0007/0009/0013. Go
  1.24 pin in every module.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**:

- Wire-protocol-fidelity section: the 0013 entry can now be
  qualified — explain and SSA are both achievable from a
  schema-dynamic component server, *if* the component registers
  a typed Go wrapper under the GVK. The "typed-model-dependent
  features" line from 0013 was too pessimistic; SSA is typed-
  Scheme-dependent, not typed-*resource*-dependent.
- Resource-modeling-freedom section: the typed-vs-unstructured
  line needs a third term: "typed wrapper, untyped content".
  Scheme registration must be typed; content walk can be untyped
  (SMD deduces).

For **EXPERIMENTS.md**:

- 0017 complete under Wire protocol fidelity (primary) with
  cross-references under Resource modeling freedom.
- The "Can the SSA typed-converter be driven by an OpenAPI
  schema shipped over the wire at component-server startup?"
  open question resolves to "yes, if a typed Scheme wrapper is
  registered; documented in 0017."
- 0018 (KRM parity with 0009 S3) and 0019 (polyglot backend)
  can now assume SSA works; their focus shifts to whether
  real-world backends can round-trip managedFields (0018) and
  whether non-Go backends can ship OpenAPI that drives our
  stack (0019).

## Open questions raised

- **List-keyed merge behavior under the dyn.Object path.** SMD's
  ability to interpret `listType: map` and `listMapKeys` against
  a schema where the Go field under inspection is
  `map[string]interface{}` is untested. For scalars and unkeyed
  lists (our Note has neither), the current implementation is
  indistinguishable from a fully-typed path. A synthetic test
  with a keyed list in the backend's schema would tell us
  whether this is a real gap or a theoretical one.
- **Runtime-generated typed structs (reflect.StructOf) per
  GVK.** Out of scope for 0017 and not implemented. Would let
  the component server present a Go type with fields matching
  the backend's schema. Not obviously better than the current
  `Content map[string]any` bag, but would close the keyed-list
  merge question if it turns out to matter.
- **`Apply` RPC utility.** We added it and the backend
  implements it, but the library never calls it today (library
  SSA flows through our Update). The use case is a backend
  that wants to run its own field-manager — e.g. persist
  a separate "applied intent" object alongside the current
  object. No experiment yet demands that; keeping it in the
  proto costs nothing.
- **Fallback for `supports_server_side_apply=false`.** The
  current component implements `rest.Patcher` unconditionally,
  so SSA requests against a backend that doesn't opt in will
  still be attempted by the library. If the backend rejects
  them or mis-handles managedFields, the behavior is a quiet
  apply that doesn't round-trip. A cleaner implementation
  would register a non-Patcher storage when the flag is false,
  returning MethodNotSupported early for SSA requests. Left as
  a seam.
- **Multi-resource component server.** Still one backend = one
  resource. Neither 0013 nor 0017 exercises a component server
  that fans out to multiple backends.
