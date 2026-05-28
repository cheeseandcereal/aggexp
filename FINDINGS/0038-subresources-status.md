# FINDINGS/0038-subresources-status

## What we were trying to learn

Whether the standard Kubernetes `/status` subresource pattern — separate
Update paths for spec vs status, with SSA tracking field ownership per
subresource — can be supported in the library-mode AA via registering a
second `rest.Storage` at `"widgets/status"` in the Resources map. The
specific questions: does `runtime/group`'s `Resources` map support the
`"resource/subresource"` key syntax? Does the genericapiserver machinery
automatically route `/status` requests to the separate storage? Does SSA
track field ownership per subresource out of the box?

## What we did

Built a library-mode AA for `widgets.aggexp.io/v1 Widget` with both
`spec` (color, size) and `status` (phase, message) fields. Registered
two entries in the `Group.Resources` map:

- `"widgets"` → a `SpecREST` wrapper around the main `runtimestorage.REST`
  adapter. Its `Update` method wraps `UpdatedObjectInfo` to preserve
  `status` from the existing stored object on every write.
- `"widgets/status"` → a `StatusREST` adapter implementing only
  `rest.Getter` + `rest.Updater`. Its `Update` method wraps
  `UpdatedObjectInfo` to preserve `spec` from the existing stored object.

Both wrappers share the same underlying in-memory backend and the same
`runtimestorage.REST` (and its broadcaster). Deployed single-replica to
a kind cluster.

## What we observed

**Subresource registration works via the Resources map key syntax.**
Setting `"widgets/status"` as a key in `Group.Resources` is all that's
needed. The genericapiserver's `InstallAPIGroup` recognizes the
`resource/subresource` pattern in map keys and routes
`/apis/widgets.aggexp.io/v1/namespaces/{ns}/widgets/{name}/status`
requests to the corresponding storage.

**Discovery correctly reports the subresource.** The
`/apis/widgets.aggexp.io/v1` APIResourceList includes both:
- `"widgets"` with verbs `[create, delete, get, list, patch, update, watch]`
- `"widgets/status"` with verbs `[get, patch, update]`

The verbs are automatically inferred from which `rest.*` interfaces the
storage implements. `StatusREST` only implements `rest.Getter` +
`rest.Updater`, so the discoverable verbs are `get`, `patch`, `update`
(the apiserver synthesizes `patch` from `Updater`).

**Spec/status preservation works in both directions:**
- Main resource PUT with `status.phase=Failed` → status preserved as the
  stored value (`Active`), spec updated.
- Status subresource PUT with `spec.color=green` → spec preserved as
  stored value (`red`), status updated.
- `kubectl patch --subresource=status` works end-to-end.

**SSA field ownership per subresource works out of the box.**
Two separate `--field-manager` values applied via different subresource
paths produce correct `managedFields`:

```
manager=spec-mgr   operation=Apply  subresource=""       fields: f:spec.{f:color, f:size}
manager=status-mgr operation=Apply  subresource="status" fields: f:status.{f:message, f:phase}
```

The `subresource` field in managedFields entries is populated
automatically by the apiserver machinery — no opt-in required. Conflict
detection works correctly: `spec-mgr` trying to include status fields in
a main-resource apply gets a proper 409 conflict citing `status-mgr`
with subresource `"status"`.

**SSA requires specific OpenAPI metadata.** Two requirements:
1. The schema must declare `apiVersion` and `kind` as top-level string
   properties. Without them, the SSA typed-converter fails with
   `.apiVersion: field not declared in schema`.
2. The ObjectMeta `$ref` must be produced via the `ReferenceCallback`
   (e.g., `ref("k8s.io/apimachinery/pkg/apis/meta/v1.ObjectMeta")`)
   rather than hardcoded as `#/definitions/io.k8s...`. The typed-
   converter resolves refs through the definitions map using the
   Go-module-path key format, not the OpenAPI-dot-separated format.
   The `Dependencies` field on `OpenAPIDefinition` must also be set.

**No special OpenAPI extension needed for subresource declaration.**
Unlike CRDs (which require `x-kubernetes-subresource: {status: {}}`
in the CRD spec), aggregated apiservers declare subresources purely
through the Resources map registration. The genericapiserver machinery
handles everything else: discovery, routing, verb inference.

**`kubectl explain` works with per-field descriptions** for both spec
and status fields.

**`kubectl get` table rendering requires `TableRow.Object` to be
populated.** An empty `runtime.RawExtension{}` in the Object field
causes "object does not implement the Object interfaces" errors during
serialization. Setting `Object: runtime.RawExtension{Object: deepCopy}`
resolves it.

## What surprised us

**The simplicity of subresource registration.** The entire mechanism is
a naming convention in a `map[string]rest.Storage`. No special
configuration, no annotations, no CRD-style subresource declarations.
The genericapiserver figures everything out from the slash in the key.

**SSA tracks the `subresource` field in managedFields without any
opt-in.** The apiserver's Apply handler internally knows which
subresource the request targeted and stamps it into the managedFields
entry. This means conflict detection naturally respects the spec/status
boundary: a spec-manager and a status-manager never conflict with each
other (they operate on different subresources), but two spec-managers
competing for the same field do conflict.

**The OpenAPI schema requirements for SSA are stricter than for basic
CRUD.** The 0032 experiment (which doesn't use SSA) worked fine with
just `metadata` as a `$ref` and no `apiVersion`/`kind` properties.
SSA requires those additional fields because the typed-converter
validates the entire object structure against the schema.

## Fundamentals touched

### Resource modeling freedom

The spec/status split is a **fundamental** pattern of the Kubernetes
resource model. This experiment confirms that library-mode AAs can
implement it with minimal wiring: two `rest.Storage` entries sharing one
backend, two thin wrappers that enforce field preservation. The pattern
generalizes to any subresource (not just `/status` — you could register
`"widgets/scale"` or `"widgets/log"` the same way).

The key design decision: the subresource's storage typically shares the
same backing data as the main resource (same Get, same backend) but
differs only in what Update preserves/allows. The `UpdatedObjectInfo`
wrapper pattern (intercepting `UpdatedObject` to restore preserved
fields) is clean and composes with SSA, strategic merge patch, and
JSON merge patch — all of which flow through the same Update path.

### Wire protocol fidelity

The subresource pattern produces wire behavior identical to built-in
Kubernetes resources:
- Discovery shows `widgets/status` with appropriate verbs
- `kubectl patch --subresource=status` works
- `kubectl apply --server-side --subresource=status` works
- managedFields track subresource ownership
- Conflict detection respects the subresource boundary

No custom client code needed; standard kubectl flags work.

## Consequents

**The `apiVersion`/`kind` fields in OpenAPI are required for SSA but
not for CRUD.** This is a consequent of the `managedfields.NewTypeConverter`
implementation in k8s.io/apiserver v0.32.3: it validates the full object
against a `schema.Schema` which requires those top-level TypeMeta fields
to be declared. Earlier experiments (0032, 0035) that didn't exercise SSA
didn't hit this because their OpenAPI lacked those fields and nobody
called the typed-converter.

**The `Dependencies` field and `ReferenceCallback` usage for `$ref`
resolution is a consequent of how `openapi.NewDefinitionNamer` and
`managedfields.TypeConverter` interact.** The definition namer transforms
Go module paths (`k8s.io/apimachinery/pkg/apis/meta/v1.ObjectMeta`) into
dot-separated OpenAPI names (`io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta`).
The `ref` callback ensures the correct transformation is applied. Hardcoding
the `#/definitions/...` path skips the namer and breaks the typed-converter's
ref resolution.

**`TableRow.Object` must be populated for table rendering.** This is a
consequent of how the apiserver's table serializer works in v0.32.3 — it
attempts `meta.Accessor()` on the raw extension's object, and a zero-value
`RawExtension` fails that check. This is not documented anywhere obvious
and is easily missed.

## What this changes for SYNTHESIS and EXPERIMENTS

SYNTHESIS: Under **Resource modeling freedom**, the spec/status split
via subresource registration is now a confirmed pattern for library-mode
AAs. The mechanism is simpler than expected — a naming convention in a
map, not a library-level configuration object.

EXPERIMENTS: The `status-conditions-in-aa` candidate under Resource
modeling freedom is now more approachable — the subresource mechanics
are confirmed working, so that experiment can focus purely on whether
`kubectl wait --for=condition=Ready` works with our watch implementation.

## Open questions

- Can this pattern be promoted to the substrate as a helper? Something
  like `storage.NewWithStatusSubresource(backend)` that returns both the
  main REST and the StatusREST, with the preservation wrappers
  pre-applied. Would save ~60 lines of per-experiment boilerplate.
- Does the SSA typed-converter cache the schema at `PrepareRun` time
  (like the V3 OpenAPI endpoints), or does it pick up dynamically-
  installed groups? Relevant for multiplex middleware scenarios (0027).
- The current `UpdatedObjectInfo` wrapper pattern works but creates a
  new allocation per Update. At high throughput, a strategy-based
  approach (implementing `rest.RESTUpdateStrategy.PrepareForUpdate`)
  might be more efficient. Worth measuring if subresource Updates
  become a hot path.
