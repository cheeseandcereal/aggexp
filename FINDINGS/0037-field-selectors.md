# FINDINGS: 0037 — field-selectors

## What we were trying to learn

Whether resource-specific field selectors can be implemented cleanly in
the library layer (the v1 `runtime/storage` adapter pattern) without
modifying the substrate. Specifically: what's the wiring cost, where
does the library intercept, does watch filtering compose, and what does
the error shape look like for unknown fields.

## What we did

Built a library-mode AA serving `widgets.aggexp.io/v1` with spec fields
`color`, `size`, `priority`. Pre-populated 10 widgets at startup. Forked
the storage adapter locally to add field-selector parsing and filtering
on both List and Watch paths. Deployed to a kind cluster and exercised
all five required scenarios plus inequality operators.

## What we observed

**The apiserver library validates field selectors before the storage
handler runs.** This was the central surprise of the experiment. The
library's request handler chain calls
`scheme.ConvertFieldLabel(gvk, label, value)` for every requirement in
the field selector. If no conversion function is registered for the GVK,
it rejects all field labels except `metadata.name` and
`metadata.namespace` with a 400 BadRequest. The error reads:

```
"<field>" is not a known field selector: only "metadata.name", "metadata.namespace"
```

The fix is `scheme.AddFieldLabelConversionFunc(gvk, func)` — a
one-liner per GVK that declares which fields are valid. This function is
called at request parse time, before the `List` or `Watch` handler is
invoked. It's purely validation; it doesn't filter.

**After registration, the field selector arrives in
`metainternalversion.ListOptions.FieldSelector` as a `fields.Selector`
interface.** The adapter must:

1. Extract requirements via `sel.Requirements()`.
2. For each requirement, resolve the value from the object via an
   accessor function.
3. Match equality/inequality manually.

The substrate's `ListOptions` only has `LabelSelector`; field selectors
are a parallel concern. The adapter handles them independently of (and
after) label filtering.

**Watch filtering works correctly.** Both the initial snapshot (prefix
events) and the ongoing event stream are filtered. A newly created
object matching the field selector appears in the watch; a non-matching
object does not. This composes with label selectors: both filters are
applied as an AND, consistent with how kube-apiserver handles built-in
resources.

**The accessor function is trivial for flat specs.** A `switch` on known
field paths (`spec.color`, `spec.size`, `spec.priority`) is 15 lines.
For nested specs, a generic dotted-path walker would be more complex but
still mechanical — it's pure field extraction with no business logic.

**Inequality operators work.** `spec.color!=red` correctly returns all
non-red widgets. The `fields.Selector.Requirements()` API exposes the
operator (`=`, `==`, `!=`) per requirement.

**The ConvertToTable path needed a fix unrelated to field selectors.**
In k8s 1.32, the apiserver requires `TableRow.Object` to be set to a
`runtime.RawExtension{Object: item}` for table encoding to succeed. This
isn't specific to field selectors but was surfaced by this experiment.
The substrate's adapter doesn't set it either — it works there only
because it goes through the substrate's internal table conversion path
differently. This is a minor bug that could be back-ported.

## What surprised us

1. **The validation is library-side, not aggregation-layer-side.** Initial
   hypothesis assumed the aggregation layer passes field selectors through
   transparently and the AA validates/filters on its own. In reality,
   the AA's own `k8s.io/apiserver` library validates before the handler
   runs. This means `AddFieldLabelConversionFunc` is a hard requirement,
   not optional polish.

2. **The substrate deliberately omitted field selectors** (see
   FINDINGS/0007 line 120-126) because the `Backend` interface was designed
   for simplicity — backends return everything, and the adapter filters
   defensively. That design still holds: field selector filtering happens
   in the adapter, not the backend. But the **declaration** of which fields
   are selectable must be registered in the Scheme, which is a level above
   the adapter — it's in the server/type-registration layer.

3. **No changes to the substrate's `Backend` interface were needed.** The
   field selector filtering is purely adapter-level. The `SelectableFields`
   method we added is on the local backend struct, not on the substrate
   interface. For a substrate promotion, it would be an optional interface
   (like `WritableBackend`), not a required method.

## Fundamentals touched

### Resource modeling freedom

Field selectors are a resource-modeling concern: they define which fields
of a resource type are queryable. The implementation shows that declaring
queryable fields is lightweight (one scheme registration + one accessor
function) but mandatory — you can't add field selectors post-hoc to
an existing library-mode AA without touching the scheme registration.

For a substrate-level library, the natural shape is:
- Backend optionally implements `FieldSelectableBackend` declaring its
  selectable fields and an accessor.
- The server/group registration layer calls
  `AddFieldLabelConversionFunc` based on the backend's declaration.
- The adapter filters defensively after the backend returns, using the
  accessor.

### Wire protocol fidelity

Field selectors are part of the standard Kubernetes wire protocol for
List and Watch. `kubectl --field-selector` is documented, widely used,
and expected to work. Without `AddFieldLabelConversionFunc`, an
aggregated API silently denies all custom field selectors with a 400
that looks like a kube-apiserver error, not an AA error. This could
confuse users who expect field selectors to "just work" on custom
resources (they don't work on CRDs either, for the same reason — CRDs
only support `metadata.name` and `metadata.namespace` field selectors
natively).

## Consequents noted

- **`AddFieldLabelConversionFunc` is tied to the Scheme and GVK.** It
  must be called at init time, before the server starts. There's no
  way to dynamically add selectable fields at runtime without
  re-registering (this matters for the component-server/multiplex
  pattern where resources are discovered dynamically).
- **The `fields.Selector` interface doesn't expose a way to iterate
  field paths without calling `Requirements()`.** This is fine for
  validation but means the adapter can't use `fields.Set.Matches()`
  directly without pre-building the full field set for every object
  (expensive for large objects with many selectable fields).
- **`ConvertToTable` requiring `TableRow.Object` set is a 1.32
  consequent.** Earlier versions may not require it. The substrate's
  adapter should be fixed regardless.
- **The error format for unknown field selectors is consistent with
  CRDs.** Attempting `kubectl get pods --field-selector spec.bogus=x`
  on built-in types produces the same shape. Users familiar with the
  error on native types will recognize it on AAs.

## What this changes for SYNTHESIS and EXPERIMENTS

For SYNTHESIS: field selectors in the library layer are confirmed
implementable with low cost (~50 lines of adapter logic + 1 scheme
registration). The key insight is that `AddFieldLabelConversionFunc` is
a hard gate — without it, the library rejects all custom field
selectors. This is a scheme-level concern that sits above the Backend
interface.

For EXPERIMENTS: the field selector pattern is now documented. Future
experiments that need field selectors can copy the pattern. A substrate
promotion would add an optional `FieldSelectableBackend` interface and
have the group/server registration automatically call
`AddFieldLabelConversionFunc` based on the backend's declaration.

## Open questions

- For the component-server/multiplex pattern (0027): can
  `AddFieldLabelConversionFunc` be called after `PrepareRun`? If not,
  dynamically-installed resources can't support custom field selectors
  without a scheme mutation at runtime.
- Performance at scale: field selector filtering is O(items) per List.
  For backends that could pre-filter (e.g. a database), the adapter
  should optionally pass the field selector through to the backend. The
  `Backend.List` signature would need extension.
- Compound selectors with many requirements: is there a practical limit
  on the number of requirements in a single field selector? The library
  doesn't seem to impose one.
