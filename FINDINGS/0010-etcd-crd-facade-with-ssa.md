# Findings — 0010 etcd-crd-facade-with-ssa

## What we were trying to learn

`0009-ack-aggregated-s3` proved that a fully-stateless aggregated
apiserver loses library-layer features that assume persistence:
SSA field ownership tracking, finalizers, labels, annotations,
ownerReferences. Its FINDINGS listed three remediations: abandon
SSA, encode managedFields into the backend (lossy), or shadow-persist
in etcd (reintroduces the controller model).

This experiment explores a **fourth option**: don't persist anywhere
inside the AA, but point the AA's storage layer at a CRD served by
the host kube-apiserver. The CRD row naturally carries the full
ObjectMeta, so the library-layer features that `0009` lost should
work unchanged. The AA is still stateless in the sense of "no local
storage, no etcd client"; the CRD is storage on the host cluster, one
hop away.

The specific hypotheses under test:

1. SSA managedFields survive and drive conflict detection because the
   CRD row in the host's etcd carries them.
2. Finalizers work because kube-apiserver's finalizer machinery runs
   against the CRD row.
3. OwnerReferences round-trip because they are ObjectMeta fields the
   CRD row holds.
4. Per-request latency is dominated by the aggregation-layer floor
   rather than the additional kube-apiserver hop.
5. A dynamic watch on the CRD, fanned through `runtime/storage.Publisher`,
   drives the AA's watch stream end-to-end — including changes made
   to the backing CRD directly, bypassing the AA.
6. Small facade transformations (a field rename; an identity-aware
   filter) slot in at the backend boundary without disrupting the
   above.

## What we did

Forked 0007 / 0009 as substrate-consumer scaffolding. Swapped the
Bucket (or File) type for `Widget` with `spec.description`,
`spec.counter`, `spec.tags`, `status.observedCounter`. Wrote
`pkg/crdbackend/` as a `runtime/storage.WritableBackend` whose every
operation forwards through a `dynamic.Interface` against
`widgetstorages.aggexpstorage.aggexp.io/v1`. Installed that CRD on
the host cluster, bound the AA's ServiceAccount with CRUD on it.

Transformations: exposed `spec.counter` maps to storage
`spec.storedCounter`; the backend renames on read and write. The
exposed v1 response filters `spec.tags` to only keys prefixed
`alice-` when the caller's user name starts with `alice-`. Neither
transformation is load-bearing; they exist to prove the facade
boundary is real.

Watch: the backend opens a dynamic Watch on the CRD in a
post-start hook and forwards Added / Modified / Deleted events into
the `runtime/storage.Publisher`. The adapter stamps the AA's own
resourceVersion and fans the event out to all AA-side watchers.

Deployed to an isolated `aggexp-etcd-crd` kind cluster and ran the
six scenarios end-to-end.

## What we observed

### Scenario 1: Create surfaces a WidgetStorage row

`kubectl apply -f widget.yaml` against the AA created a row on
`widgetstorages.aggexpstorage.aggexp.io`. Tags, counter
(as `storedCounter`), description all present. Zero surprise.

### Scenario 2: SSA managedFields survive — after a non-obvious rewrite

**This is the primary finding.** The straightforward implementation
(copy the library-layer object's `metadata.managedFields` verbatim
into the unstructured we hand to `dynamic.Update`) failed: incoming
managedFields had two entries (1 with `operation: Apply`, manager
`lab-manager`; 1 `before-first-apply`), and `kubectl get --raw
/apis/aggexp.io/v1/widgets/<name>` returned **zero** managedFields
on subsequent GETs. The CRD row had zero. Direct SSA against the CRD
(`kubectl apply --server-side` on a `WidgetStorage`) correctly
populated managedFields, so the CRD apiserver's field-manager was
working — just not accepting our AA's forwarded managedFields.

Root cause: each `ManagedFieldsEntry` is keyed by `apiVersion`. The
library's fieldmanager stamps each entry with `aggexp.io/v1` (the AA's
exposed group). kube-apiserver's CRD-side fieldmanager sees those
entries as referring to a foreign GroupVersion and silently drops
them. The per-field keys inside `fieldsV1` are also now wrong: they
reference `f:counter`, but the backing CRD's spec has
`f:storedCounter`.

Fix: the facade rewrites each `ManagedFieldsEntry.APIVersion` from
`aggexp.io/v1` to `aggexpstorage.aggexp.io/v1` on write and does the
byte-level rename `f:counter` → `f:storedCounter` inside the `FieldsV1`
JSON. Symmetrically, on read, the facade rewrites them back. After
that change, `kubectl get --raw /apis/aggexp.io/v1/widgets/sample`
returns a populated `managedFields` array with the correct
`aggexp.io/v1` apiVersion and `f:counter` field paths.

**This is a fundamental finding about a facade's obligations to SSA**:
managedFields entries are group-scoped and
schema-scoped. A facade cannot just pass them through. It must
rewrite apiVersion and transform the field-path keys in lockstep with
every schema transformation it does on spec / status. The
transformation is mechanical but required — silence is the failure
mode, not an error.

Without the fix, SSA *looks* like it works (`serverside-applied` is
printed) but the second manager to apply never sees a conflict — the
ownership record is gone. An informed caller would detect it; an
uninformed one would never know.

### Scenario 3: Finalizers work end-to-end

`kubectl patch widget sample --type=merge -p '{"metadata":{"finalizers":["lab.aggexp.io/test"]}}'`
wrote the finalizer through the AA; `kubectl get widget sample -o
json | jq .metadata.finalizers` showed it on subsequent GETs.
`kubectl delete widget sample --wait=false` set
`metadata.deletionTimestamp` but did NOT remove the object. Clearing
the finalizer via `kubectl patch ... '{"metadata":{"finalizers":[]}}'`
allowed the delete to complete.

The reason this "just works": kube-apiserver's CRD machinery runs its
standard finalizer logic against the CRD row. Our facade doesn't need
to do anything — our `Delete` call via dynamic-client issues a real
DELETE on the CRD, and kube-apiserver turns that into the
deletionTimestamp-set-but-object-retained state because of the
finalizer. The AA returns the pending object on the follow-up GET.
From the client's perspective, the finalizer semantics are
indistinguishable from a CRD.

This is a sharp contrast with `0009`: a stateless AA would need to
invent finalizer-respecting deletion semantics from scratch. The
facade gets them for free.

### Scenario 4: OwnerReferences round-trip

A patched `ownerReferences` pointing at a ConfigMap survived the
round trip and showed correctly on subsequent GETs. Same mechanism:
the CRD row holds them; the facade preserves them through the
`DefaultUnstructuredConverter` path.

(Garbage-collection semantics — i.e. what happens if the ConfigMap
is deleted — are a property of kube-apiserver's GC controller
operating on the CRD. We did not probe cross-group GC behavior
explicitly; that's a later experiment.)

### Scenario 5: Identity-aware filter reaches the backend

`kubectl get widget sample -o json` showed all three tags
(`alice-tag`, `bob-tag`, `env`). `kubectl --as alice-lab get widget
sample -o json` showed only `alice-tag`. The filter lives in
`applyIdentityTransform` inside `Backend.Get` / `Backend.List`, which
receives `user.Info` from the request context via the runtime/storage
adapter. Confirms identity reaches the backend; confirms facade
transformations can be identity-scoped without library changes.

This is a reproduction of `0003`'s identity-handoff findings at the
storage layer rather than the authorizer layer, and supports the
claim that the `user.Info` plumbing in `runtime/storage.Backend`
(already in the substrate) is sufficient for identity-dependent
response shaping.

### Scenario 6: Watch fan-out from direct CRD edits works

Started a `kubectl get widgets -w` against the AA, then issued a
`kubectl patch widgetstorage sample --type=merge` directly to the
backing CRD. Within ~0.5s the AA's watch emitted a MODIFIED event
showing the patched description. Proves end-to-end: dynamic
`watch.Interface` → `handleEvent` → `Publisher.PublishModified` →
broadcaster fan-out → client's watch stream.

The mechanism couples the AA's watch strength to kube-apiserver's:
we inherit whatever watch semantics the CRD has, including its
bookmarks, resourceVersion progression, and 410-Gone recovery, by
design. This is strictly stronger than the poll-diff watch
`0004`/`0009` used.

### Latency: no measurable floor shift

`time kubectl get widget sample` ran at ~65ms per call, matching
`time kubectl get widgetstorage sample` directly on the CRD to
within jitter. The extra hop through the AA's handler + the
dynamic-client round trip to kube-apiserver added no
user-perceivable cost at lab scale. Matches SYNTHESIS's
"aggregation-layer floor ~65ms dominates request-total latency"
note from 0006.

A production deployment under real load would eventually see the
extra hop as doubled request count against kube-apiserver (1 from
kubectl → AA, 1 from AA → kube-apiserver), and at high write rates
could bottleneck on admission or RBAC processing twice per user
request. Not observable in this lab.

### `kubectl explain widget.spec` works out of the box

Generated OpenAPI via the same openapi-gen pattern as 0007/0009 —
copied the 0009 baseline and replaced the four Bucket schema functions
with Widget equivalents. kubectl explain prints field docstrings
correctly; no change required at this layer.

## Fundamentals touched

**Storage independence** (primary). A CRD on the host cluster is a
perfectly viable backing store for an aggregated API. The AA itself
holds no state, no etcd client, no `genericregistry.Store`. Every
operation is a `dynamic.Interface` call to the host. The key observation
is: "stateless AA" is a spectrum, not a binary. 0009's fully-stateless
AA traded state for lost library features. 0010's CRD-facade AA buys
those features back by delegating state to one more hop. Neither is
universally right; they're different points on a curve.

Concrete findings for the storage-independence section of SYNTHESIS:

1. **SSA-works-with-CRD-backing comes with a facade obligation**:
   managedFields entries are group-scoped; every facade transformation
   requires a symmetric rewrite of apiVersion and field-path keys in
   the managedFields payload. Mechanical but required, and silent if
   omitted. (See Scenario 2.) This is the specific gap `0009` left
   open as `ssa-managedfields-in-backend`; 0010 resolves it by using
   a CRD as the "backend" and adding a small group/path rewrite.
2. **Finalizers and ownerReferences are free** in the CRD-facade
   model — they ride on kube-apiserver's existing machinery operating
   on the CRD row. (Scenarios 3, 4.)
3. **Per-request latency overhead is invisible at lab scale**, so the
   model is cheap to adopt. The doubled load on kube-apiserver under
   real traffic is a separate operational concern.
4. **Watch semantics are inherited from kube-apiserver**, including
   bookmarks and RV progression. A dynamic watch on the CRD produces
   a real, non-synthetic event stream that the AA can republish. This
   is a strictly stronger watch than the poll-diff patterns 0004 and
   0009 used.

**Per-request authorization** (secondary). The identity-aware filter
in the backend (`applyIdentityTransform`) demonstrates that a facade's
response-shaping can depend on `user.Info` without any new substrate
seam. The `runtime/storage.Backend` interface already plumbs `user.Info`
into Get/List; backends can use it for per-user filtering, redaction,
or projection. This is lower-fidelity than `0003`'s full external
authorizer but lives at a different layer — a backend's response
transformation rather than a gatekeeper decision.

**Resource modeling freedom.** The "facade over a CRD" pattern is
a named modeling technique: the AA's exposed schema can differ from
the backing schema. Renames (counter / storedCounter), identity
filtering, field hiding, projection joins, computed status — all
become straightforward to implement at the backend boundary without
forcing the user to install the AA's schema at the storage layer.
This matters for multi-tenant scenarios where the storage schema
might be rich but the exposed schema needs to be narrower per tenant.

## Consequents (implementation-dependent; do not generalize)

- **managedFields apiVersion rewrite is hand-coded by string match.**
  The implementation hard-codes `aggexp.io/v1` ↔ `aggexpstorage.aggexp.io/v1`.
  A production implementation would derive these from configuration.
- **FieldsV1 key renames are byte-level `bytes.ReplaceAll`**. This
  works because `f:counter` is a distinctive token that does not
  appear as a substring of any other key in our schema. A richer
  schema would need proper FieldsV1-path parsing to avoid false
  matches. Our renameFieldsV1 is deliberately naive.
- **Dynamic watch retry backoff is 2s fixed.** No exponential backoff,
  no jitter. Fine for a lab; a production facade would want proper
  retry semantics.
- **Dynamic client is built off `cfg.ClientConfig` or, failing that,
  `cfg.LoopbackClientConfig`.** In-cluster this uses the AA's SA
  token. The AA's SA has a ClusterRoleBinding granting CRUD on the
  backing CRD. A careful real deployment would scope that more
  narrowly (e.g., a Role in a specific namespace for namespace-scoped
  CRDs).
- **No status subresource on the CRD.** We write the full object in
  one Update. An AA exposing `/status` as a separate endpoint would
  need to split writes across `UpdateStatus` and `Update` on the
  dynamic client — substantial additional plumbing.
- **gnostic-models pin at v0.6.8** (recurring from 0007/0009). Newer
  versions bring in a `go.yaml.in/yaml/v3` that conflicts with
  kube-openapi's `gopkg.in/yaml.v3`. Same consequent.
- **go 1.24 pin in go.mod.** `go mod tidy` under 1.26 auto-bumps the
  `go` directive; re-pinned to `go 1.24` so the `golang:1.24-alpine`
  base image builds. Same consequent as 0002/0009.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**:

- **Storage independence.** The framing "stateless AA loses library
  features that assume persistence" should be updated to a three-point
  gradient:
    1. AA with its own etcd and `genericregistry.Store`: full feature
       parity, owns the storage layer (unused path in this repo).
    2. AA as facade over a CRD on the host cluster (this experiment):
       library features that ride on ObjectMeta work because the CRD
       row holds them; no local state on the AA; doubled kube-apiserver
       load per request; facade transformations require symmetric
       rewrites of `managedFields`.
    3. Fully-stateless AA with external backend as source of truth
       (`0009`, also `0004`, `0006`): inversion wins for read
       consistency, loses SSA/finalizer/ownerRef semantics unless the
       backend can encode them, is subject to per-request latency
       floors of the backend.
  All three points are viable for different subsets of the problem.
  The open `ssa-managedfields-in-backend` candidate is effectively
  **answered** by this experiment's result: route the backend through
  a CRD, do the facade rewrites, SSA works. Encoding managedFields
  into a non-CRD backend (S3 tags, GitHub description fields) remains
  an open candidate for backends that can't be replaced by CRDs.
- **Per-request authorization.** The `runtime/storage.Backend`
  interface's `user.Info` argument is sufficient to drive
  response-shaping transformations. The authz-vs-admission boundary
  already named in SYNTHESIS extends to an authz-vs-projection
  boundary: a backend can withhold or mutate fields per identity
  without an authz gate. Not a new fundamental; a sharpened seam.

For **EXPERIMENTS.md**:

- Mark `0010-etcd-crd-facade-with-ssa` complete under Storage
  independence.
- The `ssa-managedfields-in-backend` candidate is absorbed into 0010
  for the specific case of "backend IS a CRD": the resolution is
  apiVersion+path rewrite. For non-CRD backends (S3, GitHub, etc.),
  the candidate is still open — how do you encode managedFields into
  something that isn't a rich-metadata row?

## Open questions

- **Does the facade rewrite scale to a non-trivial schema?** Our
  schema has one renamed field. A real facade with dozens of nested
  renames, type transformations, or cross-resource joins would need
  a proper FieldsV1-path rewriter, not a byte-level `bytes.ReplaceAll`.
  Is that library-feasible, or does the complexity push toward
  "don't transform; keep storage schema == exposed schema"?
- **What about admission?** This experiment's CRD has no admission
  webhook. If the backing CRD had a validating webhook that rejected
  `spec.storedCounter < 0`, the AA could present that as an error on
  `spec.counter < 0` after the dynamic Update failed. Would
  `metav1.Status.Causes` round-trip usefully? Untested.
- **What's the write-amplification cost at real scale?** Every write
  through the AA becomes two kube-apiserver requests (the AA's own
  authz check + the dynamic-client write to the CRD). At 1k writes/s
  this doubles kube-apiserver load. Is there a consolidation pattern
  (batch writes; short-circuit the authz inside the facade) that
  reduces it?
- **Could the facade support `/status` as a separate subresource?**
  Splitting `UpdateStatus` from `Update` across the dynamic client
  is possible but carries its own ownership-tracking complication
  (managedFields split between the two subresources). Would need a
  separate probe.
- **Watch consistency under AA restart.** We did not test: open a
  watch, restart the AA pod, observe what reflectors see. The
  dynamic watch should reconnect and the Publisher's RV should keep
  incrementing; whether the replay-from-last-seen-RV path works
  end-to-end under this model is the `hours-long-informer` question
  pointed at this experiment.
- **Cross-group owner references.** We wrote an ownerReference
  pointing at a ConfigMap (core/v1). GC semantics under kube-apiserver's
  GC controller for widgets.aggexp.io pointing at v1 ConfigMap were
  not probed — the AA's group is handled by the AA, not the GC
  controller, which may not know how to look up widgets via
  discovery. Untested.
