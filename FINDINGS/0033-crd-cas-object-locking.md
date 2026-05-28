# Findings: 0033 — CRD-CAS Object Locking

This experiment probed whether a custom CR with resourceVersion-based
CAS (compare-and-swap) can provide per-object write ownership for a
multi-replica aggregated apiserver, and compared the ergonomics and
failure modes against 0032's Lease-based approach.

## What we were trying to learn

The production-library-readiness arc (0032-0040) asks what a
production-grade AA library needs beyond `runtime/storage`. This
experiment specifically probed **lock state as its own storage axis**:
can we implement correct multi-writer serialization using CRDs and
optimistic concurrency, without depending on the Lease API's opinionated
lifecycle semantics (holderIdentity, leaseDuration, renewTime)?

The hypothesis was that a `lockedBy` + `lockExpires` field on a CRD,
guarded by CAS on the CR's resourceVersion, can provide equivalent
safety to Leases — with the expected tradeoffs of more retries under
contention (every CAS loss is "redo the whole read-modify-write") and
GC being the operator's problem rather than the Lease controller's.

## What we did

Built a library-mode AA (`gizmos.aggexp.io/v1 Gizmo`) with an
in-memory writable backend wrapped by a `locking.Backend` layer.
The locking layer intercepts every Create/Update/Delete and acquires
a lock CR on the host kube-apiserver before forwarding the mutation.
Two granularities: per-object (`ObjectLock`, one CR per gizmo
instance) and per-resource (`ResourceLock`, one CR for all gizmos).

The CAS algorithm: GET the lock CR; if NotFound, CREATE with our
identity (CAS-loss on Create = retry); if found and we hold it,
proceed (renew if close to expiry); if found and expired, steal via
UPDATE with the stale RV (CAS-loss = retry); if found and fresh and
held by another, return 409. Max 8 retries with 25ms fixed sleep.

Deployed as a 2-replica StatefulSet in kind (k8s 1.32), with pod
identity injected via Downward API `POD_NAME`.

## What we observed

**Scenario 1 — distinct objects**: Creating two different gizmos
succeeds immediately, each producing its own ObjectLock CR. Lock CRs
are released (lockedBy="") after the write completes. No contention.

**Scenario 2 — cross-replica contention**: When a lock CR is held by
another replica with a future expiry, writes return HTTP 409 Conflict
immediately (after one GET of the lock CR). The error message is
clear: `lock held by "aggexp-1" (expires ...)`. Under a 5-way
parallel patch storm from the same replica (no cross-replica
contention), all 5 succeed — because the same pod re-acquires its
own lock without CAS contention. This confirms that per-object
locking is contention-free for single-replica workloads.

**Scenario 3 — holder crash / expired lock takeover**: Manually
setting an expired `lockExpires` timestamp (simulating a crashed
holder) and then writing succeeds: the locking layer detects expiry,
steals the lock via CAS UPDATE, and proceeds. The takeover completes
in a single CAS round-trip when uncontested.

**Scenario 4 — CAS retry probe**: With 5 concurrent patches all
blocked by an external holder, all 5 get 409 immediately (no retries
needed because the lock is freshly held — the algorithm returns 409
on the first attempt rather than retrying). When the holder is
cleared, 5 concurrent patches from the same replica all succeed
(0 conflicts).

**Latency**:
- Read (GET, no lock): ~80ms (standard aggregation-layer hop)
- Write (PATCH, lock acquire + release): first write ~100ms,
  subsequent writes ~800ms (dominated by SSA merge machinery,
  not by locking)
- Raw lock CR create: ~90ms from kubectl (in-cluster dynamic
  client: ~5-10ms estimated from the AA's perspective)
- Raw lock CR update (CAS): ~100ms from kubectl
- Lock acquisition overhead attributable to CAS: ~10-20ms
  (one GET + one CREATE/UPDATE on the lock CR, both in-cluster)

The ~800ms spikes on subsequent writes are NOT from the locking
layer. They come from the library's SSA path (field-ownership
tracking, merge, managedFields), which kicks in on any write after
the first. The lock itself adds ~10-20ms.

**Per-resource vs per-object**: Per-resource mode produces a single
ResourceLock CR for the entire resource type. Both writes to
different objects contend on the same lock, confirming it's a
coarse serialization guard. The mode is useful for write-rare
resources or debugging, not for production multi-object workloads.

## What surprised us

1. **The API-group routing trap.** The original design placed lock
   CRDs under the same group as the exposed resource (`aggexp.io`).
   This caused a recursive routing loop: the AA's locking layer
   tried to create `objectlocks.aggexp.io` via the dynamic client,
   which kube-apiserver routed back to our AA (because the
   APIService registration for `v1.aggexp.io` captures ALL resources
   in that group). Fix: move locks to a separate group
   (`locks.aggexp.io`). This is a **fundamental** discovery for the
   "lock state as storage axis" pattern: lock CRDs MUST live in a
   different API group than the served resources.

2. **No CAS retries in practice at lab scale.** With two replicas
   behind a Service and kube-apiserver's aggregation proxy reusing
   connections, all requests route to the same pod. Cross-replica
   contention only appears when an external holder (another pod or
   manual intervention) sets the lock. The retry loop (8 attempts,
   25ms sleep) was never exercised in normal operation. The CAS
   budget exists for races at the lock-CR level (two replicas both
   trying to CREATE the same lock CR at the same instant), not for
   sustained contention.

3. **The 409 is immediate, not retried.** When a lock is held by
   another replica and fresh, the algorithm returns 409 to the
   caller on the first attempt — no retries. Retries only happen on
   CAS-level conflicts (Create-already-exists, Update-RV-mismatch).
   This means the client (kubectl, controller) gets a fast failure
   and can decide whether to retry, rather than blocking for the
   full retry budget.

## Fundamentals touched

### Storage independence (primary)

Lock state is confirmed as its own storage axis — distinct from:
1. Business-data persistence (in-memory Gizmo map)
2. KRM-metadata persistence (the fifth axis from 0024)
3. Watch/consistency state

The lock CRD lives on the host kube-apiserver in its own etcd space.
It composes with any backend: the locking.Backend wraps any
WritableBackend regardless of what that backend uses for storage.
This confirms the hypothesis that locking is orthogonal to data
storage.

Key constraint: the lock CRDs must use a different API group than
the resource they gate. This isn't a storage-independence finding
per se — it's a routing constraint from the APIService registration
model — but it means "lock state as CRD" requires at minimum two
API groups per locked resource type.

### Watch and consistency semantics (secondary)

The CAS pattern uses the CRD's own resourceVersion as the
concurrency primitive — the same mechanism client-go uses for
optimistic concurrency. This is the natural building block for
0039's planned optimistic-concurrency experiment. The two compose:
0033 prevents cross-replica conflicts (who owns the right to write);
0039 prevents stale-read-then-write within one client.

## Consequents

**kube-apiserver aggregation connection reuse**: the aggregation
proxy opens a single HTTPS connection to the backend Service and
reuses it for all requests. This means the Service's round-robin
load balancing doesn't effectively distribute requests to different
pods. Cross-replica contention at the locking layer is therefore
rare in practice unless replicas are addressed directly (e.g. via
pod-specific Services or headless Service endpoints).

**APF (Priority and Fairness) requires RBAC or must be disabled**:
the k8s.io/apiserver library's built-in APF controllers try to list
FlowSchemas and PriorityLevelConfigurations. Without RBAC for these,
the reflectors fail and the server never becomes ready. Standard fix:
`--enable-priority-and-fairness=false`.

**SSA dominates write latency, not locking**: the 800ms write spikes
are from the library's field-ownership/merge machinery. The lock
acquire+release adds ~10-20ms. A future optimization would be to
benchmark with SSA disabled or with a simpler patch path.

**CRD API rate under retry storm**: with the 8-attempt × 25ms
budget, a single write under contention makes at most 8 CRD API
calls (GETs + Create/Update). At lab scale this is invisible. At
production scale with many concurrent writers, the CRD apiserver
(host kube-apiserver) becomes the bottleneck. The lock CRDs are
cluster-scoped and low-traffic; etcd contention on them is unlikely
unless thousands of objects are written concurrently.

## Comparison with 0032's Lease approach

Both 0032 and 0033 use CAS on a kube-apiserver object for write
ownership. The differences:

| Aspect | Lease (0032) | CRD-CAS (0033) |
|--------|-------------|----------------|
| Lock object | `coordination.k8s.io/v1 Lease` | Custom CR under `locks.aggexp.io/v1` |
| Built-in semantics | holderIdentity, leaseDuration, renewTime, acquireTime | None (all fields are ours) |
| Expiry detection | leaseDuration field vs. renewTime comparison | lockExpires field vs. wall clock |
| GC | Leases are managed by kube-controller-manager (if in kube-system) | No built-in GC; expired locks are just stealable |
| CAS mechanism | Identical: Update with stale RV → 409 | Identical |
| API group conflict | None (coordination.k8s.io is its own group) | Must use separate group from served resources |
| Semantic overhead | Must understand Lease renewal semantics | Raw: we own all fields |
| Lock discovery | `kubectl get leases` everywhere | `kubectl get objectlocks.locks.aggexp.io` (custom) |

The key finding: **both approaches are isomorphic at the CAS layer**.
The difference is in what you get for free (Lease gives you
holderIdentity + duration semantics + kube-controller-manager GC) vs.
what you have to build yourself (CRD gives you full control but
requires implementing expiry logic and GC). For a library that wants
to own its locking semantics without depending on coordination.k8s.io
conventions, the CRD path is cleaner. For quick deployment where the
defaults are acceptable, Lease is less code.

## What this changes for SYNTHESIS and EXPERIMENTS

For SYNTHESIS: confirms lock state as a storage axis (alongside the
five already identified). The API-group-routing constraint is
generalizable: any CRD the AA uses internally must be in a different
API group than the resources it serves.

For EXPERIMENTS: 0039 (optimistic concurrency) now has a clear
composition model — 0033's locking handles cross-replica ownership;
0039's RV-conflict handles within-client staleness. The combination
covers the full multi-writer safety space.

## Open questions

- Should lock + metadata (from 0024) be one CRD? The metadata CRD
  already has per-object state (uid, resourceVersion, labels, etc.);
  adding lockedBy/lockExpires would merge two storage axes into one
  object. Pro: fewer CRD API calls per write. Con: couples lock
  lifecycle to metadata lifecycle; GC semantics become tangled.
- At what scale does the lock-CR CAS retry storm become a
  bottleneck? The 8-attempt budget is arbitrary; under heavy write
  contention (hundreds of concurrent writers), the CRD apiserver
  would see O(writers × retries) requests per lock acquisition
  window.
- Does the per-object lock CR population grow unboundedly? Without
  GC, expired ObjectLock CRs accumulate forever. 0028's GC pattern
  would apply directly (sweep, match against backend, delete
  unmatched).
