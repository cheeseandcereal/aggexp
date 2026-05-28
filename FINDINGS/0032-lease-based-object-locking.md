# Findings — 0032 lease-based-object-locking

## What we were trying to learn

Whether Kubernetes `coordination.k8s.io/v1 Lease` objects are
suitable for per-object or per-resource write locking in a
multi-replica aggregated apiserver. The goal: horizontal write
scaling without global leader election. Two granularities tested:
per-object (one Lease per resource instance) and per-resource
(one Lease per resource type).

## What we did

Built a library-mode AA (`runtime/storage` + `runtime/server`)
serving `widgets.aggexp.io/v1` with 2 replicas (StatefulSet) on
kind cluster `aggexp-0032`. The locking layer wraps
`WritableBackend`: before Create/Update/Delete, it acquires a
Lease in a dedicated `aggexp-locks` namespace; on success
proceeds with the write; on contention returns 409 Conflict.
After the write completes, the Lease is immediately deleted
(release-on-write-complete semantics).

The Lease API is used directly via client-go's
`coordinationv1.Lease` typed client — NOT via client-go's
`resourcelock.LeaseLock` package, which is designed for leader
election (single holder, periodic renew, transition callbacks)
and doesn't fit short-lived per-request locking.

## What we observed

### Per-object mode works

Five concurrent patches to the same object: 3 succeeded in
sequence (each acquiring/releasing the lock in turn), 2 received
409 Conflict after exhausting the retry budget (3 attempts). This
is correct behavior — the lock protected against simultaneous
writes to the same object.

Creating distinct objects on different replicas simultaneously
succeeded — each object has its own Lease, so no contention.

### The release-delete race is the first surprise

Initial implementation: acquire creates a Lease; release deletes
it. Under concurrency, a second writer sees AlreadyExists on
Create (Lease exists from the first writer), then does a Get —
but between AlreadyExists and Get, the first writer's release
has already deleted the Lease. The Get returns NotFound.

Fix: retry loop (Create → AlreadyExists → Get → NotFound → retry
from Create). Three retries cover the race. This is not a bug
per se — it's the fundamental race condition in "create then
delete" lock semantics. An alternative: never delete; use
holderIdentity="" to mean "released" and always Create-or-Update.
This eliminates the race but leaves Lease objects behind (one per
unique object ever written).

### Holder-crash recovery via TTL works cleanly

Manually created a Lease with expired `renewTime` + short
`leaseDurationSeconds`, simulating a pod that crashed mid-write.
The next writer's acquire path detected the expiry, issued a CAS
Update (with `resourceVersion` from the Get), and took ownership.
The update to `beta` succeeded through the expired lock.

### Per-resource mode is functionally equivalent at lab scale

With release-on-write-complete, the lock window is sub-ms. Five
concurrent creates of different objects all succeeded even in
per-resource mode (which should serialize all writes). The lock
hold time is so short that by the time concurrent writers hit the
acquire path, the previous holder has already released.

This doesn't mean per-resource mode is useless — under real load
(hundreds of writes/second, backend latency adding ms to the lock
hold window), contention would surface. But at lab scale (kubectl,
human speed) per-resource and per-object modes are
indistinguishable.

### Lock acquisition latency

Not measurable at lab scale. A single Create RPC to the
kube-apiserver adds ~1-2ms (etcd write + kube-apiserver
processing). This is invisible under the 60-80ms aggregation
layer floor that dominates kubectl latency.

### client-go's resourcelock.LeaseLock is NOT suitable

It was designed for leader election: one holder, periodic renew
(every ~2s), callbacks on acquire/lose/renew. Per-request locking
needs: instant acquire (one API call), instant release (one API
call), no background goroutine. Direct Lease Create/Get/Update/
Delete is the right approach.

## Fundamentals touched

**Storage independence** (primary). Lock state is itself a
storage axis — ancillary to business data, persisted in the host
cluster's etcd via standard Lease objects. The fifth axis from
0024 (KRM metadata CRD) is analogous but orthogonal: metadata
CRD stores per-object annotations/labels/managedFields; lock
state stores ownership + TTL. In a production system they could
potentially merge (a `lockedBy` field on the metadata CRD)
but the concerns are different: metadata persists across the
object's lifetime; lock state is transient.

**Watch and consistency semantics** (secondary). Locking is the
write-side precondition for watch consistency: without it,
concurrent writers to the same object from different replicas
produce undefined event ordering. With per-object locking,
exactly one writer succeeds per object at a time — the event
stream is serializable.

## Consequents

- **Lease object proliferation**: per-object mode creates one
  Lease per (group, resource, namespace, name) ever written.
  With release-by-delete, these are ephemeral (~sub-second
  lifetime). With release-by-clear-identity, they persist
  indefinitely (one per unique object). At 10k objects, that's
  10k Leases — etcd cost is non-trivial. A GC mechanism (like
  0028) would be needed for the clear-identity variant.

- **The Create/Delete race is specific to release-by-delete
  semantics.** If the lock protocol switches to
  release-by-clear-identity (Update holderIdentity to ""), the
  race disappears but Lease GC becomes a concern.

- **kube-apiserver rate limits**: each write generates 2-3
  additional API calls (Create/Get + Delete) to the Lease API.
  Under high write throughput, this doubles the write load on
  kube-apiserver. At lab scale: invisible. At production scale:
  would need audit.

- **Aggregation layer load-balancing is opaque**: kubectl goes
  through kube-apiserver's proxy → Service → one pod. You cannot
  control which pod handles a write via kubectl alone. Replica
  pinning requires direct port-forward (bypasses auth) or
  per-pod Services (complex). This is a deployment consequent,
  not an architectural one.

## What this changes for SYNTHESIS and EXPERIMENTS

For SYNTHESIS: Lease-based locking is viable for the write-side
of horizontal scaling. It composes with CRD-backed shared-watch
(0034) and optimistic concurrency (0039). The key design choice
is release semantics: delete (ephemeral, race-prone) vs
clear-identity (persistent, GC-needed). Neither is clearly
dominant.

For EXPERIMENTS: 0032 is complete. The findings feed into the
Phase 0 comparison with 0033 (CAS-on-CRD locking).

## Open questions

- Does the clear-identity-on-release variant perform better under
  sustained load? (Eliminates the Create/Delete race; one Update
  per release instead.)
- What's the Lease creation rate before kube-apiserver's APF
  starts throttling? This matters for the per-object variant at
  scale.
- How does this compose with the CRD-backed shared-watch from
  0034? If the shared-watch store is a CRD, the lock could live
  as a field on the same CRD (merging 0032 + 0033 + 0034 into
  one CRD object per resource instance). This is the 0033
  hypothesis.
