# FINDINGS/0035-deterministic-uids

## What we were trying to learn

Whether deriving UIDs deterministically from backend-stable identifiers
eliminates the pod-restart phantom-reconcile storm identified in
FINDINGS/0012. The hypothesis: `UID = SHA256(group/resource/namespace/name)`
formatted as a standard 8-4-4-4-12 hex UUID makes objects appear
identical across AA pod restarts, so downstream controllers (reflectors,
controller-runtime managers) see no change and fire zero reconcile events.

## What we did

Forked the 0032 library-mode AA for `widgets.aggexp.io/v1` and added a
`--uid-mode=random|deterministic` flag. The backend pre-populates three
seeded widgets on startup (simulating an external source that always
contains the same objects, like GitHub repos in 0004 or filesystem in
0007). Deployed single-replica to kind.

Three scenarios tested:

1. **Random mode, pod restart**: Record UIDs, delete pod, observe UIDs
   after new pod is ready.
2. **Deterministic mode, pod restart**: Same as above.
3. **Deterministic mode, delete/re-create same name**: Create widget
   "foo", record UID, delete it, re-create "foo", compare UIDs.

Watch events captured via `kubectl get widgets -w --output-watch-events`
during scenarios 1 and 2.

## What we observed

**Scenario 1 (random mode):** UIDs change on every restart.

```
BEFORE: alpha=625a2940-41b7-4f81-b835-f41f053128a1
AFTER:  alpha=4204a908-5209-4199-8e82-a78c7cc0622c
```

Every object gets a fresh UUID on each pod start. A reflector watching
this AA would see (in its DeltaFIFO): DELETE for each old-UID object +
ADD for each new-UID object. That's 2*N reconcile events for N objects
on every restart, regardless of whether the objects' data changed.

**Scenario 2 (deterministic mode):** UIDs are identical across restarts.

```
BEFORE: alpha=ccac22ce-ab0e-8cab-c515-466c3fc0b34b
AFTER:  alpha=ccac22ce-ab0e-8cab-c515-466c3fc0b34b
```

Verified stable across 3 consecutive restarts. A reflector comparing
its store to the fresh LIST sees the same UIDs and same data — no
DeltaFIFO delta produced, zero reconcile events fired.

**Scenario 3 (delete/re-create same name):** Same UID on re-create.

```
CREATE foo: UID=6cd9e9ac-726e-163f-6f3d-1a0e17df5090
DELETE foo
CREATE foo: UID=6cd9e9ac-726e-163f-6f3d-1a0e17df5090
```

Watch stream shows: `DELETED foo 6cd9e9ac...` then `ADDED foo 6cd9e9ac...`
(same UID). This is by design.

## What surprised us

**Nothing broke.** kubectl, the apiserver machinery, and the watch
broadcaster all accepted the deterministic UIDs without complaint.
There is no server-side validation that UIDs must be random or globally
unique. The UID field is treated as an opaque string by all machinery
we exercised.

**The reflector reconnect behavior was cleaner than expected.** When
the AA pod died and reconnected, kubectl's watch simply received the
same objects again (same UIDs). No spurious DELETED events appeared in
the kubectl watch output for the deterministic case. In the random
case, kubectl's watch showed the same pattern (ADDED events with new
UIDs) but this is because kubectl does not have a long-lived reflector
with DeltaFIFO; it simply reconnects and shows whatever it gets as
ADDED.

The real delta detection happens inside client-go's reflector, which is
what controller-runtime and ArgoCD use. The reflector compares the
LIST result to its in-memory store by UID. Same UID + same data =
no event. Different UID for same name = synthesized DELETE + ADD.

## Fundamentals touched

### Storage independence

Deterministic UIDs are a **fundamental** property of the storage layer.
Any backend that can regenerate its objects from a stable source (polling
an external API, reading a filesystem, querying a database) benefits from
deterministic UIDs. The formula `SHA256(group/resource/namespace/name)` is
purely a function of the object's identity — no persistent UID store is
required, no coordination between replicas, no external state.

This closes the "pod-restart amnesia" cost from FINDINGS/0004 and the
"phantom-reconcile storm at O(objects) cost" from FINDINGS/0012 for the
common case where the backend's objects are stable across restarts.

### Watch and consistency semantics

From a reflector's perspective, deterministic UIDs make the AA appear as
if its objects survived the restart — they have the same UIDs and (for a
stable-source backend) the same data. The reflector's DeltaFIFO produces
no deltas. This is the same behavior you'd get from a CRD-backed resource
served by kube-apiserver's etcd (where objects genuinely survive restarts).

The key distinction: the objects did NOT survive the restart. The in-memory
store was rebuilt from scratch. But because the UID is a pure function of
the identity, downstream observers cannot tell.

## Consequents

**Same-UID-on-recreate violates Kubernetes UID conventions.** The
Kubernetes documentation states that UIDs are "unique in time and space"
— if you delete object "foo" and create a new "foo", the new one should
have a different UID. With deterministic UIDs, delete+recreate produces
the same UID. This matters in two concrete scenarios:

1. **ownerReferences**: A controller that sets `ownerReference.uid` to
   track ownership across names would not distinguish a deleted-then-
   recreated owner from a surviving one. For our use case (stateless AA
   backed by external data) ownerReferences don't apply — the objects
   are projections, not owned entities.

2. **Garbage collection**: kube-controller-manager's GC uses
   ownerRef.uid to decide when to cascade-delete dependents. If the
   owner is deleted and recreated with the same UID, GC would not
   fire. Again, irrelevant for a stateless projection AA.

3. **Event correlation**: Events reference their involvedObject by UID.
   Same-UID-on-recreate means events from the old object would appear
   to belong to the new one. Acceptable when the objects represent the
   same underlying entity (which they do, in our model).

For the specific use case of a stateless AA projecting stable external
state, the convention violation is harmless and the operational benefit
(zero phantom reconciles) is significant. The key insight: deterministic
UIDs are appropriate when the identity IS the name — when there's no
meaningful distinction between "old foo" and "new foo" because both
represent the same external entity.

**The hash formula must include all identity components.** Omitting
namespace would produce UID collisions between same-named objects in
different namespaces. Omitting the group or resource would collide
across different resource types. The full `group/resource/namespace/name`
key is the minimum safe input.

**No performance cost.** SHA256 of a short string (<100 bytes) is
unmeasurably fast compared to the apiserver's request-handling overhead.

## What this changes for SYNTHESIS and EXPERIMENTS

SYNTHESIS: The "deterministic UIDs" candidate in the Storage independence
section should be updated from "not implemented" to "implemented and
validated." The claim from FINDINGS/0012 that it's "load-bearing at scale"
is confirmed — the mechanism works and the cost is zero.

EXPERIMENTS: The `repo-uid-stability` candidate under Storage independence
and the `controller-runtime-dynamic-client-phantom-reconciles` candidate
under Watch and consistency semantics are both informed by this experiment.
The former's question ("does consumer behavior improve?") is answered: yes,
phantom reconciles are eliminated. The latter's scaling question ("measure
the cost at 10k+") remains interesting but now has a mitigation in hand.

## Open questions

- At what object count does the phantom-reconcile storm become
  operationally visible without deterministic UIDs? FINDINGS/0012
  tested at 3 objects. At 100, 1000, 10000 the O(N) reconcile burst
  on restart becomes a real thundering-herd problem.
- Should deterministic UIDs be the substrate default for backends that
  implement a `StableID() string` method? Or should it remain opt-in?
- Does the same-UID-on-recreate edge case cause problems for any real
  ecosystem controller (ArgoCD, Flux, kube-controller-manager)? All
  three rely on ownerRef UIDs; none should be setting ownerRefs on our
  projected objects, but confirming this would close the concern.
