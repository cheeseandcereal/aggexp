# FINDINGS: 0043-embedded-lock-emission-filtering

## What we were trying to learn or break

0032 (Lease) and 0033 (custom-CRD CAS) both put a per-object write lock
in a *separate* object with its own lifecycle and GC story. 0033's open
questions explicitly asked whether the lock and the 0024 metadata could
be one CRD — "Pro: fewer CRD API calls per write. Con: couples lock
lifecycle to metadata lifecycle; GC semantics become tangled." 0043
takes that bet: collapse the lock into the same metadata CR that 0042
already uses as the served object's resourceVersion authority, CAS'd on
that CR's own RV.

Co-locating the lock with the served object's metadata has two
non-obvious consequences, and the experiment exists to probe them:

1. Lock acquire, release, and renewal now *write the served object's
   CR*, advancing its resourceVersion and firing the metadata informer.
   Does this "lock churn" surface as spurious MODIFIED events to
   watchers, and if so, is filtering it out *required* (not optional
   polish) for the embedded design to be usable?
2. Because acquiring the lock bumps the CR's RV, an optimistic-
   concurrency check (0039) that compares the client's RV against the
   *post-acquire* RV would 409 every conditional write. Can the OCC
   check be correctly ordered against a *pre-acquire* RV?

The thing we were trying to break: the watchability of a watch stream
under sustained lock churn, and the correctness of conditional updates
under a lock that mutates the very RV the client conditions on.

## What we did

Copied the 0042 two-CRD skeleton wholesale (metadata CR on
`widgetmeta.aggexp.io`, shared body CR on `widgetbody.aggexp.io`,
metadata-CR RV authority, per-replica informers, 3-replica StatefulSet)
and extended it:

- The metadata CR schema grew a `spec.lock`
  (`holderIdentity, acquiredAt, renewedAt, leaseDurationSeconds`) and a
  `spec.observed.bodyHash`.
- A new `pkg/locking` acquires/releases/renews the embedded lock by
  reading the raw CR, mutating only `spec.lock`, and `Update`-ing with
  the read RV (CAS). Fresh-held → fail-fast 409; expired → CAS
  takeover; CAS-level conflict → 3-attempt 25ms exponential-backoff
  retry. A renewal goroutine re-stamps `renewedAt` every `Lease/3`.
- The write path performs exactly two CR writes: **acquire** (sets
  `spec.lock`) then **commit-release** (writes the body hash + KRM
  metadata AND clears the lock in a single `Update`).
- The metastore watch path computes a *visible signature* of each CR
  (body hash + served KRM metadata, excluding RV and `spec.lock`) and
  suppresses a MODIFIED whose signature is unchanged from the last
  emission.
- The REST `Update` captures the served object's RV *before* acquiring
  the lock and runs the OCC check against that pre-acquire value.

Ran all five README scenarios on a 3-replica StatefulSet in kind
(k8s 1.32, cluster `aggexp-0043`).

## What we observed

**Two CR writes per served write, RV advancing opaquely.** A create
went CR rv 1046 (acquire) → 1048 (commit-release); an update went
1081 (acquire) → 1083 (commit). The served object's RV is always the
commit RV; the acquire RV is consumed but never surfaced. Clients
tolerate the gap (the API treats RVs as opaque).

**Scenario 1 — contention → 409.** With a fresh lock held by another
identity, a write returned `409 Conflict: object "w1" is locked by
"other-replica"` in **72 ms** — fail-fast, no acquirer-side spin
(0033 semantics).

**Scenario 2 — holder crash → lease takeover.** With a simulated
crashed holder (a fresh lock left stamped by `other-replica`), the
next write after the 15 s lease elapsed took over via a single CAS
`Update` (`lock-takeover ... from="other-replica"`) and succeeded in
**92 ms**. Single round-trip when uncontested, exactly as 0032/0033.

**Scenario 3 — renewal across a slow backend op.** With a 20 s backend
write delay (> the 15 s lease, > 3 renewal intervals), `renewedAt`
advanced every ~5 s (03:55:00 → :05 → :10 → :15) while `acquiredAt` and
`holderIdentity` stayed put. No premature expiry, no takeover; the
write committed cleanly. The renewal goroutine does what it is for.

**Scenario 4 — emission filtering (the key result).** For a single
user update on an object with a present body: the acquire write
(rv 1429) was **suppressed**, the commit write (rv 1431, body hash
changed) was **emitted** — exactly one MODIFIED, not two. All three
replicas agreed independently. For the slow update spanning four
renewals: acquire (rv 1742) suppressed, four renewal heartbeats
(rv 1749, 1757, 1764, 1772) **all suppressed**, commit (rv 1774)
emitted — exactly one MODIFIED, zero spurious events from the renewal
churn. The 30 s informer resync (same RV, same signature) was also
suppressed. `kubectl get widgets -w` saw only the body-change events.

**Scenario 5 — pre-acquire OCC ordering.** A conditional `replace`
carrying the object's current RV (1090) succeeded and advanced the
object to rv 1994 — even though lock acquisition bumped the CR's RV
in between. A second conditional `replace` reusing the now-stale RV
1090 got `409 Conflict`. The OCC check compared against the
pre-acquire RV; lock churn produced no false 409s, genuine staleness
produced a true one.

**Cross-replica read consistency (0042 inheritance) held.** After an
update, all three replicas pinned in turn returned identical
`rv=2073, size=314`.

## What surprised us

**Emission filtering is required, not polish — but only on the Update
path; the Create path hides the churn for a different reason.** On
Update of an existing object, the acquire write genuinely fires a
MODIFIED that the informer delivers, and without the filter every
write (and every renewal) would surface. The filter is load-bearing
there. On *Create*, the acquire is a CRD `Create` of a brand-new CR
followed quickly by the commit `Update`; the informer coalesced these
into a single ADDED at the commit RV, so the acquire was never even a
separate event to suppress. And during a *slow Create*, the renewal
writes fired informer events that were dropped *upstream of the
filter* — because the body did not exist yet, `StitchForRef` returned
"not present" and the event never reached the emission filter at all.
So the embedded-lock design is watchable through two distinct
mechanisms layered together: (a) body-absence suppression already
present in 0042's stitch, and (b) the new visible-signature filter for
the steady state. Only (b) is the experiment's contribution, and it is
strictly necessary for Update/renewal churn.

**Deleting the host object releases the lock for free.** Because the
lock lives on the served object's own CR, `Delete` of the CR removes
the embedded lock atomically. There is no separate lock object to GC,
no orphaned-lease window, none of 0033's "expired ObjectLock CRs
accumulate forever" problem. This is the clearest argument that
co-locating is a net simplification: the lock's lifecycle is the
object's lifecycle by construction.

**The CRD `required` field fought the embedded design.** 0042 declared
`spec.required: ["resourceRef","metadata"]`. The embedded acquire
CAS-creates a CR that carries only `resourceRef` + `lock` (metadata
isn't known until commit), so creation was rejected with
`spec.metadata: Required value` until we relaxed the requirement to
just `resourceRef`. A small thing, but it shows the embedded lock
forces the served object's CR to exist in a *pre-metadata* state — the
lock's lifecycle starts before the object's metadata does.

## Which fundamentals this touched

**Watch and consistency semantics (primary).** The headline result is
that a per-object lock *can* share the served object's RV-authority CR
and remain invisible to watchers, but only if the watch path filters on
watcher-visible state rather than on "the CR changed." Lock state is a
write-coordination concern; it must be projected out of the read/watch
model entirely. The emission filter is the mechanism that keeps the
0042 single-RV-authority guarantee intact while the same CR doubles as
the lock: the served RV still advances monotonically and opaquely
(clients tolerate the gaps), Get/List/Watch still agree cross-replica,
and the only thing that changes is *which* CR transitions become
events. This generalizes: any design that overloads the served
object's storage record with coordination metadata (lock, lease,
in-flight markers) needs an emission filter keyed on the projected,
client-visible view, or its watch stream becomes unusable.

**Per-request authorization / write coordination (secondary).** The
acquire/takeover/fail-fast contract from 0032/0033 transferred to the
embedded subfield unchanged — CAS on a kube-apiserver object's RV is
the same primitive whether the object is a Lease, a separate lock CR,
or a subfield of the served object's CR. The embedding changes the
*lifecycle and GC* story, not the concurrency primitive.

**Storage independence (tertiary).** The lock is no longer its own
storage axis; it folds into the metadata axis. 0033 had argued lock
state was a distinct storage axis. 0043 shows that for the per-object
case it does not *have* to be: it can ride the metadata record. This is
a refinement, not a contradiction — the axes are separable but not
*required* to be separate, and folding them removes a CRD, an API
group, and a GC obligation.

## Consequents (environment-specific)

- **Two host CR writes per served write** (acquire + commit-release),
  plus one renewal write per `Lease/3` while a backend op is in flight.
  Versus 0042's single metadata-CR write per served write, the embedded
  lock doubles the metadata-CR write amplification on the steady path
  and adds `op_duration / (Lease/3)` renewal writes on slow ops. Tied
  to this env's etcd latency; at lab scale (kind, one node) each write
  is single-digit-to-low-tens of ms and invisible end-to-end. 0047
  (host-etcd-write ceiling) is where this amplification would bite.
- **Fail-fast 409: ~72 ms; expired-lease takeover: ~92 ms.** Both
  dominated by the kubectl process + TLS baseline 0042/0033 already
  noted (~60–100 ms), not by the CAS itself. The marginal cost of the
  embedded CAS over a plain write is one extra in-cluster
  GET+Update round-trip, the same ~10–20 ms 0033 measured.
- **Renewal interval 5 s, lease 15 s** produced 4 renewal writes over a
  20 s op. These are arbitrary 0032/0033-inherited defaults; the
  renewal write rate scales as `3 × op_duration / Lease`.
- **The informer coalesces a Create+immediate-Update into a single
  ADDED.** This is why the Create-path acquire never needed
  suppressing. A consequent of client-go's DeltaFIFO behavior at this
  k8s version, not a property of the design — a slower commit could in
  principle let the acquire surface as its own ADDED with empty body.
- **The 30 s resync still fires same-RV MODIFIEDs**; the emission filter
  suppresses them (same signature), which is a happy side effect — 0042
  relied on SharedInformer client-side dedup for this; here the server
  filters them outright. Tied to the chosen 30 s resync.

## What this changes for SYNTHESIS and EXPERIMENTS

For SYNTHESIS: the Watch-and-consistency section should record that
overloading the served object's RV-authority record with write-
coordination state (the embedded lock) is viable *iff* the watch path
emits on a projected client-visible signature, not on raw CR change.
The storage-independence section should be softened: lock state is a
*separable* axis (0033) but not a *necessarily separate* one — for
per-object locking it can fold into the metadata record, trading a CRD
+ API group + GC obligation for doubled metadata-write amplification
and a mandatory emission filter. The per-object embedded lock is a net
simplification when watch churn is filtered and write amplification is
acceptable.

For EXPERIMENTS: the 0033 open question "should lock + metadata be one
CRD?" is answered — yes for per-object, with the emission-filter
caveat. The write-amplification consequent feeds 0047. The
per-resource lock case (0033's coarse mode) is untouched here and
would *not* fold cleanly, since a per-resource lock has no single
served object's CR to ride.

## Open questions

- The emission filter holds per-replica in-memory `lastEmitted` state.
  On replica restart it is empty, so the first post-restart event for
  each object re-emits (signature unknown). A watcher would see one
  redundant MODIFIED per object after a replica restart. Harmless
  (same RV-or-higher, idempotent body), but uncharacterized under churn
  + restart together.
- We never observed the 3-attempt CAS retry budget fire: at lab scale,
  aggregation connection reuse routes traffic to one replica, so real
  cross-replica CAS races on the same CR did not occur naturally
  (the 0033 consequent). Contention was induced by hand-stamping the
  lock. The retry budget is exercised only by genuine simultaneous
  cross-replica writes to the same object, which this harness does not
  reliably produce.
- A backend op that *fails* after acquire but before commit leaves the
  lock held until lease expiry (we `Release` on the error path, but a
  hard crash mid-op relies on lease takeover). The takeover path is
  proven; the crash-mid-commit ordering (body written, commit not) is
  the same partial-write window 0042's open questions flagged and is
  not closed here.
- Whether the doubled metadata-CR write amplification matters is an
  open scaling question for 0047, not answerable at single-node lab
  scale.
