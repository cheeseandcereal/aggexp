# Findings — 0045 read-path-reconcile-amplification

## What we were trying to learn or break

0028 left a sharp edge. In the 0024/0028 split-store model (business
body on a backend, KRM metadata on a host CRD, stitched on read), a
metadata record can outlive its backend object. 0028's garbage
collector sweeps periodically and deletes such orphans, but with a
twist: a finalizer-protected orphan lingers, and the *only* way to
clear its finalizer is to edit the metadata CR directly on the host
apiserver — because the stitched Get fails when the backend is absent,
so `kubectl patch widget` (which does a Get first) 404s and never
applies the patch. 0028 named the fix as an open question: a
"tolerant Get" that returns a record-only `Orphaned` view instead of
404.

0045 takes the *opposite* bet. Rather than make Get tolerant of
backend absence, make the **backend the source of truth for
existence** and reconcile the metadata store against it *inline on
every read*. A backend 404 is then a 404, period — finalizers don't
enter into it, so there is nothing to "tolerate" and nothing to
hand-edit. The question is whether this cleanly removes the 0028 sharp
edge, and what it costs: every Get now reaches the backend (no
store-miss short-circuit), so a flood of Gets for non-existent names
becomes a flood of backend calls. We wanted concrete amplification
numbers and to see whether a negative cache is warranted.

## What we did

Forked the 0042 metadata-CR core verbatim (the shared body CRD, the
metadata CR with host-RV authority, the stitched `rest.Storage`). 0042
was the right base precisely because its body lives on a shared
cluster-scoped CRD (`widgetbodies.widgetbody.aggexp.io`), which can be
mutated **out of band**: `kubectl apply`/`delete` of the WidgetBody CR
creates/destroys a backend object without going through the AA, so
adoption and collection on read are directly observable.

Changes on top of 0042 (~800 net new lines):

- `pkg/backend`: an authoritative existence query
  (`GetAuthoritative`) that always reads the body CR directly from the
  host apiserver — never the informer cache, never a store-miss
  short-circuit — plus `ListAuthoritative`. Both bump backend-call
  counters. An optional negative-existence cache (flag, default off).
- `pkg/widgetrest`: read-path reconcile. Get adopts (synthesizes +
  persists a record) when the backend has an object with no record,
  and collects (deletes the record, subject to a 30s `minAge` guard)
  when a record's backend object is gone. There is no tolerant-Get.
  List reconciles the same way against the authoritative backend set.
- `pkg/sweep`: a periodic sweep (the 0028 shape) that calls the
  *same* `ReconcileList`, plus a `:8444` debug endpoint exposing
  counters and runtime toggles (`/adopt`, `/gc`, `/negcache`,
  `/sweep`, `/reset`).
- `pkg/metrics`: atomic counters for backend Get/List, served
  Get/List, adopt/collect split by inline-vs-sweep, age-skips, and
  negative-cache hits/misses.

Single replica (this is about read paths, not cross-replica). Five
scenarios run end to end against kind cluster `aggexp-0045`.

## What we observed

**The tolerant-Get sharp edge is gone, cleanly.** Scenario 3: created
a Widget normally, patched a finalizer onto it (which worked, because
the body still existed), deleted its backend body out of band, then
Got it. Get returned a plain 404 — not a record-only `Orphaned` view —
and the finalizer-protected record was collected. The collect log line
records `finalizers=["lab.aggexp.io/test"]` at deletion time: the
finalizer was present and ignored. There is no scenario in which an
operator must hand-edit the metadata CR, because there is no state in
which the metadata store's opinion overrides the backend's. The 0028
escape hatch (`kubectl patch resourcemetadata ...`) is simply unneeded.

**Adopt-on-read works inline.** Scenario 1: an out-of-band WidgetBody,
no record. A single `kubectl get` synthesized a record
(`aggexp.io/adopted-by: read`), stitched the body, returned the object
with a real host RV and UID, and the object then appeared in List. One
served Get, one backend Get.

**The minAge guard holds, and inline collect and sweep collect are the
same code.** Scenario 2: deleting a body out of band while its record
is younger than 30s makes Get 404 but leaves the record
(`collectSkippedAge` increments). Past 30s, the next read collects it.
With GC toggled off, neither the read path nor the sweep touches the
orphan; toggling GC back on, the very next Get collects it inline
(`collect from=read`). A manual sweep with no intervening Gets adopts
foreign objects via the sweep counters (`adopt from=sweep`). Because
both paths call one `ReconcileList`/`adopt`/`collect`, "the inline path
and the sweep agree" is true by construction, not by coincidence.

**Read amplification under 404-heavy load is exactly 1:1, and the
negative cache collapses it.** Scenario 4, measured on this
backend/env:

- 50 Gets for *random* non-existent names, neg-cache **off**: 50
  backend Gets. Amplification **1.0**. Every miss is a backend
  round-trip; there is no store-miss short-circuit by design.
- 50 Gets for the *same* non-existent name, neg-cache **on** (TTL
  2s): **5** backend Gets, 45 cache hits. Amplification **0.1**. The 5
  misses correspond to the ~2s TTL expiring across the ~10s wall time
  of the kubectl loop.
- 30 high-QPS Gets for an *existing* name: 30 backend Gets,
  amplification **1.0**, zero cache hits — the negative cache caches
  only negatives, so live objects always reach the backend.

**Adoption noise on a shared backend is real, and the toggle is the
lever.** Scenario 5: five foreign WidgetBody objects created out of
band. With adoption on, all five surface as `widgets` (each adopted on
first read or by the sweep). With adoption off, Get 404s for each and
List omits all of them.

**Sweep cost is single-digit milliseconds** (2–14 ms at this scale),
consistent with 0028.

## What surprised us

**Adoption is a *visibility* decision, not just a *record-creation*
decision — and getting that wrong is silent.** The first
implementation treated "adoption off" as "don't persist a record" but
still *stitched and served* any backend object it found (with a
synthetic UID/RV). That made adoption-off identical to adoption-on
from the client's point of view: every foreign object still appeared
in Get and List. The fix was to make adoption-off *suppress the
object*: a record-less backend object 404s on Get and is omitted from
List. This is the load-bearing semantic of scenario 5, and it was not
obvious from the README's framing — "adopt" sounds like a bookkeeping
choice, but on a source-of-truth-is-the-backend model it is actually
the filter that decides what the AA is willing to call its own.

**The negative cache reintroduces a small version of the staleness the
whole experiment was trying to banish.** The cache caches *absence*.
But absence is exactly the thing the backend is authoritative for, and
the cache is, by definition, not the backend. Two concrete bites:

1. A create-through-the-AA right after a failed Get serves a stale
   404 for the rest of the TTL — because the kubectl `apply` does a
   client-side Get first (planting the negative entry) and the
   subsequent create writes the body. We fixed this by invalidating
   the negative entry on `bodies.Put`.
2. An *out-of-band* create cannot be fixed the same way: nothing calls
   `bodies.Put`, so a negative entry planted by a prior Get masks the
   new object for up to the TTL. We observed this directly: with the
   cache on, an out-of-band-created object 404s for ~2s, then is
   adopted on the next Get.

So the negative cache trades amplification for a bounded window of
"backend is no longer the source of truth for existence." The window
is the TTL. For 404-heavy *read* load that never expects out-of-band
creates of the names being probed, it is a clean win (10× fewer
backend calls in our run). For a backend that is genuinely mutated out
of band — the case this experiment is built around — it is a direct
compromise of the model's central invariant. That tension is the most
interesting thing the cache surfaced.

## Fundamentals touched

**Storage independence (primary).** 0028 framed the split-store GC
obligation as "you owe the world a periodic sweep." 0045 shows the
obligation has a *cleaner discharge*: if the backend is treated as
authoritative for existence and reconciled inline on read, the
metadata store becomes a pure overlay with no independent opinion
about what exists. This is **fundamental**, not implementation
detail: it flows from the architecture of a split-state aggregated
API. With one store authoritative for existence, the failure mode
0028 hit (two stores disagreeing about existence, with the read path
trusting the wrong one) cannot occur — there is only one answer to
"does this exist," and it is the backend's. The tolerant-Get sharp
edge was a symptom of letting the metadata store's record imply
existence; removing that implication removes the symptom.

The cost is also fundamental: making the backend authoritative for
existence means every existence query (every Get, every List) is a
backend round-trip. There is no correct way to short-circuit a Get to
a 404 from the overlay, because the overlay is not authoritative. The
amplification is 1:1 by construction. Whether that is acceptable is a
backend-capacity question, not an architecture question — and the only
mitigation that preserves the model is a cache that *also* observes
the backend (push/watch-driven invalidation), not a blind TTL.

**Resource modeling freedom (secondary).** The adoption toggle is a
modeling knob: it decides whether the AA's resource set is "exactly
the backend's objects" (adoption on; the AA is a faithful projection)
or "only the objects the AA has explicitly taken ownership of"
(adoption off; the AA is a curated subset of a shared backend). On a
*dedicated* backend, adoption-on is the natural choice and there is no
noise. On a *shared* backend, adoption-on means every foreign writer's
objects become your resources — which is either a feature (you really
do want to project the whole backend) or noise (you share storage with
other tenants). The toggle is a coarse lever; the finer alternative,
not implemented here, is backend-side filtering in `ListAuthoritative`
(e.g. a label or name-prefix selector), which would let the AA project
a *named slice* of a shared backend without adopting everything.

## Consequents (implementation-dependent; do not generalize)

- **1:1 amplification and the 0.1 cache figure are tied to this
  backend (a host CRD read per existence query) and this harness
  (kubectl-driven Gets, ~0.5–1s apart).** The *ratio* generalizes (no
  short-circuit ⇒ 1 backend call per Get); the *absolute* cost and the
  exact number of cache misses (5/50) are a function of the 2s TTL
  against the kubectl loop's wall time. A different backend (S3,
  GitHub, postgres) or a real high-QPS client would produce different
  absolute numbers.
- **Negative-cache TTL of 2s** is arbitrary and short on purpose, so
  expiry is visible within a kubectl loop. A production value would be
  tuned to the create-out-of-band latency the operator is willing to
  mask.
- **The negative cache is invalidated on `bodies.Put` but not on
  out-of-band writes** — it cannot observe what doesn't go through it.
  This is inherent to a blind TTL cache, not a bug; a push/watch-driven
  invalidation would close it but requires a backend that emits
  existence events (0025-style).
- **Existence is queried by a direct host-apiserver read, not the
  informer cache.** This is the right call for an out-of-band-mutable
  backend (the cache lags), but it means we do not benefit from the
  informer cache for existence at all — every Get is a real API call.
  A backend whose informer is the authority (no out-of-band mutation)
  could serve existence from the cache and drop the amplification to
  near zero; that is a different experiment's backend.
- **Debug endpoint `:8444` is unauthenticated.** Lab only; same
  posture as 0028.
- **Adoption-off omits unadopted objects from List by skipping them
  post-reconcile**, which still pays the full `ListAuthoritative` cost
  (the backend objects are fetched, then dropped). Backend-side
  filtering would avoid fetching them at all.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**, under Storage independence: the split-store GC
obligation 0028 named has two discharge strategies, and they trade
different sharp edges.

- *Periodic-sweep-only with a tolerant Get* (the 0028 direction):
  cheap reads (the overlay can short-circuit), but the metadata store
  retains an independent opinion about existence, which produces the
  finalizer-clear sharp edge and requires operators to understand the
  backing CRD.
- *Backend-as-source-of-truth with inline reconcile* (0045): no sharp
  edge — the metadata store is a pure overlay, a 404 is a 404, and
  there is nothing to hand-edit — at the cost of 1:1 read
  amplification against the backend. A blind negative cache reduces
  amplification but reintroduces a bounded staleness window that
  directly compromises the "backend is authoritative for existence"
  invariant; only a backend-observing (push/watch) cache preserves
  both.

The clean answer to the experiment's framing question: **yes, backend-
as-source-of-truth-for-existence removes the 0028 tolerant-Get sharp
edge, and it is a genuinely cleaner model** (one authority, no
hand-editing) — but it *does* trade the sharp edge for an amplification
problem on the read path. The trade is favorable when the backend's
existence query is cheap or pushable, and unfavorable when it is
expensive and the negative cache (the obvious mitigation) is unsafe
because the backend is mutated out of band. SYNTHESIS should record
this as a real fork in the split-store design space, not a strict
improvement.

For **EXPERIMENTS**: 0045 is complete. The push-backed-watch direction
(0025) is now doubly motivated — it is the only negative-cache
invalidation strategy that preserves the source-of-truth invariant.
Backend-side `List` filtering for shared-backend projection is a small
unclaimed follow-on.

## Open questions

- **Push/watch-driven negative cache.** A backend that emits existence
  events could invalidate negative entries on out-of-band creates,
  giving low amplification *and* a preserved source-of-truth invariant.
  Worth probing on a 0025-style push backend.
- **Backend-side List filtering for shared backends.** The adoption
  toggle is all-or-nothing. A label/prefix selector pushed into
  `ListAuthoritative` would let an AA project a named slice of a shared
  backend without adopting every foreign object, and without paying to
  fetch-then-drop them. Small scope.
- **Amplification under a real high-QPS client.** Our numbers are
  kubectl-driven. The 1:1 ratio is structural, but the backend-
  capacity implications (does the host apiserver throttle the AA's
  existence reads under sustained load?) are uncharacterized.
- **minAge vs. inline collection under write races.** 0028's
  in-flight-Update-vs-GC race still exists here; the inline collect
  fires on a read that could interleave with a concurrent write. The
  minAge guard and idempotent delete keep the blast radius small (same
  as 0028), but we did not adversarially probe it.
