# Findings — 0028 metadata-store-gc

## What we were trying to learn

`FINDINGS/0024-metadata-crd-store` closed a big chunk of the
stateful-middleware-refinement arc by showing that splitting
business data (backend) from KRM metadata (host-cluster
ResourceMetadata CRD) works end-to-end. It also named one
follow-up as an open question: **if a backend object disappears
out of band — wiped, deleted directly against its storage, a
whole DB nuked — its ResourceMetadata record leaks.** 0028 is
the garbage collector that cleans it up.

The deeper question is whether the GC is itself interesting or
merely plumbing. My bet going in: the policy surface (what to
skip, what to collect) and the races are where the learning
lives; the mechanism is plumbing.

## What we did

Forked 0024 verbatim (same s3-mock, same backend-s3, same
stitchedrest, same metastore package, same CRD). Added
`component/gc/gc.go` — a ~300-line package with a `Reconciler`
that:

- lists metastore records filtered to its one (group, resource),
- lists backend objects via the existing `Backend.List` RPC,
- diffs by `(namespace, name)`,
- applies a policy per orphan (skip on finalizer, ownerReferences,
  deletionTimestamp, or record-age-below-grace-window; delete
  otherwise),
- serializes periodic and manual sweeps behind a mutex.

A tiny HTTP server on `:8444` exposes `POST /gc/run` (trigger one
sweep synchronously, return JSON) and `GET /gc/last` (last
result). `aggexp-gc-debug` Service fronts it so the demo reaches
it via `kubectl port-forward`.

Default periodic interval: 5 min in code; the demo manifest
forces 2 min so scenarios don't block. Default grace window: 30s
in code; 20s in the demo. Both arbitrary; recorded in README.

Four scenarios, all pass:

1. **Happy path.** 3 CRs, 3 backend objects. GC sweep: `seen=3
   backend=3 orphans=0 deleted=0`. No false positives.
2. **Partial orphan.** Delete one bucket (`beta`) directly against
   the s3-mock via a transient curl pod. backend-s3's live
   `ListBuckets` now returns only `alpha` and `gamma`; the
   metastore still has three records. GC sweep: `seen=3
   backend=2 orphans=1 deleted=1 deletions=[...beta]`. The other
   two records are untouched.
3. **Full wipe.** Delete the entire `s3-mock` pod; the new one
   comes up with an empty in-memory store. GC sweep: `seen=2
   backend=0 orphans=2 deleted=2`. Matches intent.
4. **Finalizer protection.** Create `sticky`, patch a
   finalizer, wipe backend. GC sweep: `seen=1 backend=0
   orphans=1 deleted=0 skipped=[{reason:"finalizers=[...]"}]`.
   Record survives. Subsequent GC with the finalizer still
   present: same skip. Clearing the finalizer on the Record
   directly and re-GC: clean delete.

An additional post-hoc probe, not in the scenario script:

5. **Grace-window race.** Create a bucket, immediately delete it
   against the s3-mock, then trigger GC before the minAge
   elapses. GC sees the record as an orphan but skips it with
   `reason="age<20s"`. Sleep past minAge and re-GC: cleanly
   collects. This is the protection against a brand-new Create
   whose metastore.Put has landed but whose backend.Create
   hasn't been re-listed yet (backend-s3 polls s3-mock every
   15s; during that window the backend.List omits the new
   bucket even though it exists).

## What we observed

### The mechanism is cheap

Sweep duration is 2–13 ms across every scenario, dominated by
the one host-apiserver LIST against `resourcemetadatas` and one
gRPC List to backend-s3. At the 3-record scale this says nothing
about scale. But the algorithm is O(records + backend objects)
so a 10k-record resource would still be under a second if the
backend's List is also sub-second.

### The grace window is load-bearing

Without it, 0028 would collect its own Creates whenever a
polling backend lags. With a 20s window and backend-s3 on a 15s
poll cycle, the window comfortably covers the worst case (create
at T=0, next poll at T=14.9s, first eligible GC at T>=20s, so
the backend has already re-listed).

For push-capable backends (0025) the window could shrink to near
zero because the backend knows about a new object the moment the
gRPC Create returns. The current policy is polling-safe by
default, which is the correct direction.

### Finalizer protection has a sharp edge under stitching

When an orphan is finalizer-protected, the Record lingers. A
user who wants to clean up must clear the finalizer. But: the
normal way to clear a finalizer — `kubectl patch bucket sticky
--type=merge -p '{"metadata":{"finalizers":[]}}'` — goes through
the stitchedrest Update path, which first does a Get. The Get
calls the backend. The backend 404s because the backend object
is gone. kubectl reports `NotFound: buckets.aggexp.io "sticky"
not found` and the patch never happens. **The only way out is
to patch the ResourceMetadata CR directly** on the host
apiserver (a GVR the user may not even know exists):

```
kubectl patch resourcemetadata aggexp-io.buckets.cluster.sticky \
    --type=json -p='[{"op":"remove","path":"/spec/metadata/finalizers"}]'
```

After which the next GC sweep cleans up.

This is a consequent: it's specific to how the 0024 stitched
Get fails when the backend is absent. A middleware that
tolerates backend-absence on Get (e.g. stitches with
`status.phase=Orphaned` and returns the Record-only view) would
close this sharp edge. It's worth lifting back into SYNTHESIS
as an addendum to 0024's "stitch-on-read" pattern: **stitch
failure modes should probably not hard-fail when only one of the
two stores has the object.**

### Races observed between GC and normal writes

Three distinct races, all manageable:

- **Create vs. GC** — the one the minAge window catches. A
  create that hasn't been re-listed by a polling backend looks
  like an orphan. Observed directly in scenario 5.
- **Delete-in-progress vs. GC** — a Record with deletionTimestamp
  set is skipped. This avoids racing with the stitchedrest
  Update's finalizer-clear path, which itself transitions the
  backend DELETE + metastore Delete pair.
- **In-flight Update vs. GC** — the pathological case. A client
  is mid-Update; stitchedrest has written the Record but not yet
  called backend.Update. If the backend is *in truth* absent
  (say, wiped between the client's previous GET and Update), GC
  might fire between these steps. In practice:
  - The record is already past the grace window (we're talking
    about a resource that has existed long enough to be updated).
  - The next sweep would see the same orphan anyway.
  - The delete is idempotent; no torn state.

  But: the response the client sees for its Update depends on
  the ordering. If GC deletes the Record *before* stitchedrest
  finishes its backend.Update call, the Record is re-created by
  stitchedrest.Put (which does a Get-then-Update or a Create).
  This effectively resurrects the Record with fresh metadata,
  possibly without managedFields if the library's handling
  branch was partial. I did not reproduce this against kubectl
  but the window exists and its consequence is subtle metadata
  loss, not data loss. An informed substrate would serialize
  against the GC (e.g. GC takes a shared Record lock during
  sweep; writes take an exclusive). This experiment did not go
  that far.

No torn writes observed in the five scenarios actually run.

### List-vs-Exists: the choice paid off

I chose to use the existing `Backend.List` RPC rather than add
an `Exists(ids []string)` RPC. The cost I expected — List returns
heavy bodies where Exists would return bools — was invisible at
the lab scale (the biggest List in any scenario moved 3 JSON
objects). The benefit — zero proto change, zero backend
modifications — was immediate: the 0028 backend-s3 is *byte-
identical* to 0024's. Every existing KRM backend (0013, 0017,
0018, 0019, 0021) could have its records GC'd without changing
a line.

**At scale this flips.** A backend with millions of objects
cannot be GC'd by streaming every object every sweep; paginated
List + cursor, or a real Exists RPC with bounded batch size,
becomes necessary. That's a substrate concern for the eventual
v2 promotion, not an experiment concern. 0028 records the
trade-off and moves on.

### Periodic sweep fires reliably

The periodic goroutine fires at the configured interval (2m in
the demo) with trigger=periodic in logs; the manual /gc/run
trigger is observed live in scenario runs. The serialization
mutex behaves: overlapping manual+periodic sweeps return 409
on the HTTP path.

### Scenario 4's secondary observation

When `sticky` was protected by the finalizer, I ran GC a third
time after the protecting user attempted `kubectl delete bucket
sticky`. kubectl reported a 404 (because the stitched Get fails
when the backend is gone), but GC still showed the Record as
present and still skipped it. Two state stores behaving
independently — the host apiserver has the Record, the backend
does not, and each operation sees its own ground truth. This is
*exactly* the behavior the 0024 thesis predicts; 0028 just
surfaces it more loudly.

## What surprised me

- **kubectl patch of the exposed resource doesn't work when the
  backend is gone** — because the stitched Update's Get phase
  fails against the absent backend. The operator's escape hatch
  is to edit the ResourceMetadata CR directly. In production
  this is unergonomic enough that the middleware should
  probably grow a tolerant-Get mode. (See above; queued as an
  open question.)
- **backend-s3's `seen` map was a red herring.** I initially
  worried that the backend's internal poll-diff cache would
  return stale data to GC's List call. It doesn't: the backend's
  `List` handler does a live `ListBuckets` against s3-mock,
  ignoring `seen`. The cache is only used for the watch-event
  diff. Less work to design around than I feared.
- **How cheap everything is.** Five ms sweeps. The host
  apiserver's LIST on a 3-record CRD returning ~1.5 KiB; a gRPC
  List returning three small JSON objects. The whole GC loop's
  operational cost at lab scale is rounding error. This tips my
  intuition about whether GC should run on a frequent schedule:
  unless the backend's List is expensive, there's no operational
  reason to keep the interval as long as the default 5 min. The
  correctness reason stands (it's a reconciler; slow is fine).

## Fundamentals touched

**Storage independence** (primary). 0024's storage-axis variant
— business data on backend + KRM metadata in host CRD — has a
**fundamental** requirement that I hadn't fully internalized
from 0024 alone: two independent state stores *must* be
reconciled, because neither is authoritative for the other's
existence. This applies regardless of backend (S3, postgres,
GitHub, whatever), RPC shape (gRPC or HTTP, 0026), or schedule
(polling, pushed, or webhook-driven). The reconciliation *is
inherent* to the split. The specific policy (what to skip, when
to sweep, what triggers on-demand) is consequent — a different
middleware could make different choices and still honor the
fundamental.

Notably, the 0024 CRD-facade pattern **does not have this
reconciliation problem** because the host kube-apiserver is the
single source of truth (one store, not two). The cost of the
metadata-CRD-store pattern relative to the CRD-facade pattern
is "you owe the world a GC." 0028 shows the bill is small.

**Watch and consistency semantics** (secondary). The races
between GC and normal writes are the interesting observations
here. The simplest form — "GC might run concurrently with a
client Update" — has a real window but a small blast radius
(subtle metadata overwrite in the worst case, never data loss).
The grace window closes the most common race (new-Create vs.
polling-backend-lag). The remaining races require either a lock
(overkill for the scale this experiment targets) or a "heal on
next sweep" tolerance, which the reconciler pattern already has
by nature.

## Consequents (implementation-dependent; do not generalize)

- **backend-s3's 15s poll interval drives the minimum grace
  window.** For a different backend the appropriate minAge is
  different. For a push-capable backend (0025) it could be near
  zero. Operators would tune.
- **List-vs-Exists decision** is scale-dependent. Fine at lab
  scale; would flip for ≥10⁴ records.
- **Debug endpoint `:8444` is unauthenticated.** Fine for lab.
  A production deployment would want admission or a killswitch
  flag.
- **Sweep interleaving uses a plain `sync.Mutex`.** Fine;
  rejecting overlapping sweeps with 409 is ergonomic enough.
- **Record-age vs. host-CR creationTimestamp**: the reconciler
  uses the embedded Record's `.spec.metadata.creationTimestamp`
  (the KRM metadata's creationTimestamp) as the age proxy,
  rather than the host CR's own `.metadata.creationTimestamp`.
  The two can diverge if a Record was ever stitch-synthesized
  for an existing backend object before being written. I
  accepted the approximation; a stricter implementation would
  consult both.
- **Stitched Get fails when the backend is gone**, which makes
  kubectl patch of a finalizer-protected orphan's exposed
  resource impossible. Operator must edit the ResourceMetadata
  CR directly. A middleware change — tolerant Get — could
  close this. Consequent of the current stitchedrest, not
  fundamental.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**, under Storage independence:
- Note that the "business data on backend + KRM metadata in
  host-cluster CRD" axis carries a **recurring reconciliation
  obligation** (the "fifth axis" 0024 named).  0028 shows the
  obligation is real (orphans happen) and the cost is small
  (single-digit-ms sweeps; ~300 LOC). Add the claim: **any
  split-state-store architecture inherits this GC obligation as
  a fundamental, not an optional polish.**
- Under Watch and consistency semantics, add that in
  split-store architectures GC and normal writes have a small
  but non-zero race window; the grace-window pattern handles
  the common case (polling-backend-lag vs. new-Create) and the
  residual races heal on subsequent sweeps.

For **EXPERIMENTS.md**: mark `0028-metadata-store-gc` complete
under the stateful-middleware arc with cross-references to
`0024-metadata-crd-store` (the store being GC'd) and
`0025-push-backed-watch` (how a push-capable backend narrows
the grace window).

No candidate retires. 0027 (the multiplex reconciler) will need
per-APIDefinition GC scoping; 0030 (v2 promotion) inherits all
of this.

## Open questions raised

- **Tolerant stitched Get.** The kubectl-patch-can't-clear-
  finalizer sharp edge deserves a follow-up. Simplest fix: when
  `backend.Get` returns NotFound but the metastore Record
  exists, stitch a Record-only view with `status.phase=Orphaned`
  (or equivalent) instead of returning 404. Let kubectl patch
  through. Risks: a client doing `kubectl get bucket && kubectl
  apply` would see a phantom, then a re-Create. The right semantic
  is context-dependent.
- **Lock-free or locked reconciliation?** This experiment
  didn't serialize GC against in-flight writes. At the scale
  we run it (sub-ms operations) no race bit us; at production
  scale (long-lived in-flight Updates, backend-side deprovision
  taking seconds) the GC-vs-Update race might. A per-Record
  short-lived lock or a version-number check would close the
  window; a richer experiment would probe this.
- **Push-backed GC.** When a backend emits DELETE events
  (0025), the middleware can reconcile *immediately* rather
  than waiting for the next sweep. A hybrid — push reduces
  sweep urgency; periodic sweep remains the safety net — is
  the natural follow-on. Tiny scope; worth doing as part of
  the v2 promotion.
- **`Exists(ids)` RPC for large backends.** At 10⁴+ records the
  List-based approach is wasteful. Adding an optional `Exists`
  RPC (backend returns it or not; middleware falls back to
  List) is a natural proto v2 concern.
- **Cascade-aware GC.** Skipping on ownerReferences is
  conservative but leaky. A proper implementation would check
  whether the owner still exists (across aggregated groups)
  and cascade-collect on "owner gone too." This is the same
  problem Kubernetes' GC controller solves for CRDs in etcd;
  getting it right for a cross-store, cross-group AA is
  substantial and not in scope here.
