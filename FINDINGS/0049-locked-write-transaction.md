# FINDINGS: 0049-locked-write-transaction

## What I was trying to learn or break

The 0048 capstone surfaced a sharp edge it could not fix without leaving
the composition arc: the embedded per-object lock (0043) serializes lock
*acquisition*, but the two writes that follow acquisition — the body-CR
`Put` and the metadata commit-release — were single-shot. Under genuine
cross-replica write contention some losers got a **500 InternalError**
(a CAS conflict on the body or the commit) instead of a clean **409
Conflict**. The lock behaved as an admission gate, not a transaction.
0048 named two candidate fixes and deferred them.

0049's question is narrow and load-bearing: **can the embedded lock be
made transactional cheaply — converting those 500s into clean 409s (or
successful retries), with no divergence and no lost writes — and which
of the two fix shapes is the right one to promote to the multi-replica
substrate?** I wanted to first *reproduce* the 500 (a regression
baseline pins the bug), then fix it, then prove the fix holds under
heavier contention than 0048 ever drove, and confirm the happy path pays
nothing for it.

## What I did

Copied the 0048 composed AA verbatim into 0049 (duplication over import;
the substrate stays frozen), renamed the module, and dropped the
controller-runtime / client-go ecosystem probes (0049 needs only the AA
plus a contention driver). Then:

- Added `locking.WriteTxn`: the post-acquire body+commit critical
  section now runs inside an outer retry loop. On a recoverable CAS
  conflict at *any* of acquire / body-`Put` / commit-release, it
  releases, re-reads, and retries the whole sequence within a budget;
  exhausting the budget returns **409**, never 500. The 0043
  pre-acquire OCC check is re-evaluated once per attempt against the
  served object's current authoritative RV (a genuine OCC failure — a
  peer committed a different RV — is a legitimate 409 and is *not*
  retried). The discipline is behind a flag (`--lock-txn`, default on);
  `--lock-txn=false` reproduces the 0048 single-shot behavior.
- Rewired `widgetrest.Create` / `Update` through `WriteTxn` with body-
  write and record-build closures, and instrumented `writeRetry`,
  `writeOk`, `writeConflict`, and `maxWriteDepth` counters
  (`:8444/metrics`).
- Wrote `cmd/contend`: a write-contention harness that hammers ONE
  object with N concurrent writers (each its own connection pool) and
  tallies 200/409/500, checking RV monotonicity. Crucially it has an
  `-endpoints` mode that dials each replica's serving port DIRECTLY
  (round-robin per writer) to defeat the aggregation-layer connection
  reuse that hid the bug in 0048.

Ran on kind `aggexp-0049`, a 3-replica StatefulSet, both through the
aggregation layer and via the three per-pod endpoints.

## What I observed

**The bug reproduces, hard.** Fix OFF, 12 writers × 20 rounds (240
blind writes) through the aggregation layer: **98 × 500**, 98 × 409,
44 OK. Pod logs show the 500s are exactly the predicted CAS conflicts —
`backend.Put: ... the object has been modified` (body-CR CAS) and the
analogous `metastore.Commit`. So scenario 1 pins it.

**The fix erases the 500s.** Fix ON, identical burst: **0 × 500**, 9 ×
409, 231 OK. Heavier: 20 writers / 30 rounds ramp → 0 × 500, 70 × 409,
262 OK. 50 writers / 10 rounds → 0 × 500, 432 × 409, 68 OK. Across every
shape, zero 500s; every loser either succeeded on retry or surfaced a
clean 409. **98 → 0 is the headline.**

**It holds under *genuine* cross-replica contention too.** Through the
aggregation layer, all writes collapsed onto a single replica (the 0048
connection-reuse hazard: commits=495 on aggexp-1, 0/0 on the others).
The `-endpoints` mode fixed that — commits spread 23/15/23 (fix off) and
210/200/187 (fix on) across the three replicas. Fix off, cross-replica:
9 × 500 reproduced (fewer than the same-replica run — see below). Fix
on, cross-replica: 12w×20r → 0×500, 1×409, 239 OK; 24w×30r ramp →
0×500, 24×409, 356 OK. Per-replica retry instrumentation under the
24-way cross-replica run: ~360–384 retries and ~200 successful writes
each, 7–10 budget-exhausted 409s per replica, **max retry depth 4** (the
5-attempt budget was occasionally fully used). The fix is doing real,
measurable work.

**No divergence, no lost writes.** After a 16w×25r burst (377 OK, 23
409, 0 500) the final served Widget (`c24-7`, size 24007, a coherent
round-24/writer-7 submission), the body CR, and the metadata CR all
agreed: same uid, same RV (9065 — matching the served RV), an empty
`spec.lock` (no orphaned lease), and a `spec.observed.bodyHash` that I
recomputed by hand from the body content and confirmed byte-identical.
RV advanced monotonically throughout (the harness watches for
regressions; none fired).

**The fast path pays nothing.** Pinned to one replica, three uncontended
writes (a create + two patches): `writeRetry=0`, `maxWriteDepth=0`, zero
conflicts. The transaction loop runs exactly once. CR-write accounting
is unchanged from 0043/0047 — per served write: 1 acquire CR write
(lock-create on create, a free-lock CAS update on update) + 1
commit-release CR write + 1 body write. The discipline adds writes
*only* when a CAS conflict actually occurs, and the cost is proportional
to contention.

## What surprised me

**Same-replica re-entrant contention produces MORE 500s than genuine
cross-replica contention** (98 vs 9 in the matched 12w×20r runs). The
reason is structural and worth recording: the embedded lock's holder
identity is the *replica* (HOSTNAME). When two concurrent writes land on
the *same* replica, the second sees the lock "held by self" and takes
the re-entrant-acquire path (`existing.HolderIdentity == l.identity`) —
so BOTH writers proceed past acquisition and race the body+commit
writes. Across *different* replicas, the lock genuinely blocks the loser
(fresh-lock 409), so fewer writers reach the racy post-acquire region;
the 500s that remain come from the narrow window where a lease is taken
over after expiry while a body `Put` is in flight on the original
holder's replica. Either way the *same* CAS race is the culprit and the
*same* fix covers it — but the re-entrant path means the bug is
reachable even on a single replica, which makes it both more common and
easier to reproduce than the "cross-replica only" framing suggested.

**The body-CR `Put` is the dominant 500 source, not the metadata
commit.** This is what decides between the two candidate fixes. The body
lives on a separate, RV-blind CRD (0042); its CAS conflict is entirely
independent of the metadata CR's resourceVersion. Candidate (b) (fold
the commit into the acquire-release CAS so there is a single CAS point)
would tidy the *metadata* commit but would do nothing for the body race
— which is the one that fires most. In a split-store design, "one CAS
point per logical write" is not achievable for free because there are
genuinely two stores. So (a) is not just simpler here; it is strictly
more complete.

## Fundamentals

**Watch and consistency semantics (primary).** The embedded lock CAN be
made transactional cheaply: wrapping the post-acquire body+commit writes
in a re-read-and-retry loop with the same CAS discipline the acquire
path already had converts every observed cross-replica (and
same-replica) 500 into a clean 409 or a successful retry, with no
divergence and no lost writes, and zero added cost on the uncontended
path. The transactional unit is *acquire → body → commit, retried as a
whole on a recoverable conflict*; the lock's job is reduced to reducing
contention (so most writers don't collide), while the retry loop is what
makes the writes that *do* collide correct. This closes the last
correctness gap the 0048 capstone left open before the multi-replica
substrate promotion.

A subtle consistency point fell out: the body-CR `Put` and the
metadata-CR commit are two distinct CAS surfaces, and the lock does not
make them one. Holding the lock across both, and retrying the pair on
*either* one's conflict, is what keeps them from diverging. The held
lock alone is insufficient because (i) it is re-entrant per replica and
(ii) a lease can be taken over mid-write; the retry loop is the part
that actually enforces "body and metadata never disagree." That is a
fundamental consequence of split-store + per-object lock, not an
artifact of this lab.

**Per-request authorization / resource modeling freedom (untouched).**
0049 changed only the write-path control flow; the owner-stamped body,
the per-watcher identity gate, the generated served type, and the
read-path reconcile are all inherited from 0048 unchanged and were not
re-probed.

## Consequents (explicitly; tied to this lab's versions)

- **The exact 500/409 counts and the 5-attempt / depth-4 retry profile
  are tied to this lab's single-node kind etcd** (kube 1.32) and the
  blind last-writer-wins write pattern the harness drives. A different
  etcd, a different contention pattern, or OCC writes would shift the
  numbers. The *structural* results — that the fix eliminates 500s,
  that retry depth stays small and bounded, and that the happy path adds
  no writes — generalize; the magnitudes do not.
- **The same-replica-produces-more-500s asymmetry is a consequent of
  the holder identity being the replica id** (HOSTNAME) plus the
  re-entrant-acquire path. A lock keyed on a per-request token rather
  than per-replica identity would not re-enter, and the asymmetry would
  invert. Recording it because it determined how easy the bug is to
  hit, but it does not generalize to lock designs with finer-grained
  holder identity.
- **The aggregation-layer connection reuse that collapsed all writes
  onto one replica** (commits 495/0/0 through the LB Service) is the
  same consequent 0048 reported. The `-endpoints` direct-dial mode is
  the lab workaround; in production the aggregator's pooling behavior
  would govern real spread.
- **Direct per-pod dialing works because the pod's
  DelegatingAuthentication accepts the kubeconfig's cluster-CA-signed
  client cert.** Convenient for the harness; tied to kind's single
  cluster-CA posture, not a general property.

## What this changes for SYNTHESIS and EXPERIMENTS

For SYNTHESIS (Watch and consistency semantics): the 0048 claim that "a
per-object embedded lock serializes acquisition but not the writes that
follow it" should be sharpened to its resolution — the writes that
follow acquisition must be wrapped in a re-read-and-retry transaction
that CASes each store and surfaces budget exhaustion as 409. The lock
reduces contention; the retry loop provides correctness. In a
split-store design (body on one RV-blind CRD, metadata+RV on another),
"fold the commit into the lock release as one CAS" is *not* sufficient,
because the body store is a second, independent CAS surface — the
dominant conflict source. This is the shape to promote.

For EXPERIMENTS: 0048's follow-on (1) ("make the post-acquire body +
commit writes retry-on-CAS so cross-replica write races surface as 409,
not 500") is now done and validated. The recommended substrate shape is
fix (a) — commit-path retry under the held lock, covering both the body
and commit CAS surfaces — with a small bounded retry budget (5 here;
observed max depth 4). Follow-on (2) (the generated `$ref` SSA/strict-
decode edge) is untouched and remains open.

## Open questions

- The transaction-retry budget of 5 was sized to this lab's contention.
  What is the right budget under *sustained* multi-replica write load
  (the unbuilt 0047 host-etcd-write-ceiling question)? The retry depth
  histogram (not just the max) under sustained load is uncharacterized;
  a deep tail could indicate a need for adaptive backoff rather than a
  fixed budget.
- Under genuine cross-replica contention the writes still collapsed onto
  one replica through the aggregation layer; the `-endpoints` workaround
  proves the fix works when they *don't* collapse, but the real-world
  spread an aggregator produces under a normal client mix is still
  uncharacterized (the same open question 0048 left).
- The re-entrant-acquire path means two same-replica writers both enter
  the critical section. The retry loop makes that correct, but it also
  means the lock provides *no* mutual exclusion within a replica — every
  same-replica concurrent write is a guaranteed retry. Would a
  per-request (rather than per-replica) holder identity reduce retry
  churn at the cost of more acquire-side 409s? Not probed; it is a lock-
  design question the transaction fix makes safe to defer.
