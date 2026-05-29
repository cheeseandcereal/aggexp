# Experiment 0049: locked-write-transaction

Close the design gap the 0048 capstone surfaced: the embedded per-object
lock (0043) serializes lock *acquisition* but not the post-acquire body
+ commit writes, so under genuine cross-replica write contention some
losers receive **500s** (a CAS conflict on the body or metadata commit)
rather than clean **409 Conflict**. The lock behaves as an admission
gate, not a transaction. This experiment makes the locked write a
transaction and validates it under real cross-replica contention, so the
fix is experiment-validated *before* it is promoted to substrate.

Builds directly on 0048 (the composed multi-replica AA) and 0043 (the
embedded lock).

## Status

in-progress

<!-- valid values: in-progress, complete, abandoned -->

## Prior findings this builds on

- `FINDINGS/0048-library-multireplica-vertical-slice.md` — surfaced the
  gap: the lock serializes acquisition but not commit, so cross-replica
  losers can get 500s instead of 409s; only the acquire path retries.
- `FINDINGS/0043-embedded-lock-emission-filtering.md` — the embedded
  `spec.lock` CAS'd on the metadata CR's resourceVersion; 3-attempt
  retry budget on *acquire*; emission filtering; pre-acquire OCC.
- `FINDINGS/0042-metadata-cr-rv-authority.md` — host metadata-CR RV
  authority; shared metadata + body CRDs.

## Hypothesis

- **Watch and consistency semantics (primary).** If the body write and
  the metadata commit-release are performed *inside* the held-lock
  critical section with the same retry/CAS discipline the acquire path
  already has — i.e. the locked write is one transaction
  (acquire → body → commit, each CAS-checked, the whole sequence
  retried on a recoverable CAS conflict, and a hard failure surfacing as
  409 rather than 500) — then under genuine cross-replica write
  contention the winner commits cleanly and every loser observes a
  **409 Conflict**, never a 500. No lost writes, no half-applied state
  (body written but metadata commit failed, or vice versa).

## Hard load-bearing decision

The locked write is a transaction, not an admission gate. The fix is one
of (validate both; pick the simpler that holds):

1. **Commit-path retry under the held lock** — keep the lock held across
   body + commit; on a CAS conflict in body/commit, re-read and retry
   within the same budget; exhausting the budget returns 409, not 500.
2. **Acquire+commit as one CAS sequence** — fold the commit into the
   same CR write that releases the lock, so there is a single CAS point
   per logical write and a conflict is always a clean 409.

Whichever is chosen, the failure mode under contention must be 409 (or a
successful retry), and the body and metadata must never diverge.

## What this is (files to create)

- Copy the 0048 composed AA (or, if leaner, the 0043 embedded-lock build
  + 0042 core) into this experiment dir; duplication over import per lab
  ethos; the substrate stays frozen. Rename the go.mod module to
  `github.com/cheeseandcereal/aggexp/experiments/0049-locked-write-transaction`,
  keep `replace github.com/cheeseandcereal/aggexp => ../..`, `go 1.24`.
- Apply the transaction fix in `pkg/locking` + the write path of
  `pkg/widgetrest`.
- A **contention harness** (`cmd/contend/` or a hack script) that drives
  genuinely concurrent writes to the *same* object from *different*
  replicas (the 0048 finding noted that connection reuse hid this; force
  distinct connections / pin writers to distinct per-pod Services so the
  contention is real, not collapsed by a single keepalive connection).
- Instrumentation: count 200 / 409 / 500 responses and confirm body vs
  metadata never diverge after a contended burst.
- `manifests/` — 3-replica StatefulSet + per-pod Services (the 0042/0034
  shape) so cross-replica contention is exercised.

## How to run

```
./hack/gen-certs.sh
kind create cluster --name aggexp-0049
kubectl --context kind-aggexp-0049 create namespace aggexp-system
kubectl config use-context kind-aggexp-0049
./experiments/0049-locked-write-transaction/hack/deploy.sh
```

### Scenario 1 — reproduce the 500 (regression baseline)

With the fix disabled (a flag), drive concurrent same-object writes from
two replicas and confirm the 0048 behavior reproduces: some losers get
500s. This pins the bug before fixing it.

### Scenario 2 — fix yields clean 409s

Enable the fix. Repeat the contended burst. Confirm every non-winner
gets a **409 Conflict** (or succeeds on retry), **zero 500s**, across
many rounds and increasing writer counts.

### Scenario 3 — no divergence / no lost writes

After a heavy contended burst, confirm the served object's body and its
metadata record agree (no half-applied write), the final value is one of
the submitted writes (no corruption), and the object's RV advanced
monotonically.

### Scenario 4 — fast path unaffected

Confirm uncontended writes still take the same number of CR writes as
0043/0047 measured (no extra writes added on the happy path by the
transaction discipline).

### Cleanup

```
kind delete cluster --name aggexp-0049
```

## Decisions made

<!-- filled in at implementation -->
- Retry budget reused from 0043 (3 attempts, 25ms exponential backoff)
  unless measurement says otherwise.

## Prerequisites

- kind cluster `aggexp-0049` (not the default `aggexp`).
- Serving cert from `hack/gen-certs.sh`.
- The 0048 (or 0043+0042) build copied in. No external secrets.

## What we're looking to learn

- **Watch and consistency semantics.** Does making the locked write a
  transaction (commit-path retry, or acquire+commit as one CAS) convert
  the 0048 cross-replica 500s into clean 409s with no divergence and no
  lost writes — and is the fix simple enough to promote to substrate?

## Expected FINDINGS shape

- **Fundamental:** whether the embedded lock can be made transactional
  cheaply, closing the last correctness gap before the multi-replica
  substrate promotion, and which of the two fixes is the right shape.
- **Consequent:** measured 409/500 counts before and after, retry
  depth under contention, any extra write cost — tied to this lab's
  single-node etcd.
