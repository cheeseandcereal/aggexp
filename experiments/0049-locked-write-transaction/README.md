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

complete

<!-- valid values: in-progress, complete, abandoned -->

## Prior findings this builds on

- `FINDINGS/0049-locked-write-transaction.md` — surfaced the
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
# Deploy with the fix DISABLED for the regression baseline:
LOCK_TXN=false ./experiments/0049-locked-write-transaction/hack/deploy.sh
# Deploy with the fix ENABLED (default):
LOCK_TXN=true  ./experiments/0049-locked-write-transaction/hack/deploy.sh
```

`deploy.sh` does a `rollout restart` so flipping `LOCK_TXN` between
scenarios takes effect even when the image tag is unchanged. Every
kubectl call pins `--context kind-aggexp-0049`.

### The contention harness (`cmd/contend`)

`cmd/contend` drives genuinely concurrent writes to ONE object and
tallies 200/409/500. Build it on the host: `go build -o /tmp/contend
./cmd/contend` (use `GOTOOLCHAIN=local`).

Two contention modes:

- **Default (through the aggregation layer).** N independent clients,
  each its own connection pool, write through the host kube-apiserver.
  The aggregator's backend connection reuse tends to pin all writes to
  ONE replica — which still reproduces the bug, because the embedded
  lock's holder identity is the *replica*, so two concurrent writes on
  the same replica both take the re-entrant-acquire path and race the
  post-acquire body+commit writes.
- **Genuine cross-replica (`-endpoints`).** Pass
  `-endpoints https://localhost:18443,https://localhost:18453,https://localhost:18463`
  after `kubectl -n aggexp-system port-forward pod/aggexp-{0,1,2}
  18443:8443 / 18453:8443 / 18463:8443`. Each writer dials a per-pod
  serving endpoint DIRECTLY (the pod's DelegatingAuthentication accepts
  the kubeconfig's cluster-CA client cert; serving-cert SANs don't cover
  `localhost`, so the harness sets `Insecure`). Writer `i` targets
  endpoint `i % len`, so a round spreads across all three replicas —
  defeating the connection-reuse collapse 0048 documented.

### Scenario 1 — reproduce the 500 (regression baseline)

`LOCK_TXN=false`, then `/tmp/contend -writers 12 -rounds 20` (and the
`-endpoints` cross-replica variant). Confirm losers get 500s.

### Scenario 2 — fix yields clean 409s

`LOCK_TXN=true`, repeat. Confirm zero 500s across rounds and increasing
writer counts (`-ramp -writers 24`).

### Scenario 3 — no divergence / no lost writes

`/tmp/contend -cleanup=false -name divergence ...`, then compare the
served Widget, the body CR (`widgetbodies body.<ns>.<name>`), and the
metadata CR (`resourcemetadatas widgets-aggexp-io.widgets.<ns>.<name>`):
color/size/uid/RV agree; `spec.observed.bodyHash` matches a sha256 of
the body; `spec.lock` is empty; RV monotonic.

### Scenario 4 — fast path unaffected

Pin to one replica (`hack/pin-replica.sh aggexp-0`), do an uncontended
create + a couple of patches, and confirm `writeRetry=0`,
`maxWriteDepth=0` on `:8444/metrics` — the transaction loop runs exactly
once, adding no CR writes on the happy path.

### Cleanup

```
kind delete cluster --name aggexp-0049
```

## Decisions made

- **Chose fix (a): commit-path retry under the held lock.** It closes
  BOTH 500 sources (body-CR `Put` CAS conflict AND metadata
  commit-release CAS conflict) with one outer retry loop, and degrades
  cleanly to the 0048 single-shot behavior when disabled (the regression
  baseline). Fix (b) (fold commit into the acquire-release CAS) would
  collapse the metadata commit to a single CAS point but does NOT cover
  the body-CR `Put` race — the body lives on a *separate* RV-blind CRD
  (0042), so its CAS conflict is independent of the metadata CR's RV.
  The body race is the dominant 500 source observed (see FINDINGS), so
  (a) is both simpler in this split-store design and strictly more
  complete.
- **Transaction-retry budget = 5 attempts** (distinct from the 0043
  acquire budget of 3). A contended commit re-reads the served object
  after a peer commits, which is a heavier round-trip than a bare
  acquire CAS, so the outer loop gets a touch more headroom. Chosen
  arbitrarily; under 16–24-way contention the observed max retry depth
  was 4 (i.e. the budget was occasionally fully used), so 5 is the
  right order of magnitude. Backoff reuses 0043's 25ms exponential
  (capped at 2^4).
- **The fix is a flag (`--lock-txn`, default true);** `--lock-txn=false`
  reproduces the original 500 for the regression baseline.
- **Direct per-pod dialing for genuine cross-replica contention.** The
  aggregation layer's backend connection reuse collapses cross-replica
  spread (the 0048 hazard); the harness's `-endpoints` mode dials each
  pod's serving port directly to defeat it. Lab convenience: SANs don't
  cover `localhost`, so TLS verify is skipped on those direct dials.

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
