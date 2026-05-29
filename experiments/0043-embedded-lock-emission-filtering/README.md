# Experiment 0043: embedded-lock-emission-filtering

Collapse the *separate* per-object lock object validated by 0032
(Lease) and 0033 (custom-CRD CAS) **into** the 0024 metadata CR as an
embedded `spec.lock` subfield, CAS'd on the CR's own resourceVersion.
Then probe the two consequences of co-locating a lock with the served
object's metadata: resourceVersion churn on watch, and
optimistic-concurrency ordering.

Builds on the metadata-CR core from 0042.

## Status

complete

<!-- valid values: in-progress, complete, abandoned -->
<!-- All five scenarios pass on a 3-replica StatefulSet in kind
     (aggexp-0043). See FINDINGS/0043-embedded-lock-emission-filtering.md. -->

## Prior findings this builds on

- `FINDINGS/0024-metadata-crd-store.md` — the metadata CR that now also
  carries the lock.
- `FINDINGS/0032-lease-based-object-locking.md` — per-object Lease
  locking; CAS takeover after `leaseDuration`; 3-attempt retry budget.
  Lock lived in a *separate* `coordination.k8s.io/v1 Lease`.
- `FINDINGS/0033-crd-cas-object-locking.md` — per-object CAS on a
  *separate* custom CR's resourceVersion; same-group routing-recursion
  hazard; fail-fast 409 vs retry.
- `FINDINGS/0039-optimistic-concurrency.md` — RV-conflict detection on
  Update (one writer 200, stale writer 409).
- Experiment 0042 — the host-CR RV authority for the stitched object.

## Hypothesis

- **Watch and consistency semantics (primary).** A per-object lock can
  live as a `spec.lock` subfield on the same metadata CR as the served
  object's KRM metadata, CAS'd on that CR's resourceVersion — removing
  0032/0033's separate lock object and its independent lifecycle/GC.
  The cost is that lock acquire, release, and renewal **advance the
  served object's resourceVersion and fire the metadata informer** (RV
  churn). Two mechanisms make this tolerable:
  1. **Emission filtering** — the watch path emits an event only when
     watcher-visible state changed (the observed body hash or the
     served KRM metadata); a CR transition whose only delta is within
     `spec.lock` (acquire/release/renewal) is suppressed before
     emission, so lock churn does not surface as spurious MODIFIED.
  2. **Pre-acquire OCC ordering** — because acquiring the lock writes
     the CR and advances its RV, the optimistic-concurrency check
     (0039) must compare the client's resourceVersion against the RV
     captured **before** lock acquisition, not after, or every
     conditional write would spuriously 409.

## Hard load-bearing decision

The lock is embedded, not separate. A single CR write performs the
"lock + body-change + lock" sequence's metadata mutations; CAS is on
the CR's own RV. Lease-expiry takeover, retry budget, and fail-fast 409
follow 0032/0033. Renewal is **on by default** (re-stamp `renewedAt`
every `LeaseDuration/3`) so a slow backend call doesn't lose its lease
mid-operation.

## Architecture

```
write path (Create/Update/Delete) on one replica:
   read metadata CR  ──►  servedRV := CR.RV         (pre-acquire)
   OCC: client RV vs servedRV → 409 if mismatch
   CAS acquire: set spec.lock.holderIdentity   ──► CR.RV bumps (churn)
   Backend.<verb>(body)
   renewal goroutine: re-stamp spec.lock.renewedAt every Lease/3  (churn)
   write body-hash + metadata, CAS release spec.lock ──► CR.RV bumps
                                │
                          metadata informer fires for every CR write
                                │
                    emission filter: drop lock-only / renewal deltas
                    emit MODIFIED only on body-hash / KRM-metadata change
```

## What this is (files to create)

- Copy the 0042 `pkg/metastore`, `pkg/backend`, `pkg/apis`,
  `pkg/server`, `cmd/`, metadata CRD, and manifests into this
  experiment dir (duplication over cross-experiment import; the
  substrate stays frozen).
- Extend the metadata CR schema with a `spec.lock`
  (`holderIdentity, acquiredAt, renewedAt, leaseDurationSeconds`) and a
  `spec.observed.bodyHash` subfield.
- `pkg/locking/locker.go` — CAS acquire/release/renew on the embedded
  lock; lease-expiry takeover; retry budget. Adapt the *contract* from
  `runtime/library/locker.go` but target the embedded subfield, not a
  separate Lease.
- Emission-filter logic in the watch path: compare the new CR's
  body-hash + served KRM metadata against last-emitted; suppress
  lock-only transitions.
- Pre-acquire-RV capture in the Update path for the OCC check.
- `manifests/` with a 3-replica StatefulSet so contention is real.

## How to run

```
./hack/gen-certs.sh
kind create cluster --name aggexp-0043
kubectl --context kind-aggexp-0043 create namespace aggexp-system
kubectl config use-context kind-aggexp-0043
./experiments/0043-embedded-lock-emission-filtering/hack/deploy.sh
```

### Scenario 1 — contention → 409

Two concurrent writers to the same object from two replicas: one
acquires the embedded lock and wins; the other sees a fresh held lock
and gets a fail-fast 409 (0033 semantics).

### Scenario 2 — holder crash → lease takeover

Kill the replica holding a lock mid-write; confirm another replica
takes over via CAS after `leaseDurationSeconds` elapses.

### Scenario 3 — renewal across a slow backend op

Configure the in-memory backend to delay an Update past
`leaseDuration`; confirm the renewal goroutine keeps the lease fresh
(no premature takeover), and the write completes cleanly.

### Scenario 4 — emission filtering (the key result)

With `kubectl get widgets -w` open, perform one user Update. Confirm the
watcher sees **exactly one** MODIFIED, not three (acquire / body /
release). Then run a long backend op spanning several renewal
intervals; confirm **zero** spurious MODIFIED from renewal heartbeats.
Confirm the served object's RV still advances opaquely across the lock
writes (clients tolerate RV gaps).

### Scenario 5 — pre-acquire OCC ordering

A conditional Update carrying the object's current resourceVersion must
succeed (the OCC check compares against the pre-acquire RV, not the
post-lock-acquire RV). A genuinely stale conditional Update must 409.

### Cleanup

```
kind delete cluster --name aggexp-0043
```

## Decisions made

- `leaseDurationSeconds` default 15s; renewal interval `Lease/3` = 5s;
  retry 3 attempts at 25ms exponential backoff (0032/0033 defaults).
- Fail-fast 409 on fresh-held lock (0033), not acquirer-side retry.
  The 3-attempt retry budget applies only to CAS-level conflicts (two
  replicas racing the same CR write), never to a fresh held lock.
- Renewal **on by default**; `--disable-lock-renewal` turns it off and
  `--lock-lease-duration` tunes the lease (and thus the `Lease/3`
  renewal interval) for backends with sub-lease write latencies.
- Emission filter keys on `spec.observed.bodyHash` + the served KRM
  metadata fields (uid, deletionTimestamp, labels, annotations,
  finalizers, ownerReferences). It deliberately EXCLUDES the
  resourceVersion and `spec.lock`; `spec.lock` deltas are never
  watcher-visible.
- The single served-object metadata CR is the lock's CAS surface. A
  served write performs exactly **two** host CR writes — acquire
  (sets `spec.lock`) and commit-release (writes body hash + KRM
  metadata AND clears `spec.lock` in one Update) — plus one renewal
  write per `Lease/3` interval while a backend op is in flight.
- Relaxed the metadata CRD's `spec.required` from
  `["resourceRef","metadata"]` to `["resourceRef"]`: on the Create
  path the lock is CAS-created onto a CR that does not yet carry
  metadata (metadata + body hash land at commit). Required by the
  embedded design; arbitrary in that the prior 0042 requirement was
  never load-bearing.
- A `WIDGET_BACKEND_DELAY_SECONDS` env var (read at startup) injects a
  backend write delay so scenario 3 can force a Put to outlast the
  lease and exercise the renewal heartbeat. Debug knob, not a
  production feature.
- Delete removes the whole metadata CR (and with it the embedded lock)
  rather than acquiring the lock first. Deleting the host object
  inherently releases its embedded lock — a lifecycle simplification
  the separate-lock designs (0032/0033) did not get for free.
- `insecureSkipTLSVerify: true` on the APIService (inherited from
  0042) — a lab convenience for replica pinning, not a security
  posture.
- All `hack/` scripts pin every kubectl call to `--context
  kind-aggexp-0043`. Multiple aggexp-00NN clusters can exist
  concurrently on one machine; relying on the ambient current-context
  is unsafe.

## Prerequisites

- kind cluster `aggexp-0043` (not the default `aggexp`).
- Serving cert from `hack/gen-certs.sh`.
- The 0042 metastore core copied in (this experiment does not run
  without it). No external secrets.

## What we're looking to learn

- **Watch and consistency semantics.** Does embedding the lock in the
  metadata CR work as a single CAS surface, and is emission filtering
  sufficient to keep lock/renewal churn invisible to watchers? Is the
  pre-acquire OCC ordering correct under contention?

## Expected FINDINGS shape

- **Fundamental:** whether co-locating the lock with metadata is a net
  simplification over separate lock objects (one CAS surface,
  lifecycle-bound) and whether emission filtering is *required* (not
  optional polish) for the embedded-lock design to be watchable.
- **Consequent:** lock-churn write counts, takeover timing, renewal
  overhead — tied to this environment's etcd latency.
