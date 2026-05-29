# Experiment 0047: host-etcd-write-ceiling

Measure the host kube-apiserver / etcd **write rate** produced by
composing the metadata-CR mechanisms this arc introduces — 0042's
observed-body-hash RV pump, 0043's embedded-lock
acquire/release/renewal churn, and 0044's per-watcher poll — under
realistic event / write / watcher counts, and find the scaling ceiling.

Builds on 0043 (embedded lock) and 0044 (per-watcher watch); both
write to the metadata CR, and this experiment quantifies the aggregate.

## Status

complete

<!-- valid values: in-progress, complete, abandoned -->
<!-- The composed 0043+0044 AA ran on a 3-replica StatefulSet in kind
     aggexp-0047; all four scenarios produced numbers. See
     FINDINGS/0047-host-etcd-write-ceiling.md. -->

## Prior findings this builds on

- `FINDINGS/0034-shared-watch-cross-replica.md` — cross-replica
  propagation via the host CRD's RV stream (~4 ms in lab); the metadata
  informer fires on every CR write.
- Experiment 0042 — every observed backend change CAS-writes
  `spec.observed.bodyHash` to the metadata CR (the RV pump).
- Experiment 0043 — lock acquire/release/renewal each write the
  metadata CR (churn that emission filtering hides from *watch* but not
  from *etcd*).
- Experiment 0044 — per-watcher poll: N watchers re-observe and may
  each attempt the observed-hash CAS write (deduped by CAS, but still
  attempted).

## Hypothesis

- **Watch and consistency semantics (largely consequent / scale).**
  The total host-etcd write rate is roughly: backend-event rate ×
  (hash-pump writes, CAS-deduped toward 1× across replicas/watchers) +
  lock acquire/release/renewal writes per served write + reconciler
  cycle writes. There is a write-rate ceiling beyond which host etcd
  becomes the bottleneck. We want to locate it and attribute the
  contributions, especially:
  - how much the lock-churn (acquire/release + renewal heartbeats)
    adds per write, and how renewal interval trades off against it;
  - whether per-watcher poll meaningfully multiplies hash-pump write
    *attempts* even though CAS dedups the *successful* writes;
  - whether the rate scales to ~100 events/sec without saturating
    a kind single-node etcd.

## Hard load-bearing decision

This is a measurement experiment. It composes the 0043 and 0044 builds
(duplicated in, per lab ethos) and adds load generators and a metrics
harness; it introduces no new mechanism. Findings are expected to be
largely **consequent** (tied to kind's single-node etcd) but
operationally load-bearing for any real deployment.

## Architecture

```
load generators                       measurement
  • writer pool: K clients doing      • apiserver_request_total{verb,resource}
    Create/Update at rate R             for the metadata CRD group
  • watcher pool: N kubectl/client-go • etcd_request_duration_seconds
    watches (per-watcher poll)        • apiserver_storage_* (host)
  • slow-backend mode to force        • count of CR writes by kind:
    renewal heartbeats                   create/update/delete/lock/observed-hash
            │
   3-replica StatefulSet (0042 core + 0043 lock + 0044 per-watcher watch)
            │
   host kube-apiserver ◄──► etcd   ← the thing under measurement
```

## What this is (files to create)

- Compose the 0043 build (metadata-CR core + embedded lock + emission
  filtering) and the 0044 per-watcher watch into one AA binary
  (duplicated into this experiment dir).
- `cmd/loadgen/` — configurable writer pool (rate R, K clients) and
  watcher pool (N watches), plus a slow-backend toggle that forces
  renewal heartbeats.
- A metrics-scrape script that records, per run: metadata-CRD write
  rate broken down by kind (create/update/delete/lock/observed-hash),
  host etcd request latency, and per-watcher backend call volume.
- `manifests/` — 3-replica StatefulSet; expose `/metrics`.

## How to run

```
./hack/gen-certs.sh
kind create cluster --name aggexp-0047
kubectl --context kind-aggexp-0047 create namespace aggexp-system
kubectl config use-context kind-aggexp-0047
./experiments/0047-host-etcd-write-ceiling/hack/deploy.sh
```

### Scenario 1 — write-rate attribution

Hold watchers at 0; ramp the writer pool (R = 1, 10, 50, 100 writes/s).
Record metadata-CR writes by kind. Attribute the multiplier per served
write (expected ~3 CR writes: lock-acquire + body/observed-hash +
lock-release, plus renewals for slow ops).

### Scenario 2 — renewal contribution

Enable the slow-backend mode (Update spans several renewal intervals).
Measure the extra CR-write rate from renewal heartbeats; vary the
renewal interval and observe the tradeoff against takeover latency.

### Scenario 3 — per-watcher poll multiplication

Hold writes steady; ramp watchers (N = 1, 25, 100) in per-watcher poll
mode, then `SharedPoll`. Measure observed-hash CAS *attempts* vs
*successful* writes (CAS dedup), and the host-etcd write rate in each
mode.

### Scenario 4 — locate the ceiling

Increase combined load until host-etcd request latency degrades; record
the event/write/watcher counts at the knee.

### Cleanup

```
kind delete cluster --name aggexp-0047
```

## Decisions made

- Load levels: scenario 1 ramped served-write target R ∈ {1,10,50,100}/s
  by setting the writer-pool size (writers ∈ {2,8,24,48}); scenario 3
  ramped watchers N ∈ {1,25,100}; scenario 4 pushed writers to {200,300}
  with distinct objects to find the etcd knee. Arbitrary, tuned to where
  the knee appeared.
- **Composed the build from 0044 as the base** (it already carried the
  0042 metadata-CR core + shared body CRD + per-watcher watch +
  `cmd/watchload`) and folded 0043's `pkg/locking` + the embedded-lock
  CAS surface (`GetRawDirect`/`CreateRawWithLock`/`UpdateRaw`/
  `PutBodyHashAndMeta`/`SetLockOn`/`LockFrom`) into the metastore, plus
  `Record.Lock`/`Record.BodyHash` and the `observed`/`lock` CRD schema.
- **Emission filter on the per-watcher path.** 0043 filtered lock churn
  in the single-broadcaster `EventSink` path; 0044 replaced that with a
  `RawSink` that bypasses it. So the filter was re-implemented in the
  per-watcher REST adapter's `OnMetadataEvent`, keyed on
  `metastore.VisibleSignature(rec)` (body hash + KRM metadata, excluding
  RV and `spec.lock`), suppressing lock-acquire/renewal MODIFIEDs before
  the hub fan-out.
- **Raised the AA's dynamic-client QPS to 500/burst 1000** (flag
  `--client-qps`/`--client-burst`). client-go's default QPS=5/burst=10
  was the throughput gate (~1 served write/s), not host etcd — defeating
  the ceiling measurement. With it raised, host etcd / the aggregation
  hop is the gate. This is a measurement-harness fix, not a design knob.
- **Relaxed the metadata CRD `spec.required` from `[resourceRef,
  metadata]` to `[resourceRef]`** — the Create-path lock acquire
  CAS-creates a CR carrying only `resourceRef` + `lock` before metadata
  exists (same fix 0043 made). Added `spec.observed.bodyHash` and
  `spec.lock` to the structural schema or CRD pruning would drop them.
- Lease 15s / renewal every lease/3 (5s) inherited from 0032/0033/0043;
  scenario 2 also tested lease 30s (renewal 10s).
- Slow-backend mode is a fixed `--backend-write-delay` on every body
  `Put` (20s in scenario 2) to force the renewal heartbeats.
- Metrics scraped via `hack/scrape-metrics.sh`: host kube-apiserver
  `apiserver_request_total{resource=resourcemetadatas|widgetbodies}` and
  `etcd_request_duration_seconds` through `kubectl get --raw /metrics`
  (RBAC for the `/metrics` nonResourceURL granted to
  `system:authenticated`), plus the AA's `aggexp-0047-metrics` klog line
  for the per-kind metadata-CR write attribution (lock-acquire-create /
  lock-acquire-update / lock-renew / lock-release / commit-release /
  delete) the host metrics cannot break out (the host only sees the verb,
  not the intent). Counters are reset between scenarios by a StatefulSet
  rollout restart; per-scenario figures are before/after deltas.
- kind single-node etcd is the system under test; results are a
  lower-bound proxy for production etcd.


## Prerequisites

- kind cluster `aggexp-0047` (not the default `aggexp`).
- Serving cert from `hack/gen-certs.sh`.
- The 0043 and 0044 builds composed in. No external secrets.

## What we're looking to learn

- **Watch and consistency semantics (scale).** What host-etcd write
  rate does the composed metadata-CR design produce per served write
  and per watcher, and where is the ceiling? Which contribution
  (hash-pump, lock churn, renewal, per-watcher poll) dominates, and
  which knobs (renewal interval, `SharedPoll`) move it most?

## Expected FINDINGS shape

- **Consequent (primarily):** measured write rates, multipliers, and
  the ceiling on kind's single-node etcd — directional for production.
- **Fundamental (if any):** whether the embedded-lock + RV-pump design
  has an inherent write-amplification property that constrains the
  achievable object/event/watcher scale regardless of etcd tuning.
