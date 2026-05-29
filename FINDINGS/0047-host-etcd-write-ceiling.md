# FINDINGS: 0047-host-etcd-write-ceiling

## What we were trying to learn

This is a measurement experiment. It introduces no new mechanism. The
multi-replica library-composition arc had built two things on top of the
0042 metadata-CR core: 0043's embedded per-object lock (acquire/release/
renewal churn on the served object's own RV-authority CR) and 0044's
per-watcher identity-aware watch (one backend poll/subscription per
watcher). 0043's own FINDINGS flagged that the doubled metadata-CR write
amplification "is an open scaling question for 0047, not answerable at
single-node lab scale." This experiment composes both builds into one AA
binary, adds a writer pool, a watcher pool, a slow-backend toggle, and a
metrics harness, and answers: what is the host-etcd write rate per served
write and per watcher, which contribution dominates, and where is the
ceiling? And the fundamental question behind it: does the embedded-lock
+ RV-pump design have an inherent write-amplification property that
constrains achievable object/event/watcher scale regardless of etcd
tuning?

## What we did

Took 0044 as the base (it already carried the 0042 stitched core, the
shared body CRD, the per-watcher hub, and `cmd/watchload`) and folded in
0043's `pkg/locking` plus the embedded-lock CAS surface on the metastore
(raw GET / CAS-create-with-lock / CAS-update / commit-body-hash-and-
release, `SetLockOn`/`LockFrom`, `Record.Lock`/`Record.BodyHash`, and the
`spec.observed`/`spec.lock` CRD schema). The write path now does the
0043 dance — acquire the embedded lock, write the body, commit-release in
one CR write — while the watch path stays the 0044 per-watcher inversion.

Two compositional frictions were real, both small:

- 0043 filtered lock churn (acquire/renewal MODIFIEDs that change nothing
  watcher-visible) in the single-broadcaster `EventSink` path. 0044
  *replaced* that path with a `RawSink` that forwards `(type, ref, rv)`
  straight to the per-watcher hub, bypassing the filter entirely. So the
  emission filter had to be re-implemented in the per-watcher REST
  adapter's `OnMetadataEvent`, keyed on a `VisibleSignature` of the
  record (body hash + KRM metadata, excluding RV and `spec.lock`). The
  filter is load-bearing here for the same reason it was in 0043:
  without it every lock acquire and every renewal heartbeat would fan out
  to every watcher.
- The CRD `spec.required: [resourceRef, metadata]` rejected the
  Create-path acquire (which CAS-creates a CR carrying only `resourceRef`
  + `lock`), exactly as 0043 hit. Relaxing it to `[resourceRef]` and
  adding the `observed`/`lock` properties to the structural schema (or
  CRD pruning silently drops them) fixed it. The composition is otherwise
  mechanical: the two builds slot together with no conceptual conflict,
  which is itself the integration signal — embedded-lock writes and
  per-watcher reads are orthogonal concerns on the same CR.

Added AA-side per-kind write counters (lock-acquire-create / lock-
acquire-update / lock-renew / lock-release / commit-release / delete)
because the host apiserver metrics only see the *verb* (PUT), not the
*intent* (lock vs commit). Ran on a 3-replica StatefulSet in kind
`aggexp-0047`, scraping host `apiserver_request_total` and
`etcd_request_duration_seconds` via `kubectl get --raw /metrics`
alongside the klog counter line.

## What we observed

**Scenario 1 — the multiplier is exactly 2.0 metadata-CR writes per
served write, dead flat across the rate ramp.** At target R = 1/10/50,
with each writer's load attributable from the counter deltas: every
served Create produced one `lockAcquireCreate` + one `commitRelease`;
every served Update produced one `lockAcquireUpdate` + one
`commitRelease`. 60 served writes → 120 metadata-CR writes; 240 → 480;
1048 → 2096. **2.00× every time.** On top of that sits one body-CRD
write per served write (a separate CRD/etcd key) and one metadata-CR
DELETE per object teardown. The host apiserver totals corroborate the
shape under sustained load: `resourcemetadatas` PUT ≈ 50k against
`widgetbodies` PUT ≈ 24k over the same window — the metadata CR is
written almost exactly twice as often as the body. So the steady-state
per-served-write host-etcd write cost of the composed design is **3
writes** (2 metadata-CR + 1 body-CR), plus a heavy *read* tax on the
metadata CR (the pre-acquire GetDirect + the acquire GET): metadata-CR
GET volume ran ~50k against ~50k PUT, i.e. roughly one extra GET per
write beyond the informer cache.

**Scenario 2 — renewal adds ~3·(op_duration/lease) writes per slow op,
and the renewal interval trades linearly against takeover latency.**
With a 20 s backend write delay (op spans the 15 s lease and ~4 renewal
intervals), three slow served writes produced 11 `lockRenew` heartbeat
writes (~3.7 per op, matching `op_duration / (lease/3)` = 20/5 = 4).
Re-running with lease 30 s (renewal every 10 s) cut it to 6 renewals for
the same three ops (~2 per op). Halving the renewal frequency halved the
renewal write rate at the cost of doubling worst-case takeover latency
(30 s vs 15 s before a crashed holder's lock is stealable). Renewal is
zero-cost on the fast path (no renewals fired when the backend Put was
sub-second) and only bites for genuinely slow backends — but for those
it is a real, tunable contributor on top of the 2× steady amplification.

**Scenario 3 — per-watcher poll multiplies backend READS linearly in N
but produces ZERO additional metadata-CR (host-etcd) writes.** Pinned to
one replica, with watchers impersonating the owning identity: per-watcher
poll `ListFor` volume scaled linearly — Δ 6 / 133 / 509 backend list
calls at N = 1 / 25 / 100 (≈ N × window/interval). `--shared-poll`
collapsed it to a flat ~5–7 list calls *regardless of N* (~100× fewer at
N = 100). The decisive number: across all of N = 1/25/100, in both
per-watcher and shared-poll mode, the metadata-CR write counters did not
move at all (`commitRelease` and `lockAcquireUpdate` both flat). In this
composed design the observed-hash CAS write belongs to the *writer's*
commit-release, not to any watcher path; watchers only *read* (the body
CRD informer / list). So the README's "observed-hash CAS attempts vs
successful writes" question resolves to **zero write attempts from
watchers** — the per-watcher fan-out reads the metadata CR's RV but never
writes it. Per-watcher watch multiplies backend read load, not host-etcd
write load.

**Scenario 4 — the ceiling is the embedded LOCK, not etcd.** Ramping
combined load against a fixed working set: writers = 16 (R = 50) achieved
53.8/s with 0 errors and 4.8 ms mean etcd latency; writers = 32 (R = 100)
achieved 96.4/s, 5.2 ms, 3 errors — the knee. But writers = 64 (R = 200)
*regressed* to 76.7/s with 384 errors, and writers = 128 (R = 400)
regressed further to 62.9/s with 1496 errors — while etcd mean latency
stayed flat at 3–4 ms the entire time. The errors are fail-fast 409s: at
writers ≥ ~2× the object count, concurrent writers collide on the
per-object embedded lock and bounce off (0033/0043 fail-fast semantics),
so adding writers makes aggregate throughput *worse*, with etcd nowhere
near saturated. Removing the contention (one object per writer) confirmed
the other ceiling: throughput then scaled to 190/s at 128 writers and
254/s at 300 writers, with etcd mean latency climbing 4.5 ms → 7.0 ms →
7.9 ms (~75% degradation) and ~1800 etcd req/s at the top — the genuine
host-etcd knee on kind's single node. So there are two distinct ceilings,
and the lock one is reached first and far more sharply for any realistic
workload that touches hot objects.

The emission filter held throughout: under sustained update load the AA
logs showed `emission-filter-suppress` on lock-only transitions and the
watchers received one event per real body change, confirming the lock
churn (now 2× the served-write rate plus renewals) stays invisible to
watch streams even though it is fully visible to etcd.

## What surprised us

The headline surprise is that **the host-etcd write rate was never the
binding constraint** at the scales the README anticipated. The hypothesis
framed the ceiling as "the point beyond which host etcd becomes the
bottleneck." On kind's single-node etcd, etcd request latency was still a
healthy 3–8 ms at every load level we could drive; what actually capped
useful throughput was the embedded per-object lock's fail-fast contention.
The 2× write amplification is real and per the prediction, but its first
operational consequence is not etcd saturation — it is that *every*
served write, including the doomed ones, serializes through a lock
acquire, so contended hot objects collapse throughput by burning round-
trips on 409s long before etcd notices. That reframes the design's
scaling story: the amplification's cost is paid in per-write latency and
in lock-contention serialization, not (at lab scale) in etcd write
bandwidth.

The second surprise was how cleanly per-watcher watch decouples from the
write rate. Coming from 0044 the intuition was that N watchers each
re-observing might pump observed-hash CAS *attempts* even if CAS dedups
the successes. In this composition that simply does not happen: the
observed-hash write is the writer's commit, and the watcher path is
strictly read-only against the body CRD. Watcher scale and write scale
are independent axes here.

## Fundamentals

**Watch and consistency semantics (primary), at scale.** Two findings
generalize. First, the embedded-lock-on-the-RV-authority-CR design has an
*inherent* write-amplification property: co-locating write-coordination
state (the lock) on the served object's storage record means every served
write costs at minimum two writes to that record (acquire + commit), and
slow ops cost `~3·op/lease` more for renewal. This is fundamental to the
*co-location decision*, not to etcd or to kind — any backing store would
see the served object's record written ≥2× per served write. It is the
direct, quantified cost of 0043's "fold the lock into the metadata CR"
simplification, and it is the answer to 0043's deferred question: yes, the
amplification is real and inherent, but on single-node etcd it manifests
as latency and lock-contention serialization, not write-bandwidth
saturation. Second, and orthogonally, the per-watcher fan-out is *not*
part of the write amplification: it reads the RV authority, never writes
it, so watcher count does not multiply the host-etcd write rate. The
write-amplification axis (driven by served writes × lock churn) and the
watch-fan-out axis (driven by watchers × backend read cost) are
genuinely independent — a useful decomposition for reasoning about scale.

**Per-request authorization / write coordination (secondary).** The
embedded lock's fail-fast-on-contention contract (from 0032/0033/0043) is
what produces the sharper-than-etcd ceiling. Under contention the lock
converts would-be writes into 409s, which is correct for correctness but
means the design's *useful* write throughput on hot objects is bounded by
how often writers collide, not by storage capacity. A coarser lock, or
an OCC-only path for uncontended objects, would move that ceiling; the
embedded per-object lock as built trades throughput-under-contention for
the GC simplicity 0043 prized.

## Consequents (environment-specific — explicitly called out)

- **Every absolute number here is tied to kind's single-node etcd and is
  a LOWER BOUND proxy for production.** A real multi-node etcd with SSDs
  and tuned heartbeat/election intervals would push the etcd knee far
  higher (the 254 served/s, ~1800 etcd-req/s, 7.9 ms-at-knee figures are
  kind-on-a-laptop numbers). The *shapes* — the 2.0× metadata-write
  multiplier, the `~3·op/lease` renewal term, the flat-in-N watcher write
  cost, the lock-contention-before-etcd ceiling ordering — are the
  generalizable part; the magnitudes are not.
- The 2.0× multiplier is exact for this two-write protocol (acquire +
  commit-release). The body-CRD write (the third write per served write)
  is specific to the 0042 shared-body-CRD storage choice; a backend that
  is not a host CRD would move that write off etcd entirely, leaving the
  2× metadata amplification as the etcd-resident cost.
- client-go's default dynamic-client QPS=5/burst=10 was itself a hard
  throughput gate (~1 served write/s) until raised to 500/1000. That is a
  pure harness artifact — anyone benchmarking an AA whose hot path uses a
  dynamic client to the host apiserver must raise it or they will measure
  client-go's rate limiter, not their design.
- The lock-contention ceiling's exact location (writers ≈ 2× objects)
  depends on the 25 ms × 3-attempt CAS retry budget and the working-set
  size, both arbitrary 0033/0043-inherited knobs.
- The aggregation-layer hop (~60–100 ms per kubectl call) and the AA's
  multi-round-trip served-write chain (pre-acquire GET + acquire +
  body PUT + commit) cap *per-writer* throughput; aggregate scale comes
  from writer concurrency, so the achievable rate is `concurrency /
  per-write-latency` until etcd or the lock intervenes.

## What this changes for SYNTHESIS and EXPERIMENTS

For SYNTHESIS (Watch and consistency semantics, scale): record that the
embedded-lock-on-RV-authority-CR design carries an inherent ~2× host-
write amplification per served write plus a tunable renewal term, and
that — at least on single-node etcd — its *first* binding constraint is
lock-contention fail-fast on hot objects, not etcd write bandwidth.
Record that per-watcher watch is read-only against the RV authority and
therefore does not contribute to the write rate: watcher scale and write
scale are independent axes. The `--shared-poll` knob trades per-user
watch authz for ~N× fewer backend reads but changes neither the write
rate nor the etcd picture.

For EXPERIMENTS: 0043's deferred scaling question is answered. A natural
follow-on would probe the lock-contention ceiling directly — a coarser
or OCC-hybrid lock that lets uncontended writes skip the acquire CR
write would attack both the 2× amplification and the contention ceiling
at once; whether it can keep 0043's GC-for-free property is the open
design tension.

## Open questions

- On real multi-node etcd, does the ordering flip — i.e. is there a
  production regime where the 2× write amplification saturates etcd
  *before* lock contention bites? The kind result says contention is
  first; a tuned etcd that absorbs far more writes/s might let the
  amplification dominate at high enough served-write rates.
- The pre-acquire GetDirect + acquire GET roughly double the metadata-CR
  *read* load (≈1 extra GET per write beyond the informer cache). Could
  the acquire read off the informer cache instead of GetRawDirect without
  losing CAS correctness, and how much etcd read load would that recover?
- The lock-contention ceiling was induced by a small shared working set.
  Real workloads' hot-object distribution (a few objects updated by many
  writers vs. many objects each by one) sets where the ceiling actually
  lands; this experiment bracketed the extremes but did not characterize
  a realistic mixed distribution.
- Renewal writes were measured for a single in-flight slow op at a time;
  many concurrent slow ops would each carry their own renewal stream, so
  the renewal contribution to the aggregate write rate scales with
  in-flight-slow-op count, not just per-op duration. Unmeasured.
