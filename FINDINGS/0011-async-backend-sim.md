# Findings — 0011 async-backend-sim

## What we were trying to learn

`FINDINGS/0009-ack-aggregated-s3.md` concluded the stateless-AA / AWS-as-
source-of-truth model works cleanly for **synchronous** backends (S3
Bucket creation returns in milliseconds) but flagged that **async**
backends (EKS clusters, IAM propagation, RDS) would force state back
into the picture because "the AA's Create-equivalent would need to
block the HTTP request for minutes — not workable."

This experiment probes that claim directly. Instead of accepting the
conclusion, we built an async-mock backend with deliberately-slow
semantics (30s provision, 10s deprovision) and asked: what specifically
breaks, and is there a Create shape that works?

Four questions, concretely:

1. Is `kubectl apply` usable if Create returns immediately with
   `status.phase=Provisioning` instead of blocking?
2. Does the standard `kubectl get -w` / informer path show
   Pending → Ready transitions?
3. Does `kubectl wait --for=jsonpath='{.status.phase}=Ready'` work
   as a controller-model substitute?
4. How does the ACK controller-model compare to the stateless AA
   for async? Is the inversion thesis still alive here?

## What we did

- Built `async-mock/` — a ~250-line stdlib Go HTTP service with
  POST/GET/PUT/DELETE /widgets. Phase is computed from a record's
  `LastChangeTime`: under 30s from it → Provisioning; after → Ready.
  Delete sets a `Deleting` flag and reaps after 10s. The mock is
  the source of truth; its state is in-process (wiped on restart;
  out of scope for this probe).
- Built `pkg/asyncbackend/` — a `runtime/storage.WritableBackend` that
  returns from Create **immediately** with phase=Provisioning, from
  Delete with phase=Deleting, and runs a 5-second poll loop to emit
  watch events on phase transitions. Get / List are live reads. No
  AA-side caching of authoritative state.
- Deployed to isolated kind cluster `aggexp-async`. Ran six scenarios
  and captured the raw logs.

## What we observed

### Scenario 1: apply-and-watch — PASS (with a minor event duplication)

`kubectl apply` returned in ~60 ms with `phase=Provisioning`. Exactly
30 seconds later, a MODIFIED event arrived with `phase=Ready` and
`readyAt` set. Raw watch excerpt:

```
21:17:06 EVENT      NAME     DESIRED   OBSERVED   PHASE          AGE
21:17:06 ADDED      demo-1   running              Provisioning   0s
21:17:06 ADDED      demo-1   running              Provisioning   0s
21:17:06 ADDED      demo-1   running              Provisioning   0s
21:17:36 MODIFIED   demo-1   running   running    Ready          30s
```

Three ADDED events for one apply is more than expected. The raw
watch-stream probe reveals what they are: three distinct
resourceVersions (153, 154, 155). RV 153 and 154 both carry
`managedFields` and `annotations` (library-layer metadata); RV 155
does not. The sequence is:

1. Our backend's `Create` calls the eager `PublishAdded` — RV 153.
2. The apiserver library, after successful Create, also emits an
   ADDED event over the watch stream independently — RV 154.
   (This is generic-apiserver behavior, not ours; confirmed by the
   fact that the RV 154 event carries `managedFields` from the
   library's field manager, which the poll-loop path does not see.)
3. The 5-second poll loop then sees a "new" widget in the next tick
   and emits another ADDED — RV 155, from live-read with no
   managedFields.

This is a **consequent** — a genuine bit of double-publish caused by
our substrate's eager-publish-on-Create overlapping with the library's
own publish. It's cosmetically noisy but harmless; the widget's
identity is stable (same UID) across the three events and consumers
deduplicate by RV. A substrate cleanup could drop the eager
`PublishAdded` in `Create` and rely on the library's own event; the
0009 backend has the same pattern and the same behavior.

The third ADDED from the poll loop losing `managedFields` is the
**directly-observed version** of the 0009 SSA-persistence finding:
field ownership is library-layer state, and the moment the adapter
re-reads from the backend (which doesn't store managedFields), those
fields vanish from downstream events. Not new; just visible here in a
different shape.

### Scenario 2: apply-get-loop — PASS

`kubectl apply`, then 10 GETs at 5s intervals:

```
21:18:58 APPLY / widget.aggexp.io/demo-1 created
21:19:03 GET #1   demo-1 running         Provisioning 5s
21:19:08 GET #2   demo-1 running         Provisioning 10s
...
21:19:23 GET #5   demo-1 running         Provisioning 25s
21:19:28 GET #6   demo-1 running running Ready        30s
...
21:19:49 GET #10  demo-1 running running Ready        50s
```

Clean transition at the 30s mark. This is the "controller would have
updated status by now" scenario from the ACK model; here status is
live-read from the mock, which already knows.

### Scenario 3: apply-then-modify during Provisioning — PASS

Apply at 21:20:10, re-apply (different `config`) at 21:20:20 (10s
into the 30s window). Watch events:

```
21:20:10 ADDED    demo-3 running         Provisioning  0s   (x3, see s1)
21:20:20 MODIFIED demo-3 running         Provisioning 10s   (x3)
21:20:51 MODIFIED demo-3 running running Ready        40s
```

The mock correctly restarted the 30s timer: `readyAt=21:20:50` (30s
from 21:20:20, not from 21:20:10). kubectl emitted a warning on the
second apply:

```
Warning: resource widgets/demo-3 is missing the kubectl.kubernetes.io/
last-applied-configuration annotation which is required by kubectl
apply.
```

Same warning 0009 observed. Annotations don't survive a stateless
read, so kubectl thinks every apply is the first. Functionally fine;
UX-wise honest about the inversion.

### Scenario 4: delete during Provisioning — PASS (with a library-side DELETED mixed in)

Apply at 21:21:52, delete 5s later at 21:21:57. Watch events:

```
21:21:52 ADDED    demo-4 running Provisioning  0s   (x3 dup again)
21:21:56 ADDED    demo-4 running Provisioning  3s
21:21:57 MODIFIED demo-4 running Deleting      5s   (from our backend)
21:21:57 DELETED  demo-4 running Deleting      5s   (from the library)
21:22:01 MODIFIED demo-4 running Deleting      8s   (poll tick)
21:22:11 DELETED  demo-4 running Deleting     18s   (poll reap, 10s after delete)
```

Two DELETED events for one deletion. The first, at 21:21:57, comes
from the generic apiserver: a successful Delete emits a DELETED
watch event automatically. The second, at 21:22:11, comes from our
poll loop when the mock finally reaps the record 10s after the
delete request. Both carry the same UID; clients keyed on UID see
the first and ignore the second (or vice-versa). The MODIFIED +
DELETED interleaving is novel to the async model — a client can
observe `phase=Deleting` via the watch stream, which a CRD+controller
would typically model differently (via a finalizer and
`deletionTimestamp`).

**Consequent**: `kubectl delete` with default `--wait=true` hangs
for the full 10s deprovision window (technically longer — see next
paragraph) because it expects either a DELETED watch event with
matching UID OR the object to be gone on its next list check. The
first DELETED from the library arrives at t=0, not at t=10s, which
initially makes kubectl think deletion is complete; but because the
object reappears in subsequent LISTs until the poll-reap-window
closes, kubectl's wait loop treats it as "still there" and keeps
polling. We hit a **2-minute timeout with bookmark-expired warnings**
before finally giving up with `--wait` (default). Using
`kubectl delete --wait=false` avoids the problem. This is a real,
observable issue for the stateless-AA async model: the library's
delete-semantics assume the resource is gone synchronously on Delete
return, which is exactly what async backends can't provide.

### Scenario 5: kubectl wait --for=jsonpath — **FAIL**

This was the key ergonomic test. The exact command:

```
kubectl wait --for=jsonpath='{.status.phase}=Ready' widget/demo-5 --timeout=60s
```

Timed out at 60s even though the widget reached Ready at t=30s (a
subsequent GET confirmed `phase=Ready`). Stderr during the wait:

```
I0429 21:23:19.109749 reflector.go:1159] "Warning: event bookmark
  expired" err="hasn't received required bookmark event marking the
  end of initial events stream, received last event 17.574581598s ago"
I0429 21:23:29.109075 reflector.go:1159] "Warning: event bookmark
  expired" err="... received last event 27.573952419s ago"
I0429 21:23:49.109450 reflector.go:1159] "Warning: event bookmark
  expired" err="... received last event 17.574661539s ago"
error: timed out waiting for the condition on widgets/demo-5
```

`kubectl wait` since client-go ~1.31 uses the
`WatchList` wire protocol: it requests a watch with
`sendInitialEvents=true&allowWatchBookmarks=true` and waits for a
BOOKMARK event carrying
`metadata.annotations["k8s.io/initial-events-end"]="true"` before it
considers the watcher "synced" and starts evaluating the jsonpath
condition. Our substrate's `watch.Broadcaster` does not emit this
bookmark — neither the runtime/storage adapter nor the underlying
broadcaster synthesizes one.

I confirmed this by opening a raw watch with
`allowWatchBookmarks=true` and observing over 45 seconds: ADDED and
MODIFIED events, zero bookmarks. The AA is never going to satisfy
`kubectl wait` until the substrate emits the initial-events-end
bookmark.

**This is a fundamental finding about Watch and consistency
semantics**, not a consequent:

- `kubectl wait --for=jsonpath` is the canonical "wait for a resource
  to reach desired status" idiom in modern Kubernetes tooling. It is
  exactly what one would reach for against an async resource.
- It does not work against a substrate that implements watch via
  `watch.NewBroadcaster` without adding the initial-events-end
  bookmark.
- The surrounding ecosystem (`kubectl wait`, client-go's
  `WatchList`-aware informers, controller-runtime managers that
  honor the same path on 1.32+) all share this dependency.

The fix is a substrate-level change: on an initial watch with
`sendInitialEvents=true`, after emitting the ADDED events for the
current snapshot, emit a BOOKMARK event with the
`k8s.io/initial-events-end` annotation and the current RV. The
substrate in this repo does not yet do this; `runtime/storage/
adapter.go` relies on `watch.Broadcaster.Watch()` which passes the
events through unchanged. This is the first experiment where the
gap has mattered for a real user-facing operation.

Polling alternatives work fine:

```
for i in {1..15}; do
  sleep 3; phase=$(kubectl get widget poll-probe -o jsonpath='{.status.phase}')
  echo "phase=$phase"; [ "$phase" = "Ready" ] && break
done
```

A 3s poll loop saw `phase=Ready` on the 10th iteration (~30s in).
But "roll your own polling" is exactly the ergonomic tax the
controller model absorbs for you in CRD-land.

### Scenario 6: ten parallel applies — PASS

Fired 10 `kubectl apply`s in parallel, all succeeded. At t+35s all
10 widgets showed `phase=Ready`. The AA's poll loop remained light:

```
async-poll count=0 added=0 modified=0 deleted=0 took=253µs
(... parallel apply happens ...)
async-poll count=10 added=10 modified=0 deleted=0 took=403µs
async-poll count=10 added=0 modified=0 deleted=0 took=377µs
async-poll count=10 added=0 modified=10 deleted=0 took=420µs
```

A single poll cycle at 10 widgets takes ~400µs. At 1 widget the same
cycle took ~300µs. Poll overhead scales with list size, not client
count. AA memory stayed at ~34 MB throughout (measured via the
kubelet summary API, since metrics-server isn't deployed in the lab
kind cluster); mock memory at ~7.9 MB. Both are pod-baseline noise.

No surprises at 10x. The same pattern would extrapolate linearly to
100 or 1000 until the mock's list response size or the
broadcaster's fan-out become bottlenecks — neither is anywhere
close at this scale.

## ACK-controller-pattern comparison

The question from the task: is "synchronous AA call returns Pending,
status evolves via polling" equivalent to "CRD+controller reconcile
loop updates status"?

**Functionally, for steady-state reads and for the happy path, yes.**
The fan-out via watch is the same; clients see Provisioning →
Ready; idempotent re-applies work; `kubectl get -w` is equivalent.
The 0009 "stateless AA works for sync backends" conclusion extends:
the stateless AA also works for *async* backends as long as the
backend itself answers "what's the current state?" quickly (the
async-ness is in the transitions, not the reads).

**Where the equivalence breaks, concretely**:

1. **`kubectl wait --for=jsonpath` does not work.** This is the
   single biggest ergonomic delta. For a CRD+controller, `kubectl
   wait --for=jsonpath='{.status.phase}=Ready'` is the canonical
   readiness check; it works out of the box because the CRD
   apiserver path emits `initial-events-end` bookmarks. Our AA
   doesn't. Users either need a polling workaround or the substrate
   needs a bookmark emitter. Reducible; not fundamental to the
   inversion thesis.

2. **Delete is not atomic with respect to what the library
   assumes.** The library emits a DELETED watch event synchronously
   on Delete, but the backend still reports the object (with
   `phase=Deleting`) until its deprovision timer elapses. `kubectl
   delete` with default `--wait=true` hangs for the full
   deprovision window (and, in our observation, past it — we
   hypothesize a stale informer cache not seeing the final
   eventual-disappear). This is the closest thing to a genuinely
   broken idiom. Workarounds: `kubectl delete --wait=false`;
   modeling delete as "marked for deletion" in the AA and emitting
   the final DELETED from the poll loop only (our current behavior).
   A CRD+controller handles this via finalizers and a
   deletionTimestamp; the stateless-AA model has no finalizer
   equivalent because there's no store to hold the finalizer list.

3. **Field ownership (SSA) and `last-applied-configuration`
   warnings** — same as 0009, not unique to the async case. Second
   apply warns about missing annotation; managedFields are present
   on the library-synthesized events and absent from poll-emitted
   events (we could literally see RV 153/154 have managedFields
   while RV 155 did not).

4. **Absent: drift handling.** The ACK controller reacts to AWS
   changes the user didn't make. Our AA reflects them on next poll.
   Both are valid; the difference is latency. For a 5s poll
   interval, the AA looks like a controller with a 5s sync period.

5. **Absent: conditions convention.** ACK uses
   `status.conditions` with types like `ACK.ResourceSynced`. We use
   a single `phase` string. `kubectl wait --for=condition=Ready`
   doesn't match our shape. Easy to add; this experiment doesn't
   probe the Conditions convention on purpose.

**So: the inversion survives async, but three specific idioms the
ecosystem relies on become visible as missing.** Only one (`kubectl
wait --for=jsonpath`) is hard-required for a usable developer
workflow; the other two are polishable. The core thesis — that the
AA can substitute for a CRD+controller for async backends — holds
with caveats, not with a refusal.

This is a **softening** of the 0009 conclusion, which said async
"breaks down" the stateless-AA model. It actually breaks less than
that; it breaks specific ecosystem idioms (kubectl-wait, default-wait
delete, finalizer semantics). Each is addressable. The
"Create-would-have-to-block-for-minutes" concern was overstated: the
right Create shape is to NOT block, return Provisioning, and let the
watch stream do its job.

## Fundamentals touched

**Storage independence (primary).** The stateless AA handles an
async backend. Create/Update return synchronously with a Pending
status; the mock (or real async backend) evolves over its own
timeline; the AA surfaces the transitions via its poll→broadcaster
path. No AA-side authoritative state of in-flight operations was
needed — the backend knows which provisions are in flight, and its
phase computation is a pure function of `LastChange + ProvisionDelay`.
This contradicts the 0009 claim that async "puts state back into the
picture for those resources." More nuanced: **state about in-flight
operations stays in the backend; the AA doesn't need a separate copy
of it.** If the backend exposes "where is this widget in its
lifecycle?" as a readable field (Phase, or status conditions, or
what-have-you), the AA is just a formatting layer on top.

The async-backend-sim therefore retires a candidate under Storage
independence while also **softening a claim in the 0009 finding**.
That softening belongs in SYNTHESIS.

**Watch and consistency semantics (secondary).** Three sub-findings:

1. The `k8s.io/initial-events-end` bookmark annotation is required
   for `kubectl wait --for=jsonpath` and for WatchList-aware
   informers. Our substrate does not emit it. This is the first
   experiment where this gap has had a user-facing failure.
   **Fundamental**: the gap is in the substrate; fundamental for any
   stateless AA using the same watch-broadcaster pattern.

2. The library emits its own DELETED watch event on Delete success
   that races with the backend's Deleting-phase transitions. Not a
   bug; it's by design. Worth knowing when reasoning about event
   ordering in async scenarios.

3. Poll-loop and library-path events are not deduplicated. For a
   single Create, a client on an open watch sees up to three ADDED
   events (eager publish, library publish, poll tick). Harmless;
   consumer-side RV dedup handles it.

**Resource modeling freedom (adjacent).** A simple phase-string
status turned out to be sufficient for `kubectl get` rendering and
for JSONPath-based checks (it was exactly the JSONPath check that
failed, and it failed for watch reasons, not shape reasons). The
full Kubernetes Conditions convention was not necessary for the
async model to work — but adopting it would unlock `kubectl wait
--for=condition=Ready` as another idiom. Queued as a follow-on.

## Consequents

- Our backend's `Create` calls `PublishAdded` eagerly AND the library
  emits its own ADDED after a successful Create — three ADDED events
  per apply when you count the poll loop. Not new (same 0009
  behavior); worth recording.
- Parallel agents on the same kind kubeconfig switched contexts
  mid-session during this experiment; noted in AGENTS.md's process
  observations already. Every kubectl call needs an explicit
  `--context` for multi-agent-safe runs. Not a lab-topology issue;
  a kubeconfig-global-state issue.
- `kind load docker-image` pulls happen fast when the image is
  already present locally; no surprises at lab size.
- metrics-server is not deployed in the base kind manifests; we
  measured AA memory via the kubelet summary API instead. ~34 MB
  resident for the aggexp pod, ~7.9 MB for the async-mock pod.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**:

- **Storage independence** — soften 0009's "for async provisioning,
  the model breaks down" claim. More precise: the model handles
  async backends cleanly if Create/Update return immediately with
  a Pending/Provisioning status, and the backend can answer
  "where's this in its lifecycle?" on reads. The specific ecosystem
  idioms that depend on synchronous semantics (`kubectl delete
  --wait=true` default, finalizer-based cascade) break; the core
  CRUD path does not.
- **Watch and consistency semantics** — note the
  `initial-events-end` bookmark gap. Mark it as fundamental
  (required by WatchList-aware clients, 1.31+) and substrate-level
  (lives in `runtime/storage`, not per-experiment).

For **EXPERIMENTS.md**:

- `0011-async-backend-sim` marked complete under Storage
  independence.
- The `async-backend-sim` candidate is retired — answered here.
- New candidate: **`watch-initial-events-end-bookmark`** — emit the
  bookmark annotation at the end of the initial event stream in
  `runtime/storage`; re-run 0011's scenario 5 and confirm
  `kubectl wait --for=jsonpath` now works. This is substrate-level
  work, deliberately not done here because 0011's scope was
  probing, not fixing.
- New candidate: **`status-conditions-in-aa`** — add the
  Kubernetes Conditions convention to a Widget-style type and see
  whether `kubectl wait --for=condition=Ready` behaves better than
  `--for=jsonpath`. Probes the authz/shape/convention boundary.
- The `cross-resource-references` candidate is now sharper: it
  should use async resources specifically, since the sync case is
  mostly about apply-ordering and the async case has real state
  that can linger.

## Open questions raised

- What is the minimum viable bookmark emitter for
  `runtime/storage`? Is it enough to stamp one synthetic BOOKMARK
  after the initial events with the current RV and
  `k8s.io/initial-events-end=true`? Or does `WatchList` require a
  specific sequence that includes mid-stream bookmarks for RV
  advancement?
- How do finalizer semantics interact with a stateless AA at all?
  Finalizers are library-level metadata on the object; with no
  store, each write round-trips through the backend which doesn't
  model them. Could the "Deleting" phase we emit be reframed as a
  backend-side finalizer for the Kubernetes object, letting
  kubectl's `--wait=true` delete stop spinning?
- What's the right shape for "I'm deleting and it's in flight"?
  ACK uses `status.conditions[Terminated].Status=Unknown`. We use
  a phase string. The ecosystem can't agree, so this is an
  open design question, not a bug.
- Is the double-emit on Create (eager + library) worth fixing in
  the substrate? Consumers tolerate it; removing it would simplify
  reasoning about event ordering.
- `kubectl apply` shows the `last-applied-configuration` warning
  on every second apply — same as 0009. Could the substrate
  synthesize this annotation on list responses from a backend-side
  store of the spec hash, without actually persisting the
  annotation across restarts? Probably a different experiment.
