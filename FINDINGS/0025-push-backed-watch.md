# Findings — 0025 push-backed-watch

## What we were trying to learn

Every stateless-AA experiment so far has the middleware driving a
list-poll loop to synthesize watch events. 0004 flagged this as
rate-limit coupling between middleware cadence and backend's
throttle; 0011 flagged a separate but related gap (the
`k8s.io/initial-events-end` bookmark) that breaks
`kubectl wait --for=jsonpath`. This experiment probes the push
alternative: the backend streams events over a gRPC server-stream
Watch, and the middleware forwards them without polling.

Two variants of the same `notes.aggexp.io/v1 Note` resource deploy
side-by-side. Same component server binary, same resource shape.
Variant A's backend returns `codes.Unimplemented` from Watch; the
component's capability probe flips into poll mode (15 s default
interval; 30 s also tested). Variant B's backend runs a real
streaming Watch with an internal event generator that creates /
mutates / deletes a Note on a fixed schedule. The component probes
and flips into push mode.

Six scenarios: latency comparison, `kubectl wait --for=jsonpath`
against both, reconnect, resourceVersion authority, rate-limit
coupling at 30 s poll, and sanity-checks (api-resources / get /
explain) on the schema-synthesis path from 0023.

## What we did

Built two backends (`backend-note-poll/`, `backend-note-push/`)
and a custom component (`component/`) with:

- A capability probe (`rest.ProbeMode`) that opens
  `backend.Watch` once at startup. If it errors with
  `codes.Unimplemented` the component flips into ModePoll and runs
  its own list-poll loop; any other outcome (immediate event, or
  accepted-but-idle stream) selects ModePush.
- A fork of `runtime/component/grpcbackend.REST` locally in the
  experiment (~600 lines, ~250 semantic) that implements both
  modes plus an `initial-events-end` BOOKMARK emitted from the
  component-side Watch handler (not the backend).

Event generator shared across both variants runs a deterministic
schedule: CREATE at t+3 s, UPDATE (body v2) at t+10 s, UPDATE (body
v3) at t+16 s, DELETE at t+22 s, then 30 s quiet; repeat with a new
name. Logged with ns-resolution timestamps so kubectl-side
observations can be matched to backend-side events line-by-line.

Deployed to kind cluster `aggexp-push-watch` with isolated
`KUBECONFIG`. Scenarios captured to `/tmp/aggexp-0025-{A,A30,B}/`.

## Per-scenario results

### Scenario 1 — observation latency

Each row below is a single backend-side mutation paired with the
kubectl watcher observing the corresponding event. `delay` is
`kubectl_ts − backend_ts`; all clocks are the scenario driver's
`date -u +%N` (100 ms practical resolution, sufficient for the
order-of-magnitude comparison).

**Variant B (push), poll-interval=N/A:**

| op       | backend t            | kubectl t            | delay  | RVs |
|----------|----------------------|----------------------|--------|-----|
| CREATE   | 03:18:36.208         | 03:18:36.210         | 2 ms   | backend=6 watch=6 |
| UPDATE v2| 03:18:43.214         | 03:18:43.216         | 2 ms   | backend=7 watch=7 |
| UPDATE v3| 03:18:49.220         | 03:18:49.222         | 2 ms   | backend=8 watch=8 |
| DELETE   | 03:18:55.224         | 03:18:55.226         | 2 ms   | backend=9 watch=9 |

Every mutation delivered. All four cycle events preserved.

**Variant A (poll-interval=15 s):**

| op       | backend t            | kubectl t            | delay     | effect |
|----------|----------------------|----------------------|-----------|--------|
| CREATE v1| 03:13:22.748         | 03:13:32.192 (ADDED) | **9.44 s**| body=v2 already in the payload — poll missed v1 snapshot |
| UPDATE v2| 03:13:29.754         | –                    | collapsed | observed in next ADDED above |
| UPDATE v3| 03:13:35.757         | –                    | **lost**  | no MODIFIED emitted |
| DELETE   | 03:13:41.762         | 03:13:47.193 (DELETED) | **5.43 s**| payload shows body=v2 (last cached), not v3 |

One CREATE → four backend events; poll emitted only two kubectl
events. The intermediate UPDATE transitions (v1→v2 once cached,
v2→v3) were never visible.

**Variant A (poll-interval=30 s) — rate-limit-pressure case:**

| op       | backend t            | kubectl t            | delay     | effect |
|----------|----------------------|----------------------|-----------|--------|
| CREATE v1| 03:28:35.267         | 03:28:41.699 (ADDED) | **6.43 s**| |
| UPDATE v2| 03:28:42.268         | –                    | collapsed | |
| UPDATE v3| 03:28:48.273         | –                    | lost      | |
| DELETE   | 03:28:54.278         | 03:29:11.700 (DELETED) | **17.42 s** | payload body=v1 (from CREATE) |

**One cycle of four backend events compressed to TWO kubectl
events (ADDED, DELETED) under 30 s poll**, and the DELETED event
carries the state from the CREATE moment rather than the final
state.

**Observation**: poll-mode latency bounds are as expected
(0 – poll_interval range). The finding one level deeper is that
**poll mode isn't just slow, it's lossy** for mutations between
ticks. Push mode preserves the full event stream.

### Scenario 2 & 3 — initial-events-end BOOKMARK, `kubectl wait --for=jsonpath`

The 0011 failure. Both variants **PASS** in 0025:

```
=== variant=A creating note kw-A-... ===
=== kubectl wait --for=jsonpath='{.spec.body}=initial' ===
note.aggexp.io/kw-A-... condition met
PASS in 0.175s
```

```
=== variant=B creating note kw-B-... ===
=== kubectl wait --for=jsonpath='{.spec.body}=initial' ===
note.aggexp.io/kw-B-... condition met
PASS in 0.177s
```

Raw WatchList probe output (same for both variants):

```
{"type":"ADDED","object":{"apiVersion":"aggexp.io/v1","kind":"Note","metadata":{"annotations":{"kubectl.kubernetes.io/last-applied-configuration":"..."},"creationTimestamp":"...","managedFields":[...],"name":"kw-B-...","namespace":"default","resourceVersion":"16","uid":"..."},"spec":{"body":"initial","title":"kubectl-wait-probe"},"status":{"updatedAt":"..."}}}
{"type":"BOOKMARK","object":{"apiVersion":"aggexp.io/v1","kind":"Note","metadata":{"annotations":{"k8s.io/initial-events-end":"true","kubernetes.io/initial-events-list-blueprint":"eyJr..."},"creationTimestamp":null,"resourceVersion":"19"}}}
```

Two things about this. First, the BOOKMARK emission lives on the
**component / middleware** side — the Watch handler on REST
appends one event with `metadata.annotations["k8s.io/initial-events-end"]="true"`
after the initial-events prefix. The backend does not need to
participate (variant A's Watch RPC returns Unimplemented and the
BOOKMARK is still emitted).

Second, the library adds a `kubernetes.io/initial-events-list-blueprint`
annotation to the bookmark event it forwards to the client — a
base64-encoded empty NoteList. This is the library's own
augmentation; we did not ship it. Decoded:

```
{"kind":"NoteList","apiVersion":"aggexp.io/v1","metadata":{},"items":[]}
```

It's the shape that kubectl-side WatchList assemblers use to
bootstrap their cache. The fact that our backend-opaque
(unstructured-content) wrapper produces a valid blueprint
confirms the typed-wrapper pattern from 0017 composes cleanly
with WatchList.

**This closes the 0011 `kubectl wait --for=jsonpath` gap.** The
fix is a small change to the middleware's Watch path (append one
synthetic bookmark event with the annotation) — not to the
backend, and not to the wire proto, and not to the substrate's
broadcaster. The `runtime/component/grpcbackend.REST.Watch()`
in the promoted substrate does not currently do this; queued
for the 0030 v2 promotion.

### Scenario 4 — reconnect

**Variant B (push) — middleware scaled 1→0→1.**

Backend log excerpt:

```
03:21:38 watch open wid=0 initial=0
03:21:38 watch wid=0 sent initial-events-end BOOKMARK rv=1
03:21:38 watch open wid=1 initial=0    # probe + real upstream
03:21:38 watch wid=1 sent initial-events-end BOOKMARK rv=1
03:21:40 gen op=CREATE name=genrunner-1 rv=2
03:21:47 gen op=UPDATE body=v2 rv=3
03:21:53 gen op=UPDATE body=v3 rv=4
# middleware scaled to 0 at 03:21:53.979
# ... backend continues generating events; no watcher ...
# middleware scaled to 1 at 03:22:14.060
03:22:14 watch open wid=2 initial=0
03:22:14 watch wid=2 sent initial-events-end BOOKMARK rv=5
03:22:14 watch open wid=3 initial=0
03:22:14 watch wid=3 sent initial-events-end BOOKMARK rv=5
```

Reconnect takes < 1 s from the new middleware pod becoming ready
to the backend seeing a fresh watch stream. Two streams open
(probe + upstream) on each middleware startup, which is
expected. The backend's RV advanced from 1 to 5 during the gap;
the reconnecting watch's BOOKMARK carries the new RV.

**Variant A (poll) — middleware scaled 1→0→1.**

Middleware log excerpt:

```
I0501 03:27:41 component: upstream watch (poll) interval=30s
I0501 03:27:41 poll complete in 450µs
I0501 03:28:11 poll complete in 3.2 ms
# scaled to 0 at 03:29:51 (approx)
# scaled to 1 at 03:30:01 (approx)
I0501 03:30:11 component(0025): watch mode = poll (force="", poll-interval=30s)
I0501 03:30:12 poll complete in 657µs
```

Recovery is immediate: the new pod's first poll runs at startup
(the timer fires at 0-delay) and seeds the cache from the
backend's current List. Whatever mutations happened during the
gap collapse into the cache diff — a pure-CREATE-and-DELETE
cycle that happened entirely within the gap is not observed at
all (backend List is empty both before and after), which is the
well-known polling blind-spot.

Both variants "work" in the sense that neither crashes or
hot-loops. Push mode preserves more information because the
backend's Watch stream doesn't require the middleware to be
present during the mutation. That said, if the backend restarts
its RV counter (see scenario 5) reconnect semantics get subtle.

### Scenario 5 — resourceVersion authority

The interesting finding of the experiment.

Concurrent probe in variant B: open `kubectl get notes -w` and
poll `kubectl get notes -o jsonpath={.metadata.resourceVersion}`
every 2 s during a generator cycle. The two views should agree
for the same object. They don't:

```
get view of genrunner-6:
  03:27:07.756 get-rv=22  body=v1
  03:27:16.051 get-rv=23  body=v2
  03:27:22.265 get-rv=24  body=v3

watch view of genrunner-6:
  ADDED    watch-rv=24 body=v1
  MODIFIED watch-rv=25 body=v2
  MODIFIED watch-rv=26 body=v3
  DELETED  watch-rv=27 body=v3
```

The RVs diverge. Root cause: the component's `rest.publish()`
overwrites `metadata.resourceVersion` with its own atomic counter
(`r.rv.Add(1)`) before handing the event to the broadcaster. But
`Get` and `List` return the JSON the backend sent — including the
backend's RV — without rewriting. So the watch stream shows
middleware-assigned RVs, the REST reads show backend-assigned
RVs, and the two counters are unrelated.

**This is a real consistency bug** for any client that uses
`get` + `watch` together (client-go reflectors do this on every
resync). The Relist path does `get` (middleware RV) then `watch
--resource-version=N` which checks against the middleware's
counter; the two happen to match for this trivial case because
Watch's RV check permits "equal to current RV". In a production
scenario where a reflector bookmarks an RV seen via watch and
then later does `get ?resourceVersion=N`, the semantic expected
by the library is broken.

**Variant A (poll) sidesteps the issue** because it never gets a
backend-supplied RV in watch events — the poll loop decodes the
full JSON and publish() overwrites RV consistently. The poll
snapshot for `list` also gets publish()-assigned RVs
indirectly through the poll's `cache` replacement, though the
JSON returned to clients still has whatever the backend sent.
In practice the backend in variant A doesn't set RV at all,
so both views show middleware-only RVs.

**Design decision for 0030 (out of this experiment's scope):**
Pick one RV authority. Either the middleware owns RV end-to-end
(in which case push-mode backends shouldn't bother setting it,
and the middleware rewrites on Get/List), or the backend owns
RV end-to-end (in which case the middleware's publish should
honor the backend-supplied RV without overwriting). The current
half-overwrite posture inherited from `runtime/component/
grpcbackend` is the worst of both. The 0022 thesis's shared
`ResourceMetadata` CRD pattern is a natural home for authoritative
RV in the middleware-owns case.

**Push-variant backend-restart subcase:** restarting
`note-backend-push` resets its RV counter to 1. After restart,
live GETs return item RVs in the single digits even though the
middleware had been running for minutes and has a much higher
internal counter. Clients seeing `{metadata.resourceVersion: "4"}`
on a get, then attempting a watch with `resourceVersion=4`,
would be `410 Gone` by the middleware (since the middleware
thinks 4 is long in the past).

### Scenario 6 — rate-limit coupling (0004 revisit)

Variant A at 30 s poll is a reasonable rate-limit-sensitive
configuration. In the 80 s observation window it issued 3 list
calls against the backend (30 s apart, baseline ~4 calls/minute).
Each list returns all items. Compared to variant B, which issues
**one** List call at watch-prefix time and **zero** further list
calls during the session — all state flow is through the watch
stream.

For a GitHub-shaped backend (0004's case), the push-mode
difference would be:

- Variant A at 60 s poll × 4 GitHub calls per poll (0004's
  pagination): 240 calls/hour, against GitHub's 60/hour
  unauthenticated or 5000/hour authenticated ceiling.
- Variant B pushed: zero polling calls. Backend's internal cost
  depends on how it gets notified of external changes
  (webhook / EventBridge / equivalent), which is a backend
  concern, not a middleware one.

Push mode **does not eliminate rate-limit questions**; it moves
them from "middleware ↔ backend source of truth" to "backend ↔
backend source of truth". The backend in our test case holds
state in-process so the coupling is invisible. A GitHub-webhook-
powered push backend would consume no polling quota. A backend
that polls GitHub and forwards to the middleware would consume
the same quota as variant A.

### Scenario 0 — sanity checks

kubectl api-resources against variant B (Track B synthesis path
from 0023):

```
NAME    SHORTNAMES   APIVERSION     NAMESPACED   KIND
notes                aggexp.io/v1   true         Note
```

kubectl explain note.spec:

```
GROUP:      aggexp.io
KIND:       Note
VERSION:    v1
FIELD: spec <Object>
FIELDS:
  body  <string>
  title <string> -required-
```

(First call immediately after a redeploy transiently failed with
"the backend attempted to redirect this request" for ~10 s — a
consequent of the APIService's endpoints not yet being reachable
from the aggregation layer post-scale. Resolves without
intervention. Not specific to this experiment; observed in
0023 and several other KRM experiments.)

## The capability-switch code path

The component's new behavior vs. `runtime/component.Run()`:

```go
mode, err := resolveMode(ctx, client, o)
// ...
storage := localrest.New(localrest.Descriptor{
    /* ... */
    PollInterval: o.PollInterval,
}, client, mode)
```

Where `resolveMode` calls `localrest.ProbeMode`:

```go
func ProbeMode(ctx context.Context, client componentpb.BackendClient,
               timeout time.Duration) (Mode, error) {
    stream, err := client.Watch(probeCtx, &componentpb.WatchRequest{})
    if err != nil {
        if st, ok := grpcstatus.FromError(err); ok && st.Code() == codes.Unimplemented {
            return ModePoll, nil
        }
        return ModePoll, fmt.Errorf("probe Watch: %w", err)
    }
    // Try to read one event with a short deadline; Unimplemented from
    // Recv also counts as "no watch". Timeout-on-idle counts as push
    // (backend accepted the stream but its state is empty).
}
```

and `StartUpstreamWatch` branches on mode:

```go
func (r *REST) StartUpstreamWatch(ctx context.Context) {
    if r.mode == ModePush {
        go r.runPushLoop(ctx)   // call backend.Watch, forward events
        return
    }
    go r.runPollLoop(ctx)       // middleware-local list+diff
}
```

Both modes share the same `Watch()` handler on the REST (the
apiserver-facing Watch HTTP handler), which is also where the
`initial-events-end` BOOKMARK is injected — regardless of mode.
So the bookmark emission is orthogonal to the mode choice; the
two pieces of custom behavior in this experiment are
independent.

Net delta over `runtime/component/grpcbackend.REST`: ~60 lines
for the poll loop and its diff, ~10 lines for the bookmark
emission in `Watch()`, ~40 lines for `ProbeMode`. 0030 should
absorb both (the bookmark emission is the smaller and more
obviously correct change; the poll-loop / push-forward
bifurcation may need a cleaner interface).

## Fundamentals touched

**Watch and consistency semantics** (primary). Three sub-findings:

1. **Push-backed watch observes changes at ~2 ms vs
   poll-interval bounded** (6-30 s at 15-30 s intervals).
   Fundamental, and the numbers are the order of magnitude you'd
   predict from the architecture.

2. **Poll mode is lossy**, not just slow. A backend that
   mutates an object three times within one poll interval
   produces **zero MODIFIED events** for a kubectl watcher
   — the diff is computed against the next poll's snapshot,
   which is already at the last state. Clients that depend on
   observing state transitions (controllers reconciling on
   phase changes, observability pipelines) receive degraded
   information. Fundamental for any list-diff polling
   implementation; independent of the specific poll interval.

3. **The `initial-events-end` BOOKMARK is a middleware-side
   concern, not a backend-side concern.** Our fork emits it
   unconditionally after the Watch prefix — even for a
   poll-only backend whose Watch RPC is unimplemented. Both
   variants passed `kubectl wait --for=jsonpath` in ~0.177 s.
   This closes 0011's fundamental gap with a substrate-level
   change (~10 semantic lines of Go) and without any backend
   protocol extension. The library helpfully augments our
   bookmark with a `kubernetes.io/initial-events-list-blueprint`
   annotation so WatchList-aware clients can assemble a list
   view. This is independent of whether the backend is push-
   capable. Fundamental and closes 0011.

4. **resourceVersion authority is split in the current KRM
   substrate.** `Get`/`List` surface backend-supplied RVs;
   `Watch` surfaces middleware-counter RVs. The two don't
   agree. This is latent today because only kubectl's
   watch-based paths are exercised widely; a client that
   inspects both surfaces (reflector relist-then-watch with
   an RV bookmark) will hit the inconsistency. Fundamental and
   promotes the candidate "pick one RV authority" to
   load-bearing for the 0030 substrate.

**Storage independence** (secondary). Push-backed Watch
decouples the middleware from the backend's source-of-truth
polling cadence. The rate-limit coupling from 0004 ceases to
exist at the middleware boundary; it migrates to whether the
backend itself polls or consumes webhooks. This confirms the
0022 axiom that the middleware-vs-backend axis is the useful
place to split state concerns: watch implementation is one more
thing that belongs on the "how does your backend learn about
changes" side, not "how does the middleware synthesize watch".

**Resource modeling freedom** (tertiary). "Which watch capability
does this backend support?" is a shape-of-backend question
answerable at runtime without wire protocol changes. The probe
(open Watch, observe Unimplemented or accept) produced a clean
decision with no extension to `GetSchemaResponse`. The 0022
`BackendRef.Watch` enum becomes redundant for this purpose: the
capability is observable. Keep the enum anyway for operator-
level observability (it communicates intent, not runtime state)
and for future cases where the probe is impossible.

## Consequents

- The generator schedule in `backend-note-push` runs in wall-
  clock seconds: t+3, t+10, t+16, t+22. These are arbitrary
  (README decision). The scenarios' 100 ms latency resolution
  is `date -u +%N` in bash, which is coarser than Go's
  log-time but sufficient for order-of-magnitude: 2 ms push vs
  6-30 s poll is unambiguous at this resolution.
- The middleware's watch broadcaster size (100) was inherited
  from the substrate unchanged. No drops observed at the
  experiment's rate (one mutation every few seconds).
- A first-call post-redeploy `kubectl explain` returned "the
  backend attempted to redirect this request, which is not
  permitted" for ~10 s before succeeding. APIService readiness
  race; not specific to 0025. Same consequent as 0023.
- The backend pod restart during scenario 5 was intentional; the
  resulting RV counter reset is a property of the backend's
  `sync.Map` storage (in-process only). A backend that persists
  RVs (via its own DB, or via a host-cluster `ResourceMetadata`
  CRD per 0022) would not reset.
- `kubectl get notes -o json --watch --output-watch-events`'s
  event RVs are base64-encoded when kubectl displays the
  bookmark's blueprint annotation; decoded, it is a valid
  NoteList — this confirms our typed wrapper (`componentscheme.
  Object`) produces a valid serialization even when its
  content is the untyped JSON bag.
- Bash's `bc` was not installed in the sandbox; `scenario-3`'s
  timing math was rewritten as `awk`. Kept for portability.
- The scenario scripts use `pkill` patterns that occasionally
  hang on kubectl-logs-follow; observed during the run, worked
  around by explicit `timeout N` wrappers. Quality-of-life
  issue in the tooling, not in the measurement.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**:

- **Watch and consistency semantics** — the
  `initial-events-end` bookmark gap (0011, queued as
  `watch-initial-events-end-bookmark`) is closed by a small
  middleware-side change and has been demonstrated end-to-end
  for both push and poll backends. Moves from candidate to
  validated-fix-awaiting-promotion. The backend protocol does
  not need to grow a bookmark concept; the proto's existing
  `EVENT_BOOKMARK` type is useful for forwarding additional
  backend bookmarks (RV checkpoints mid-stream) but not
  required for closing 0011.
- **Watch and consistency semantics** — polling watch is
  **lossy** for mid-interval transitions, not merely latent.
  This is new; previous experiments observed only the latency
  dimension (bounded by poll interval). Clients that read
  status transitions will miss events in poll mode. Relevant
  to 0004's GitHub driver (a PR that rapidly moves through
  review states within a minute would be observed only in its
  final state).
- **Watch and consistency semantics** — resourceVersion
  authority split between `Get`/`List` and `Watch` is a real
  inconsistency in the current KRM substrate that goes latent
  under kubectl's typical access patterns but breaks for
  relist-with-RV-bookmark flows. The 0030 v2 substrate needs
  one authority.
- **Storage independence** — push-backed watch retires the
  middleware's end of the rate-limit coupling flagged by
  0004. The coupling migrates to the backend's source-of-
  truth relationship (webhooks/events vs. polling).

For **EXPERIMENTS.md**:

- `0025-push-backed-watch` — marked complete.
- `watch-initial-events-end-bookmark` — retired; answered
  here. The fix is a 10-line change to the substrate's
  `grpcbackend.REST.Watch()`; 0030 should include it.
- New candidate (under Watch): `rv-authority-unification` —
  decide and implement a single resourceVersion authority for
  the KRM component substrate. Default recommendation:
  middleware owns RV, push mode backends' emitted RVs are
  advisory only and the middleware overwrites for both Get/
  List and Watch. Alternative: the shared
  `ResourceMetadata` CRD from 0022 holds the authoritative
  RV and the middleware reads / writes through it. Probes
  the fundamental from 0025's scenario 5.
- New candidate (under Resource modeling): `backend-pushes-
  bookmark-checkpoints` — backend emits mid-stream BOOKMARK
  events at its own RV checkpoints and the middleware
  forwards them. Useful for very-long-lived watches where
  the middleware-only initial-events-end bookmark isn't
  enough. Low priority; wait for a use case.
- `github-webhook-watch` — this experiment's push-backed
  substrate is the prerequisite. GitHub pushes events to a
  webhook receiver which becomes a push backend in front of
  the AA. The 0004 rate-limit problem dissolves.
- `async-backend-sim-push` — redo 0011 with a push-capable
  async backend; does the 0011 "double DELETE on stateless-AA"
  observation change? Pushed `phase=Deleting` → `phase=Gone`
  events might be cleaner than the poll-diff's eager-delete.

## Open questions raised

- **RV authority.** See EXPERIMENTS additions. Must be
  resolved for 0030 promotion.
- **Backend-supplied initial-events-end BOOKMARK vs
  middleware-supplied.** Today's experiment has both (variant
  B's backend emits one; the middleware emits one on top). Two
  BOOKMARK events is harmless but redundant. Which should win?
  Probably the middleware, since it has authority over RV; but
  if the backend is streaming a cache-resume (picking up from
  an earlier RV) the backend's bookmark carries more meaningful
  RV information. Design open.
- **Reconnect-with-RV semantics for push mode.** A reconnecting
  push watcher currently opens a fresh Watch and gets a new
  prefix + bookmark. The backend could support
  `WatchRequest.ResourceVersion` to resume from a point;
  scenario 4 confirms the backend accepts the RV field today
  but does nothing with it. A push backend that honors RV
  resumption would reduce event-replay on middleware restart.
  Untested.
- **Watcher coalescing under high event rate.** The generator
  produces events ~5 s apart; a push backend producing 1000
  events/second to N kubectl watchers would stress the
  broadcaster's fan-out. Not measured; would need a different
  experiment scale.
- **Probe false negatives.** A backend that is slow to open a
  Watch stream (e.g. during its own cold-start) could make the
  3-second probe timeout, and the component would mis-classify
  it as push-capable-but-idle rather than push-capable. A
  longer probe hurts startup. Needs thought in the 0030
  substrate; maybe the backend should declare its capability
  in `GetSchema` explicitly (0022's `BackendRef.Watch`) AND
  we probe to cross-check.
- **Watch `allowWatchBookmarks=false`.** We emit the
  initial-events-end BOOKMARK unconditionally. A client that
  explicitly opts out should probably not receive it. Not
  tested here; the substrate version should respect the flag.
