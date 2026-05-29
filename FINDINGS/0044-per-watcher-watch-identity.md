# FINDINGS: 0044-per-watcher-watch-identity

## What I was trying to learn or break

0025 and 0034 both validated a **single global watch**: one backend
push stream (0025) or one shared metadata-CR informer (0034) fanned
out to every client. That shape cannot enforce per-user authorization
on a watch stream — every client sees the same events. The whole
identity-handoff thread (0003, 0006, 0016) had the AA carry the
caller's `user.Info` into Get/List/Create, but never into *watch*.

The question: can each client watch subscription drive its own backend
access carrying that caller's identity — so a backend can scope a
watch stream per user — and **what does that cost** as the watcher
count grows? Is per-watcher watch a viable generalization of the
single-global-watch shape, or does the cost make it unworkable? Where
is the crossover that makes a cheap shared-poll the right default? Is
the internal-multiplex escape hatch ergonomic?

This was framed as the highest-risk experiment in the multi-replica
library-composition arc. I pushed to make it actually work and to
measure the cost honestly rather than declare a wall.

## What I did

Copied the 0042 metadata-CR-RV-authority core (shared cluster-scoped
metadata CR as the single RV authority; shared body CRD for
cross-replica readable bodies; stitched on read) and inverted its
watch path.

The body backend (`pkg/backend`) gained a server-stamped `owner` field
(set from `user.Info` on Create, overwriting any client value) and
three identity-aware reads — `GetFor`, `ListFor`, and a push
`WatchFor` — that filter to the bodies a caller owns (system
identities see all). This is option (a) from the brief: keep the
shared body CRD (so cross-replica Get/List/Watch still works, the
load-bearing 0042 finding) and make per-user authz observable through
an owner field, rather than reverting to a per-replica in-memory
backend that would re-break cross-replica reads.

`pkg/watch` is the inversion. A `Hub` (one per replica) owns the
registry of active `PerWatcher` pipelines and consumes the metadata-CR
informer events through a new `metastore.RawSink` seam (forwarding
`(eventType, ref, rv)` instead of a pre-stitched object). Each client
`Watch()` gets its own `PerWatcher` carrying that caller's identity,
its own initial owner-filtered RV-stamped replay, an
`initial-events-end` BOOKMARK, and a live source:

- **push** — one `backend.WatchFor(user)` subscription per watcher.
  The single shared body informer is the one upstream stream; the
  backend fans it out to every per-watcher channel, owner-filtered
  (the internal multiplex).
- **poll** — one `backend.ListFor(user)` loop per watcher at the
  configured interval, diffing against a per-watcher snapshot.

The shared metadata-CR informer remains the single RV authority and
the cross-replica trigger: on each metadata event the Hub fans out to
every watcher, doing one `Backend.GetFor(watcherUser, ns, name)` per
distinct `(identity, ns, name)` via a per-event dedup cache scoped to
that single fan-out. Both live sources can fire for a change committed
on the local replica, so a per-watcher `(ns/name)→lastRV` dedup
collapses the duplicate.

A `--shared-poll` flag selects one system-identity `ListFor` loop for
all watchers, fanned out selector-filtered only (no owner filter) —
the cheap opt-in. `--upstream-budget` caps concurrent backend push
subscriptions; over budget, a watcher falls back to per-watcher poll.
Instrumentation counters (backend Watch/List/Get, active watches,
Get-cache hit/miss, fan-out events, watcher count) are logged every 5s
as a structured klog line the scenarios read by counter delta.

Ran on a 3-replica StatefulSet in kind `aggexp-0044`, exercising all
four README scenarios with `kubectl --as` impersonation and a
client-go N-watcher load tool (`cmd/watchload`).

## What I observed

**Per-user authz on the watch stream works, in both push and poll.**
With alice and bob each owning widgets, `kubectl --as alice get
widgets -w` streamed only alice's objects — alice's new and modified
widgets appeared live; bob's never did — in push mode and, separately,
in poll mode. `kubectl --as alice get widget <bob's>` returns
NotFound; `--as alice` List shows only alice's. Impersonating
`--as-group=system:masters` sees all (the system bypass). This is the
thing 0025/0034 could not do.

**SharedPoll does not enforce per-user authz on watch — exactly as
designed.** Under `--shared-poll`, alice's watch stream included bob's
b7 ADDED as well as her own a7. (Interestingly, the unary List path
*still* owner-filters even under SharedPoll, because List goes through
`ListFor`; only the watch loses per-user authz. The mode name is
about the *watch* path.)

**Backend-call volume vs watcher count (the core cost measurement),
pinned to one replica, single identity:**

- *Per-watcher push.* Concurrent backend Watch subscriptions track N
  exactly: at N = 1/5/25/100 the active backend-watch count was
  2/6/26/101 (the +1 is a background controller watch). One backend
  Watch per client watch. `ListFor` also grows ~N from the one initial
  replay each watcher does.
- *Per-watcher poll, 3s interval, ~12s window (≈4 cycles).* `ListFor`
  calls over the window were ≈ 8 / 30 / 104 / 505 at N = 1/5/25/100 —
  linear in N and inversely proportional to the interval
  (≈ N × window/interval).
- *SharedPoll, 3s interval, same window.* `ListFor` calls were
  ≈ 3–5 **regardless of N** (1, 5, 25, or 100). One List per interval,
  flat. At N = 100 that is ~100× fewer backend List calls than
  per-watcher poll.

**Per-event Get dedup cache hit rate (push, N watchers sharing
identity alice, 5 modifications triggered):**

- N = 5  → hits +20, misses +10 → **66.7%**
- N = 25 → hits +120, misses +10 → **92.3%**
- N = 50 → hits +245, misses +10 → **96.1%**

Misses stayed ≈ constant (governed by the number of distinct
`(identity, ns, name)` keys per event, not by N); hits scaled with N.
With two distinct identities among 25 watchers (alice+bob, both sides
patched) the hit rate was 88.5% with misses ≈ 2 per object event. So
the cache turns the naive "one Get per watcher per event" into "one
Get per distinct identity per object per event": hit rate →
(watchers−identities)/watchers. It collapses the per-event fan-out Get
cost to be independent of watcher count *when watchers share
identity*, and degrades gracefully toward zero only if every watcher
has a unique identity.

**Internal multiplex under a constrained budget works.** With
`--upstream-budget=10` and N = 50 push watchers: active backend
subscriptions capped at exactly 10, `watchOpened` = 10,
`watchRejected` = 41, and all 51 watchers (50 + background) stayed
alive — the 41 over-budget watchers fell back to per-watcher poll. The
load tool observed 450 events total (every watcher got its full
initial replay), confirming none were starved. One shared informer
fed all 10 push subscribers; the budget did not exhaust.

**Cross-replica per-watcher watch holds.** A watch via the
load-balanced Service (aggregation layer routes it to any replica)
streamed an object created on a different replica, with monotonic host
RVs — because the per-watcher inversion left the 0042/0034 RV
authority untouched: the metadata informer (all replicas observe the
same etcd RV stream) is still the trigger and the RV source.

## What surprised me

The Get dedup cache is more valuable than the framing implied. I
expected it to matter "when watchers share an identity/selector." In
practice the **realistic** case for a watch is many clients of the
*same* human or controller identity (a dashboard with 50 browser tabs,
a controller with many informers) — exactly where the cache wins
hardest (96% at N = 50). The pathological no-share case (one unique
identity per watcher) is the one that is rare in practice. So the
per-event Get cost is, for typical workloads, effectively decoupled
from watcher count even in pure per-watcher mode.

The double-emission was a real trap. Both the per-watcher backend
source and the shared metadata informer fire for a locally-committed
write, so the first naive run delivered every event ~2–4×. The fix
(per-watcher RV dedup, metadata informer as sole RV authority) is the
same lesson 0042/0034 taught from the other direction: designate one
RV authority and route everything through it.

The BOOKMARK carrier cost ~30 minutes: a `PartialObjectMetadata`
bookmark closes the aggexp.io watch stream silently (watcher opened and
stopped within 1 ms, no error surfaced to the client). The substrate's
`runtime/library/bookmark.go` already knew this — it uses the served
type as the carrier — but the per-watcher path is a parallel
implementation and had to relearn it. The fix: carry the BOOKMARK on
an empty `*Widget`.

## Fundamentals

**Watch and consistency semantics (primary).** Per-watcher watch is a
*viable* generalization of the single-global-watch shape. It does not
break the RV-authority model: the single shared metadata-CR informer
stays the sole RV source and cross-replica trigger, and the
per-watcher pipelines are emission/authz front-ends on top of it. The
architectural cost is linear backend access in N — one backend Watch
per push watcher, or one List/interval per poll watcher — which is
fundamental to carrying identity per subscription (you cannot ask a
backend "what may THIS user see" without a per-user call). What is
*not* forced to be linear is the per-event cross-replica Get: the
`(identity, ns, name)` dedup cache makes that sublinear (constant in
watcher count when identities are shared). This is the line between
"per-watcher watch is unworkable" (false) and "per-watcher watch costs
one backend subscription/poll-loop per watcher, which the backend must
be able to absorb or multiplex" (true).

**Identity handoff (secondary).** `genericapirequest.UserFrom(ctx)`
works on the Watch handler's context exactly as it does on Get/List
(confirmed via `kubectl --as`). Carrying that identity into a
per-subscription backend Watch/List and into the per-event Get is
straightforward — no new framework support needed, the same slot 0006
used. Impersonation-erases-extras (0006) still applies: the watcher's
identity is name + `system:authenticated`, no extras, which is enough
for an owner-field gate but would not carry an upstream credential's
strength.

**Per-request authorization (secondary).** The watch stream is now an
authorization surface, not just Get/List. The gate exists in three
places that must agree: the initial replay (`ListFor`), the live
source (push `WatchFor` owner-filter / poll `ListFor`), and the
cross-replica metadata-event path (`GetFor`). All three enforce the
same owner predicate, so a user cannot see another user's object
appear on any path. SharedPoll deliberately drops the gate on the live
watch path only — a clean demonstration that the cost saving and the
authz guarantee are the same tradeoff dial.

## Consequents (directional, lab-scale, not absolute)

- The concrete call volumes (8/30/104/505 List calls for poll;
  2/6/26/101 backend watches for push; 3–5 flat for SharedPoll) are at
  lab scale with ~9 objects, a 3s/5s interval, and a kind single-node
  cluster. The *shapes* (linear in N for per-watcher, flat for
  SharedPoll, sublinear Get with the dedup cache) are the
  generalizable part; the magnitudes are not.
- The cache hit rates (66.7 / 92.3 / 96.1% at N = 5/25/50 same
  identity) are a function of watchers-per-distinct-identity; they
  generalize as (W−I)/W, but the specific percentages depend on the
  test's identity mix.
- Backend body store here is a host CRD reached via the dynamic client
  and an informer; the per-watcher poll's `ListFor` hits the local
  informer cache, not the host apiserver, so the "cost" measured is
  in-process list+filter, not network round trips. A backend whose
  List is a remote paginated call would make per-watcher poll far more
  expensive and move the SharedPoll crossover much lower.
- The push subscription is an in-process channel fed by a shared
  informer, so "one backend Watch per watcher" is cheap *here*. A
  backend whose upstream Watch is a real network stream (GitHub
  events, a database CDC feed) with limited concurrency is exactly the
  case the `--upstream-budget` + internal multiplex addresses.
- BOOKMARK-carrier-must-be-a-served-type is a kube 1.32
  watch-encoder consequent, not a fundamental.

## What this changes for SYNTHESIS and EXPERIMENTS

For SYNTHESIS (Watch and consistency semantics): the single-global
watch validated by 0025/0034 is **not** the only viable shape.
Per-watcher watch carrying caller identity is viable and enables
per-user authz on watch streams, at a cost that is linear in watcher
count for backend access but sublinear (cache-recoverable) for the
per-event cross-replica Get. The RV-authority model is orthogonal to
the fan-out model: you can invert the fan-out (per-watcher) while
keeping the single host-CRD RV authority. SharedPoll is the correct
default precisely when per-user watch authz is not required and the
backend's List is expensive; per-watcher is the default when watch
streams must be identity-scoped. The crossover is set by the backend's
per-call cost and the watchers-per-identity ratio, not by watcher
count alone.

For EXPERIMENTS: the menu's per-watcher-watch candidate is answered.
The internal-multiplex pattern (one upstream stream → many per-watcher
channels, with a budget + poll fallback) is ergonomic enough to be a
backend-author convention rather than framework machinery — the brief
predicted the framework would not multiplex upstream subscriptions
itself, and that held: ~30 lines of fan-out + a budget check in the
backend is all it took. A future "consumer watch authz" substrate
helper could codify the three-place owner gate + the per-event Get
dedup cache, but only after a second consumer demands it.

## Open questions

- TTL on the Get dedup cache. It is currently cleared between events
  (no cross-event reuse) to avoid stale bodies. A short TTL would let
  bursts of events for the same object within a window share a Get, at
  some staleness risk. Unmeasured.
- The per-watcher poll snapshot is per-pipeline; N poll watchers of the
  same identity each keep their own snapshot and each List. A
  per-identity shared poll loop (between full SharedPoll and full
  per-watcher) would cut the List cost to one-per-identity while
  keeping per-user authz. Not built; it is the obvious next dial.
- At 10⁴+ objects the initial replay (list-as-prefix) replays every
  owned object on every reconnect, per watcher. Same scaling concern
  0034 raised, now multiplied by watcher count.
- Slow-consumer behavior under sustained event load: the per-watcher
  channel drops on full (DropIfChannelFull) and relies on reflector
  relist. Not stress-tested.
- Real network-backed backends (where one Watch per watcher is a real
  upstream stream) would exercise the budget/multiplex path under
  genuine pressure; here the upstream is a cheap in-process informer.
