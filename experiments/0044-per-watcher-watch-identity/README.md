# Experiment 0044: per-watcher-watch-identity

Invert the single-global watch validated by 0025 and 0034. Instead of
one backend watch fanned out to many clients, open one backend watch
(push) or one poll loop (poll) **per client watch subscription**, each
carrying that subscriber's `user.Info`, so backends can enforce
per-user authorization on watch streams. Measure what that costs.

This is the highest-risk, least-proven experiment in the arc: 0025 and
0034 both validated a *single* global watch, never per-watcher backend
access. Builds on the metadata-CR core from 0042.

## Status

in-progress

<!-- valid values: in-progress, complete, abandoned -->
<!-- Scaffolded brief: hypothesis + run plan written; implementation
     pending. Copy the 0042 metastore core into this experiment first. -->

## Prior findings this builds on

- `FINDINGS/0025-push-backed-watch.md` — push vs poll watch, but a
  **single** event source fanned out to all clients.
- `FINDINGS/0034-shared-watch-cross-replica.md` — one shared informer
  per replica, re-broadcast to that replica's clients. Single global
  watch, not per-watcher.
- `FINDINGS/0003-custom-authorizer-external-policy.md`,
  `FINDINGS/0006-identity-broker-github-app.md`,
  `FINDINGS/0016-aa-authz-aware-controllers.md` — the identity-handoff
  thread: the AA receives the caller's `user.Info`; per-user backend
  access is possible. None applied it to **watch** streams.
- Experiment 0042 — the host-CR RV authority the per-watcher emissions
  stamp onto events.

## Hypothesis

- **Watch and consistency semantics (primary); identity handoff and
  per-request authorization (secondary).** Each client watch
  subscription can drive its own backend access carrying that caller's
  identity:
  - **push:** one `Backend.Watch(ctx, user, ns, opts)` per
    subscription; the backend may scope emitted events to what the
    caller may see.
  - **poll:** one `Backend.List(ctx, user, ns, opts)` loop per
    subscription, at the configured interval, with that caller's
    identity.
  - **cross-replica path:** when the metadata informer fires for an
    object, the per-watcher emission re-fetches the body via
    `Backend.Get(ctx, watcherUser, ...)` — the watcher's identity, so
    backend authz applies on the cross-replica path too.

  The open question is **cost**: N subscriptions ⇒ N backend watches /
  poll loops, and a `Backend.Get` per (watcher, object) on every
  informer event. A per-event cache keyed by `(identity, ns, name)`
  should deduplicate the Get when watchers share an identity/selector.
  An opt-in shared, deduplicated, **system-identity** poll recovers the
  0025/0034 single-global-watch cost profile when per-user watch authz
  is not needed.

## Hard load-bearing decision

Per-watcher backend access is the default; `SharedPoll` (one
system-identity loop for all watchers) is the opt-in cheaper
alternative. The per-event cross-replica `Backend.Get` uses the
watcher's identity, deduplicated within one event fan-out by
`(identity, ns, name)`. The framework does not multiplex upstream
subscriptions itself — a backend with limited upstream capacity is
expected to multiplex internally (one upstream stream serving many
per-watcher channels by filtering).

## Architecture

```
client watch A (user alice) ─┐
client watch B (user bob)   ─┤   each subscription:
client watch C (user alice) ─┘     • push: own Backend.Watch(user)
                                     • poll: own Backend.List(user) loop
                                     • informer event → Backend.Get(user)
                                       deduped by (identity,ns,name)
                                            │
                  metadata-CR informer (shared, cross-replica RV)
                                            │
              SharedPoll=true → one system-identity List for all watchers
```

## What this is (files to create)

- Copy the 0042 `pkg/metastore`, `pkg/apis`, `pkg/server`, `cmd/`,
  metadata CRD, and manifests.
- `pkg/backend/inmem.go` — an in-memory `Widget` backend that (a)
  implements a push `Watcher` and (b) supports poll via `List`, and
  **filters by caller identity** (e.g., a `owner` field or label per
  object) so per-user authz on watch is observable. Adapt the
  poll-diff shape from `runtime/library/pollwatch.go`, but per-watcher.
- `pkg/watch/perwatcher.go` — per-subscription emission pipeline:
  initial replay + live events from (push channel | poll loop |
  metadata informer), per-watcher selector filtering, per-event
  `Backend.Get` with the `(identity,ns,name)` dedup cache, RV stamping
  from the metadata CR.
- A `--shared-poll` flag selecting the single system-identity loop.
- Instrumentation: counters for backend Watch/List/Get calls, watcher
  count, and Get-cache hit/miss, logged or exposed for the scenarios.
- `manifests/` — 3-replica StatefulSet; per-pod Services.

## How to run

```
./hack/gen-certs.sh
kind create cluster --name aggexp-0044
kubectl --context kind-aggexp-0044 create namespace aggexp-system
kubectl config use-context kind-aggexp-0044
./experiments/0044-per-watcher-watch-identity/hack/deploy.sh
```

### Scenario 1 — per-user authz on watch (push and poll)

Seed objects owned by `alice` and by `bob`. Watch as `alice`
(`kubectl --as alice get widgets -w`) and confirm only alice's objects
stream. Repeat in poll mode. Confirm `SharedPoll` mode does **not**
enforce per-user authz (only label/field selectors filter).

### Scenario 2 — backend-call volume vs watcher count

Open N concurrent watches (N = 1, 5, 25, 100) via background
`kubectl get -w` or a client-go script. Record backend Watch/List
invocations per interval as a function of N, in per-watcher mode vs
`SharedPoll`. This is the core cost measurement.

### Scenario 3 — per-event cross-replica Get cache hit rate

With several watchers sharing an identity+selector, trigger object
changes and record the `(identity,ns,name)` Get-cache hit rate during
each event fan-out (how much the cache saves vs one Get per watcher).

### Scenario 4 — internal multiplex pattern

Constrain the in-memory backend to a small upstream-subscription budget
and implement internal multiplexing (one upstream stream → many
per-watcher channels). Confirm N watchers do not exhaust the budget,
and note how intuitive the pattern is to implement.

### Cleanup

```
kind delete cluster --name aggexp-0044
```

## Decisions made

- Default poll interval 5s (low-cost in-memory backend); per-watcher by
  default, `SharedPoll` opt-in.
- Per-watcher channel buffer 100 (0025/0034 broadcaster default);
  slow subscribers are dropped and reconnect.
- Per-event Get cache is scoped to a single fan-out (cleared between
  events) to avoid serving stale bodies across events; revisit TTL if
  measurement shows churn.
- Backend models ownership via an `owner` field set from `user.Info` so
  per-user watch authz is observable in a self-contained way.

## Prerequisites

- kind cluster `aggexp-0044` (not the default `aggexp`).
- Serving cert from `hack/gen-certs.sh`.
- The 0042 metastore core copied in. No external secrets (identities
  are exercised with `kubectl --as`).

## What we're looking to learn

- **Watch and consistency semantics + identity handoff.** Does
  per-watcher backend access deliver per-user authz on watch streams,
  and at what backend-call cost as watcher count grows? How much does
  the per-event Get dedup cache recover? Where is the crossover that
  makes `SharedPoll` the right default for a given backend?

## Expected FINDINGS shape

- **Fundamental:** whether per-watcher watch is a viable generalization
  of the single-global-watch shape, and the architectural cost it
  imposes (the question 0025/0034 did not answer). The internal-
  multiplex pattern's ergonomics.
- **Consequent:** measured call volumes, cache hit rates, and latency
  at this lab's scale — directional, not absolute.
