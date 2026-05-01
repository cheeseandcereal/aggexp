# Experiment 0025: push-backed-watch

The stateless-AA experiments so far (0004 GitHub, 0009 S3, 0011
async-mock) all had the middleware drive a polling loop on the
backend's List method to synthesize watch events. 0025 probes the
push-backed alternative: the backend streams events to the middleware
as they happen, and the middleware forwards them to kubectl clients
without any middleware-side poll loop.

Two variants of the same Note resource are deployed side-by-side and
compared on watch latency, reconnect behavior, and the
`initial-events-end` bookmark gap 0011 surfaced.

## Hypothesis

- **Watch and consistency semantics** (primary). Push-backed watch
  should show sub-second observation latency vs. the poll-interval-
  bounded latency of the polling variant. A backend-emitted
  `BOOKMARK` event carrying `metadata.annotations["k8s.io/initial-events-end"]="true"`
  should pass through the middleware's watch fan-out and satisfy
  `kubectl wait --for=jsonpath` / WatchList-aware clients (the 0011
  failure).
- **Storage independence** (secondary). A pushed-watch backend
  should decouple middleware liveness from backend rate limits
  (the 0004 coupling). No middleware poll → no per-interval backend
  pressure.
- **Resource modeling freedom** (tertiary). "Which watch mode does
  this backend support?" is a shape-of-backend concern that the
  middleware discovers at runtime; the same resource shape
  (`notes.aggexp.io/v1`) is served by both variants from the same
  component-server binary.

## How to run

One-time:
```
./hack/gen-certs.sh
kind create cluster --name aggexp-push-watch
kubectl config use-context kind-aggexp-push-watch
kubectl create namespace aggexp-system
```

Build images (three: shared component, poll backend, push backend):
```
docker build -t aggexp-note-aa-0025:dev \
  -f experiments/0025-push-backed-watch/component/Dockerfile .
docker build -t aggexp-note-backend-poll:dev \
  -f experiments/0025-push-backed-watch/backend-note-poll/Dockerfile .
docker build -t aggexp-note-backend-push:dev \
  -f experiments/0025-push-backed-watch/backend-note-push/Dockerfile .
kind load docker-image aggexp-note-aa-0025:dev --name aggexp-push-watch
kind load docker-image aggexp-note-backend-poll:dev --name aggexp-push-watch
kind load docker-image aggexp-note-backend-push:dev --name aggexp-push-watch
```

Deploy base + variant A (poll):
```
AGGEXP_IMAGE=aggexp-note-aa-0025:dev ./hack/deploy.sh deploy/manifests
AGGEXP_IMAGE=aggexp-note-aa-0025:dev \
  NOTE_BACKEND_IMAGE=aggexp-note-backend-poll:dev \
  ./hack/deploy.sh experiments/0025-push-backed-watch/manifests/variant-a-poll
kubectl -n aggexp-system rollout status deploy/aggexp
kubectl -n aggexp-system rollout status deploy/note-backend
```

Run scenarios (see `scenarios/` for scripts used to gather numbers
that went into the FINDINGS file).

Teardown variant A, deploy variant B (push):
```
kubectl -n aggexp-system delete deploy aggexp note-backend
AGGEXP_IMAGE=aggexp-note-aa-0025:dev \
  NOTE_BACKEND_IMAGE=aggexp-note-backend-push:dev \
  ./hack/deploy.sh experiments/0025-push-backed-watch/manifests/variant-b-push
```

## Status

complete

## Decisions made

- Both variants live in the same kind cluster `aggexp-push-watch`,
  deployed sequentially. Less cluster churn; consistent environment
  for the comparison numbers.
- Variant A's poll interval defaulted to 15s (matches the task
  brief). The component sets this from `--poll-interval`; variant A
  also runs a 30s scenario to exercise the extreme end of the 0004
  rate-limit coupling.
- Variant B's event generator creates one Note at t+3s, mutates it
  at t+10s (incrementing spec.body), mutates it again at t+16s, and
  deletes it at t+22s (wall-clock offsets from backend start). Fixed
  offsets rather than random so kubectl-side observations can be
  matched to backend-side events deterministically. The generator
  logs each mutation with its ISO8601 timestamp; the scenario
  scripts time-diff against the kubectl observation timestamp.
- The component does NOT use `runtime/component.Run()` as-is,
  because `runtime/component/grpcbackend.REST.Watch()` does not
  emit an `initial-events-end` BOOKMARK. This experiment forks a
  small REST storage (`component/rest/rest.go`) that adds bookmark
  emission and a capability-switch between push (call
  `backend.Watch`) and poll (middleware-local list-differ).
- Watch capability is detected at component startup via a probe:
  open a `backend.Watch` stream; if the backend immediately returns
  `codes.Unimplemented`, fall back to poll mode. No proto change
  required in the substrate.
- Resource shape matches 0021/0023: `spec.title` + `spec.body`,
  `status.updatedAt`. Schema source is Track B from 0023 (plain
  JSON Schema from backend, middleware synthesizes OpenAPI).
  Synthesis code is a verbatim copy of 0023's — kept in the
  experiment rather than promoted to avoid substrate churn.
- Latency measurements use kubectl's `get -w -o json` with
  `--output-watch-events` to get a per-event timestamp in the
  stream, compared against the backend's structured log timestamps.
  A 100ms resolution is fine for the order-of-magnitude comparison
  (seconds vs. milliseconds).

## Prerequisites

- kind cluster `aggexp-push-watch`.
- `hack/gen-certs.sh`, `hack/deploy.sh deploy/manifests` for base
  resources.

## What we're looking to learn

1. **Observation latency.** How long from a backend-side state
   change to a kubectl watcher observing the event? Variant A:
   bounded by poll interval. Variant B: expected sub-second.
2. **initial-events-end bookmark gap (0011).** Can a push-backed
   backend plus the substrate's existing `EVENT_BOOKMARK` proto
   type close the 0011 `kubectl wait --for=jsonpath` gap?
3. **Reconnect behavior.** Does a backend Watch stream reconnect
   cleanly after the middleware restarts? How does poll mode
   compare?
4. **Resource version semantics.** The backend in variant B claims
   authority over resourceVersion. What happens to monotonicity
   across backend restarts?
5. **Rate-limit coupling (from 0004).** With a 30-second poll
   interval in variant A, how often does kubectl see updates vs.
   variant B?
