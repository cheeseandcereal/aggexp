# Experiment 0011: async-backend-sim

A stateless aggregated apiserver fronting a backend with **minute-scale
async provisioning**. This probes the sync-vs-async boundary flagged
by `FINDINGS/0009-ack-aggregated-s3.md`: S3 CreateBucket returns in
milliseconds, so the stateless-AA model worked cleanly for it; EKS
cluster creation, IAM role propagation, and RDS provisioning take
minutes. That would force state back into the picture, or would
it?

The specific resource: `widgets.aggexp.io/v1`. Spec: `desiredState` +
`config`. Status: `phase` (Pending / Provisioning / Ready / Deleting /
Failed), `observedState`, `readyAt`, `message`.

## Hypothesis

1. **Non-blocking Create.** `kubectl apply` on a Widget returns
   immediately with `status.phase=Provisioning`. The AA does NOT
   block 30 seconds waiting for the backend.
2. **Watch-driven ergonomics.** Standard tooling (`kubectl get -w`,
   `kubectl wait --for=jsonpath=...`) picks up the
   Provisioning->Ready transition via the AA's poll-driven watch
   stream, without any async-specific library support.
3. **The ACK controller pattern becomes mostly redundant.** If
   (1) and (2) hold, then "synchronous AA call returns Pending,
   status evolves via polling" is functionally equivalent to
   "CRD+controller reconcile loop updates status," with fewer
   moving parts (no separate controller Deployment; no etcd
   shadow; no reconcile queue).

Named fundamentals probed:
- **Storage independence** (primary). Can a stateless AA handle an
  async backend cleanly?
- **Watch and consistency semantics** (secondary). How do async
  phase transitions flow through the synthetic watch stream?

## What this is

- `async-mock/` — stdlib Go HTTP service simulating async cloud
  semantics. POST /widgets returns 202 with phase=Provisioning;
  30s later GET /widgets/{name} reports Ready. DELETE /widgets/{name}
  reports Deleting for 10s then 404. Source of truth for the experiment.
- `pkg/asyncbackend/` — runtime/storage.WritableBackend against the
  mock. Create/Update return immediately; a 5s poll loop drives
  watch events on phase transitions.
- `pkg/apis/aggexp/v1/` — Widget types.
- `pkg/server/`, `cmd/aggexp-widgets/` — thin wiring over the
  runtime/ substrate, mirroring the 0007/0009 pattern.

## How to run

Uses an isolated kind cluster named `aggexp-async` so it does not
collide with other experiments' clusters.

```
# From repo root.
./hack/gen-certs.sh

# Create an isolated cluster for this experiment.
kind create cluster --name aggexp-async
kubectl --context kind-aggexp-async create namespace aggexp-system || true

./hack/deploy.sh deploy/manifests  # applies base manifests; image refs are placeholders
# (we override the deployment image below with the async-specific manifests)

docker build -t aggexp-widgets:dev  -f experiments/0011-async-backend-sim/Dockerfile .
docker build -t aggexp-async-mock:dev experiments/0011-async-backend-sim/async-mock/
kind load docker-image aggexp-widgets:dev   --name aggexp-async
kind load docker-image aggexp-async-mock:dev --name aggexp-async

AGGEXP_IMAGE=aggexp-widgets:dev \
ASYNC_MOCK_IMAGE=aggexp-async-mock:dev \
  ./hack/deploy.sh experiments/0011-async-backend-sim/manifests

kubectl --context kind-aggexp-async -n aggexp-system rollout status deploy/async-mock
kubectl --context kind-aggexp-async -n aggexp-system rollout status deploy/aggexp

rm -rf ~/.kube/cache/discovery/
kubectl --context kind-aggexp-async get widgets

# Apply a widget and watch its phase transition
cat <<'YAML' | kubectl --context kind-aggexp-async apply -f -
apiVersion: aggexp.io/v1
kind: Widget
metadata:
  name: demo-1
spec:
  desiredState: running
  config: {color: blue}
YAML
kubectl --context kind-aggexp-async get widget demo-1 -o yaml
kubectl --context kind-aggexp-async wait --for=jsonpath='{.status.phase}=Ready' widget/demo-1 --timeout=60s
```

Tear down with `kind delete cluster --name aggexp-async`.

## Status

in-progress

## Decisions made

- **Provision window = 30s, delete window = 10s.** Short enough
  for a lab loop to feel responsive; long enough that the
  Pending/Provisioning phase is unambiguously observable.
- **Poll interval = 5s.** Fast enough to catch a 30s phase
  transition mid-window; slow enough to not dominate mock CPU.
  Arbitrary; no tuning basis.
- **Async mock is JSON over HTTP, NOT the S3 XML format from
  0009.** This experiment is about async lifecycle, not wire
  fidelity. Keeps the mock and backend tiny.
- **Resource name == mock's widget name.** No escaping. Kubernetes
  DNS-safe subset applies.
- **Create and Update return immediately with phase=Provisioning.**
  Key design choice under test. The alternative (block the HTTP
  request until Ready) would hang kubectl for 30s and is plainly
  unworkable at minute-scale; we don't even explore it.
- **Delete returns with phase=Deleting, not 202 + tombstone.** The
  object remains reachable through GET for the 10s deprovision
  window; 404 only after. Matches what real async backends do
  (AWS EKS DeleteCluster returns a cluster-in-DELETING response,
  not a 404).
- **No SSA managedFields persistence.** Same as 0009; we inherit
  the limitation. This is not what the experiment is probing.
- **One phase vocabulary.** Pending / Provisioning / Ready /
  Deleting / Failed. The full Kubernetes Conditions convention is
  out of scope; this experiment tests phase-based readiness.

## Prerequisites

- kind + kubectl + docker in PATH.
- A dedicated kind cluster (this experiment uses `aggexp-async`).
- Serving certs via `./hack/gen-certs.sh`.

## What we're looking to learn

- What specifically breaks when an AA's backend has minute-scale
  async provisioning?
- Does `kubectl apply` gracefully accommodate Pending->Ready
  transitions under the stateless-AA model?
- Does `kubectl wait --for=jsonpath=...` work against the AA's
  polling-driven watch stream? If yes, that's a major finding
  (the controller-model substitute).
- How does the AA's Create path have to behave when the backend
  is async? (Answer: return immediately, don't block.)
