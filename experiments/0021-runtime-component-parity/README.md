# Experiment 0021: runtime-component-parity

First consumer of the `runtime/component/` substrate. Demonstrates
that a new KRM-style aggregated API with a thin gRPC backend is
~50 lines of Go (`cmd/note-aa/main.go`) plus whatever the backend
needs, once the substrate is in place.

Promoted from experiments 0013 + 0017 + 0018. The component-server
shape, the typed `dyn.Object` wrapper, the OpenAPI composition
helpers, and the gRPC-backed `rest.Storage` adapter all now live in
`runtime/component/`. This experiment is the parity probe: does the
extracted substrate actually let a new KRM consumer skip 0017's
~1500 lines of handwritten component code?

## Hypothesis

- **Wire protocol fidelity** (primary, by inheritance from 0017).
  The substrate's behavior should be identical to 0017's for CRUD
  + watch + explain + SSA against an in-memory Note backend.
- **Substrate payoff** (secondary, meta-level). A new KRM consumer
  should be a small `main.go` plus a backend, not a fork of
  0013/0017's component server.

## How to run

```
./hack/gen-certs.sh
kind create cluster --name aggexp-substrate-component
kubectl config use-context kind-aggexp-substrate-component
kubectl create namespace aggexp-system

./hack/deploy.sh deploy/manifests

docker build -t aggexp-note-aa:dev \
  -f experiments/0021-runtime-component-parity/cmd/note-aa/Dockerfile .
docker build -t aggexp-note-backend-0021:dev \
  -f experiments/0021-runtime-component-parity/backend-note/Dockerfile .
kind load docker-image aggexp-note-aa:dev --name aggexp-substrate-component
kind load docker-image aggexp-note-backend-0021:dev --name aggexp-substrate-component

AGGEXP_IMAGE=aggexp-note-aa:dev \
  NOTE_BACKEND_IMAGE=aggexp-note-backend-0021:dev \
  ./hack/deploy.sh experiments/0021-runtime-component-parity/manifests

kubectl -n aggexp-system rollout status deploy/aggexp
kubectl -n aggexp-system rollout status deploy/note-backend

kubectl apply -f experiments/0021-runtime-component-parity/sample-note.yaml
kubectl get notes
kubectl explain note.spec
kubectl apply --server-side --field-manager=alice \
  -f experiments/0021-runtime-component-parity/sample-note.yaml
kubectl get note hello -o yaml --show-managed-fields
```

Expectation: every scenario behaves identically to 0017's `kubectl
get notes` / `explain` / SSA outputs. If it does, the substrate
extraction held.

## Status

complete

<!-- See FINDINGS/0021-runtime-component-parity.md. -->

## Decisions made

- **Single experiment go.mod** (rather than 0017's split component /
  gen / backend). The substrate owns the proto package, so there's
  no per-experiment gen module. Both `cmd/note-aa/main.go` and
  `backend-note/cmd/note-backend/main.go` compile under one
  experiment module with `replace github.com/cheeseandcereal/aggexp
  => ../..`.
- **Typed wrapper default ON** (`--use-typed-wrapper=true`) in the
  manifest. 0017 made this opt-in while probing; the substrate
  defaults it to on because SSA working end-to-end is the expected
  baseline.
- **Backend is a near-verbatim copy of 0017's note-backend**, with
  imports repointed at the substrate's `runtime/component/proto`
  package. No behavioral changes.
- **Kind cluster `aggexp-substrate-component`.** Distinct from
  every other experiment's cluster.

## Prerequisites

- kind cluster `aggexp-substrate-component`.
- `hack/gen-certs.sh`, `hack/deploy.sh deploy/manifests` for base
  resources.

## What we're looking to learn

- Does the substrate's `runtime/component` hold up under a fresh
  consumer without further modification? The success condition is
  simply "the experiment runs and behaves like 0017." Any required
  patch to the substrate to make this experiment go is itself a
  signal that the extraction was premature.
- How small does the consumer's code get? Line-count comparison
  vs 0017's handwritten component code in FINDINGS.
