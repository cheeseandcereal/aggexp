# Experiment 0019: krm-polyglot-backend

Proves the language-agnostic claim of the KRM middle-layer arc.
0013, 0017, and 0018 all used Go backends behind the component
server. 0019 implements the same Backend gRPC service in Python —
`backend-note-py/` — and points the unchanged 0017 component
server at it.

**Thesis: the backend is language-agnostic.** The component server
cannot tell the backend's implementation language apart; the only
contract is the wire proto.

## Hypothesis

- **Resource modeling freedom** (primary). Backend implementation
  language is an implementation detail. A backend in any language
  that can speak gRPC + JSON can stand behind 0017's component
  server and serve a Kubernetes resource indistinguishably from the
  Go reference backend.
- **Wire protocol fidelity** (secondary). The 0017 proto transports
  cleanly across language boundaries; no hidden Go-isms leak.

## How to run

```
./hack/gen-certs.sh
kind create cluster --name aggexp-krm-poly
kubectl config use-context kind-aggexp-krm-poly
kubectl create namespace aggexp-system

./hack/deploy.sh deploy/manifests

# Component server image — same Dockerfile as 0017, rebuilt against
# this experiment's tag. The binary is unchanged.
docker build -t aggexp-krm-component-v2:dev \
  -f experiments/0017-krm-protocol-refinement/component/Dockerfile .
docker build -t aggexp-note-backend-py:dev \
  -f experiments/0019-krm-polyglot-backend/backend-note-py/Dockerfile .
kind load docker-image aggexp-krm-component-v2:dev --name aggexp-krm-poly
kind load docker-image aggexp-note-backend-py:dev --name aggexp-krm-poly

AGGEXP_IMAGE=aggexp-krm-component-v2:dev \
  NOTE_BACKEND_PY_IMAGE=aggexp-note-backend-py:dev \
  ./hack/deploy.sh experiments/0019-krm-polyglot-backend/manifests

kubectl -n aggexp-system rollout status deploy/aggexp
kubectl -n aggexp-system rollout status deploy/note-backend

kubectl apply -f experiments/0019-krm-polyglot-backend/sample-note.yaml
kubectl get notes
kubectl explain note.spec
kubectl apply --server-side --field-manager=alice \
  -f experiments/0019-krm-polyglot-backend/sample-note.yaml
kubectl get note hello -o yaml --show-managed-fields
```

Expect identical user-facing behavior to 0017 — rich explain,
populated `metadata.managedFields` after SSA, clean CRUD + watch.

## Status

in-progress

<!-- See FINDINGS/0019-krm-polyglot-backend.md for results. -->

## Decisions made

- **Python, not Rust.** Python's grpcio + grpcio-tools is the
  stock well-known combo; rust would have been a bigger
  dependency surface and slower feedback loop. Rust remains open
  as a follow-on.
- **Single-file backend** (`main.py`). The Go reference is also
  single-file; keeping the shapes symmetric makes the line-count
  comparison meaningful.
- **In-memory dict keyed on `(namespace, name)` tuples.** Exact
  mirror of the Go backend.
- **Reuse 0017's component server via a rebuilt tag
  `aggexp-krm-component-v2:dev`** instead of poking at the
  existing `aggexp-krm-component:dev`. The binary is literally
  unchanged; the retag is only to satisfy the task's "rebuild
  per-experiment" convention without touching 0017's artifacts.
- **Generated python bindings are NOT committed.** `gen.sh`
  regenerates them at the workstation; the Dockerfile runs the
  same generation at build time. Committing them would bloat
  the repo with machine output that's re-derivable in seconds.
- **grpcio / grpcio-tools / protobuf versions pinned** in
  requirements.txt. Picked the latest patch of each on the 5.x
  protobuf / 1.66.x grpc line at time of writing; no particular
  reason beyond "current stable".
- **`main_thread` receives SIGTERM, not the grpc server threads.**
  Standard python-grpc lifecycle; nothing exotic.
- **Kind cluster `aggexp-krm-poly`.** Distinct from every other
  experiment.

## Prerequisites

- A kind cluster named `aggexp-krm-poly`.
- `hack/gen-certs.sh` to produce the serving cert.
- `hack/deploy.sh deploy/manifests` for base resources.
- `python3` + `grpc_tools` only if regenerating bindings locally;
  the Dockerfile generates them at build time so local tooling
  is optional.

## What we're looking to learn

- Does the 0017 component server work unchanged against a
  non-Go backend? (Expected yes; confirms the polyglot claim.)
- Line-count comparison: python `main.py` vs. Go
  `cmd/note-backend/main.go`. Which is shorter? Where does the
  cost differ?
- Latency comparison: how does a Python gRPC backend compare
  to Go for `kubectl get` end-to-end?
- Does python's gRPC Watch stream semantics match Go's over
  long-running kubectl `-w` sessions?
