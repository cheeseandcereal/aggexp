# Experiment 0029: declarative-admission-in-config

Declarative admission rules (CEL validations + JSONPath mutations)
live in an `APIDefinition`-shaped YAML config loaded by the
middleware at startup. Rules are evaluated entirely in the
middleware — no backend round-trip. This composes **additively**
with the 0020-style backend Validate/Mutate gRPC RPCs: middleware
rules run first, backend RPCs run second.

Forked from `0020-krm-admission-hook` (not from the 0022 arc's
component/v2, because v2 does not exist until 0030; this experiment
pre-dates that promotion).

## The boundary this explores

0020 closed the authz-vs-admission boundary (from 0003) by putting
admission logic in the backend as Validate/Mutate RPCs. That works,
but it forces every policy — including "required field" or "value
range" rules — to be a backend-side Go (or Python, or ...) function.
For the common case (CEL-expressible validations, simple default
values) the round-trip and the code are gratuitous.

0029 asks: can we declare the common cases in middleware config and
let the backend stay small? And does it ALWAYS pay off, or is it
tax for the cases CEL can't cover?

## The shape

- **Config file** (YAML, loaded via `--admission-config`): a
  simplified `APIDefinition` with a `backend` block (informational
  only in this experiment) and an `admission` block carrying:
    - `mutations[]`: `{jsonPath, op, value, operations?}`
    - `validations[]`: `{expression, message, fieldPath?, operations?}`
- **CEL library**: `github.com/google/cel-go v0.22.0` (matches
  kube-apiserver 1.32's version; already in go.sum as an indirect
  dep of apiserver).
- **Mutation ops supported**: `set` (always write), `default`
  (write if missing/empty). No `remove`, no JSON-patch, no merge.
  Decision recorded below.
- **JSONPath**: dotted paths (`spec.title`,
  `metadata.annotations.aggexp.io/foo`). No array indexing. The
  `metadata.annotations` / `metadata.labels` segments are
  special-cased so dotted annotation keys work. Decision recorded
  below.
- **Composition with 0020**: the grpcbackend REST adapter's `admit`
  now has two layers. Layer 1 (middleware) mutates then validates
  using the engine. If denied, the request never reaches the
  backend. If allowed, Layer 2 (backend RPCs) runs exactly as in
  0020 (Mutate RPC → Validate RPC).

## Hypothesis

- **Resource modeling freedom** (primary). Admission that can be
  expressed declaratively (CEL validations + JSONPath defaults)
  should live in config, not in backend code. Backends that serve
  simple-shaped resources can avoid implementing Validate/Mutate
  RPCs entirely.
- **Wire protocol fidelity** (secondary). The denial shape is
  identical to 0020's: HTTP 422 with `metav1.Status.Message`
  carrying the CEL rule's `message`. `apierrors.NewInvalid` with
  a `field.ErrorList` naturally emits multiple causes in one
  response — useful for declarative rules where you want to
  show every failure, not just the first.

## How to run

```
./hack/gen-certs.sh
kind create cluster --name aggexp-0029
kubectl config use-context kind-aggexp-0029
kubectl create namespace aggexp-system

./hack/deploy.sh deploy/manifests

docker build -t aggexp-krm-component:dev \
  -f experiments/0029-declarative-admission-in-config/component/Dockerfile .
docker build -t aggexp-note-backend:dev \
  -f experiments/0029-declarative-admission-in-config/backend-note/Dockerfile .
kind load docker-image aggexp-krm-component:dev --name aggexp-0029
kind load docker-image aggexp-note-backend:dev --name aggexp-0029

AGGEXP_IMAGE=aggexp-krm-component:dev \
  NOTE_BACKEND_IMAGE=aggexp-note-backend:dev \
  ./hack/deploy.sh experiments/0029-declarative-admission-in-config/manifests

kubectl -n aggexp-system rollout status deploy/aggexp
kubectl -n aggexp-system rollout status deploy/note-backend

# Scenarios:
kubectl apply -f experiments/0029-declarative-admission-in-config/samples/01-valid.yaml
kubectl get note test-valid -o yaml

# Middleware rejects (title >30; backend would have passed):
kubectl apply -f experiments/0029-declarative-admission-in-config/samples/02-middleware-rejects.yaml

# Backend rejects (prefix rule; middleware had no opinion):
kubectl apply -f experiments/0029-declarative-admission-in-config/samples/03-backend-rejects.yaml

# Mutation demo (spec.priority defaulted, annotation stamped):
kubectl apply -f experiments/0029-declarative-admission-in-config/samples/04-mutation-demo.yaml
kubectl get note test-mutate-demo -o yaml

# Middleware rejects (missing required body on CREATE):
kubectl apply -f experiments/0029-declarative-admission-in-config/samples/05-missing-body.yaml
```

Teardown: `kind delete cluster --name aggexp-0029`.

## Status

complete

## Decisions made

- **Static config file, no CRD reconciler.** 0027 has the dynamic
  CRD-driven multiplex story. 0029 only needs to prove the
  declarative-admission thesis, so a file loaded at startup via
  `--admission-config` suffices. If we later want hot-reload, it's
  straightforward to add an fsnotify watcher against the same
  path.
- **Mutation op subset: `set` and `default` only.** No `remove`,
  no JSON-patch, no strategic-merge. `set` covers stamping
  required annotations/labels; `default` covers optional fields.
  Removal is rare enough in admission and adds complexity around
  "what if the path doesn't exist"; skip for now.
- **JSONPath implementation: hand-rolled, dotted-path only.** No
  array indexing. The `metadata.annotations.foo.bar.baz`
  ambiguity (is `foo.bar.baz` a dotted key or a path through
  three nested maps?) is resolved by special-casing
  `metadata.annotations` / `metadata.labels`: once the walker
  enters those, the remainder is one key. A real JSONPath library
  would be safer; for the scope of this experiment the edge cases
  aren't worth a dependency.
- **CEL version: `github.com/google/cel-go v0.22.0`** — whatever
  `k8s.io/apiserver@v0.32.3` pulls transitively. Pinning the same
  version kube-apiserver does minimizes the risk of surprising
  CEL semantics differences between our declarations and the
  1.32 ValidatingAdmissionPolicy bindings.
- **CEL bindings: `object` and `oldObject`.** Matches the names
  used by CEL admission in kube-apiserver 1.30+ (VAP). `oldObject`
  is `null` on CREATE; on UPDATE it's the stored pre-image.
  Rules that only make sense on CREATE use the `operations:
  [CREATE]` filter rather than `oldObject == null` — cleaner.
- **CEL eval error → deny.** A runtime error (e.g. a type mismatch
  in an expression) is turned into a denial with the CEL error
  appended to the message. Fail-closed matches the kube-apiserver
  VAP default.
- **CEL expression must return `bool`.** Config-load time check.
  A non-bool expression is a startup error, not a runtime deny.
- **All middleware failures surface as a single HTTP 422** via
  `apierrors.NewInvalid(GroupKind, name, field.ErrorList{...})`.
  Multiple failing validations produce multiple `causes[]`
  entries. This is what kube-apiserver does for its own VAP.
- **Middleware layer runs FIRST; backend RPCs run SECOND.** If
  middleware denies, the backend never sees the request. If
  middleware allows, the backend's own mutate-then-validate
  runs. This keeps the backend authoritative for what it alone
  can see (e.g. cross-resource rules, external lookups) while
  letting the middleware do cheap local rules with no round-trip.
- **Backend-note keeps its 0020 admission wholesale.** Preserving
  the backend's existing Validate (name prefix, DNS-1123,
  title-length 3-64) and Mutate (accepted-at annotation) is what
  makes the "both layers compose" demo possible — same image,
  same advertised capabilities, plus a middleware config layered
  on top.
- **Kind cluster: `aggexp-0029`.** Named per the task spec.
- **No tests.** Per repo ethos; the lab exercises the code, the
  engine is small enough to read.
- **No metrics.** Per task spec.

## Prerequisites

- A kind cluster named `aggexp-0029`.
- `hack/gen-certs.sh` to produce serving cert.
- `hack/deploy.sh deploy/manifests` for base resources.

## What we're looking to learn

Primary: **Resource modeling freedom**.

- Can a middleware-only CEL + JSONPath admission layer replace
  hand-written Go for the common cases (required fields, value
  ranges, content rules, default values)?
- When the CEL answer is "can't express it" (cross-resource,
  external lookup), does the backend-RPC seam still cover the
  gap cleanly?
- Is the two-layer design gratuitous — i.e. is there a class of
  backends where middleware admission feels like tax rather than
  leverage?

Secondary: **Wire protocol fidelity**.

- Does the HTTP 422 shape emitted by the middleware match the
  backend-emitted shape from 0020?
- Does kubectl print a multi-cause 422 usefully?

Out of scope: identity handoff, storage, per-request authorization,
watch semantics. Admission is orthogonal to all four; 0020 confirmed
that already.
