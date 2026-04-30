# Experiment 0017: krm-protocol-refinement

Refines the 0013 KRM middle-layer skeleton to close two gaps that
experiment surfaced:

1. **`kubectl explain` returned only the catch-all description** —
   the backend's OpenAPI wasn't threaded through the library's
   explain-rendering path.
2. **Server-Side Apply broke at `managedfields.NewTypeConverter`** —
   the unstructured-types path didn't expose a typed model the
   library could walk.

This experiment forks 0013's structure and refines:

- The proto (`proto/backend.proto`) — adds an explicit `Apply` RPC,
  propagates field-manager name on Create/Update, adds
  short-names and `supports_server_side_apply` in `GetSchema`, and
  tightens the OpenAPI contract (backend's JSON must include the
  `x-kubernetes-group-version-kind` extension on the top-level
  schema; the component server stamps it defensively too).
- The component server — parses the backend's OpenAPI v3 JSON at
  startup and composes it into the `GetOpenAPIDefinitions` map,
  so `kubectl explain` works end-to-end including per-field docs.
- The component server (optional, via `--use-typed-wrapper`) —
  registers a typed Go wrapper `dyn.Object` under the target GVK
  instead of `*unstructured.Unstructured`. This lets
  `Scheme.New(gvk)` produce a kind-stamped object (via
  `typeToGVK` reflection, which only works for typed structs),
  which in turn lets the library's SSA empty-object path succeed.
- The backend — ships a real OpenAPI schema with properties +
  descriptions, implements the new `Apply` RPC, and persists
  `metadata.managedFields` as raw JSON so SSA state survives
  across calls.

## Hypothesis

- **Wire protocol fidelity** (primary). A schema shipped over gRPC
  at component-server startup can drive both `kubectl explain`
  (per-field docs) and the library's SSA typed-converter —
  closing the two library-feature gaps 0013 drew.
- **Resource modeling freedom** (secondary). The component server
  still needs no compile-time knowledge of any resource type;
  what it does need is a *typed Go wrapper* registered under the
  GVK so the library's empty-object-GVK assumption is satisfied.

## What's in the box

- `proto/backend.proto` — the refined gRPC contract. ~250 lines.
- `gen/` — regenerated Go bindings (committed).
- `component/` — the refined component server.
  - `pkg/scheme` — builds the runtime.Scheme with either
    `*unstructured.Unstructured` or `*dyn.Object` depending on
    the `--use-typed-wrapper` flag.
  - `pkg/dyn` — the typed Go wrapper. Implements `runtime.Object`
    but deliberately **not** `runtime.Unstructured`, so the
    Scheme treats it on the typed branch where `Scheme.New(gvk)`
    attributes the Kind via `typeToGVK[reflect.Type]`.
  - `pkg/server` — threads the backend's OpenAPI into the defs
    map; composes with the generated meta/v1 + runtime defs.
  - `pkg/grpcbackend` — the REST storage adapter that proxies to
    the backend, now handling both object types.
- `backend-note/` — reference Go backend serving Notes. Persists
  managedFields. In-memory. No `k8s.io/apiserver` import.
- `manifests/` — component Deployment + Service, backend
  Deployment + Service. Component runs with
  `--use-typed-wrapper=true`.

## How to run

```
./hack/gen-certs.sh
kind create cluster --name aggexp-krmp
kubectl config use-context kind-aggexp-krmp
kubectl create namespace aggexp-system

./hack/deploy.sh deploy/manifests

docker build -t aggexp-krm-component:dev \
  -f experiments/0017-krm-protocol-refinement/component/Dockerfile .
docker build -t aggexp-note-backend:dev \
  -f experiments/0017-krm-protocol-refinement/backend-note/Dockerfile .
kind load docker-image aggexp-krm-component:dev --name aggexp-krmp
kind load docker-image aggexp-note-backend:dev --name aggexp-krmp

AGGEXP_IMAGE=aggexp-krm-component:dev \
  NOTE_BACKEND_IMAGE=aggexp-note-backend:dev \
  ./hack/deploy.sh experiments/0017-krm-protocol-refinement/manifests

kubectl -n aggexp-system rollout status deploy/aggexp
kubectl -n aggexp-system rollout status deploy/note-backend

# wire / read / write
kubectl apply -f experiments/0017-krm-protocol-refinement/sample-note.yaml
kubectl get notes
kubectl explain note.spec                       # <- rich per-field docs
kubectl apply --server-side --field-manager=alice \
  -f experiments/0017-krm-protocol-refinement/sample-note.yaml
kubectl get note hello -o yaml --show-managed-fields
```

Expect `kubectl explain note.spec` to print `title` and `body`
with their descriptions. Expect SSA to succeed and the subsequent
GET to show a populated `metadata.managedFields` block.

## Status

complete

<!-- See FINDINGS/0017-krm-protocol-refinement.md for results. -->

## Decisions made

- **Forked 0013 via `cp -r`** rather than abstracting shared code.
  Experiments stay self-contained per repo ethos; the gen/ proto
  bindings live under `experiments/0017-krm-protocol-refinement/gen/`.
- **Kept 0013's JSON-bytes-on-the-wire** shape. The refined proto
  is wire-compatible for Get/List/Delete/Watch; Create/Update gain
  a `field_manager` field; a new Apply RPC is added.
- **Typed wrapper gated behind a flag.** `--use-typed-wrapper=true`
  in the manifest override. The unstructured path remains
  reachable for comparison against 0013's findings.
- **Typed wrapper registers under both external AND internal
  versions** in the scheme. The library's SSA machinery passes
  objects through `toUnversioned()` as part of the managedFields
  merge flow; without an internal-version registration we hit
  "no kind Note is registered for the internal version of group
  aggexp.io". Registering the same Go type under both versions
  satisfies `typeToGVK` without forcing a real conversion func.
- **Backend persists managedFields as `json.RawMessage`** rather
  than modeling the full schema. Keeps the backend free of
  `k8s.io/apimachinery` dependency while still round-tripping the
  field. Cost: the backend can't inspect or enforce ownership
  itself, which is fine for this experiment.
- **Apply RPC exists but is not called by the component today.**
  The library's default SSA path routes through our Update. The
  Apply RPC is present for future experiments where the backend
  wants to run its own field-manager logic (e.g. persisting a
  diff of intent vs observed state).
- **Kind cluster: `aggexp-krmp`.** Distinct from every other
  experiment's cluster name.

## Prerequisites

- A kind cluster named `aggexp-krmp`.
- `hack/gen-certs.sh` to produce serving cert.
- `hack/deploy.sh deploy/manifests` for base resources.
- `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` only if
  regenerating `gen/`.

## What we're looking to learn

- Can the backend-provided OpenAPI drive the library's typed-
  converter? **Yes, when a typed Go wrapper is registered.**
- Can we get `kubectl explain` to show per-field docs without
  code-generated schemas per resource? **Yes, threading the
  backend's JSON into the defs map under the Go canonical name
  is enough.**
- What does the refined protocol need to carry to support SSA?
  Answer: nothing new on the wire; the object's own
  `metadata.managedFields` is the state. The field-manager name
  is convenience (for observability on the backend side).
- Where does the typed-wrapper approach still fall short? Answer
  is in FINDINGS: structured-merge-diff treats the `Content`
  bag as atomic, so list-keyed merges inside `spec.*` degrade to
  replace-whole-list semantics.
