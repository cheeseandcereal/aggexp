# Experiment 0013: krm-component-skeleton

A deployable, resource-agnostic **component server** that speaks the
Kubernetes wire protocol, paired with a thin **backend service** that
speaks a simpler RPC. The component server does not know about any
specific resource type at compile time: at startup it asks its backend
over gRPC for the schema (GVR, OpenAPI, table columns) and registers
the resource dynamically.

This is the first experiment in a new arc. Previously the substrate
(`runtime/`) was a Go library experiments linked against. Here the
substrate becomes a component you deploy, and your "backend" becomes a
non-Kubernetes-aware service that answers CRUD over gRPC.

If this works, a non-Go backend (python, rust, node) could expose a
Kubernetes API without ever importing `k8s.io/apiserver`.

## Hypothesis

- **Wire protocol fidelity** (primary). The component server can fully
  honor the Kubernetes wire contract (list/get/create/update/delete/
  watch/table rendering) while delegating all CRUD to a backend that
  only knows about JSON payloads and caller identity.
- **Resource modeling freedom** (secondary). A generic, unstructured
  registration path should suffice for the lab-level cases. Where it
  falls short of typed registration (SSA, explain, deep validation)
  is itself a finding.

## What's in the box

- `proto/backend.proto` — the gRPC contract.
- `gen/` — generated Go bindings (committed; consumers do not need
  `protoc`).
- `component/` — the generic component server (its own Go module,
  pins `k8s.io/apiserver`, imports the substrate's `runtime/server`,
  `runtime/group`).
- `backend-note/` — a reference Go backend serving a `Note` resource
  (`notes.aggexp.io/v1`). In-memory. No `k8s.io/apiserver` import.
- `manifests/` — component Deployment + Service, backend Deployment
  + Service, APIService for `notes.aggexp.io/v1`.

## How to run

From the repo root:

```
./hack/gen-certs.sh
kind create cluster --name aggexp-krm
kubectl config use-context kind-aggexp-krm
kubectl create namespace aggexp-system

./hack/deploy.sh deploy/manifests

docker build -t aggexp-krm-component:dev \
  -f experiments/0013-krm-component-skeleton/component/Dockerfile .
docker build -t aggexp-note-backend:dev \
  -f experiments/0013-krm-component-skeleton/backend-note/Dockerfile .
kind load docker-image aggexp-krm-component:dev --name aggexp-krm
kind load docker-image aggexp-note-backend:dev --name aggexp-krm

AGGEXP_IMAGE=aggexp-krm-component:dev \
  NOTE_BACKEND_IMAGE=aggexp-note-backend:dev \
  ./hack/deploy.sh experiments/0013-krm-component-skeleton/manifests

kubectl -n aggexp-system rollout status deploy/aggexp
kubectl -n aggexp-system rollout status deploy/note-backend

kubectl get --raw /apis/notes.aggexp.io/v1 | jq .
kubectl apply -f experiments/0013-krm-component-skeleton/sample-note.yaml
kubectl get notes
kubectl get notes hello -o yaml
kubectl get notes -w &
kubectl delete note hello
```

## Status

complete

<!-- See FINDINGS/0013-krm-component-skeleton.md for results. -->

## Decisions made

- **gRPC, not HTTP+JSON.** Wanted bidirectional streaming for Watch
  without inventing our own framing. `protoc`-generated clients and
  servers in both processes.
- **Objects on the wire are JSON bytes inside protobuf `bytes` fields.**
  Avoids generating proto messages for every user resource type.
  Component server serializes typed objects to JSON at the boundary.
- **Component server uses `*unstructured.Unstructured`.** A schema is
  not known at compile time; building typed Go structs from a schema
  at startup is out of scope for this skeleton. The substrate's
  `Scheme.AddKnownTypeWithName` accepts unstructured. Tradeoff:
  Server-Side Apply's smart-merge is not exercised (it relies on
  typed schemas + managedFields), OpenAPI-driven explain is limited
  to what we can wire through the schema the backend supplies.
- **Backend ships schema at `GetSchema`.** One call at component-
  server startup. The backend pushes (GroupVersionResource, Kind,
  SingularName, NamespaceScoped, Writable, TableColumns, OpenAPI JSON).
  No re-fetch; if the backend's schema changes you restart the
  component.
- **gRPC `Watch` is a server-streaming RPC.** Component opens one
  stream at startup; the backend pushes events. Component fans them
  into the same `watch.Broadcaster` pattern the substrate uses.
- **Backend address hardcoded into component via flag.** For an
  in-cluster deployment: `--backend-addr=note-backend.aggexp-system.svc:9090`.
  No service discovery, no mTLS between component and backend
  (skeleton; out of scope).
- **Namespace-scoped Note resource.** Kubernetes' default machinery
  assumes cluster-scoped for aggregated APIs unless told otherwise;
  exercising namespaced mode is a mild stretch of the skeleton.
- **Resource name: `notes.aggexp.io`.** Distinct from existing
  experiments' `aggexp.io` types (repos, files, etc.) to avoid
  APIService collisions if any get deployed in the same cluster.
- **Generated proto Go committed under `gen/`.** Consumers of the
  repo don't need `protoc` to build. Re-generate with the command
  at the top of `proto/backend.proto`.
- **Kind cluster: `aggexp-krm`.** Fresh cluster distinct from
  `aggexp` / `aggexp-runtime` / other experiments.

## Prerequisites

- A kind cluster named `aggexp-krm`.
- `hack/gen-certs.sh` to produce serving cert.
- `hack/deploy.sh deploy/manifests` for base resources.
- `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` only if
  regenerating `gen/`.

## What we're looking to learn

- Can a generic component server register any resource type based
  on data fetched at startup, rather than compile-time types?
- What protocol does the thin-backend path actually want? Take
  a first pass; see where the seams hurt.
- What breaks when the component server uses unstructured types
  throughout (SSA? explain? watch? discovery?). Each of those is
  a FINDING.
- Compare cost: is the `note-backend` shorter than 0007's fs
  backend once you factor in the amortized component server?
