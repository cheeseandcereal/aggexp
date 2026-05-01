# Experiment 0026: http-json-backend-transport

All prior KRM experiments use gRPC for component↔backend
communication. This one tests an alternative: **HTTP/JSON for CRUD +
SSE (Server-Sent Events) for watch**. Same protocol semantics as the
gRPC version (GetSchema / Get / List / Create / Update / Delete /
Watch), only the wire encoding changes.

The point is to reduce tooling cost for polyglot backends. gRPC is
universal but requires `protoc` + language-specific codegen. HTTP/JSON
is reachable from `curl`, any stdlib HTTP server, and almost every
language's default library set.

The component server grows a `--backend-transport=grpc|http` flag.
gRPC stays the default; HTTP is an opt-in alternate. Both map to the
same `componentpb.BackendClient` interface so the rest of the
component (scheme, REST storage, watch broadcaster, OpenAPI synthesis)
is reused verbatim.

## Hypothesis

- **Wire protocol fidelity** (primary). The backend↔middleware
  transport is interchangeable. kubectl behavior against the AA
  (CRUD, watch, explain, SSA) is identical whether the component
  reaches its backend over gRPC or HTTP/JSON+SSE.
- **Resource modeling freedom** (secondary). A stdlib-only HTTP
  backend is a smaller authoring surface than a stdlib-plus-grpc
  backend — fewer imports, fewer generated files, debuggable with
  `curl`. Quantify by LOC against the 0013/0017/0019 Go gRPC
  reference.

## HTTP/JSON mapping

| Method | Path                              | Semantics                                  |
|--------|-----------------------------------|--------------------------------------------|
| GET    | `/schema`                         | GetSchemaResponse (plain JSON Schema + metadata) |
| GET    | `/objects/{namespace}/{name}`     | Get                                        |
| GET    | `/objects/{namespace}`            | List                                       |
| POST   | `/objects/{namespace}`            | Create (body = CreateRequest)              |
| PUT    | `/objects/{namespace}/{name}`     | Update (body = UpdateRequest)              |
| DELETE | `/objects/{namespace}/{name}`     | Delete                                     |
| GET    | `/watch/{namespace}`              | SSE stream of WatchEvents                  |

Caller identity (proto `UserInfo`) travels in HTTP headers:

- `X-Aggexp-User-Name`
- `X-Aggexp-User-Groups` (comma-separated)
- `X-Aggexp-User-Uid`
- `X-Aggexp-User-Extra-<key>` (repeatable; values comma-separated)

Errors follow standard HTTP semantics: 404 not found, 409 conflict,
400 bad request, 403 forbidden, 5xx internal. Errors carry a JSON
`{"message": "..."}` body that the component surfaces into the
Kubernetes Status message.

## Schema source

Per 0023 Track B. The backend ships a plain JSON Schema (just
spec+status) from `GET /schema`; the component lifts it into full
Kubernetes OpenAPI v3 at startup via a copy-forked synthesis
function. 0023's `synthesis` package has not been promoted to
runtime yet (that's queued for 0030), so this experiment copies
the 0023 function into its own `component/synthesis.go`.

## How to run

```
./hack/gen-certs.sh

kind create cluster --name aggexp-http-tx
kubectl config use-context kind-aggexp-http-tx
kubectl create namespace aggexp-system

./hack/deploy.sh deploy/manifests

docker build -t aggexp-krm-component-http:dev \
  -f experiments/0026-http-json-backend-transport/component/Dockerfile .
docker build -t aggexp-note-backend-http:dev \
  -f experiments/0026-http-json-backend-transport/backend-note/Dockerfile .
kind load docker-image aggexp-krm-component-http:dev --name aggexp-http-tx
kind load docker-image aggexp-note-backend-http:dev --name aggexp-http-tx

AGGEXP_IMAGE=aggexp-krm-component-http:dev \
  NOTE_BACKEND_HTTP_IMAGE=aggexp-note-backend-http:dev \
  ./hack/deploy.sh experiments/0026-http-json-backend-transport/manifests

kubectl -n aggexp-system rollout status deploy/aggexp --timeout=120s
kubectl -n aggexp-system rollout status deploy/note-backend --timeout=120s

kubectl apply -f experiments/0026-http-json-backend-transport/sample-note.yaml
kubectl get notes
kubectl explain note.spec
kubectl apply --server-side --field-manager=alice \
  -f experiments/0026-http-json-backend-transport/sample-note.yaml
kubectl get note hello -o yaml --show-managed-fields

# Debuggability probe — curl the backend directly.
kubectl -n aggexp-system port-forward svc/note-backend 8080:8080 &
curl -s http://localhost:8080/schema | jq .
curl -s http://localhost:8080/objects/default | jq .
curl -s http://localhost:8080/objects/default/hello | jq .

# Watch SSE wire bytes (raw, no jq).
curl -N -s http://localhost:8080/watch/default
```

## Status

complete

<!-- See FINDINGS/0026-http-json-backend-transport.md for results. -->

## Decisions made

- **SSE is the real spec**, not hand-rolled NDJSON. Each event is
  `data: {json}\n\n`. Parser splits on `\n\n`. Per the task hard-rule.
- **Backend port 8080** (HTTP norm) to distinguish from 9090 (gRPC
  norm in sibling experiments).
- **Backend is stdlib-only.** No imports beyond `net/http`,
  `encoding/json`, `sync`, `time`, `crypto/rand`. UUIDs
  hand-minted with `crypto/rand` rather than pulling
  `github.com/google/uuid`, because the point is "smallest possible
  backend dependency footprint". This widens the LOC gap against the
  0017 Go gRPC backend (which imports grpc + uuid) in a way that
  reflects the actual stack cost, not stylistic preference.
- **Component HTTP client uses `net/http` defaults** with a
  custom `*http.Client` carrying a 30s timeout. No retries in-band;
  Kubernetes-level retries (the reflector) are the primary recovery
  path, same as 0017's gRPC shape.
- **Identity header names** use the `X-Aggexp-User-*` prefix to
  avoid any confusion with `X-Remote-*` (aggregation layer into the
  middleware) or `X-Forwarded-*` conventions.
- **Schema source**: Track B from 0023. Backend ships plain JSON
  Schema; component lifts. Copy-forks the 0023 synthesis function
  into `component/synthesis.go` because 0023's `synthesis` package
  lives in an experiment directory and is not yet promoted.
- **SSE reconnect interval**: 2s, matching 0017's grpc reconnect
  backoff. Arbitrary; either is fine at lab scale.
- **Component supports BOTH transports** via `--backend-transport`
  flag. Default: `grpc`, to preserve backwards compatibility with
  0021's `cmd/note-aa`. The experiment runs with `http`.
- **Kind cluster `aggexp-http-tx`**. Distinct from every other
  experiment.

## Prerequisites

- A kind cluster named `aggexp-http-tx`.
- `hack/gen-certs.sh` to produce the serving cert.
- `hack/deploy.sh deploy/manifests` for base resources.

## What we're looking to learn

- Does swapping gRPC→HTTP/JSON at the component↔backend seam
  preserve kubectl-level wire fidelity? Same 6 kubectl scenarios
  as 0019/0021 are expected to pass.
- How much does the backend-author LOC budget change when the
  backend is stdlib-only HTTP vs stdlib-plus-grpc?
- How much faster or slower is `kubectl get` against an HTTP
  backend compared to the gRPC baseline at lab scale?
- Can a human `curl` the backend for live debugging? Capture the
  actual bytes.
- What does the SSE wire look like? Commit the raw bytes.

This experiment feeds into 0030's substrate promotion: the
`thesis.BackendRef.Transport` enum shipped in 0022 will carry both
values if the result is "HTTP is materially cheaper"; if the result
is "gRPC is materially better", `http` becomes a documented but
not-default option.
