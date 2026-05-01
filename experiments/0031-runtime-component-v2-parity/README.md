# Experiment 0031: runtime-component-v2-parity

First post-promotion consumer of `runtime/component/v2/`. Wires the v2
substrate's multiplex, MetadataStore, GC, declarative admission, and
dual-transport commitments together in a single ~200-line binary and
exercises the 0024-stitched-Get + 0025-unified-RV + 0029-admission +
0027-dynamic-install sweep in one run.

Two APIs on one multiplex middleware, two different transports:

- `widgets.aggexp.io/v1` — backed by an HTTP/JSON + SSE backend
  (same shape 0026/0027 established).
- `gadgets.aggexp.io/v1` — backed by a gRPC backend (0013/0017/0018
  shape, ported to v2's proto).

Both run in Go. Polyglot was nice-to-have; the transport swap (HTTP vs
gRPC) is what this experiment actually tests, and two Go backends are
enough to prove it.

## Hypothesis

The v2 substrate holds under a first external consumer with zero
substrate patches. A ~200-line consumer can demonstrate: multiplex
via `APIDefinition` CRDs, metadata CRD-backed storage, GC loop,
declarative admission (CEL + JSONPath), graceful SIGTERM, and
both transports in one process.

Fundamentals this primarily probes: **resource modeling freedom**
(multi-API via config, transport by descriptor) and **storage
independence** (fifth axis — metadata on host CRD + business data
on backend).

## How to run

```
# (1) certs, kind cluster, base manifests
./hack/gen-certs.sh
kind create cluster --name aggexp-0031
kubectl config use-context kind-aggexp-0031
kubectl create namespace aggexp-system

# (2) build images
docker build -t aggexp-v2-parity-aa:dev \
  -f experiments/0031-runtime-component-v2-parity/cmd/v2-parity-aa/Dockerfile .
docker build -t aggexp-0031-widget-http:dev \
  -f experiments/0031-runtime-component-v2-parity/backend-widget-http/Dockerfile .
docker build -t aggexp-0031-gadget-grpc:dev \
  -f experiments/0031-runtime-component-v2-parity/backend-gadget-grpc/Dockerfile .
kind load docker-image aggexp-v2-parity-aa:dev --name aggexp-0031
kind load docker-image aggexp-0031-widget-http:dev --name aggexp-0031
kind load docker-image aggexp-0031-gadget-grpc:dev --name aggexp-0031

# (3) CRDs (APIDefinition + ResourceMetadata) — embedded in v2 substrate.
# We extract them to files so kubectl apply is straightforward. An
# operator would either import-and-apply programmatically, or ship
# them as static YAML. We ship them static here.
kubectl apply -f experiments/0031-runtime-component-v2-parity/manifests/00-crds.yaml

# (4) base apiserver manifests (namespace, sa, rbac, service, tls secret,
# APIService record — note: APIService is per-group; in multiplex mode
# it's created dynamically. We skip deploy/manifests/50-apiservice.yaml
# in favor of the experiment-specific deployment override.)
./hack/gen-certs.sh  # (already done above; idempotent)
AGGEXP_IMAGE=aggexp-v2-parity-aa:dev \
  ./hack/deploy.sh experiments/0031-runtime-component-v2-parity/manifests

kubectl -n aggexp-system rollout status deploy/aggexp --timeout=120s
kubectl -n aggexp-system rollout status deploy/widget-backend --timeout=60s
kubectl -n aggexp-system rollout status deploy/gadget-backend --timeout=60s

# (5) declare the two APIs. The multiplex reconciler watches these and
# dynamically installs the widgets + gadgets groups.
kubectl apply -f experiments/0031-runtime-component-v2-parity/samples/apidef-widgets.yaml
kubectl apply -f experiments/0031-runtime-component-v2-parity/samples/apidef-gadgets.yaml

# wait for the reconciler to create the APIServices and for kube-apiserver
# to mark them Available
kubectl wait --for=condition=Available apiservice/v1.widgets.aggexp.io --timeout=120s
kubectl wait --for=condition=Available apiservice/v1.gadgets.aggexp.io --timeout=120s

# (6) the parity probe
experiments/0031-runtime-component-v2-parity/hack/run-scenarios.sh
```

Teardown:

```
kind delete cluster --name aggexp-0031
```

## Status

complete

<!-- See FINDINGS/0031-runtime-component-v2-parity.md. -->

## Line count

- `cmd/v2-parity-aa/main.go` — 258 lines (201 semantic).
- `cmd/v2-parity-aa/yaml.go` — 19 lines (13 semantic).
- `backend-widget-http/main.go` — 452 lines (stdlib-only HTTP/SSE).
- `backend-gadget-grpc/main.go` — 279 lines (gRPC, UnimplementedBackendServer-embedded).
- Total handwritten Go: 1,008 lines; consumer (middleware wiring
  excluding backends): 277. Backends are both frozen-fork-ish
  (widget is a 0027 fork; gadget is a clean-room single-kind gRPC
  backend built on the v2 proto).
- Comparison:
  - 0021 (v1 single-AA, library-mode parity consumer): `main.go` 38 LOC.
  - 0027 (v1 multiplex experiment, now substrate as v2/multiplex): `main.go` + `synthesis.go` + `http_client.go` ≈ 1,300 LOC (~800 of it reconciler).
  - 0031 (v2 multiplex, post-promotion consumer): 277 LOC consumer.

The jump from 0021's 38 LOC → 0031's 277 LOC is the multiplex-
vs-single-AA cost, not the v2-vs-v1 cost. The drop from 0027's
800+ LOC reconciler → ~100 LOC of `multiplex.New(...)` + `AttachServer`
+ post-start hooks is the substrate payoff.
## Decisions made

- **Dynamic install via multiplex + APIDefinition CRDs.** Not static
  install. The multiplex package is the only multi-AA path in v2; the
  task authorizes either path. Dynamic confirms the 0030 known-gap
  boundary (SSA + `kubectl explain` degrade on dynamically-installed
  groups) behaves as expected and bounded, not as surprise breakage.
- **Two Go backends, different transports.** Widget is HTTP/SSE
  (reinforces 0026); Gadget is gRPC (reinforces the original 0013
  shape). Polyglot backends were nice-to-have; focusing on the
  transport swap gets the substrate more signal with less toolchain
  drag.
- **Backend code is a minimal fork of 0027's backend-http.** 0027 is
  frozen; copying was authorized. Both backends are ~200 LOC each
  (trimmed from 0027's 583 because per-kind envcrap is no longer
  needed — each backend is hard-coded for one resource here).
- **Declarative admission on widgets only.** Validation forbids
  `color=black`; mutation defaults `spec.title` to `"Untitled"` on
  CREATE when absent. Both rules are trivially CEL- and JSONPath-
  expressible and exercise the 0029 composition path. First pass
  used `.spec.title` (leading dot, looked natural alongside the
  `fieldPath` convention kubectl prints) and silently no-op'd: the
  substrate's `splitPath` treats `.spec.title` as
  `["", "spec", "title"]` and bails out writing to the empty-string
  top-level key. Documented as a consumer rough-edge in FINDINGS.
- **Single GC Reconciler wired against widgets only.** GC Config is
  per-(group, resource); we wire one instance to demonstrate the
  substrate primitive rather than multiplexing GC per-AA. Sweep
  interval 30s, grace 10s — both arbitrary to see activity in a
  short demo. An operator deploying many APIs would spawn one GC
  per APIDefinition in `multiplex.buildInstall`; v2 alpha does not
  do this automatically.
- **Widget supports push watch; gadget supports only poll.** Confirms
  ModePoll and ModePush both work from one multiplex binary.
- **Kind cluster `aggexp-0031`.** Isolated from every other
  experiment's cluster.

## Prerequisites

- kind cluster `aggexp-0031`.
- `hack/gen-certs.sh`, `hack/deploy.sh deploy/manifests` for the base
  resources (namespace, SA, RBAC, Service, serving-cert secret).

## What we're looking to learn

Primary: **resource modeling freedom** and **storage independence**
under v2 — can one multiplex process host two APIs with two
different transports against the v2 substrate, and does the fifth
storage axis (metadata CRD + backend business data) hold up in that
shape?

Secondary: consumer-facing rough edges. What did the v2 package
surface that was confusing, needed spelunking, or tempted a patch?
These feed back into a hypothetical v2.1.

Tertiary: compare LOC to 0021 (v1 parity, ~38 LOC main.go) and to
0027 (multiplex experiment, ~800 LOC main.go). The substrate should
be landing *between* those: 0021 is a single-AA library consumer
(tiniest possible); 0027 was the experiment that invented the
multiplex shape. 0031 is the first consumer of the substrate-form
multiplex.
