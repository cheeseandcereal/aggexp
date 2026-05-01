# Experiment 0023: schema-source-exploration

First empirical experiment in the stateful-middleware-refinement arc
(see `FINDINGS/0022-stateful-middleware-thesis.md`). Probes where the
OpenAPI schema that drives kubectl explain + SSA should come from
when the middleware is a generic component server and the backend is
polyglot.

Three tracks stand up the same `notes.aggexp.io/v1 Note` resource
(matching 0013 / 0017 / 0019 / 0021 for side-by-side comparison) and
differ only in how the middleware obtains the OpenAPI:

- **Track A — backend-ships-openapi (baseline).** Backend's
  `GetSchema` RPC returns full Kubernetes-flavored OpenAPI v3 JSON,
  with `x-kubernetes-group-version-kind` extension and ObjectMeta
  wrapping. Same shape as 0017/0021. Rebuilt here so the comparison
  is apples-to-apples.
- **Track B — middleware-synthesizes.** Backend's `GetSchema`
  returns a plain JSON Schema describing only spec+status: no GVK
  extension, no ObjectMeta, no apiVersion/kind properties, no List
  wrapper. The middleware's `synthesis` package lifts this into
  full Kubernetes OpenAPI at startup.
- **Track C — config-resident OpenAPI.** Full OpenAPI lives in an
  `APIDefinition` CRD on the host cluster (`aggexpapidefinition.aggexp.io/v1`).
  Middleware reads it via the dynamic client at startup and never
  calls `backend.GetSchema`. Backend serves CRUD+watch only.

## Hypothesis

- **Wire protocol fidelity** (primary). All three tracks should
  produce identical kubectl behavior — `api-resources`, `get`,
  `apply`, `explain`, `apply --server-side`, `get -w`. If any track
  differs, that's a finding.
- **Resource modeling freedom** (secondary). The authoring burden
  (lines of code, number of Kubernetes concepts a backend author
  must know) should differ sharply across tracks. The recommendation
  to the rest of the arc is the track with the lowest backend burden
  that still delivers the full wire behavior.

## How to run

One-time:
```
./hack/gen-certs.sh
kind create cluster --name aggexp-schema-src
kubectl config use-context kind-aggexp-schema-src
kubectl create namespace aggexp-system
```

Each track tears down the previous track's notes / APIService first.
Per-track run:
```
# Build image(s)
docker build -t aggexp-note-aa-0023a:dev \
  -f experiments/0023-schema-source-exploration/track-a-backend-openapi/component/Dockerfile .
docker build -t aggexp-note-backend-0023a:dev \
  -f experiments/0023-schema-source-exploration/track-a-backend-openapi/backend-note/Dockerfile .
kind load docker-image aggexp-note-aa-0023a:dev --name aggexp-schema-src
kind load docker-image aggexp-note-backend-0023a:dev --name aggexp-schema-src

AGGEXP_IMAGE=aggexp-note-aa-0023a:dev \
  NOTE_BACKEND_IMAGE=aggexp-note-backend-0023a:dev \
  ./hack/deploy.sh deploy/manifests
AGGEXP_IMAGE=aggexp-note-aa-0023a:dev \
  NOTE_BACKEND_IMAGE=aggexp-note-backend-0023a:dev \
  ./hack/deploy.sh experiments/0023-schema-source-exploration/track-a-backend-openapi/manifests

kubectl -n aggexp-system rollout status deploy/aggexp
kubectl -n aggexp-system rollout status deploy/note-backend

# Scenarios
kubectl api-resources | grep notes
kubectl apply -f experiments/0023-schema-source-exploration/sample-note.yaml
kubectl get notes
kubectl get note hello -o yaml
kubectl explain note.spec
kubectl apply --server-side --field-manager=alice \
  -f experiments/0023-schema-source-exploration/sample-note.yaml
kubectl get notes -w  # ctrl-c after a modification

# Teardown before next track
kubectl delete apiservice v1.aggexp.io --ignore-not-found
kubectl -n aggexp-system delete deploy aggexp note-backend --ignore-not-found
```

Track B and Track C repeat the sequence with their own image names
and manifest directories.

## Status

complete

## Decisions made

- One kind cluster for all three tracks (`aggexp-schema-src`).
  Tracks deploy sequentially; each teardown removes the previous
  track's APIService + Deployments before the next applies. Less
  cluster churn; keeps the comparison under one environment.
- Track A's note-backend is a near-verbatim copy of 0021's (same
  GetSchema body, same Go code). Track B's backend ships a
  spec+status-only JSON Schema; Track C's GetSchema is a stub.
- Track B's synthesis package lives at
  `track-b-middleware-synthesis/synthesis/` (not promoted). Single
  public function `LiftJSONSchemaToOpenAPI(gvk, jsonSchema []byte)`.
- Track C's `APIDefinition` CRD is installed per-track (not
  cluster-wide). The CRD schema mirrors the `thesis.APIDefinition`
  Go type but is intentionally small — group, version, resource,
  kind, singular, namespaced, openapiV3 (string, full Kubernetes
  OpenAPI v3 JSON). Backend address is known to the component
  via flag.
- Component images include a `-0023a` / `-0023b` / `-0023c` suffix
  so rollouts don't collide across tracks.
- Note resource shape across all three tracks:
  - spec.title: string, required, 3-64 chars (validated by JSON
    Schema minLength/maxLength where applicable)
  - spec.body: string, optional
  - status.updatedAt: string, format date-time

## Prerequisites

- kind cluster `aggexp-schema-src`.
- `hack/gen-certs.sh`, `hack/deploy.sh deploy/manifests` for base
  resources.

## What we're looking to learn

- Does Track B's middleware-synthesis path produce kubectl behavior
  indistinguishable from Track A? If so, the backend author can
  skip learning Kubernetes' OpenAPI conventions.
- Does Track C's config-resident OpenAPI path work, and what is the
  operator ergonomics cost? If so, the backend stops caring about
  schema entirely.
- Which path has the lowest backend-author concept count across
  Go / Python / Rust / Node?
