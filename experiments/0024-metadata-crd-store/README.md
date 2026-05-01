# Experiment 0024: metadata-crd-store

Load-bearing experiment of the stateful-middleware-refinement arc.
Separates KRM metadata from business data: business data
(spec+status) lives on an external backend; KRM metadata
(uid, resourceVersion, creationTimestamp, labels, annotations,
managedFields, finalizers, ownerReferences, deletionTimestamp)
lives in a **shared** host-cluster CRD
`resourcemetadatas.aggexpmeta.aggexp.io/v1`. The middleware
stitches them together on every Get.

Rebuilds the 0018 S3 Bucket AA, but this time the backing metadata
store is a cluster-scoped CRD under a **separate APIGroup** from
the exposed resource. The goal is to recover 0010's library
features (SSA, finalizers, ownerReferences, labels/annotations)
while avoiding 0015's double-tracking issue where ArgoCD saw
both the exposed Widget and the backing WidgetStorage.

## Hypothesis

1. **Stitching works.** The middleware can combine backend-provided
   spec/status with MetadataStore-provided KRM metadata on every
   request path (Get/List/Create/Update/SSA/Delete/Watch) and
   produce user-facing behavior indistinguishable from 0010.
2. **ArgoCD does not double-track.** Unlike 0015 where the backing
   `widgetstorages.aggexpstorage.aggexp.io` CRD was visible as a
   per-Widget shadow resource, the cluster-scoped
   `resourcemetadatas.aggexpmeta.aggexp.io` CRD is a single shared
   store. ArgoCD may discover it, but it shouldn't pattern-match
   tracking annotations to individual Bucket objects because
   each ResourceMetadata's name is a composite (group.resource.namespace.name)
   and the body carries no exposed-resource annotations.
3. **Performance.** The stitch adds one extra kube-apiserver hop
   per request. At lab scale this should be comparable to 0010's
   facade overhead (~65ms aggregation-layer floor dominates).

Fundamentals probed:
- **Storage independence** (primary) — a new storage-axis variant:
  metadata-only in host etcd + business data on the backend.
- **Wire protocol fidelity** (secondary) — all kubectl/SSA/watch
  must continue to work.
- **Resource modeling freedom** (tertiary) — confirms you can
  expose a backend resource without any mirror CRD for the
  business data.

## How to run

From the repo root:

```sh
# 1. Create dedicated kind cluster.
kind create cluster --name aggexp-meta-crd
kubectl config use-context kind-aggexp-meta-crd

# 2. Generate serving cert for the AA.
hack/gen-certs.sh

# 3. Build the three images (from repo root).
docker build -f experiments/0024-metadata-crd-store/s3-mock/Dockerfile \
    -t aggexp-0024-s3-mock:dev .
docker build -f experiments/0024-metadata-crd-store/backend-s3/Dockerfile \
    -t aggexp-0024-backend-s3:dev .
docker build -f experiments/0024-metadata-crd-store/component/Dockerfile \
    -t aggexp-0024-component:dev .

# 4. Load images into kind.
kind load docker-image --name aggexp-meta-crd \
    aggexp-0024-s3-mock:dev aggexp-0024-backend-s3:dev aggexp-0024-component:dev

# 5. Install the ResourceMetadata CRD.
kubectl apply -f experiments/0024-metadata-crd-store/metadata-crd/resourcemetadata-crd.yaml

# 6. Deploy the base manifests + experiment overlay.
AGGEXP_IMAGE=aggexp-0024-component:dev \
    hack/deploy.sh deploy/manifests
AGGEXP_IMAGE=aggexp-0024-component:dev \
S3_MOCK_IMAGE=aggexp-0024-s3-mock:dev \
BACKEND_S3_IMAGE=aggexp-0024-backend-s3:dev \
    hack/deploy.sh experiments/0024-metadata-crd-store/manifests

# 7. Wait for rollout, then run the scenarios.
kubectl --context kind-aggexp-meta-crd rollout status \
    -n aggexp-system deployment/aggexp
kubectl --context kind-aggexp-meta-crd api-resources | grep buckets

# 8. Run the 6 scenarios from experiments/0024-metadata-crd-store/hack/scenarios.sh
# or reproduce by hand.
```

## Status

complete

See `FINDINGS/0024-metadata-crd-store.md`.

## Decisions made

- ResourceMetadata CRD is **cluster-scoped** even though Buckets
  in this experiment are cluster-scoped; same rule applies to
  namespaced resources (name encodes the namespace). Keeps the
  metadata store a single shared kind.
- ResourceMetadata name format: `<group-with-dashes>.<resource>.<namespace-or-cluster>.<name>`.
  Dots in the group are replaced with `-` (so `aggexp.io` becomes
  `aggexp-io`) to keep DNS-1123-subdomain label boundaries
  predictable. The literal string `cluster` stands in for an empty
  namespace on cluster-scoped resources. Hash fallback
  (`rmeta-<hex24>`) if the composed name exceeds 253 chars or has
  other DNS-1123 violations.
- Stitch-on-read populates a metadata Record lazily: the first time
  the middleware sees a backend object with no metadata, it creates
  one with a fresh UID and initial resourceVersion. This keeps
  out-of-band backend writes observable through the middleware without
  requiring a reconciler.
- Watch fan-out: two goroutines, one on the backend gRPC stream and
  one on the metadata CRD dynamic watch. Each event is re-stitched
  before republishing. Events are keyed by resourceRef; we union
  changes from both streams.
- Finalizer semantics: DELETE with non-empty finalizers sets
  `metadata.deletionTimestamp` on the Record and returns the stitched
  (still-present) object. A subsequent PATCH that clears finalizers
  triggers the real backend DELETE plus metadata tombstone.
- Synthetic resourceVersion: the middleware uses the metadata CRD's
  resourceVersion as the authoritative RV for stitched objects, but
  bumps an internal atomic counter for watch events that are
  business-data-only (no metadata change). This keeps RV monotonic
  from the client's perspective.
- ArgoCD install: upstream `argocd/stable/manifests/install.yaml` at
  pin `v3.0.12`. No custom config.
- Permissive RBAC on `aggexpmeta.aggexp.io/resourcemetadatas` for the
  component's ServiceAccount. Arbitrary choice: full CRUD.
- Log prefix `middleware:backend.*` vs `middleware:metastore.*` on
  every write to make the split traceable from kubectl.
- **Ref format**: lifted OpenAPI uses `#/definitions/...` refs for
  ObjectMeta/ListMeta/item so the aggregated `/openapi/v2` output
  passes strict OpenAPI consumers (ArgoCD). The substrate's
  `runtime/component/openapi.WrapAsList` still emits v3-style
  refs; we override locally. See FINDINGS for the rationale.

## Prerequisites

- kind cluster `aggexp-meta-crd`.
- Docker on host; `kind load docker-image` reachable.
- `kubectl`, `envsubst`, `go 1.24`.

## What we're looking to learn

Storage independence (primary), wire protocol fidelity
(secondary), resource modeling freedom (tertiary).

Specifically:

1. Can the stitched-metadata pattern match 0010's feature parity
   (SSA, finalizers, ownerRefs, labels/annotations) without
   introducing a per-resource shadow CRD?
2. Does ArgoCD's cluster cache see the shared metadata CRD as a
   double-tracking surface the way it saw 0015's per-resource
   WidgetStorage? If yes, what's the mitigation?
3. What is the per-request latency cost of the metadata stitch
   relative to 0010's single-CRD facade?
4. Does the split "metadata in host etcd, business data on
   backend" introduce any consistency oddities
   (metadata written, backend write failed; or vice versa)?
