# Experiment 0028: metadata-store-gc

Forks the 0024 metadata-CRD-backed middleware and adds a garbage
collector: a periodic reconciler that removes `ResourceMetadata`
records whose corresponding backend business-data object has
disappeared out of band (backend wiped, row deleted directly
against the backend store, etc.). 0024 flagged this as a follow-up
in its Open Questions; 0028 implements it.

## Hypothesis

The metadata-CRD store (a second state store distinct from the
backend) can grow orphans. A periodic sweep that asks the backend
"which of these still exist?" and deletes records for the absent
ones reconciles the two stores into eventual agreement. The need
for *some* such reconciliation is **fundamental** to having two
state stores — regardless of the specific backend, RPC shape, or
schedule. The *policy* (sweep interval, which records to protect,
what triggers a sweep) is **consequent**.

Fundamentals probed:
- **Storage independence** (primary) — the cost of splitting
  business data and KRM metadata across two stores: they must be
  reconciled.
- **Watch and consistency semantics** (secondary) — races between
  GC and normal writes surface here.

## What 0028 adds to 0024

- `component/gc/gc.go`: a GC reconciler with a periodic sweep and
  a configurable grace window. Uses the existing
  `runtime/component/proto.Backend.List` RPC — no new proto.
- `/gc/run` and `/gc/last` HTTP endpoints on port 8444 (exposed
  via the `aggexp-gc-debug` Service) so scenarios can trigger a
  sweep on demand.
- `--gc-interval` and `--gc-min-age` flags on the component.

Unchanged from 0024: the backend (`backend-s3`), the mock S3
(`s3-mock`), the stitched REST adapter, the metastore package,
the ResourceMetadata CRD.

## How to run

From the repo root:

```sh
# 1. Dedicated kind cluster.
kind create cluster --name aggexp-0028
kubectl config use-context kind-aggexp-0028

# 2. Serving cert for the AA.
hack/gen-certs.sh

# 3. Build the three images (context = repo root).
docker build -f experiments/0028-metadata-store-gc/s3-mock/Dockerfile \
    -t aggexp-0028-s3-mock:dev .
docker build -f experiments/0028-metadata-store-gc/backend-s3/Dockerfile \
    -t aggexp-0028-backend-s3:dev .
docker build -f experiments/0028-metadata-store-gc/component/Dockerfile \
    -t aggexp-0028-component:dev .

# 4. Load images into kind.
kind load docker-image --name aggexp-0028 \
    aggexp-0028-s3-mock:dev aggexp-0028-backend-s3:dev aggexp-0028-component:dev

# 5. Install the ResourceMetadata CRD.
kubectl apply -f experiments/0028-metadata-store-gc/metadata-crd/resourcemetadata-crd.yaml

# 6. Deploy base manifests + experiment overlay.
AGGEXP_IMAGE=aggexp-0028-component:dev hack/deploy.sh deploy/manifests
AGGEXP_IMAGE=aggexp-0028-component:dev \
S3_MOCK_IMAGE=aggexp-0028-s3-mock:dev \
BACKEND_S3_IMAGE=aggexp-0028-backend-s3:dev \
    hack/deploy.sh experiments/0028-metadata-store-gc/manifests

# 7. Wait for rollout.
kubectl --context kind-aggexp-0028 rollout status \
    -n aggexp-system deployment/aggexp --timeout=120s
kubectl --context kind-aggexp-0028 rollout status \
    -n aggexp-system deployment/backend-s3 --timeout=120s

# 8. Run the scenarios (port-forwards internally to the gc-debug svc).
cd experiments/0028-metadata-store-gc && hack/scenarios.sh
```

## Status

complete

See `FINDINGS/0028-metadata-store-gc.md`.

## Decisions made

- **Use existing `Backend.List` RPC rather than adding an
  `Exists(ids)` RPC.** No proto change, zero backend-author
  burden, works against every backend 0013/0017/0021 ever shipped.
  Cost: List returns full objects where Exists would return bools.
  At lab scale the difference is invisible; at scale where it
  matters (tens of thousands of records) an `Exists` RPC would be
  worth the proto churn — but that's a v2-substrate concern, not an
  experiment concern.
- **Sweep cadence: default 5 minutes in code; 2 minutes in the
  demo Deployment manifest (`--gc-interval=2m`).** Chose 5 minutes
  arbitrarily as the "feels safe" operational default; chose 2
  minutes for the demo so scenarios don't need to block for five
  minutes waiting on a periodic trigger. Both are arbitrary.
- **Grace window: 30s default in code; 20s in the demo
  (`--gc-min-age=20s`).** Bound below by backend-s3's 15s poll
  interval (a brand-new bucket isn't visible to the backend's
  self-List until the next poll, so a same-sweep race would
  mis-collect). 30s doubles that with slack; 20s is tight but
  fine for the demo because scenarios wait an explicit `sleep 25`
  between create and GC.
- **Skip on finalizers.** A Record with any finalizer is never
  GC'd, regardless of orphan status. Matches Kubernetes' own CRD
  semantics: whatever controller registered the finalizer claims
  ownership of the delete path.
- **Skip on ownerReferences.** Conservative. Full cascade-aware
  cross-resource GC is its own (large) problem; 0028 avoids it.
- **Skip on `deletionTimestamp` set.** An in-progress delete is
  already going through the normal Update path; don't race.
- **Skip if Record age < minAge.** Records younger than the grace
  window are protected even without finalizers. Prevents racing a
  brand-new Create whose metastore.Put landed but whose
  backend.Create hasn't been observed yet.
- **`/gc/run` debug endpoint is unauthenticated, bound on the pod's
  interface, reached via kubectl port-forward to a separate
  `aggexp-gc-debug` Service.** Simpler than adding auth for a
  demo-only endpoint; an operator deploying this for real would
  gate it behind an admission webhook or kill-switch the flag.
- **On `runOnce`, serialize with a mutex and reject overlapping
  sweeps with 409 Conflict on the HTTP path.** Avoids pathological
  interleaving of periodic + manual triggers.
- **GC issues `store.List(group, resource)` filtered at the
  metastore layer, not a full CRD List.** Today there's one
  reconciler per resource; multi-AA deployments (0027) would need
  per-group/resource GC filters.

## Prerequisites

- kind cluster `aggexp-0028`.
- Docker on host; `kind load docker-image` reachable.
- `kubectl`, `envsubst`, `go 1.24`, `curl`.

## What we're looking to learn

Storage independence (primary), watch and consistency semantics
(secondary).

Specifically:

1. Is the simple orphan-detection loop (List-both-sides, diff)
   sufficient for most out-of-band deletion modes?
2. What are the race windows between GC and normal writes? (brand-
   new Create, in-progress finalizer clear, SSA that straddles a
   sweep boundary)
3. How well does the grace-window policy work — is 20–30s enough
   to hide the backend-poll-delay race, or do we need something
   ack-based?
4. Does the skip-on-finalizer rule produce meaningful protection,
   or does it leak orphans that will never be cleaned up
   (because the finalizer-owning controller is itself gone)?
