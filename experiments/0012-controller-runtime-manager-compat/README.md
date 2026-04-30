# Experiment 0012: controller-runtime-manager-compat

Can a standard `sigs.k8s.io/controller-runtime` manager — caches,
reconcile loops, leader election, finalizer handling, owner-reference
handling — operate against an aggregated apiserver that doesn't share
etcd's persistence model?

This is the natural next step after `0008-long-lived-informer`:
`0008` established that client-go's raw `SharedInformer` survives a
synthetic-RV AA. The manager layer on top of it has more assumptions
(SSA-capable client, finalizer lifecycle, ownerReference GC,
lease-based leader election). We drive those against the 0007 AA
(`aggexp.io/v1 File`, read-only, fs-backed) because 0007 is the
simplest substrate-backed AA we have — and the write-path issues
that the controller will hit against a read-only AA are *themselves*
the interesting finding.

## Hypothesis

- **The manager starts and reconciles fire** (watch and consistency
  semantics behave the same through controller-runtime as through a
  raw reflector; cf. `0008`).
- **SSA patches succeed on the wire but managedFields do not persist**
  because the 0007 backend is stateless and read-only. This extends
  the persistence-gap finding from `0009-ack-aggregated-s3`.
- **Finalizers, UPDATE-based annotations, and ownerReferences all
  fail at the wire level** with `MethodNotSupported` because 0007's
  backend doesn't implement `WritableBackend` at all. The failure
  mode is a valid result — we want to characterize exactly what the
  controller-runtime reconcile loop does when every mutation is
  rejected.
- **Leader election works** because it targets host-cluster
  `coordination.k8s.io/leases`, not our AA.
- **AA restart produces the same consumer-side behavior `0008`
  characterized for a raw informer** (synthesized DeleteFuncs +
  AddFuncs after relist).

Fundamentals probed, in order of salience:

1. **Watch and consistency semantics** — primary. Does controller-
   runtime's cache + reconcile loop compose cleanly on top of the
   synthetic-RV AA?
2. **Wire protocol fidelity** — secondary. Does the SSA patch path
   actually make it through the aggregation layer with a correct
   `managedFields` response, even if the backend can't persist?
3. **Resource modeling freedom** — tertiary. Finalizers and
   ownerReferences are resource-modeling commitments baked into
   ObjectMeta; we're asking whether a stateless read-only AA can
   even participate in those conventions.

## How to run

Everything from the experiment directory unless otherwise noted.
Assumes `kind`, `kubectl`, `docker`, and `go` on PATH.

```
# repo root. Generate serving cert; create a dedicated kind cluster.
cd /path/to/aggexp
./hack/gen-certs.sh
kind create cluster --name aggexp-ctrl

# Build the 0007 AA image (it lives under experiments/0007-...,
# but we build from repo root and never touch that tree).
docker build -f experiments/0007-runtime-fs-driver/Dockerfile \
  -t aggexp-files:dev .
kind load docker-image aggexp-files:dev --name aggexp-ctrl

# Build the controller image from this experiment dir.
cd experiments/0012-controller-runtime-manager-compat/controller
docker build -t aggexp-file-controller:dev .
cd -
kind load docker-image aggexp-file-controller:dev --name aggexp-ctrl

# Base manifests (namespace, serving-cert Secret, APIService).
kubectl config use-context kind-aggexp-ctrl
./hack/deploy.sh deploy/manifests

# Experiment manifests: permissive File RBAC, copied-in 0007 AA
# Deployment + sample-files ConfigMap, controller RBAC + Deployment.
./hack/deploy.sh experiments/0012-controller-runtime-manager-compat/manifests

kubectl -n aggexp-system rollout status deploy/aggexp
kubectl -n aggexp-system rollout status deploy/aggexp-file-controller

# Observe reconciles arriving; let it cook.
kubectl -n aggexp-system logs -l app=aggexp-file-controller -f
```

## Status

in-progress

<!-- flipped to `complete` once FINDINGS/0012-... is written. -->

## Decisions made

- **Target AA is 0007 as-is, not a bespoke writable shim.** Task
  brief named 0007. The fs backend is read-only; that's deliberate
  and is itself part of the finding. We copied 0007's Deployment
  and sample-file ConfigMap into `manifests/` so this experiment
  never writes under `experiments/0007-runtime-fs-driver/`.
- **Controller uses `unstructured.Unstructured` for File**, not a
  typed Go struct. Pulling 0007's types module in would create a
  transitive build-path dependency that isn't needed and would
  pollute this experiment's go.mod; unstructured exercises the
  exact same REST/cache paths.
- **controller-runtime v0.20.2** — pairs with `k8s.io/*@v0.32.x` by
  convention; matches the rest of the repo's `@v0.32.3` pins.
- **Leader election on leases in `aggexp-system`** — host-cluster
  coordination.k8s.io, not our AA. The point isn't to test leader
  election itself; it's to confirm that configuring it away from
  the AA works.
- **Requeue after 30s.** Arbitrary; gives us multiple reconcile
  attempts per scenario without spamming.
- **SSA `ForceOwnership`**. The controller uses a dedicated field
  manager `aggexp-file-controller`; `ForceOwnership` ensures the
  apply is tested even if kubectl's `last-applied-configuration`
  annotation implicitly claims ownership of managedFields entries
  first. Won't matter for 0007 — managedFields aren't persisted —
  but documented so the behavior is reproducible on a writable AA.
- **Controller grants itself far more verbs on `aggexp.io/files`
  than 0007 implements** (full CRUD in RBAC, but the AA will 405).
  This intentionally separates "what the controller would do on a
  writable AA" from "what 0007 permits", so the observed failure
  is a backend-shape failure, not an RBAC denial.

## Prerequisites

- `kind` cluster `aggexp-ctrl` (dedicated; don't reuse `aggexp`,
  `aggexp-runtime`, `aggexp-informer`, etc.).
- Serving cert produced by `hack/gen-certs.sh`.
- 0007 AA image `aggexp-files:dev` built from repo root.
- Controller image `aggexp-file-controller:dev` built from this
  experiment's `controller/` directory.

## What we're looking to learn

Named questions (findings per scenario):

1. Does `mgr.Start(ctx)` return cleanly; what do caches-synced logs
   look like for `aggexp.io/v1 files`?
2. Does `Reconcile()` fire on a freshly-appearing File within the
   poll interval?
3. Does the SSA patch path succeed at the wire level against 0007,
   and what do `managedFields` look like on the GET that follows?
4. Does the UPDATE-based finalizer add path work, or does it hit
   the adapter's `MethodNotSupported`?
5. Does `kubectl delete cm aggexp-files-parent` cascade-delete the
   owning Files? (Expected: no — our AA has no GC controller — but
   the failure mode needs to be characterized.)
6. Does lease-based leader election on the host cluster's
   `aggexp-system` work under a controller-runtime manager whose
   target API lives at a different apiserver?
7. What does the controller-runtime cache do when the AA is scaled
   0 and then back to 1 mid-reconcile?
