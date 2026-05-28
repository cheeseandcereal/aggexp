# Experiment 0035: deterministic-uids

Library-mode AA for `widgets.aggexp.io/v1` with a `--uid-mode` flag
that switches between random (uuid.New) and deterministic
(SHA256(group/resource/namespace/name) formatted as a standard UID)
UID assignment. Demonstrates that deterministic UIDs eliminate the
pod-restart phantom-reconcile storm identified in FINDINGS/0012.

## Hypothesis

Deriving UIDs deterministically from backend-stable identifiers
(`SHA256(group + "/" + resource + "/" + namespace + "/" + name)`)
eliminates pod-restart phantom-reconcile storms. Currently each pod
restart regenerates all UIDs, causing controllers to see delete+add
pairs for every object -- O(objects) wasted reconciles.

Fundamentals touched:
- **Storage independence** (primary). UID stability across restarts
  is a storage-layer concern; the backend has no persistent UID store.
- **Watch and consistency semantics** (secondary). Downstream
  controllers' view of events on restart depends on UID stability.

## How to run

```bash
# 1. Create kind cluster
hack/make-kind.sh aggexp-0035

# 2. Build
cd experiments/0035-deterministic-uids
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o widget-aa ./cmd/widget-aa

# 3. Build and load image
docker build -t aggexp-0035:latest .
kind load docker-image aggexp-0035:latest --name aggexp-0035

# 4. Generate certs and create secret
../../hack/gen-certs.sh --force
export KUBECONFIG=$(kind get kubeconfig --name aggexp-0035)
kubectl create namespace aggexp-system || true
kubectl -n aggexp-system create secret tls aggexp-certs \
  --cert=deploy/certs/tls.crt --key=deploy/certs/tls.key || true

# 5. Deploy (random mode)
kubectl apply -f manifests/all.yaml

# 6. Wait for ready
kubectl -n aggexp-system wait --for=condition=Ready pod -l app=aggexp --timeout=60s

# 7. Test
kubectl create -f manifests/sample-widgets.yaml
kubectl get widgets -A -o yaml  # observe UIDs

# 8. Simulate restart
kubectl -n aggexp-system delete pod -l app=aggexp
kubectl -n aggexp-system wait --for=condition=Ready pod -l app=aggexp --timeout=60s
kubectl get widgets -A -o yaml  # UIDs changed (random mode)

# 9. Redeploy with deterministic mode
# Edit manifests/all.yaml: --uid-mode=deterministic
kubectl apply -f manifests/all.yaml
kubectl -n aggexp-system rollout restart deployment aggexp
# Repeat steps 7-8; UIDs should be stable
```

## Status

complete

## Decisions made

- UID format: SHA256 of `group + "/" + resource + "/" + namespace + "/" + name`
  truncated and formatted as 8-4-4-4-12 hex (standard UUID format).
- Namespace-scoped widgets to match 0032's pattern.
- Single-replica Deployment (not StatefulSet) since locking is out of scope.
- Forked 0032's code, removed locker package (not needed), simplified to
  single-replica.
- Delete/re-create yields same UID by design -- this is the entire point.
  Document whether Kubernetes semantics consider this a violation.

## Prerequisites

- Kind cluster `aggexp-0035`
- No external secrets or network access required

## What we're looking to learn

1. Does deterministic UID assignment eliminate the phantom-reconcile storm
   on pod restart? (Measure: watch events before/after restart)
2. What are the semantics of same-UID-on-recreate? Is it a violation of
   Kubernetes conventions? (ObjectMeta.UID is documented as "unique in time
   and space" -- we're deliberately violating "unique in time" for the same
   name.)
3. Does a reflector-style watch see delete+add events on restart with
   random UIDs, and NOT see them with deterministic UIDs?
