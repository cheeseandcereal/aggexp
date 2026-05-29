#!/usr/bin/env bash
set -euo pipefail

# Deploy script for experiment 0048 (capstone: library-multireplica-
# vertical-slice). Composes 0042 RV authority + 0043 embedded lock +
# 0044 per-watcher watch + 0045 read-path reconcile on the
# 0046-generated widgets.aggexp.io/v1 Widget.
#
# Assumes:
#   - ./hack/gen-certs.sh has been run (deploy/certs present)
#   - kind cluster aggexp-0048 exists and is the current context
#     (kubectl config use-context kind-aggexp-0048)
#   - namespace aggexp-system exists

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
EXP="${ROOT}/experiments/0048-library-multireplica-vertical-slice"
CLUSTER="aggexp-0048"
IMAGE="aggexp-0048:dev"

cd "${ROOT}"

# --- 1. base manifests (SA + auth-delegator + base Service). The base
# Deployment and base APIService are NOT applied; we supply our own
# 3-replica StatefulSet and an insecureSkipTLSVerify APIService. ---
kubectl apply -f deploy/manifests/00-namespace.yaml
kubectl apply -f deploy/manifests/10-serviceaccount.yaml
kubectl apply -f deploy/manifests/20-rbac.yaml
kubectl apply -f deploy/manifests/30-service.yaml

# Serving-cert secret.
kubectl create secret tls aggexp-serving-cert \
  --cert="deploy/certs/tls.crt" --key="deploy/certs/tls.key" \
  -n aggexp-system --dry-run=client -o yaml | kubectl apply -f -

# --- 2. build + load image ---
docker build -t "${IMAGE}" -f "${EXP}/Dockerfile" .
kind load docker-image "${IMAGE}" --name "${CLUSTER}"

# --- 3. metadata + body CRDs + experiment manifests ---
kubectl apply -f "${EXP}/metadata-crd/crd.yaml"
kubectl apply -f "${EXP}/metadata-crd/body-crd.yaml"

for f in \
  "${EXP}/manifests/02-namespace.yaml" \
  "${EXP}/manifests/00-permissive-rbac.yaml" \
  "${EXP}/manifests/50-apiservice.yaml"
do
  kubectl apply -f "${f}"
done

# StatefulSet with image + flag substitution.
AGGEXP_IMAGE="${IMAGE}" \
WATCH_MODE="${WATCH_MODE:-push}" \
SHARED_POLL="${SHARED_POLL:-false}" \
POLL_INTERVAL="${POLL_INTERVAL:-5s}" \
UPSTREAM_BUDGET="${UPSTREAM_BUDGET:-0}" \
ADOPT="${ADOPT:-true}" \
GC="${GC:-true}" \
BACKEND_DELAY_SECONDS="${BACKEND_DELAY_SECONDS:-0}" \
  envsubst < "${EXP}/manifests/30-aggexp-statefulset.yaml" | kubectl apply -f -

# --- 4. wait for rollout ---
kubectl -n aggexp-system rollout status statefulset/aggexp --timeout=180s

# Flush kubectl discovery cache (stale for 10m otherwise).
rm -rf "${HOME}/.kube/cache/discovery/" || true

echo
echo "--- replicas ---"
kubectl -n aggexp-system get pods -l app=aggexp -o wide
echo
echo "--- APIService ---"
kubectl get apiservices v1.widgets.aggexp.io
