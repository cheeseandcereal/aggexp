#!/usr/bin/env bash
set -euo pipefail

# Deploy script for experiment 0042 (metadata-CR RV authority).
#
# Assumes:
#   - ./hack/gen-certs.sh has been run (deploy/certs present)
#   - kind cluster aggexp-0042 exists and is the current context
#     (kubectl config use-context kind-aggexp-0042)
#   - namespace aggexp-system exists
#
# Steps:
#   1. Apply base manifests (SA, auth-delegator RBAC, base Service)
#      from deploy/manifests, then delete the base Deployment and the
#      base APIService (we install our own 3-replica StatefulSet and
#      an insecureSkipTLSVerify APIService).
#   2. Build + load the image (build context = repo root for the
#      `replace` directive).
#   3. Apply the metadata CRD + experiment manifests.
#   4. Wait for the StatefulSet rollout.

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
EXP="${ROOT}/experiments/0042-metadata-cr-rv-authority"
CLUSTER="aggexp-0042"
IMAGE="aggexp-0042:dev"

cd "${ROOT}"

# --- 1. base manifests (SA + auth-delegator + base Service). The base
# Deployment and base APIService are removed; we supply our own. ---
# Apply only the pieces we want from deploy/manifests to avoid the
# base Deployment/APIService racing with ours. We apply SA + RBAC +
# base Service individually.
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

# --- 3. metadata CRD + experiment manifests ---
kubectl apply -f "${EXP}/metadata-crd/crd.yaml"

for f in \
  "${EXP}/manifests/02-namespace.yaml" \
  "${EXP}/manifests/00-permissive-rbac.yaml" \
  "${EXP}/manifests/50-apiservice.yaml"
do
  kubectl apply -f "${f}"
done

# StatefulSet with image substitution.
AGGEXP_IMAGE="${IMAGE}" envsubst < "${EXP}/manifests/30-aggexp-statefulset.yaml" | kubectl apply -f -

# --- 4. wait for rollout ---
kubectl -n aggexp-system rollout status statefulset/aggexp --timeout=180s

# Flush kubectl discovery cache (stale for 10m otherwise).
rm -rf "${HOME}/.kube/cache/discovery/" || true

echo
echo "--- replicas ---"
kubectl -n aggexp-system get pods -l app=aggexp -o wide
echo
echo "--- APIService ---"
kubectl get apiservices v1.aggexp.io
