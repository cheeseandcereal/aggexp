#!/usr/bin/env bash
set -euo pipefail

# Deploy script for experiment 0043 (embedded-lock + emission-filtering).
#
# Assumes:
#   - ./hack/gen-certs.sh has been run (deploy/certs present)
#   - kind cluster aggexp-0043 exists
#   - namespace aggexp-system exists in that cluster
#
# All kubectl calls are PINNED to the kind-aggexp-0043 context via the
# KUBECTL wrapper, so a concurrently running experiment that flips the
# current context cannot make this script touch the wrong cluster.
#
# Steps:
#   1. Apply base manifests (SA, auth-delegator RBAC, base Service)
#      from deploy/manifests, then install our own 3-replica
#      StatefulSet and an insecureSkipTLSVerify APIService.
#   2. Build + load the image (build context = repo root for the
#      `replace` directive).
#   3. Apply the metadata CRD + body CRD + experiment manifests.
#   4. Wait for the StatefulSet rollout.

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
EXP="${ROOT}/experiments/0043-embedded-lock-emission-filtering"
CLUSTER="aggexp-0043"
CONTEXT="kind-aggexp-0043"
IMAGE="aggexp-0043:dev"

KUBECTL=(kubectl --context "${CONTEXT}")

cd "${ROOT}"

# --- 1. base manifests (SA + auth-delegator + base Service). ---
"${KUBECTL[@]}" apply -f deploy/manifests/00-namespace.yaml
"${KUBECTL[@]}" apply -f deploy/manifests/10-serviceaccount.yaml
"${KUBECTL[@]}" apply -f deploy/manifests/20-rbac.yaml
"${KUBECTL[@]}" apply -f deploy/manifests/30-service.yaml

# Serving-cert secret.
"${KUBECTL[@]}" create secret tls aggexp-serving-cert \
  --cert="deploy/certs/tls.crt" --key="deploy/certs/tls.key" \
  -n aggexp-system --dry-run=client -o yaml | "${KUBECTL[@]}" apply -f -

# --- 2. build + load image ---
docker build -t "${IMAGE}" -f "${EXP}/Dockerfile" .
kind load docker-image "${IMAGE}" --name "${CLUSTER}"

# --- 3. metadata + body CRDs + experiment manifests ---
"${KUBECTL[@]}" apply -f "${EXP}/metadata-crd/crd.yaml"
"${KUBECTL[@]}" apply -f "${EXP}/metadata-crd/body-crd.yaml"

for f in \
  "${EXP}/manifests/02-namespace.yaml" \
  "${EXP}/manifests/00-permissive-rbac.yaml" \
  "${EXP}/manifests/50-apiservice.yaml"
do
  "${KUBECTL[@]}" apply -f "${f}"
done

# StatefulSet with image substitution.
AGGEXP_IMAGE="${IMAGE}" envsubst < "${EXP}/manifests/30-aggexp-statefulset.yaml" | "${KUBECTL[@]}" apply -f -

# --- 4. wait for rollout ---
"${KUBECTL[@]}" -n aggexp-system rollout status statefulset/aggexp --timeout=180s

# Flush kubectl discovery cache (stale for 10m otherwise).
rm -rf "${HOME}/.kube/cache/discovery/" || true

echo
echo "--- replicas ---"
"${KUBECTL[@]}" -n aggexp-system get pods -l app=aggexp -o wide
echo
echo "--- APIService ---"
"${KUBECTL[@]}" get apiservices v1.aggexp.io
