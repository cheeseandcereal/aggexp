#!/usr/bin/env bash
set -euo pipefail

# Deploy script for experiment 0049 (locked-write-transaction). Builds
# on the 0048 capstone AA (0042 RV authority + 0043 embedded lock +
# 0044 per-watcher watch + 0045 read-path reconcile on the
# 0046-generated widgets.aggexp.io/v1 Widget) and adds the locked-write
# TRANSACTION discipline (commit-path retry under the held lock).
#
# The transaction fix is toggled by the LOCK_TXN env (default true).
# Set LOCK_TXN=false to deploy the REGRESSION baseline (scenario 1) that
# reproduces the 0048 post-acquire 500s.
#
# Every kubectl call pins --context kind-aggexp-0049: the shared
# kubeconfig current-context drifts under parallel runs (a known arc
# hazard). NEVER touch the default aggexp cluster.
#
# Assumes:
#   - ./hack/gen-certs.sh has been run (deploy/certs present)
#   - kind cluster aggexp-0049 exists
#   - namespace aggexp-system exists

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
EXP="${ROOT}/experiments/0049-locked-write-transaction"
CLUSTER="aggexp-0049"
CTX="kind-aggexp-0049"
IMAGE="aggexp-0049:dev"
KCTL=(kubectl --context "${CTX}")

cd "${ROOT}"

# --- 1. base manifests (SA + auth-delegator + base Service). The base
# Deployment and base APIService are NOT applied; we supply our own
# 3-replica StatefulSet and an insecureSkipTLSVerify APIService. ---
"${KCTL[@]}" apply -f deploy/manifests/00-namespace.yaml
"${KCTL[@]}" apply -f deploy/manifests/10-serviceaccount.yaml
"${KCTL[@]}" apply -f deploy/manifests/20-rbac.yaml
"${KCTL[@]}" apply -f deploy/manifests/30-service.yaml

# Serving-cert secret.
"${KCTL[@]}" create secret tls aggexp-serving-cert \
  --cert="deploy/certs/tls.crt" --key="deploy/certs/tls.key" \
  -n aggexp-system --dry-run=client -o yaml | "${KCTL[@]}" apply -f -

# --- 2. build + load image ---
docker build -t "${IMAGE}" -f "${EXP}/Dockerfile" .
kind load docker-image "${IMAGE}" --name "${CLUSTER}"

# --- 3. metadata + body CRDs + experiment manifests ---
"${KCTL[@]}" apply -f "${EXP}/metadata-crd/crd.yaml"
"${KCTL[@]}" apply -f "${EXP}/metadata-crd/body-crd.yaml"

for f in \
  "${EXP}/manifests/02-namespace.yaml" \
  "${EXP}/manifests/00-permissive-rbac.yaml" \
  "${EXP}/manifests/50-apiservice.yaml"
do
  "${KCTL[@]}" apply -f "${f}"
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
LOCK_TXN="${LOCK_TXN:-true}" \
LOCK_TXN_ATTEMPTS="${LOCK_TXN_ATTEMPTS:-5}" \
  envsubst < "${EXP}/manifests/30-aggexp-statefulset.yaml" | "${KCTL[@]}" apply -f -

# Force a restart so flag changes (e.g. LOCK_TXN flip between scenarios)
# take effect even when the image tag is unchanged.
"${KCTL[@]}" -n aggexp-system rollout restart statefulset/aggexp

# --- 4. wait for rollout ---
"${KCTL[@]}" -n aggexp-system rollout status statefulset/aggexp --timeout=180s

# Flush kubectl discovery cache (stale for 10m otherwise).
rm -rf "${HOME}/.kube/cache/discovery/" || true

echo
echo "--- replicas (LOCK_TXN=${LOCK_TXN:-true}) ---"
"${KCTL[@]}" -n aggexp-system get pods -l app=aggexp -o wide
echo
echo "--- APIService ---"
"${KCTL[@]}" get apiservices v1.widgets.aggexp.io
