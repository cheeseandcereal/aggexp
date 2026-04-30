#!/usr/bin/env bash
set -euo pipefail

# End-to-end setup for experiment 0014 (flux-compat). Idempotent.
# Creates a dedicated kind cluster, deploys the 0004 github-driver AA
# into it, installs Flux with the default controller set, and applies
# a toy GitRepository + Kustomization that syncs vanilla k8s manifests
# (NOT aggexp resources).

CLUSTER="aggexp-flux"
CTX="kind-${CLUSTER}"
NS="aggexp-system"
GITHUB_OWNER="${GITHUB_OWNER:-kubernetes-sigs}"
GITHUB_TOKEN="${GITHUB_TOKEN:-}"

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

for bin in kind kubectl docker envsubst flux; do
  command -v "${bin}" >/dev/null 2>&1 || { echo "missing: ${bin}" >&2; exit 1; }
done

if ! kind get clusters 2>/dev/null | grep -qx "${CLUSTER}"; then
  kind create cluster --name "${CLUSTER}"
else
  echo "kind cluster ${CLUSTER} already exists; reusing"
fi

kubectl --context "${CTX}" get ns "${NS}" >/dev/null 2>&1 \
  || kubectl --context "${CTX}" create ns "${NS}"

for img in aggexp-repos:dev aggexp-policy:dev; do
  if ! docker image inspect "${img}" >/dev/null 2>&1; then
    echo "error: image ${img} not found locally; build from experiment 0004 first" >&2
    exit 1
  fi
  kind load docker-image "${img}" --name "${CLUSTER}"
done

kubectl config use-context "${CTX}"

cd "${REPO_ROOT}"

./hack/deploy.sh deploy/manifests

AGGEXP_IMAGE=aggexp-repos:dev \
POLICY_IMAGE=aggexp-policy:dev \
GITHUB_OWNER="${GITHUB_OWNER}" \
GITHUB_TOKEN="${GITHUB_TOKEN}" \
  ./hack/deploy.sh experiments/0004-github-driver-static-pat/manifests

kubectl -n "${NS}" rollout status deploy/policy-service --timeout=120s
kubectl -n "${NS}" rollout status deploy/aggexp --timeout=180s

# Install Flux with default components.
flux install

# Wait for Flux's core controllers.
kubectl -n flux-system rollout status deploy/source-controller        --timeout=300s
kubectl -n flux-system rollout status deploy/kustomize-controller     --timeout=300s
kubectl -n flux-system rollout status deploy/helm-controller          --timeout=300s
kubectl -n flux-system rollout status deploy/notification-controller  --timeout=300s

kubectl apply -f "${REPO_ROOT}/experiments/0014-flux-compat/manifests/toy-podinfo.yaml"

echo
echo "--- aggexp AA discovery ---"
kubectl get apiservices v1.aggexp.io -o jsonpath='{.status.conditions[?(@.type=="Available")].status}' || true
echo
kubectl api-resources --api-group=aggexp.io || true
echo "--- repos available ---"
kubectl get repos 2>&1 | head
echo "--- flux state ---"
kubectl -n flux-system get gitrepositories,kustomizations
