#!/usr/bin/env bash
set -euo pipefail

# End-to-end setup for experiment 0005 (argocd-compat). Idempotent.
# Creates a dedicated kind cluster, deploys the 0004 github-driver AA
# into it, installs ArgoCD, and applies a toy Application that points
# at a Git repo of plain Kubernetes manifests (NOT aggexp resources).

CLUSTER="aggexp-argocd"
CTX="kind-${CLUSTER}"
NS="aggexp-system"
GITHUB_OWNER="${GITHUB_OWNER:-kubernetes-sigs}"
GITHUB_TOKEN="${GITHUB_TOKEN:-}"

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

for bin in kind kubectl docker envsubst; do
  command -v "${bin}" >/dev/null 2>&1 || { echo "missing: ${bin}" >&2; exit 1; }
done

if ! kind get clusters 2>/dev/null | grep -qx "${CLUSTER}"; then
  kind create cluster --name "${CLUSTER}"
else
  echo "kind cluster ${CLUSTER} already exists; reusing"
fi

kubectl --context "${CTX}" get ns "${NS}" >/dev/null 2>&1 \
  || kubectl --context "${CTX}" create ns "${NS}"

# Load images the experiment depends on (already built from 0004).
for img in aggexp-repos:dev aggexp-policy:dev; do
  if ! docker image inspect "${img}" >/dev/null 2>&1; then
    echo "error: image ${img} not found locally; build from experiment 0004 first" >&2
    exit 1
  fi
  kind load docker-image "${img}" --name "${CLUSTER}"
done

# Deploy base manifests and the 0004 overlay into the new cluster.
# `hack/deploy.sh` uses the current kubectl context; pin it.
kubectl config use-context "${CTX}"

cd "${REPO_ROOT}"

./hack/deploy.sh deploy/manifests

AGGEXP_IMAGE=aggexp-repos:dev \
POLICY_IMAGE=aggexp-policy:dev \
GITHUB_OWNER="${GITHUB_OWNER}" \
GITHUB_TOKEN="${GITHUB_TOKEN}" \
  ./hack/deploy.sh experiments/0004-github-driver-static-pat/manifests

kubectl -n "${NS}" rollout status deploy/policy-service --timeout=120s
kubectl -n "${NS}" rollout status deploy/aggexp --timeout=120s

# Install ArgoCD.
kubectl get ns argocd >/dev/null 2>&1 || kubectl create ns argocd
# --server-side because the applicationsets CRD has annotations larger
# than the 262144-byte last-applied-configuration limit used by
# client-side apply. SSA avoids the annotation entirely.
kubectl apply -n argocd --server-side=true --force-conflicts \
  -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml

# Wait for the core ArgoCD workloads. The application-controller is a
# StatefulSet; the rest are Deployments.
kubectl -n argocd rollout status deploy/argocd-server --timeout=300s
kubectl -n argocd rollout status deploy/argocd-repo-server --timeout=300s
kubectl -n argocd rollout status deploy/argocd-applicationset-controller --timeout=300s
kubectl -n argocd rollout status statefulset/argocd-application-controller --timeout=300s

# Toy Application.
kubectl apply -f "${REPO_ROOT}/experiments/0005-argocd-compat/manifests/toy-application.yaml"

echo
echo "--- aggexp AA discovery ---"
kubectl get apiservices v1.aggexp.io -o jsonpath='{.status.conditions[?(@.type=="Available")].status}'
echo
kubectl api-resources --api-group=aggexp.io
echo "--- repos available ---"
kubectl get repos | head
echo "--- argocd apps ---"
kubectl -n argocd get applications
