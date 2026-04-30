#!/usr/bin/env bash
set -euo pipefail

# End-to-end setup for experiment 0015 (argocd-application-targets-aa).
# Creates a dedicated kind cluster, deploys the 0010 Widget AA (writable
# CRD facade), installs ArgoCD, spins up an in-cluster git server
# serving three Widget manifests, creates the ArgoCD Application, and
# drives three scenarios (initial sync, edit-a-widget, delete-a-widget).
#
# The Widget AA (0010) is chosen over the S3 AA (0009) because the task
# brief explicitly recommends it: 0010's CRD-backed storage preserves
# managedFields, which is exactly what ArgoCD's server-side apply
# depends on.

CLUSTER="aggexp-argo-app"
CTX="kind-${CLUSTER}"
NS="aggexp-system"

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
EXP_DIR="${REPO_ROOT}/experiments/0015-argocd-application-targets-aa"
LOG_DIR="${EXP_DIR}/logs"
mkdir -p "${LOG_DIR}"

for bin in kind kubectl docker envsubst jq; do
  command -v "${bin}" >/dev/null 2>&1 || { echo "missing: ${bin}" >&2; exit 1; }
done

# --- 1. Cluster ---------------------------------------------------------------
if ! kind get clusters 2>/dev/null | grep -qx "${CLUSTER}"; then
  kind create cluster --name "${CLUSTER}"
else
  echo "kind cluster ${CLUSTER} already exists; reusing"
fi
kubectl config use-context "${CTX}"
kubectl --context "${CTX}" get ns "${NS}" >/dev/null 2>&1 \
  || kubectl --context "${CTX}" create ns "${NS}"

# --- 2. Build/load AA image (0010 widgets) -----------------------------------
if ! docker image inspect aggexp-widgets:dev >/dev/null 2>&1; then
  echo "error: aggexp-widgets:dev image not present; build it first via:" >&2
  echo "  docker build -t aggexp-widgets:dev -f experiments/0010-etcd-crd-facade-with-ssa/Dockerfile ." >&2
  exit 1
fi
kind load docker-image aggexp-widgets:dev --name "${CLUSTER}"

# --- 3. Base manifests + 0010 overlay ----------------------------------------
cd "${REPO_ROOT}"
./hack/deploy.sh deploy/manifests
AGGEXP_IMAGE=aggexp-widgets:dev ./hack/deploy.sh experiments/0010-etcd-crd-facade-with-ssa/manifests
kubectl -n "${NS}" rollout status deploy/aggexp --timeout=180s

# --- 4. ArgoCD ---------------------------------------------------------------
kubectl get ns argocd >/dev/null 2>&1 || kubectl create ns argocd
# SSA because the applicationsets CRD blows past the client-side
# last-applied-configuration 256KiB annotation limit.
kubectl apply -n argocd --server-side=true --force-conflicts \
  -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
kubectl -n argocd rollout status deploy/argocd-server --timeout=300s
kubectl -n argocd rollout status deploy/argocd-repo-server --timeout=300s
kubectl -n argocd rollout status statefulset/argocd-application-controller --timeout=300s

# argocd-application-controller watches every discovered API group.
# From 0005 we know a LIST failure on ANY resource bricks its cluster
# cache. Our 0010 AA's ClusterRole for widgets.aggexp.io already grants
# system:authenticated, so the ArgoCD SA (which is system:authenticated)
# is already allowed read + write on widgets. But the backing CRD
# widgetstorages.aggexpstorage.aggexp.io also needs to be listable by
# argocd-application-controller's cluster cache (it's a CRD on the host,
# so kube-apiserver RBAC, not our AA's authorizer, decides). Grant it.
kubectl apply -f - <<'YAML'
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: argocd-widgetstorage-read
rules:
  - apiGroups: ["aggexpstorage.aggexp.io"]
    resources: ["widgetstorages"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: argocd-widgetstorage-read
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: argocd-widgetstorage-read
subjects:
  - kind: ServiceAccount
    name: argocd-application-controller
    namespace: argocd
  - kind: ServiceAccount
    name: argocd-server
    namespace: argocd
YAML

# --- 5. Git server (in-cluster) ----------------------------------------------
kubectl apply -f "${EXP_DIR}/manifests/00-git-server.yaml"
kubectl -n git-server rollout status deploy/git-server --timeout=120s

# --- 6. ArgoCD Application ---------------------------------------------------
kubectl apply -f "${EXP_DIR}/manifests/10-application.yaml"

echo
echo "--- discovery of aggexp.io ---"
kubectl api-resources --api-group=aggexp.io || true
kubectl get apiservices v1.aggexp.io -o jsonpath='{.status.conditions[?(@.type=="Available")].status}{"\n"}' || true

echo
echo "--- wait for initial sync (up to 3 min) ---"
for _ in $(seq 1 36); do
  SYNC=$(kubectl -n argocd get application aggexp-widgets -o jsonpath='{.status.sync.status}' 2>/dev/null || true)
  HEALTH=$(kubectl -n argocd get application aggexp-widgets -o jsonpath='{.status.health.status}' 2>/dev/null || true)
  echo "  sync=${SYNC:-unknown} health=${HEALTH:-unknown}"
  if [[ "${SYNC}" == "Synced" ]]; then break; fi
  sleep 5
done

echo
echo "--- widgets visible through the AA ---"
kubectl get widgets -o wide || true
echo
echo "--- backing widgetstorages on the host CRD ---"
kubectl get widgetstorages.aggexpstorage.aggexp.io -o name || true

echo
echo "Setup complete. Drive scenarios with: hack/scenarios.sh"
