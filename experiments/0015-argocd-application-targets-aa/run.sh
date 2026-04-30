#!/usr/bin/env bash
set -euo pipefail

# End-to-end setup for experiment 0015 (argocd-application-targets-aa).
# Creates a dedicated kind cluster, deploys the 0010 Widget AA (writable
# CRD facade), installs ArgoCD, spins up an in-cluster git server
# serving three Widget manifests, and creates the ArgoCD Application.
#
# The Widget AA (0010) is chosen over the S3 AA (0009) because the task
# brief explicitly recommends it: 0010's CRD-backed storage preserves
# managedFields, which is exactly what ArgoCD's server-side apply
# depends on.
#
# NOTE on isolation: a parallel agent clobbering the global kubectl
# context is a known hazard (see SYNTHESIS.md's Process observations).
# Every kubectl invocation below passes --context explicitly. hack/deploy.sh
# was written pre-isolation-awareness and uses the current context, so we
# `kubectl config use-context` before every call into it.

CLUSTER="aggexp-argo-app"
CTX="kind-${CLUSTER}"
NS="aggexp-system"

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
EXP_DIR="${REPO_ROOT}/experiments/0015-argocd-application-targets-aa"
LOG_DIR="${EXP_DIR}/logs"
mkdir -p "${LOG_DIR}"

K() { kubectl --context "${CTX}" "$@"; }

for bin in kind kubectl docker envsubst jq; do
  command -v "${bin}" >/dev/null 2>&1 || { echo "missing: ${bin}" >&2; exit 1; }
done

# --- 1. Cluster ---------------------------------------------------------------
if ! kind get clusters 2>/dev/null | grep -qx "${CLUSTER}"; then
  kind create cluster --name "${CLUSTER}"
else
  echo "kind cluster ${CLUSTER} already exists; reusing"
fi
K get ns "${NS}" >/dev/null 2>&1 || K create ns "${NS}"

# --- 2. Build/load AA image (0010 widgets) + git-server image ----------------
if ! docker image inspect aggexp-widgets:dev >/dev/null 2>&1; then
  echo "error: aggexp-widgets:dev image not present; build it first via:" >&2
  echo "  docker build -t aggexp-widgets:dev -f experiments/0010-etcd-crd-facade-with-ssa/Dockerfile ." >&2
  exit 1
fi
docker build -t aggexp-git-server:dev "${EXP_DIR}/git-server/"
kind load docker-image aggexp-widgets:dev --name "${CLUSTER}"
kind load docker-image aggexp-git-server:dev --name "${CLUSTER}"

# --- 3. Base manifests + 0010 overlay ----------------------------------------
# NOTE: hack/deploy.sh applies every *.yaml in the dir. 0010's manifests
# dir includes `widget.yaml` (a sample resource), which we don't want
# applied here — the AA isn't ready yet, and this experiment creates
# Widgets via ArgoCD, not directly. So we apply the 0010 overlay files
# selectively.
cd "${REPO_ROOT}"
kubectl config use-context "${CTX}"
./hack/deploy.sh deploy/manifests

OVERLAY_TMP="$(mktemp -d)"
trap 'rm -rf "${OVERLAY_TMP}"' EXIT
for f in 00-permissive-rbac.yaml 05-crd.yaml 30-aggexp-deployment-override.yaml; do
  cp "experiments/0010-etcd-crd-facade-with-ssa/manifests/${f}" "${OVERLAY_TMP}/"
done
kubectl config use-context "${CTX}"
AGGEXP_IMAGE=aggexp-widgets:dev ./hack/deploy.sh "${OVERLAY_TMP}"
K -n "${NS}" rollout status deploy/aggexp --timeout=180s

# --- 4. ArgoCD ---------------------------------------------------------------
K get ns argocd >/dev/null 2>&1 || K create ns argocd
K apply -n argocd --server-side=true --force-conflicts \
  -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
K -n argocd rollout status deploy/argocd-server --timeout=300s
K -n argocd rollout status deploy/argocd-repo-server --timeout=300s
K -n argocd rollout status statefulset/argocd-application-controller --timeout=300s

K apply -f - <<'YAML'
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
K apply -f "${EXP_DIR}/manifests/00-git-server.yaml"
K -n git-server rollout status deploy/git-server --timeout=120s

# --- 6. ArgoCD Application ---------------------------------------------------
K apply -f "${EXP_DIR}/manifests/10-application.yaml"

echo
echo "--- discovery of aggexp.io ---"
K api-resources --api-group=aggexp.io || true
K get apiservices v1.aggexp.io -o jsonpath='{.status.conditions[?(@.type=="Available")].status}{"\n"}' || true

echo
echo "--- wait for initial sync (up to 3 min) ---"
for _ in $(seq 1 36); do
  SYNC=$(K -n argocd get application aggexp-widgets -o jsonpath='{.status.sync.status}' 2>/dev/null || true)
  HEALTH=$(K -n argocd get application aggexp-widgets -o jsonpath='{.status.health.status}' 2>/dev/null || true)
  echo "  sync=${SYNC:-unknown} health=${HEALTH:-unknown}"
  if [[ "${SYNC}" == "Synced" ]]; then break; fi
  sleep 5
done

echo
echo "--- widgets visible through the AA ---"
K get widgets -o wide || true
echo
echo "--- backing widgetstorages on the host CRD ---"
K get widgetstorages.aggexpstorage.aggexp.io -o name || true

echo
echo "Setup complete. Drive scenarios with: scenarios.sh"
