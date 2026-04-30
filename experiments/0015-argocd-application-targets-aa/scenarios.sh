#!/usr/bin/env bash
set -euo pipefail

# Drives the three edit/prune scenarios against the running experiment.
# Assumes run.sh has already completed successfully.

CLUSTER="aggexp-argo-app"
CTX="kind-${CLUSTER}"
kubectl config use-context "${CTX}"

# ---- v2 content: alpha mutated -------------------------------------
# Rewrites the git-content ConfigMap with alpha bumped (counter=42,
# extra label + tag) and restarts the git-server Deployment so its
# initContainer rebuilds the bare repo.
apply_content_v2() {
  kubectl -n git-server create configmap git-content \
    --from-literal=widget-alpha.yaml='apiVersion: aggexp.io/v1
kind: Widget
metadata:
  name: alpha
  labels:
    managed-by: argocd-experiment-0015
    role: primary
    mutated: "v2"
spec:
  description: "alpha widget edited in Git to revision v2"
  counter: 42
  tags:
    env: lab
    source: argocd
    revision: v2
    extra: added-in-v2
' \
    --from-literal=widget-bravo.yaml='apiVersion: aggexp.io/v1
kind: Widget
metadata:
  name: bravo
  labels:
    managed-by: argocd-experiment-0015
    role: secondary
spec:
  description: "bravo widget synced from Git (revision v1)"
  counter: 2
  tags:
    env: lab
    source: argocd
    revision: v1
' \
    --from-literal=widget-charlie.yaml='apiVersion: aggexp.io/v1
kind: Widget
metadata:
  name: charlie
  labels:
    managed-by: argocd-experiment-0015
    role: tertiary
spec:
  description: "charlie widget synced from Git (revision v1)"
  counter: 3
  tags:
    env: lab
    source: argocd
    revision: v1
' \
    --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n git-server rollout restart deploy/git-server
  kubectl -n git-server rollout status deploy/git-server --timeout=120s
}

# ---- v3 content: charlie removed -----------------------------------
apply_content_v3() {
  kubectl -n git-server create configmap git-content \
    --from-literal=widget-alpha.yaml='apiVersion: aggexp.io/v1
kind: Widget
metadata:
  name: alpha
  labels:
    managed-by: argocd-experiment-0015
    role: primary
    mutated: "v2"
spec:
  description: "alpha widget edited in Git to revision v2"
  counter: 42
  tags:
    env: lab
    source: argocd
    revision: v2
    extra: added-in-v2
' \
    --from-literal=widget-bravo.yaml='apiVersion: aggexp.io/v1
kind: Widget
metadata:
  name: bravo
  labels:
    managed-by: argocd-experiment-0015
    role: secondary
spec:
  description: "bravo widget synced from Git (revision v1)"
  counter: 2
  tags:
    env: lab
    source: argocd
    revision: v1
' \
    --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n git-server rollout restart deploy/git-server
  kubectl -n git-server rollout status deploy/git-server --timeout=120s
}

wait_sync_rev() {
  local want="$1" timeout="${2:-60}"
  for _ in $(seq 1 "${timeout}"); do
    SYNC=$(kubectl -n argocd get application aggexp-widgets -o jsonpath='{.status.sync.status}' 2>/dev/null || true)
    REV=$(kubectl -n argocd get application aggexp-widgets -o jsonpath='{.status.sync.revision}' 2>/dev/null || true)
    echo "  sync=${SYNC:-?} rev=${REV:0:10}..."
    if [[ "${SYNC}" == "Synced" ]]; then return 0; fi
    sleep 1
  done
  return 1
}

force_refresh() {
  kubectl -n argocd annotate application aggexp-widgets \
    argocd.argoproj.io/refresh=hard --overwrite
}

case "${1:-}" in
  v2) apply_content_v2 ;;
  v3) apply_content_v3 ;;
  refresh) force_refresh ;;
  wait) wait_sync_rev "${2:-Synced}" "${3:-60}" ;;
  *)
    echo "usage: $0 {v2|v3|refresh|wait [status] [timeout]}"
    exit 2
    ;;
esac
