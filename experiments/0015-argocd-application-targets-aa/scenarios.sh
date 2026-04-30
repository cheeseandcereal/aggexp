#!/usr/bin/env bash
set -euo pipefail

# Drives the edit/prune scenarios against the running experiment.
# Assumes run.sh has already completed successfully.

CLUSTER="aggexp-argo-app"
CTX="kind-${CLUSTER}"

K() { kubectl --context "${CTX}" "$@"; }

apply_content_v2() {
  K -n git-server create configmap git-content \
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
    --dry-run=client -o yaml | K apply -f -
  K -n git-server rollout restart deploy/git-server
  K -n git-server rollout status deploy/git-server --timeout=120s
}

apply_content_v3() {
  # charlie dropped from source
  K -n git-server create configmap git-content \
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
    --dry-run=client -o yaml | K apply -f -
  K -n git-server rollout restart deploy/git-server
  K -n git-server rollout status deploy/git-server --timeout=120s
}

wait_sync() {
  local want="${1:-Synced}" timeout="${2:-120}"
  for _ in $(seq 1 "${timeout}"); do
    SYNC=$(K -n argocd get application aggexp-widgets -o jsonpath='{.status.sync.status}' 2>/dev/null || true)
    REV=$(K -n argocd get application aggexp-widgets -o jsonpath='{.status.sync.revision}' 2>/dev/null || true)
    HEALTH=$(K -n argocd get application aggexp-widgets -o jsonpath='{.status.health.status}' 2>/dev/null || true)
    printf '  sync=%s health=%s rev=%s\n' "${SYNC:-?}" "${HEALTH:-?}" "${REV:0:10}..."
    if [[ "${SYNC}" == "${want}" ]]; then return 0; fi
    sleep 1
  done
  return 1
}

force_refresh() {
  K -n argocd annotate application aggexp-widgets \
    argocd.argoproj.io/refresh=hard --overwrite
}

case "${1:-}" in
  v2) apply_content_v2 ;;
  v3) apply_content_v3 ;;
  refresh) force_refresh ;;
  wait) wait_sync "${2:-Synced}" "${3:-120}" ;;
  *)
    echo "usage: $0 {v2|v3|refresh|wait [status] [timeout]}"
    exit 2
    ;;
esac
