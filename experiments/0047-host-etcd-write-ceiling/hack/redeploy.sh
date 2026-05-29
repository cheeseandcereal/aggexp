#!/usr/bin/env bash
set -euo pipefail

# Rebuild the image, load it into kind, and roll the StatefulSet with
# the given watch configuration. Used to re-run scenarios with
# different per-watcher modes without re-applying CRDs/RBAC.
#
# Env knobs (all optional):
#   WATCH_MODE      push | poll        (default push)
#   SHARED_POLL     true | false       (default false)
#   POLL_INTERVAL   duration           (default 5s)
#   UPSTREAM_BUDGET int                (default 0 = unlimited)
#   SKIP_BUILD      1 to reuse the loaded image

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
EXP="${ROOT}/experiments/0047-host-etcd-write-ceiling"
CLUSTER="aggexp-0047"
IMAGE="aggexp-0047:dev"

cd "${ROOT}"

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  docker build -t "${IMAGE}" -f "${EXP}/Dockerfile" . >/dev/null
  kind load docker-image "${IMAGE}" --name "${CLUSTER}" >/dev/null
fi

AGGEXP_IMAGE="${IMAGE}" \
WATCH_MODE="${WATCH_MODE:-push}" \
SHARED_POLL="${SHARED_POLL:-false}" \
POLL_INTERVAL="${POLL_INTERVAL:-5s}" \
UPSTREAM_BUDGET="${UPSTREAM_BUDGET:-0}" \
LEASE_DURATION="${LEASE_DURATION:-15s}" \
BACKEND_WRITE_DELAY="${BACKEND_WRITE_DELAY:-0s}" \
  envsubst < "${EXP}/manifests/30-aggexp-statefulset.yaml" | kubectl apply -f - >/dev/null

kubectl -n aggexp-system rollout restart statefulset/aggexp >/dev/null
kubectl -n aggexp-system rollout status statefulset/aggexp --timeout=180s
echo "rolled: mode=${WATCH_MODE:-push} sharedPoll=${SHARED_POLL:-false} pollInterval=${POLL_INTERVAL:-5s} upstreamBudget=${UPSTREAM_BUDGET:-0} leaseDuration=${LEASE_DURATION:-15s} backendWriteDelay=${BACKEND_WRITE_DELAY:-0s}"
