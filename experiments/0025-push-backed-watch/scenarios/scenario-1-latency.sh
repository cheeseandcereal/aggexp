#!/usr/bin/env bash
# scenario-1-latency.sh — measures per-event latency between backend-side
# mutation (logged by the event generator) and kubectl-side observation.
#
# Assumes the variant (A or B) is already deployed and the event generator
# is running. Runs for 60s, captures both streams to temp files, then diffs.

set -euo pipefail

export KUBECONFIG="${KUBECONFIG:-/tmp/aggexp-wt/push-watch/.kube/config}"

VARIANT="${1:-unknown}"
DUR="${2:-60}"
OUT_DIR="${3:-/tmp/aggexp-0025-latency-${VARIANT}}"

mkdir -p "${OUT_DIR}"

echo "=== scenario 1 latency: variant=${VARIANT} duration=${DUR}s ==="

# Background 1: kubectl watch. We ask for JSON + --output-watch-events
# so every watch-event is framed as {"type":"...", "object":{...}}.
# kubectl doesn't natively timestamp, so wrap with `ts` from moreutils
# if available, else prepend the date ourselves.
{
  kubectl get notes -o json --watch --output-watch-events 2>/dev/null \
    | while IFS= read -r line; do
        printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" "${line}"
      done
} > "${OUT_DIR}/kubectl-watch.jsonl" &
WATCH_PID=$!

# Background 2: backend logs. Follow since now. Note: kubectl logs -f
# doesn't let us say "since now" exactly; use --since=1s.
kubectl -n aggexp-system logs -f deploy/note-backend --since=1s \
  > "${OUT_DIR}/backend.log" &
LOG_PID=$!

sleep "${DUR}"

kill "${WATCH_PID}" 2>/dev/null || true
kill "${LOG_PID}" 2>/dev/null || true
wait 2>/dev/null || true

echo "=== captured ==="
echo "kubectl watch events:"
wc -l "${OUT_DIR}/kubectl-watch.jsonl"
echo "backend log lines:"
wc -l "${OUT_DIR}/backend.log"
echo
echo "=== backend event-gen lines ==="
grep -E "^gen " "${OUT_DIR}/backend.log" || true
echo
echo "=== kubectl watch events (genrunner only) ==="
grep -E 'genrunner-' "${OUT_DIR}/kubectl-watch.jsonl" \
  | head -30 \
  | while IFS= read -r line; do
      ts="${line%% *}"
      rest="${line#* }"
      type=$(printf '%s' "${rest}" | sed -nE 's/.*"type":"([A-Z]+)".*/\1/p')
      name=$(printf '%s' "${rest}" | sed -nE 's/.*"name":"([^"]+)".*/\1/p' | head -1)
      body=$(printf '%s' "${rest}" | sed -nE 's/.*"body":"([^"]*)".*/\1/p')
      printf '%s %s name=%s body=%s\n' "${ts}" "${type}" "${name}" "${body}"
    done
echo
echo "--- output saved to ${OUT_DIR}/ ---"
