#!/usr/bin/env bash
# scenario-3-kubectl-wait.sh — the 0011 failing test.
#
# Creates a Note, then runs `kubectl wait --for=jsonpath=...` to verify
# the WatchList bookmark path works. 0011 found this to always time out
# against the substrate because the substrate doesn't emit the
# initial-events-end BOOKMARK. This experiment's custom rest package
# does emit it; we should now observe the wait succeed.

set -euo pipefail

export KUBECONFIG="${KUBECONFIG:-/tmp/aggexp-wt/push-watch/.kube/config}"
VARIANT="${1:-unknown}"
NAME="kw-${VARIANT}-$(date +%s)"

echo "=== variant=${VARIANT} creating note ${NAME} ==="
cat <<EOF | kubectl apply -f -
apiVersion: aggexp.io/v1
kind: Note
metadata:
  name: ${NAME}
  namespace: default
spec:
  title: "kubectl-wait-probe"
  body: "initial"
EOF

echo
echo "=== kubectl wait --for=jsonpath='{.spec.body}=initial' (should succeed fast) ==="
T0=$(date -u +%s.%N)
# sendInitialEvents path: wait for a value we already have.
if timeout 60 kubectl wait --for=jsonpath='{.spec.body}=initial' \
     "note/${NAME}" --timeout=45s 2>&1; then
  T1=$(date -u +%s.%N)
  awk -v a="${T0}" -v b="${T1}" 'BEGIN{printf "PASS in %.3fs\n", b-a}'
else
  T1=$(date -u +%s.%N)
  awk -v a="${T0}" -v b="${T1}" 'BEGIN{printf "FAIL (timed out) after %.3fs\n", b-a}'
fi

echo
echo "=== raw WatchList probe: allowWatchBookmarks + sendInitialEvents ==="
timeout 5 kubectl get --raw \
  "/apis/aggexp.io/v1/namespaces/default/notes?watch=true&sendInitialEvents=true&resourceVersionMatch=NotOlderThan&allowWatchBookmarks=true" \
  2>&1 | head -10 || true

kubectl delete note "${NAME}" --wait=false 2>&1 || true
