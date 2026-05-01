#!/usr/bin/env bash
# Runs the 0028 GC demonstration scenarios.
# Usage: hack/scenarios.sh (from the experiment directory).
#
# Expects the cluster + workloads to be up per README "How to run".
set -euo pipefail

CTX="${CTX:-kind-aggexp-0028}"
K="kubectl --context ${CTX}"
NS="aggexp-system"

# Start a background kubectl port-forward to the GC debug service.
# Kill it on exit.
PF_PID=""
cleanup_pf() {
  if [[ -n "${PF_PID}" ]] && kill -0 "${PF_PID}" 2>/dev/null; then
    kill "${PF_PID}" 2>/dev/null || true
    wait "${PF_PID}" 2>/dev/null || true
  fi
}
trap cleanup_pf EXIT

start_pf() {
  cleanup_pf
  $K -n ${NS} port-forward svc/aggexp-gc-debug 18444:8444 >/tmp/gc-pf.log 2>&1 &
  PF_PID=$!
  # wait until healthy
  for _ in $(seq 1 30); do
    if curl -fsS http://127.0.0.1:18444/healthz >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  echo "port-forward never came up; see /tmp/gc-pf.log" >&2
  return 1
}

run_gc() {
  local label="$1"
  printf '\n--- GC sweep (%s) ---\n' "${label}"
  curl -fsS -X POST http://127.0.0.1:18444/gc/run
  printf '\n'
}

# ===== Happy path =====
printf '\n==== Happy path: no orphans ====\n'
start_pf

# Clean slate.
$K delete buckets --all --ignore-not-found --wait=true --timeout=20s || true
$K delete resourcemetadatas --all --ignore-not-found --wait=true --timeout=20s || true

$K apply -f - <<'YAML'
apiVersion: aggexp.io/v1
kind: Bucket
metadata:
  name: alpha
spec:
  region: us-east-1
YAML
$K apply -f - <<'YAML'
apiVersion: aggexp.io/v1
kind: Bucket
metadata:
  name: beta
spec:
  region: us-east-1
YAML
$K apply -f - <<'YAML'
apiVersion: aggexp.io/v1
kind: Bucket
metadata:
  name: gamma
spec:
  region: us-east-1
YAML

sleep 25  # wait past gc-min-age=20s so records are GC-eligible
echo "--- state before GC ---"
$K get buckets
$K get resourcemetadatas
run_gc "happy path"
echo "--- state after GC ---"
$K get buckets
$K get resourcemetadatas

# ===== Partial orphan =====
printf '\n==== Partial orphan: one of three CRs orphaned ====\n'
# Wipe one bucket out-of-band directly against the s3-mock. The
# backend's List will then show only alpha and gamma; its metadata
# Record for beta becomes an orphan.
echo "--- wiping 'beta' out-of-band via s3-mock DELETE ---"
$K -n ${NS} exec deploy/backend-s3 -- /bin/sh -c "wget -q -O- --method=DELETE http://s3-mock.aggexp-system.svc/beta || true" 2>/dev/null || {
  # Fallback: exec into s3-mock directly isn't easy since distroless.
  # Use a temporary curl pod in the namespace.
  $K -n ${NS} run curl-tmp --rm -i --restart=Never --image=curlimages/curl:8.10.1 -- \
    -s -X DELETE http://s3-mock.aggexp-system.svc/beta
}

# confirm backend no longer sees beta
sleep 2
echo "--- backend-s3 list (via kubectl get buckets; should show only alpha+gamma after a backend poll) ---"
# backend-s3 poll-interval=15s in the manifest; wait past it so the
# Watch stream catches up, but more importantly GC does a fresh List.
sleep 16
$K get buckets
echo "--- metastore still has the beta Record (orphan) ---"
$K get resourcemetadatas
run_gc "partial orphan"
echo "--- state after GC (beta's Record should be gone) ---"
$K get resourcemetadatas
$K get buckets

# ===== Full wipe =====
printf '\n==== Full backend wipe: every CR orphaned ====\n'
echo "--- deleting s3-mock pod; restarts empty ---"
$K -n ${NS} delete pod -l app=s3-mock --wait=false
# Wait for a new ready s3-mock and for backend-s3 to re-list.
$K -n ${NS} rollout status deploy/s3-mock --timeout=60s
sleep 16  # let backend-s3 poll

echo "--- buckets view (should be empty after backend re-list) ---"
$K get buckets || true
echo "--- ResourceMetadata still has the orphan records ---"
$K get resourcemetadatas

start_pf
run_gc "full wipe"
echo "--- state after GC ---"
$K get resourcemetadatas

# ===== Finalizer protection =====
printf '\n==== Finalizer protection: orphan with a finalizer is not collected ====\n'
$K apply -f - <<'YAML'
apiVersion: aggexp.io/v1
kind: Bucket
metadata:
  name: sticky
spec:
  region: us-east-1
YAML
sleep 2
$K patch bucket sticky --type=merge -p '{"metadata":{"finalizers":["lab.aggexp.io/gc-test"]}}'
sleep 3

# Wipe the backend again.
$K -n ${NS} delete pod -l app=s3-mock --wait=false
$K -n ${NS} rollout status deploy/s3-mock --timeout=60s
sleep 16

echo "--- state before GC ---"
$K get resourcemetadatas
start_pf
run_gc "finalizer-protected orphan"
echo "--- state after GC (sticky's Record should still be present, marked skipped) ---"
$K get resourcemetadatas

# Clean up sticky.
$K patch bucket sticky --type=merge -p '{"metadata":{"finalizers":[]}}' || true
sleep 2
$K delete bucket sticky --wait=false --ignore-not-found || true
run_gc "final cleanup"
$K get resourcemetadatas || true

echo
echo "==== Done ===="
