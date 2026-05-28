#!/usr/bin/env bash
set -euo pipefail

# Convenience deploy script for experiment 0034. Tears down the
# default Deployment from deploy/manifests (which conflicts with our
# StatefulSet on the name "aggexp") and applies the experiment
# manifests on top.

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
EXP="${ROOT}/experiments/0034-shared-watch-cross-replica"

cd "${ROOT}"

# Base manifests (SA + auth-delegator + APIService + base Service +
# default 1-replica Deployment). The Deployment gets deleted next.
./hack/deploy.sh deploy/manifests

# Tear down the default Deployment so we can install the StatefulSet
# under the same name.
kubectl -n aggexp-system delete deployment aggexp --ignore-not-found

# Build & load image (build context must be repo root for the
# `replace` directive to resolve ../..).
docker build -t aggexp-shared-watch:dev \
  -f "${EXP}/Dockerfile" .
kind load docker-image aggexp-shared-watch:dev --name aggexp-0034

# Experiment manifests.
AGGEXP_IMAGE=aggexp-shared-watch:dev \
  ./hack/deploy.sh "${EXP}/manifests"

# Wait for all replicas to become Ready.
kubectl -n aggexp-system rollout status statefulset/aggexp --timeout=120s

# Discovery cache flush (kubectl will otherwise hold a stale
# resource map for 10 minutes).
rm -rf "${HOME}/.kube/cache/discovery/" || true

echo
echo "--- replicas ---"
kubectl -n aggexp-system get pods -l app=aggexp -o wide
echo
echo "--- per-pod services ---"
kubectl -n aggexp-system get svc -l app=aggexp
echo
echo "--- APIService ---"
kubectl get apiservices v1.aggexp.io
