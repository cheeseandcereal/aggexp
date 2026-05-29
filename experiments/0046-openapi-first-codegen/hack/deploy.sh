#!/usr/bin/env bash
set -euo pipefail

# Build the verify-aa image, load it into the kind cluster aggexp-0046,
# create the serving-cert secret, and apply the manifests.
#
# Prerequisites:
#   - kind cluster `aggexp-0046` exists.
#   - hack/gen-certs.sh has been run at the repo root (deploy/certs).
#   - namespace aggexp-system exists.
#
# Run from anywhere; paths are resolved relative to the repo root.

CLUSTER="aggexp-0046"
CONTEXT="kind-${CLUSTER}"
IMAGE="aggexp-0046:latest"
NS="aggexp-system"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${EXP_DIR}/../.." && pwd)"

CERT_DIR="${REPO_ROOT}/deploy/certs"
for f in "${CERT_DIR}/tls.crt" "${CERT_DIR}/tls.key"; do
  if [[ ! -f "${f}" ]]; then
    echo "error: missing ${f}; run hack/gen-certs.sh at the repo root first" >&2
    exit 1
  fi
done

echo "==> building image ${IMAGE} (build context = repo root)"
docker build -t "${IMAGE}" -f "${EXP_DIR}/Dockerfile" "${REPO_ROOT}"

echo "==> loading image into kind cluster ${CLUSTER}"
kind load docker-image "${IMAGE}" --name "${CLUSTER}"

echo "==> ensuring namespace ${NS}"
kubectl --context "${CONTEXT}" create namespace "${NS}" \
  --dry-run=client -o yaml | kubectl --context "${CONTEXT}" apply -f -

echo "==> creating serving-cert secret aggexp-certs"
kubectl --context "${CONTEXT}" -n "${NS}" create secret tls aggexp-certs \
  --cert="${CERT_DIR}/tls.crt" --key="${CERT_DIR}/tls.key" \
  --dry-run=client -o yaml | kubectl --context "${CONTEXT}" apply -f -

echo "==> applying manifests"
kubectl --context "${CONTEXT}" apply -f "${EXP_DIR}/manifests/"

echo "==> waiting for pod readiness"
kubectl --context "${CONTEXT}" -n "${NS}" wait \
  --for=condition=Ready pod -l app=aggexp --timeout=90s || true

echo "--- APIService status ---"
kubectl --context "${CONTEXT}" get apiservices | grep widgets.aggexp.io || true
