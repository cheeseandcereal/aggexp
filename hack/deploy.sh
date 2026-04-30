#!/usr/bin/env bash
set -euo pipefail

# Deploy the aggregated apiserver manifests. Expects hack/gen-certs.sh and
# hack/make-kind.sh to have been run first. The serving cert goes into a
# Secret; the CA bundle is inlined into APIService manifests via envsubst.

MANIFESTS_DIR="${1:-deploy/manifests}"
CERT_DIR="deploy/certs"
NS="aggexp-system"
SECRET="aggexp-serving-cert"

for bin in kubectl envsubst; do
  if ! command -v "${bin}" >/dev/null 2>&1; then
    echo "error: ${bin} not found in PATH (envsubst comes from gettext)" >&2
    exit 1
  fi
done

for f in "${CERT_DIR}/tls.crt" "${CERT_DIR}/tls.key" "${CERT_DIR}/ca.crt"; do
  if [[ ! -f "${f}" ]]; then
    echo "error: missing ${f}; run hack/gen-certs.sh first" >&2
    exit 1
  fi
done

if [[ ! -d "${MANIFESTS_DIR}" ]]; then
  echo "error: manifests dir '${MANIFESTS_DIR}' does not exist" >&2
  exit 1
fi

# base64 -w0 is a GNU extension. macOS `base64` wraps by default and doesn't
# understand -w, so fall back to stripping newlines ourselves.
if base64 -w0 </dev/null >/dev/null 2>&1; then
  CA_BUNDLE="$(base64 -w0 < "${CERT_DIR}/ca.crt")"
else
  CA_BUNDLE="$(base64 < "${CERT_DIR}/ca.crt" | tr -d '\n')"
fi
export CA_BUNDLE

# envsubst doesn't honor ${VAR:-default} syntax; pre-set defaults for any
# variable referenced bare in the manifests. Experiments override by
# setting AGGEXP_IMAGE (and any future AGGEXP_* vars) before calling us.
: "${AGGEXP_IMAGE:=aggexp:dev}"
: "${POLICY_IMAGE:=aggexp-policy:dev}"
: "${GITHUB_OWNER:=}"
: "${GITHUB_TOKEN:=}"
: "${S3_MOCK_IMAGE:=aggexp-s3-mock:dev}"
: "${NOTE_BACKEND_IMAGE:=aggexp-note-backend:dev}"
: "${BACKEND_S3_IMAGE:=aggexp-backend-s3:dev}"
export AGGEXP_IMAGE POLICY_IMAGE GITHUB_OWNER GITHUB_TOKEN S3_MOCK_IMAGE NOTE_BACKEND_IMAGE BACKEND_S3_IMAGE

# Upsert serving-cert secret. `kubectl apply` on a generated secret via
# --dry-run=client lets this be idempotent without read-modify-write dance.
kubectl create secret tls "${SECRET}" \
  --cert="${CERT_DIR}/tls.crt" --key="${CERT_DIR}/tls.key" \
  -n "${NS}" --dry-run=client -o yaml \
  | kubectl apply -f -

# Apply each YAML with env var substitution. We only substitute references
# to shell-style ${VAR}s; envsubst does exactly that.
shopt -s nullglob
files=( "${MANIFESTS_DIR}"/*.yaml "${MANIFESTS_DIR}"/*.yml )
shopt -u nullglob
if [[ ${#files[@]} -eq 0 ]]; then
  echo "warning: no YAML files found in ${MANIFESTS_DIR}"
else
  for f in "${files[@]}"; do
    envsubst < "${f}" | kubectl apply -f -
  done
fi

echo "--- APIService status ---"
kubectl get apiservices 2>/dev/null | grep aggexp || true
