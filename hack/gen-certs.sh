#!/usr/bin/env bash
set -euo pipefail

# Generate a self-signed CA and a server cert for the aggregated apiserver.
# We use openssl only to keep the dependency footprint tiny (no cfssl, no
# cert-manager). Validity is 10 years because this is a throwaway lab.

CERT_DIR="deploy/certs"
FORCE=0
if [[ "${1:-}" == "--force" ]]; then
  FORCE=1
fi

if [[ -f "${CERT_DIR}/ca.crt" && "${FORCE}" -eq 0 ]]; then
  echo "certs already present at ${CERT_DIR} (use --force to regenerate)"
  exit 0
fi

mkdir -p "${CERT_DIR}"

CN_SERVICE="aggexp.aggexp-system.svc"

# --- CA ---------------------------------------------------------------------
openssl genrsa -out "${CERT_DIR}/ca.key" 2048 2>/dev/null
openssl req -x509 -new -nodes -key "${CERT_DIR}/ca.key" \
  -subj "/CN=aggexp-ca" \
  -days 3650 -out "${CERT_DIR}/ca.crt" 2>/dev/null

# --- Server cert ------------------------------------------------------------
# SANs must match whatever the apiserver will dial. The in-cluster Service DNS
# names are what actually matter; localhost/127.0.0.1 are for ad-hoc port-forward.
openssl genrsa -out "${CERT_DIR}/tls.key" 2048 2>/dev/null

OPENSSL_CFG="$(mktemp)"
trap 'rm -f "${OPENSSL_CFG}"' EXIT

cat > "${OPENSSL_CFG}" <<EOF
[req]
distinguished_name = dn
req_extensions     = v3_req
prompt             = no
[dn]
CN = ${CN_SERVICE}
[v3_req]
keyUsage         = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName   = @alt_names
[alt_names]
DNS.1 = aggexp.aggexp-system.svc
DNS.2 = aggexp.aggexp-system.svc.cluster.local
DNS.3 = aggexp
DNS.4 = localhost
IP.1  = 127.0.0.1
EOF

openssl req -new -key "${CERT_DIR}/tls.key" \
  -out "${CERT_DIR}/tls.csr" \
  -config "${OPENSSL_CFG}" 2>/dev/null

openssl x509 -req -in "${CERT_DIR}/tls.csr" \
  -CA "${CERT_DIR}/ca.crt" -CAkey "${CERT_DIR}/ca.key" -CAcreateserial \
  -out "${CERT_DIR}/tls.crt" -days 3650 \
  -extensions v3_req -extfile "${OPENSSL_CFG}" 2>/dev/null

# Tidy: CSR and serial file are build artifacts, not outputs.
rm -f "${CERT_DIR}/tls.csr" "${CERT_DIR}/ca.srl"

echo "wrote:"
echo "  ${CERT_DIR}/ca.crt"
echo "  ${CERT_DIR}/ca.key"
echo "  ${CERT_DIR}/tls.crt"
echo "  ${CERT_DIR}/tls.key"
