# Per-experiment extras script. Run AFTER the base hack/deploy.sh
# has rendered the experiment manifests through envsubst. This
# augments the aggexp-serving-cert Secret with ca.crt so the
# middleware can mount the CA next to its serving cert and stamp
# it into APIService.caBundle.
#
# The base hack/deploy.sh uses `kubectl create secret tls` which
# only stores tls.crt + tls.key. Rather than diverge the shared
# script, we patch the already-applied Secret with the third key
# here.
set -euo pipefail

CERT_DIR="${CERT_DIR:-deploy/certs}"
NS="${NS:-aggexp-system}"
SECRET="${SECRET:-aggexp-serving-cert}"

if [[ ! -f "${CERT_DIR}/ca.crt" ]]; then
  echo "error: missing ${CERT_DIR}/ca.crt; run hack/gen-certs.sh first" >&2
  exit 1
fi

CA_B64="$(base64 < "${CERT_DIR}/ca.crt" | tr -d '\n')"
kubectl -n "${NS}" patch secret "${SECRET}" --type='json' \
  -p "[{\"op\":\"add\",\"path\":\"/data/ca.crt\",\"value\":\"${CA_B64}\"}]"

echo "patched secret ${NS}/${SECRET} with ca.crt"
