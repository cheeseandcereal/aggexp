#!/usr/bin/env bash
set -euo pipefail

# pin-replica.sh: flip the v1.aggexp.io APIService to point at a
# specific replica's per-pod Service, or back to the load-balancing
# `aggexp` Service.
#
# Usage: pin-replica.sh aggexp-0   # all kubectl traffic -> pod 0
#        pin-replica.sh lb         # load-balanced (default)
#
# Requires the kind-aggexp-0043 context to be current.

target="${1:-lb}"
case "${target}" in
  aggexp-0|aggexp-1|aggexp-2)
    svc="${target}"
    ;;
  lb|aggexp)
    svc="aggexp"
    ;;
  *)
    echo "usage: $0 {aggexp-0|aggexp-1|aggexp-2|lb}" >&2
    exit 1
    ;;
esac

KUBECTL=(kubectl --context kind-aggexp-0043)

"${KUBECTL[@]}" patch apiservice v1.aggexp.io --type=merge \
  -p "{\"spec\":{\"service\":{\"namespace\":\"aggexp-system\",\"name\":\"${svc}\",\"port\":443}}}"

# Wait for the apiservice to settle. kube-apiserver re-probes the
# new endpoint on each Available check; this can take 5-15s.
for i in $(seq 1 30); do
  status=$("${KUBECTL[@]}" get apiservice v1.aggexp.io -o jsonpath='{.status.conditions[?(@.type=="Available")].status}' 2>/dev/null || true)
  if [[ "${status}" == "True" ]]; then
    echo "APIService now backed by service/${svc} (Available=True)"
    exit 0
  fi
  sleep 1
done
echo "warning: APIService did not return to Available within 30s after pinning to ${svc}"
exit 1
