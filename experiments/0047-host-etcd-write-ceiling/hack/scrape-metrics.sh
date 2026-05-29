#!/usr/bin/env bash
set -euo pipefail

# scrape-metrics.sh — experiment 0047 measurement harness.
#
# Records, for a point in time, the host kube-apiserver / etcd write
# activity attributable to the composed metadata-CR design:
#
#   1. apiserver_request_total{resource=resourcemetadatas|widgetbodies}
#      broken down by verb (create/update/delete/patch) — the host
#      apiserver's view of the metadata-CRD + body-CRD write volume.
#   2. etcd_request_duration_seconds summary (the host-etcd latency
#      that degrades at the ceiling).
#   3. apiserver_storage_objects{resource=...} (object counts).
#   4. The AA-side per-kind write attribution from the aggexp-0047-
#      metrics klog line (lockAcquireCreate / lockAcquireUpdate /
#      lockRenew / lockRelease / commitRelease / delete), summed across
#      replicas — this is the intent breakdown the host metrics cannot
#      see (the host only knows "update", not "lock vs commit").
#
# All host-apiserver scrapes go through `kubectl get --raw /metrics`,
# which needs RBAC for the /metrics nonResourceURL (granted in
# manifests/00-permissive-rbac.yaml).
#
# Usage:
#   hack/scrape-metrics.sh [label]
# Pins the kube context to kind-aggexp-0047.

CTX="kind-aggexp-0047"
LABEL="${1:-snapshot}"
K="kubectl --context ${CTX}"

ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "==== scrape ${LABEL} @ ${ts} ===="

raw="$(${K} get --raw /metrics)"

echo "--- apiserver_request_total (metadata + body CRDs) ---"
echo "${raw}" \
  | grep '^apiserver_request_total{' \
  | grep -E 'resource="(resourcemetadatas|widgetbodies)"' \
  | sed -E 's/.*group="([^"]*)".*resource="([^"]*)".*verb="([^"]*)".*} ([0-9.e+]+)$/\2 \3 = \4/' \
  | sort | awk '{a[$1" "$2]+=$4} END{for(k in a) printf "  %-40s %s\n", k, a[k]}' \
  || echo "  (no matching series yet)"

echo "--- etcd_request_duration_seconds (count + sum, all ops) ---"
echo "${raw}" \
  | grep -E '^etcd_request_duration_seconds_(count|sum)' \
  | sed -E 's/\{.*\}/ /' \
  | awk '{a[$1]+=$2} END{for(k in a) printf "  %-40s %s\n", k, a[k]}' \
  || echo "  (none)"

echo "--- etcd_request_duration_seconds_bucket tail (le>=0.1s) ---"
echo "${raw}" \
  | grep '^etcd_request_duration_seconds_bucket' \
  | grep -E 'le="(0.1|0.25|0.5|1|2.5|\+Inf)"' \
  | sed -E 's/.*operation="([^"]*)".*le="([^"]*)".*} ([0-9.e+]+)$/\1 le=\2 \3/' \
  | sort | head -40 || echo "  (none)"

echo "--- apiserver_storage_objects (metadata + body CRDs) ---"
echo "${raw}" \
  | grep '^apiserver_storage_objects{' \
  | grep -E 'resource="(resourcemetadatas|widgetbodies)' \
  | sed -E 's/.*resource="([^"]*)".*} ([0-9.e+]+)$/  \1 = \2/' || echo "  (none)"

echo "--- AA-side metadata-CR write attribution (latest per replica, summed) ---"
# Pull the most recent aggexp-0047-metrics line from each AA pod and sum
# the metaWrites counters across replicas.
pods="$(${K} -n aggexp-system get pods -l app=aggexp -o name)"
declare -A SUM
for p in ${pods}; do
  line="$(${K} -n aggexp-system logs "${p}" --tail=400 2>/dev/null \
            | grep 'aggexp-0047-metrics' | tail -1 || true)"
  [ -z "${line}" ] && continue
  for kind in lockAcquireCreate lockAcquireUpdate lockRenew lockRelease commitRelease delete; do
    v="$(echo "${line}" | grep -oE "\"${kind}\":[0-9]+" | head -1 | grep -oE '[0-9]+$' || echo 0)"
    SUM[$kind]=$(( ${SUM[$kind]:-0} + ${v:-0} ))
  done
done
for kind in lockAcquireCreate lockAcquireUpdate lockRenew lockRelease commitRelease delete; do
  printf "  %-20s %s\n" "${kind}" "${SUM[$kind]:-0}"
done
total=$(( ${SUM[lockAcquireCreate]:-0} + ${SUM[lockAcquireUpdate]:-0} + ${SUM[lockRenew]:-0} + ${SUM[lockRelease]:-0} + ${SUM[commitRelease]:-0} + ${SUM[delete]:-0} ))
echo "  ---------------------------------"
printf "  %-20s %s\n" "TOTAL metaCR writes" "${total}"
echo
