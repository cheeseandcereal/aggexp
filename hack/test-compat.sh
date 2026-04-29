#!/usr/bin/env bash
set -euo pipefail

# Compatibility scoreboard for the deployed AA. The point of this script is
# to probe the apiserver the way a real operator would (kubectl, not curl)
# and record which bits of the machine/human contract actually work. Each
# probe is tagged:
#   expect  -> must pass; failure is a real bug (script exits non-zero)
#   observe -> informational; we record the result but never gate on it
#
# The rationale lives in VISION.md / EXPERIMENTS.md; in short, we want a
# durable record of regressions as experiments land.

GROUP="aggexp.io"
VERSION="v1"
RESOURCE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --group)    GROUP="$2";    shift 2 ;;
    --version)  VERSION="$2";  shift 2 ;;
    --resource) RESOURCE="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

# Results accumulator. Each entry is: check<TAB>tag<TAB>result<TAB>notes
RESULTS=()
FAIL=0

record() {
  local check="$1" tag="$2" result="$3" notes="$4"
  RESULTS+=("${check}"$'\t'"${tag}"$'\t'"${result}"$'\t'"${notes}")
  if [[ "${tag}" == "expect" && "${result}" != "PASS" ]]; then
    FAIL=1
  fi
  printf '[%s] %-6s %s -- %s\n' "${result}" "${tag}" "${check}" "${notes}"
}

# --- resource discovery -----------------------------------------------------
# If the caller didn't pin a resource, ask discovery for the first one in
# the group. This is best-effort: if discovery is broken we still want the
# rest of the probes to run and record their own failures.
if [[ -z "${RESOURCE}" ]]; then
  RESOURCE="$(kubectl api-resources --api-group="${GROUP}" --no-headers 2>/dev/null \
    | awk 'NR==1 {print $1}')"
fi

# --- probe: api-resources lists the group -----------------------------------
if kubectl api-resources --api-group="${GROUP}" --no-headers 2>/dev/null | grep -q .; then
  record "api-resources lists ${GROUP}" "expect" "PASS" "group present in discovery"
else
  record "api-resources lists ${GROUP}" "expect" "FAIL" "group absent from discovery"
fi

# The remaining probes need a resource name. If discovery didn't give us
# one, skip them rather than spraying confusing errors.
if [[ -z "${RESOURCE}" ]]; then
  record "kubectl get <resource>"        "expect"  "SKIP" "no resource in group ${GROUP}"
  record "kubectl explain <resource>"    "expect"  "SKIP" "no resource in group ${GROUP}"
  record "kubectl get -w streams"        "expect"  "SKIP" "no resource in group ${GROUP}"
  record "kubectl apply (client)"        "expect"  "SKIP" "no resource in group ${GROUP}"
  record "kubectl apply --server-side"   "observe" "SKIP" "no resource in group ${GROUP}"
else
  # --- probe: kubectl get returns a table -----------------------------------
  if out="$(kubectl get "${RESOURCE}" -A 2>&1)"; then
    record "kubectl get ${RESOURCE}" "expect" "PASS" "returned output"
  else
    record "kubectl get ${RESOURCE}" "expect" "FAIL" "${out%%$'\n'*}"
  fi

  # --- probe: kubectl explain -----------------------------------------------
  if out="$(kubectl explain "${RESOURCE}" 2>&1)"; then
    record "kubectl explain ${RESOURCE}" "expect" "PASS" "openapi served"
  else
    record "kubectl explain ${RESOURCE}" "expect" "FAIL" "${out%%$'\n'*}"
  fi

  # --- probe: apply a minimal Hello object ----------------------------------
  # We hardcode a Hello example because the whole point is to exercise the
  # default `aggexp.io/v1` contract. If the kind isn't registered, that's
  # interesting but not a test failure -- we SKIP the apply probes. The
  # expected schema has `spec.greeting` per the hellos.aggexp.io/v1 contract
  # established by experiments 0001 and 0002.
  HELLO_YAML="$(cat <<EOF
apiVersion: ${GROUP}/${VERSION}
kind: Hello
metadata:
  name: compat-probe
spec:
  greeting: hi
EOF
)"
  APPLY_RAN=0
  if kubectl api-resources --api-group="${GROUP}" --no-headers 2>/dev/null \
       | awk '{print $NF}' | grep -qx "Hello"; then
    if out="$(printf '%s\n' "${HELLO_YAML}" | kubectl apply -f - 2>&1)"; then
      record "kubectl apply Hello" "expect" "PASS" "${out%%$'\n'*}"
      APPLY_RAN=1
    else
      record "kubectl apply Hello" "expect" "FAIL" "${out%%$'\n'*}"
    fi
    if out="$(printf '%s\n' "${HELLO_YAML}" | kubectl apply --server-side=true --force-conflicts -f - 2>&1)"; then
      record "kubectl apply --server-side Hello" "observe" "PASS" "${out%%$'\n'*}"
    else
      record "kubectl apply --server-side Hello" "observe" "FAIL" "${out%%$'\n'*}"
    fi
  else
    # When the experiment hasn't registered a Hello kind (e.g. 0004 has
    # Repo instead), the write probes don't apply. Record as observe
    # SKIP so they don't gate the scoreboard; operator can read the
    # notes to see what happened.
    record "kubectl apply Hello"              "observe" "SKIP" "Hello kind not registered (this experiment uses a different kind)"
    record "kubectl apply --server-side Hello" "observe" "SKIP" "Hello kind not registered"
  fi
  # APPLY_RAN is informational for future expansions that want to skip
  # follow-up probes when the seed apply didn't happen; referenced but
  # not branched on today.
  : "${APPLY_RAN}"

  # --- probe: watch streams within 5s ---------------------------------------
  # kubectl get -w exits 0 only on signal; we just check it emits something
  # within the window. `timeout` isn't universal (macOS) so we background,
  # sleep, and kill by PID ourselves. Runs after the apply probe so the
  # initial ADDED event provides immediate output for a server that honors
  # level-consistent watch semantics.
  WATCH_OUT="$(mktemp)"
  kubectl get "${RESOURCE}" -A -w >"${WATCH_OUT}" 2>&1 &
  WATCH_PID=$!
  sleep 5
  kill "${WATCH_PID}" 2>/dev/null || true
  wait "${WATCH_PID}" 2>/dev/null || true
  if [[ -s "${WATCH_OUT}" ]]; then
    record "kubectl get ${RESOURCE} -w" "expect" "PASS" "stream produced output in 5s"
  else
    record "kubectl get ${RESOURCE} -w" "expect" "FAIL" "no output within 5s"
  fi
  rm -f "${WATCH_OUT}"
fi

# --- probe: APIService Available condition ---------------------------------
APISVC="${VERSION}.${GROUP}"
if cond="$(kubectl get apiservice "${APISVC}" \
      -o jsonpath='{.status.conditions[?(@.type=="Available")].status}' 2>/dev/null)"; then
  if [[ "${cond}" == "True" ]]; then
    record "APIService ${APISVC} Available" "expect" "PASS" "Available=True"
  else
    record "APIService ${APISVC} Available" "expect" "FAIL" "Available=${cond:-<missing>}"
  fi
else
  record "APIService ${APISVC} Available" "expect" "FAIL" "APIService not found"
fi

# --- cleanup the probe object so repeated runs don't accumulate state ------
if [[ -n "${RESOURCE}" ]]; then
  kubectl delete "${RESOURCE}" compat-probe --ignore-not-found=true 2>/dev/null || true
fi

# --- write markdown report --------------------------------------------------
OUT_DIR="FINDINGS/compat"
mkdir -p "${OUT_DIR}"
DATE="$(date -u +%Y-%m-%d)"
OUT="${OUT_DIR}/${DATE}.md"
# If today's file already exists, append a -NN suffix. We don't overwrite
# previous runs because the whole point of the scoreboard is the trail.
if [[ -e "${OUT}" ]]; then
  n=1
  while [[ -e "${OUT_DIR}/${DATE}-$(printf '%02d' "${n}").md" ]]; do
    n=$((n+1))
  done
  OUT="${OUT_DIR}/${DATE}-$(printf '%02d' "${n}").md"
fi

{
  echo "# compat scoreboard — ${DATE}"
  echo
  echo "Group: \`${GROUP}\`  Version: \`${VERSION}\`  Resource: \`${RESOURCE:-<none>}\`"
  echo
  echo "| Check | Result | Notes |"
  echo "| --- | --- | --- |"
  for row in "${RESULTS[@]}"; do
    IFS=$'\t' read -r check tag result notes <<<"${row}"
    echo "| ${check} (${tag}) | ${result} | ${notes} |"
  done
} | tee "${OUT}"

echo
echo "wrote: ${OUT}"
exit "${FAIL}"
