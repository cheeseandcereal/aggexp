#!/usr/bin/env bash
# Run the 0031 v2-parity scenario sweep. Captures outputs into a log
# file the FINDINGS references. Intentionally chatty.
set -u  # NOT -e; we want to see probes fail loudly rather than abort.
set +u  # (temporarily disabled — the `&` background PID capture
        # triggers `$!: unbound` otherwise; minor bash trap.)
LOG="${LOG:-experiments/0031-runtime-component-v2-parity/hack/scenarios.log}"
: > "${LOG}"

note() {
  printf '\n### %s\n\n' "$*" | tee -a "${LOG}"
}

run() {
  printf '$ %s\n' "$*" | tee -a "${LOG}"
  "$@" 2>&1 | tee -a "${LOG}"
  printf '\n' | tee -a "${LOG}"
}

note "v2 parity sweep @ $(date -u +%FT%TZ) against kind-aggexp-0031"

note "1. discovery — both APIs visible"
run kubectl api-resources --api-group=widgets.aggexp.io
run kubectl api-resources --api-group=gadgets.aggexp.io

note "2. APIDefinitions Ready"
run kubectl get apidefinitions

note "3. APIServices Available"
run kubectl get apiservices v1.widgets.aggexp.io v1.gadgets.aggexp.io

note "4. create — widget (HTTP backend, push watch)"
run kubectl apply -f experiments/0031-runtime-component-v2-parity/samples/widget-instance.yaml

note "5. create — gadget (gRPC backend, poll watch)"
run kubectl apply -f experiments/0031-runtime-component-v2-parity/samples/gadget-instance.yaml

note "6. kubectl get — table rendering on both"
run kubectl get widgets
run kubectl get gadgets

note "7. kubectl get -o yaml — stitched metadata visible (uid, resourceVersion)"
run kubectl get widget sample-widget -o yaml
run kubectl get gadget sample-gadget -o yaml

note "8. ResourceMetadata records written (0024 metastore)"
run kubectl get resourcemetadatas

note "9. watch streams — widget (push), gadget (poll)"
kubectl get widgets -w >> "${LOG}" 2>&1 &
WPID=$!
kubectl get gadgets -w >> "${LOG}" 2>&1 &
GPID=$!
sleep 3
# trigger a modification
kubectl annotate widget sample-widget demo/touched="$(date -u +%s)" --overwrite 2>&1 | tee -a "${LOG}"
kubectl annotate gadget sample-gadget demo/touched="$(date -u +%s)" --overwrite 2>&1 | tee -a "${LOG}"
sleep 3
kill "${WPID}" "${GPID}" 2>/dev/null || true
wait "${WPID}" "${GPID}" 2>/dev/null || true

note "10. declarative admission — denial path (widgets color=black)"
cat >/tmp/widget-black.yaml <<'EOF'
apiVersion: widgets.aggexp.io/v1
kind: Widget
metadata: { name: forbidden-widget, namespace: default }
spec: { color: black, size: 1 }
EOF
run kubectl apply -f /tmp/widget-black.yaml
rm -f /tmp/widget-black.yaml

note "11. declarative admission — mutation default (title defaults to 'Untitled')"
cat >/tmp/widget-notitle.yaml <<'EOF'
apiVersion: widgets.aggexp.io/v1
kind: Widget
metadata: { name: titleless-widget, namespace: default }
spec: { color: green, size: 7 }
EOF
run kubectl apply -f /tmp/widget-notitle.yaml
run kubectl get widget titleless-widget -o jsonpath='{.spec.title}'
printf '\n' | tee -a "${LOG}"
rm -f /tmp/widget-notitle.yaml

note "12. kubectl explain — known gap in dynamic install mode"
# Expected to 404: dynamic-install groups aren't in /openapi/v3 per
# 0027/0030. We probe anyway to confirm the gap's location.
run kubectl explain widget || true
run kubectl explain widget.spec || true

note "13. kubectl apply --server-side — known gap in dynamic install mode"
# Expected to fail at managedfields typed-converter per 0027/0030.
run kubectl apply --server-side --field-manager=alice \
  -f experiments/0031-runtime-component-v2-parity/samples/widget-instance.yaml || true

note "14. delete"
run kubectl delete widget sample-widget --wait=false
run kubectl delete widget titleless-widget --wait=false
run kubectl delete gadget sample-gadget --wait=false

note "15. GC probes — wait for sweep to run, then check no orphan Records remain"
sleep 40
run kubectl get resourcemetadatas

note "16. cross-API isolation — deleting widgets didn't affect gadgets API"
run kubectl api-resources --api-group=gadgets.aggexp.io
run kubectl get apiservices v1.gadgets.aggexp.io -o=jsonpath='{.status.conditions[?(@.type=="Available")].status}'
printf '\n' | tee -a "${LOG}"

echo | tee -a "${LOG}"
echo "log written to ${LOG}" | tee -a "${LOG}"
