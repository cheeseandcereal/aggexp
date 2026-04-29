#!/usr/bin/env bash
set -euo pipefail
# Scenario 4: hammer the AA with create/update/delete while the watcher
# is configured with WATCHER_SLEEP_MS to simulate a slow handler. Run
# AFTER patching the watcher Deployment to set WATCHER_SLEEP_MS.
CTX="${CTX:-kind-aggexp-informer}"
N="${N:-200}"
for i in $(seq 1 "${N}"); do
  kubectl --context "${CTX}" apply -f - >/dev/null <<YAML
apiVersion: aggexp.io/v1
kind: Hello
metadata:
  name: load-${i}
spec:
  greeting: "load greeting ${i}"
YAML
done
echo "created ${N} hellos"
for i in $(seq 1 "${N}"); do
  kubectl --context "${CTX}" delete hello "load-${i}" --ignore-not-found >/dev/null
done
echo "deleted ${N} hellos"
