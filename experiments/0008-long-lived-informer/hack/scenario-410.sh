#!/usr/bin/env bash
set -euo pipefail
# Scenario 2: kill the AA, wait, bring it back.
# Expected: watcher logs a watch error / 410, relists, sees fresh UIDs.
CTX="${CTX:-kind-aggexp-informer}"
echo "--- scale aggexp to 0 ---"
kubectl --context "${CTX}" -n aggexp-system scale deploy/aggexp --replicas=0
sleep 10
echo "--- scale aggexp back to 1 ---"
kubectl --context "${CTX}" -n aggexp-system scale deploy/aggexp --replicas=1
kubectl --context "${CTX}" -n aggexp-system rollout status deploy/aggexp
