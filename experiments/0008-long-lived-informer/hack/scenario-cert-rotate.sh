#!/usr/bin/env bash
set -euo pipefail
# Scenario 3: rotate certs and restart AA.
CTX="${CTX:-kind-aggexp-informer}"
cd "$(git rev-parse --show-toplevel)"
./hack/gen-certs.sh --force
kubectl --context "${CTX}" -n aggexp-system delete secret aggexp-serving-cert --ignore-not-found
./hack/deploy.sh deploy/manifests
# Also the APIService caBundle must be re-applied because CA changed.
kubectl --context "${CTX}" -n aggexp-system rollout restart deploy/aggexp
kubectl --context "${CTX}" -n aggexp-system rollout status deploy/aggexp
