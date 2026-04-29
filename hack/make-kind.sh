#!/usr/bin/env bash
set -euo pipefail

# Bring up the local `aggexp` kind cluster. No kind config file on purpose:
# the plan says defaults are fine for this lab. If we later need a registry
# or extra mounts we can add one, but YAGNI for now.

CLUSTER="aggexp"
NS="aggexp-system"

for bin in kind kubectl; do
  if ! command -v "${bin}" >/dev/null 2>&1; then
    echo "error: ${bin} not found in PATH" >&2
    exit 1
  fi
done

if kind get clusters 2>/dev/null | grep -qx "${CLUSTER}"; then
  echo "kind cluster '${CLUSTER}' already exists; skipping create"
else
  kind create cluster --name "${CLUSTER}"
fi

# kind create cluster sets kubectl context to kind-${CLUSTER}, but if the user
# switched contexts between invocations we don't want to silently operate on
# the wrong cluster. Pin explicitly.
CTX="kind-${CLUSTER}"

kubectl --context "${CTX}" get ns "${NS}" >/dev/null 2>&1 \
  || kubectl --context "${CTX}" create namespace "${NS}"

kubectl --context "${CTX}" cluster-info
echo "context: ${CTX}"
