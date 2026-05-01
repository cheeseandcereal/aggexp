#!/usr/bin/env bash
# Runs the six scenarios against the already-deployed 0024 cluster.
# Usage: hack/scenarios.sh  (from the experiment directory)
set -euo pipefail

CTX="${CTX:-kind-aggexp-meta-crd}"
K="kubectl --context ${CTX}"

printf '\n==== Scenario 1: Create + list ====\n'
$K apply -f - <<'YAML'
apiVersion: aggexp.io/v1
kind: Bucket
metadata:
  name: sc1-create
spec:
  region: us-east-1
  tags: {env: demo}
YAML
sleep 2
$K get buckets
$K get bucket sc1-create -o yaml
echo "--- ResourceMetadata record for sc1-create ---"
$K get resourcemetadatas -o wide || true

printf '\n==== Scenario 2: Server-Side Apply ====\n'
$K apply --server-side --field-manager=user-a -f - <<'YAML'
apiVersion: aggexp.io/v1
kind: Bucket
metadata:
  name: sc2-ssa
spec:
  region: us-east-1
  tags: {owner: alice}
YAML
sleep 1
echo "--- managedFields (via --show-managed-fields) ---"
$K get bucket sc2-ssa --show-managed-fields -o yaml | grep -A4 managedFields || true
echo "--- conflict test: different manager, same fields ---"
$K apply --server-side --field-manager=user-b -f - <<'YAML' || echo "[expected]: conflict"
apiVersion: aggexp.io/v1
kind: Bucket
metadata:
  name: sc2-ssa
spec:
  region: us-west-2
  tags: {owner: bob}
YAML

printf '\n==== Scenario 3: Finalizer round trip ====\n'
$K apply -f - <<'YAML'
apiVersion: aggexp.io/v1
kind: Bucket
metadata:
  name: sc3-finalizer
spec:
  region: us-east-1
YAML
sleep 1
$K patch bucket sc3-finalizer --type=merge -p '{"metadata":{"finalizers":["lab.aggexp.io/test"]}}'
echo "--- delete blocks because finalizer is set ---"
$K delete bucket sc3-finalizer --wait=false
$K get bucket sc3-finalizer -o json | grep -E 'deletionTimestamp|finalizers' || true
echo "--- clear finalizer; deletion completes ---"
$K patch bucket sc3-finalizer --type=merge -p '{"metadata":{"finalizers":[]}}'
sleep 2
$K get bucket sc3-finalizer 2>&1 | head -2 || true

printf '\n==== Scenario 4: OwnerReferences ====\n'
$K apply -f - <<'YAML'
apiVersion: v1
kind: ConfigMap
metadata:
  name: sc4-owner
  namespace: default
data: {k: v}
YAML
$K apply -f - <<'YAML'
apiVersion: aggexp.io/v1
kind: Bucket
metadata:
  name: sc4-owned
spec:
  region: us-east-1
YAML
CMUID=$($K get configmap sc4-owner -n default -o jsonpath='{.metadata.uid}')
$K patch bucket sc4-owned --type=merge -p "{\"metadata\":{\"ownerReferences\":[{\"apiVersion\":\"v1\",\"kind\":\"ConfigMap\",\"name\":\"sc4-owner\",\"uid\":\"${CMUID}\"}]}}"
$K get bucket sc4-owned -o json | grep -B1 -A5 ownerReferences || true
echo "--- deleting ConfigMap; does Bucket GC? (expected: no, cross-group GC is a known limit) ---"
$K delete configmap sc4-owner -n default
sleep 3
$K get bucket sc4-owned || true

printf '\n==== Scenario 5: Labels/annotations round-trip ====\n'
$K apply -f - <<'YAML'
apiVersion: aggexp.io/v1
kind: Bucket
metadata:
  name: sc5-labels
spec:
  region: us-east-1
YAML
$K label bucket sc5-labels tier=production
$K annotate bucket sc5-labels demo.aggexp.io/note="hello-metadata"
$K get bucket sc5-labels -o json | grep -B0 -A6 -e labels -e annotations || true

printf '\n==== Scenario 6: ArgoCD visibility (manual; requires ArgoCD installed) ====\n'
echo "Run: hack/install-argocd.sh and then inspect:"
echo "  $K get applications -n argocd"
echo "  $K logs -n argocd deploy/argocd-application-controller | grep -i aggexpmeta"
echo "  $K get resourcemetadatas -o wide"
