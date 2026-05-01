#!/usr/bin/env bash
# Install ArgoCD and a toy Application for scenario 6.
set -euo pipefail
CTX="${CTX:-kind-aggexp-meta-crd}"
K="kubectl --context ${CTX}"
ARGO_VER="${ARGO_VER:-v3.0.12}"

$K get ns argocd >/dev/null 2>&1 || $K create namespace argocd
$K -n argocd apply -f "https://raw.githubusercontent.com/argoproj/argo-cd/${ARGO_VER}/manifests/install.yaml"
$K -n argocd rollout status deploy/argocd-application-controller --timeout=300s || true
$K -n argocd rollout status deploy/argocd-server --timeout=300s || true
echo "ArgoCD installed. Wait ~30s more for the application-controller to walk discovery."
