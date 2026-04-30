#!/usr/bin/env bash
set -euo pipefail

# Per-pattern observation loop. Always addresses kind-aggexp-authz
# explicitly via KUBECONFIG pinned to an isolated copy so parallel
# worktrees can't clobber our context.

CLUSTER="aggexp-authz"
CTX="kind-${CLUSTER}"
NS="aggexp-system"

HERE="$(cd "$(dirname "$0")" && pwd)"
ARTIFACTS="${HERE}/artifacts"
mkdir -p "${ARTIFACTS}"

# Write our own kubeconfig view pointing at the right cluster only.
# `kind get kubeconfig` will fail if the cluster is missing.
KCFG="${HERE}/.kubeconfig-${CLUSTER}"
kind get kubeconfig --name "${CLUSTER}" > "${KCFG}"
chmod 600 "${KCFG}"
export KUBECONFIG="${KCFG}"
# sanity
kubectl config current-context

k() { kubectl "$@"; }

apply_pattern() {
  local pat="$1"
  local rules_file="$2"
  local rbac_file="$3"
  local outdir="${ARTIFACTS}/${pat}"
  mkdir -p "${outdir}"

  echo "=== Pattern ${pat} ==="

  k delete --ignore-not-found -f "${HERE}/manifests/rbac-permissive.yaml" >/dev/null
  k delete --ignore-not-found -f "${HERE}/manifests/rbac-strict.yaml"    >/dev/null

  k apply -f "${rbac_file}"      >/dev/null
  k apply -f "${rules_file}"     >/dev/null

  k -n "${NS}" rollout restart deploy/policy-service >/dev/null
  k -n "${NS}" rollout restart deploy/aggexp         >/dev/null
  k -n "${NS}" rollout status  deploy/policy-service --timeout=120s >/dev/null
  k -n "${NS}" rollout status  deploy/aggexp         --timeout=180s >/dev/null

  k -n argocd delete application toy-guestbook --ignore-not-found >/dev/null
  k apply -f "${HERE}/manifests/toy-application.yaml"              >/dev/null

  k -n argocd rollout restart statefulset/argocd-application-controller >/dev/null
  k -n argocd rollout status  statefulset/argocd-application-controller --timeout=180s >/dev/null

  echo "[${pat}] settling 120s..."
  sleep 120

  {
    echo "### kubectl api-resources aggexp.io"
    k api-resources --api-group=aggexp.io 2>&1 || true
    echo
    echo "### admin LIST repos (first 6)"
    k get repos 2>&1 | head -6 || true
    echo
    echo "### apiservice status"
    k get apiservices v1.aggexp.io -o jsonpath='{.status.conditions[?(@.type=="Available")].status}' 2>&1
    echo
  } > "${outdir}/admin-view.txt"

  {
    echo "### argocd applications"
    k -n argocd get applications -o wide 2>&1 || true
    echo
    echo "### toy-guestbook status summary"
    k -n argocd get application toy-guestbook \
      -o jsonpath='sync={.status.sync.status} health={.status.health.status}' 2>&1
    echo
    echo
    echo "### toy-guestbook conditions (messages only)"
    k -n argocd get application toy-guestbook -o json 2>/dev/null \
      | python3 -c 'import json,sys; d=json.load(sys.stdin); print(json.dumps(d.get("status",{}).get("conditions",[]), indent=2))' 2>&1 || true
    echo
    echo "### guestbook-ui deployment in default ns (missing = cluster cache blocked)"
    k -n default get deploy guestbook-ui -o wide 2>&1 || true
    echo
    echo "### pods in default ns"
    k -n default get pods 2>&1 | head -10 || true
  } > "${outdir}/argocd-view.txt"

  {
    for u in kubernetes-admin alice mallory; do
      for verb in list get create; do
        out="$(k auth can-i "${verb}" repos.aggexp.io --as "${u}" 2>&1 || true)"
        echo "can-i as=${u} verb=${verb}: ${out}"
      done
    done
    for verb in list get; do
      out="$(k auth can-i "${verb}" repos.aggexp.io \
              --as system:serviceaccount:argocd:argocd-application-controller 2>&1 || true)"
      echo "can-i as=argocd-app-ctrl verb=${verb}: ${out}"
    done
    for verb in list get; do
      out="$(k auth can-i "${verb}" repos.aggexp.io \
              --as system:serviceaccount:default:default 2>&1 || true)"
      echo "can-i as=default-sa verb=${verb}: ${out}"
    done
  } > "${outdir}/can-i.txt"

  {
    for u in kubernetes-admin alice mallory; do
      echo "--- LIST as ${u} ---"
      k --as "${u}" get repos 2>&1 | head -5 || true
      echo
    done
    echo "--- LIST as argocd-application-controller SA (impersonated) ---"
    k --as system:serviceaccount:argocd:argocd-application-controller \
        --as-group system:serviceaccounts \
        --as-group system:serviceaccounts:argocd \
        --as-group system:authenticated \
      get repos 2>&1 | head -5 || true
    echo
    echo "--- LIST as default:default SA (impersonated, no per-SA allow-list) ---"
    k --as system:serviceaccount:default:default \
        --as-group system:serviceaccounts \
        --as-group system:serviceaccounts:default \
        --as-group system:authenticated \
      get repos 2>&1 | head -5 || true
  } > "${outdir}/list-attempts.txt"

  {
    cat <<YAML > /tmp/dummy-repo.yaml
apiVersion: aggexp.io/v1
kind: Repo
metadata:
  name: alice-toy
spec:
  url: https://example.invalid/alice-toy
  description: "not a real repo; write-probe only"
YAML
    for u in kubernetes-admin alice mallory; do
      echo "--- CREATE as ${u} ---"
      k --as "${u}" apply -f /tmp/dummy-repo.yaml 2>&1 | head -4 || true
      echo
    done
  } > "${outdir}/write-attempts.txt"

  k -n "${NS}" logs deploy/policy-service --tail=400 > "${outdir}/policy-service.log" 2>&1 || true
  k -n "${NS}" logs deploy/aggexp --tail=200 > "${outdir}/aggexp.log" 2>&1 || true
  k -n argocd logs statefulset/argocd-application-controller --tail=200 \
    > "${outdir}/argocd-application-controller.log" 2>&1 || true

  # Short summary (same info, one line) into summary.txt and stdout.
  {
    echo "=== Pattern ${pat} SUMMARY ==="
    printf '  ArgoCD app: '
    k -n argocd get application toy-guestbook \
      -o jsonpath='sync={.status.sync.status} health={.status.health.status}' 2>&1
    echo
    printf '  guestbook-ui deployed: '
    if k -n default get deploy guestbook-ui >/dev/null 2>&1; then echo 'YES'; else echo 'NO'; fi
    printf '  admin LIST repos: '
    if k get repos >/dev/null 2>&1; then echo 'OK'; else echo 'FAIL'; fi
    printf '  policy-service rules loaded: '
    grep -m1 'loaded .* rules' "${outdir}/policy-service.log" || echo 'NOT FOUND'
  } > "${outdir}/summary.txt"
  cat "${outdir}/summary.txt"
}

apply_pattern A "${HERE}/manifests/rules-A-allowlist.yaml"       "${HERE}/manifests/rbac-permissive.yaml"
apply_pattern B "${HERE}/manifests/rules-B-blanket-sa.yaml"      "${HERE}/manifests/rbac-permissive.yaml"
apply_pattern C "${HERE}/manifests/rules-C-upstream-refines.yaml" "${HERE}/manifests/rbac-strict.yaml"

echo "DONE"
