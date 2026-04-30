#!/usr/bin/env bash
set -euo pipefail

# End-to-end harness for experiment 0016 (aa-authz-aware-controllers).
#
# Provisions a kind cluster, deploys the 0004 github-driver AA with a
# policy-service authorizer, installs ArgoCD, and runs through three
# authz patterns (A allow-list, B blanket-SA, C upstream-strict) in
# sequence, capturing what each does to ArgoCD's cluster cache and the
# toy Application's sync state.

CLUSTER="aggexp-authz"
CTX="kind-${CLUSTER}"
NS="aggexp-system"
GITHUB_OWNER="${GITHUB_OWNER:-kubernetes-sigs}"
GITHUB_TOKEN="${GITHUB_TOKEN:-}"

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${HERE}/../.." && pwd)"
ARTIFACTS="${HERE}/artifacts"
mkdir -p "${ARTIFACTS}"

for bin in kind kubectl docker envsubst; do
  command -v "${bin}" >/dev/null 2>&1 || { echo "missing: ${bin}" >&2; exit 1; }
done

# --- provision cluster ------------------------------------------------------

if ! kind get clusters 2>/dev/null | grep -qx "${CLUSTER}"; then
  kind create cluster --name "${CLUSTER}"
else
  echo "kind cluster ${CLUSTER} already exists; reusing"
fi

# Pin an isolated kubeconfig so parallel agents / other worktrees can't
# flip our current-context out from under us mid-run (0005 / 0013
# observed this).
KCFG="${HERE}/.kubeconfig-${CLUSTER}"
kind get kubeconfig --name "${CLUSTER}" > "${KCFG}"
chmod 600 "${KCFG}"
export KUBECONFIG="${KCFG}"

k() { kubectl "$@"; }

k get ns "${NS}" >/dev/null 2>&1 || k create ns "${NS}"

for img in aggexp-repos:dev aggexp-policy:dev; do
  if ! docker image inspect "${img}" >/dev/null 2>&1; then
    echo "error: image ${img} not found locally; build from experiment 0004 first" >&2
    exit 1
  fi
  kind load docker-image "${img}" --name "${CLUSTER}"
done

cd "${REPO_ROOT}"

# Render the base manifests against our isolated kubeconfig. hack/deploy.sh
# uses envsubst and honors the exported KUBECONFIG.
./hack/deploy.sh deploy/manifests

# Apply the AA overlay pieces from experiment 0004 (policy-service,
# github-token, and the AA deployment spec).
AGGEXP_IMAGE=aggexp-repos:dev \
POLICY_IMAGE=aggexp-policy:dev \
GITHUB_OWNER="${GITHUB_OWNER}" \
GITHUB_TOKEN="${GITHUB_TOKEN}" \
  envsubst '${AGGEXP_IMAGE} ${POLICY_IMAGE} ${GITHUB_OWNER} ${GITHUB_TOKEN}' \
    < "${HERE}/manifests/policy-service.yaml" | k apply -f -

AGGEXP_IMAGE=aggexp-repos:dev \
POLICY_IMAGE=aggexp-policy:dev \
GITHUB_OWNER="${GITHUB_OWNER}" \
GITHUB_TOKEN="${GITHUB_TOKEN}" \
  envsubst '${AGGEXP_IMAGE} ${POLICY_IMAGE} ${GITHUB_OWNER} ${GITHUB_TOKEN}' \
    < "${HERE}/manifests/github-token-secret.yaml" | k apply -f -

AGGEXP_IMAGE=aggexp-repos:dev \
POLICY_IMAGE=aggexp-policy:dev \
GITHUB_OWNER="${GITHUB_OWNER}" \
GITHUB_TOKEN="${GITHUB_TOKEN}" \
  envsubst '${AGGEXP_IMAGE} ${POLICY_IMAGE} ${GITHUB_OWNER} ${GITHUB_TOKEN}' \
    < "${HERE}/manifests/aggexp-deployment.yaml" | k apply -f -

# Start with Pattern A's rules so the AA has *some* rules ConfigMap to
# mount; the per-pattern loop will swap them.
k apply -f "${HERE}/manifests/rules-A-allowlist.yaml"
k apply -f "${HERE}/manifests/rbac-permissive.yaml"

k -n "${NS}" rollout status deploy/policy-service --timeout=120s
k -n "${NS}" rollout status deploy/aggexp         --timeout=180s

# --- install ArgoCD ---------------------------------------------------------

k get ns argocd >/dev/null 2>&1 || k create ns argocd
# --server-side because the applicationsets CRD annotations exceed
# last-applied-configuration's 262144-byte limit.
k apply -n argocd --server-side=true --force-conflicts \
  -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml

k -n argocd rollout status deploy/argocd-server                     --timeout=300s
k -n argocd rollout status deploy/argocd-repo-server                --timeout=300s
k -n argocd rollout status deploy/argocd-applicationset-controller  --timeout=300s
k -n argocd rollout status statefulset/argocd-application-controller --timeout=300s

# --- per-pattern loop -------------------------------------------------------

apply_pattern() {
  local pat="$1"
  local rules_file="$2"
  local rbac_file="$3"
  local outdir="${ARTIFACTS}/${pat}"
  mkdir -p "${outdir}"

  echo "=== Pattern ${pat} ==="

  k delete --ignore-not-found -f "${HERE}/manifests/rbac-permissive.yaml" >/dev/null
  k delete --ignore-not-found -f "${HERE}/manifests/rbac-strict.yaml"    >/dev/null

  k apply -f "${rbac_file}"  >/dev/null
  k apply -f "${rules_file}" >/dev/null

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
    echo "### admin LIST repos"
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
    echo "### toy-guestbook conditions"
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
    echo "--- LIST as argocd-application-controller SA ---"
    k --as system:serviceaccount:argocd:argocd-application-controller \
        --as-group system:serviceaccounts \
        --as-group system:serviceaccounts:argocd \
        --as-group system:authenticated \
      get repos 2>&1 | head -5 || true
    echo
    echo "--- LIST as default:default SA (not in any allow-list) ---"
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
    # --validate=false to skip kubectl's CRD-list precheck (which itself
    # is RBAC-gated under impersonation and would hide the authz signal
    # we care about).
    for u in kubernetes-admin alice mallory; do
      echo "--- CREATE as ${u} ---"
      k --as "${u}" apply --validate=false -f /tmp/dummy-repo.yaml 2>&1 | head -6 || true
      k delete repo alice-toy --ignore-not-found >/dev/null 2>&1 || true
      echo
    done
  } > "${outdir}/write-attempts.txt"

  k -n "${NS}" logs deploy/policy-service --tail=400 > "${outdir}/policy-service.log" 2>&1 || true
  k -n "${NS}" logs deploy/aggexp --tail=200         > "${outdir}/aggexp.log" 2>&1 || true
  k -n argocd logs statefulset/argocd-application-controller --tail=300 \
    > "${outdir}/argocd-application-controller.log" 2>&1 || true

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

apply_pattern A "${HERE}/manifests/rules-A-allowlist.yaml"        "${HERE}/manifests/rbac-permissive.yaml"
apply_pattern B "${HERE}/manifests/rules-B-blanket-sa.yaml"       "${HERE}/manifests/rbac-permissive.yaml"
apply_pattern C "${HERE}/manifests/rules-C-upstream-refines.yaml" "${HERE}/manifests/rbac-strict.yaml"

echo
echo "DONE. Artifacts under: ${ARTIFACTS}"
echo "Isolated kubeconfig kept at: ${KCFG}"
