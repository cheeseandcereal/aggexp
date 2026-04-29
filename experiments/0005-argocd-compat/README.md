# Experiment 0005: argocd-compat

Install ArgoCD into a dedicated kind cluster that is running the 0004
github-driver AA (`repos.aggexp.io/v1`, read-only) and observe what
ArgoCD does to an aggregated API it can discover but cannot write to.

The hook is ArgoCD's cluster cache: `argocd-application-controller`
dynamically LIST+WATCHes every API resource it sees in discovery. Our
AA's watch is polling-driven with synthetic resourceVersion. This
experiment is about whether that wire shape holds up under a long-
lived, broad-surface informer, and what ArgoCD does when our AA
briefly goes away.

## Hypothesis

1. **Wire protocol fidelity / Watch and consistency semantics.**
   ArgoCD's dynamic informer against `repos.aggexp.io/v1` stays
   healthy over tens of minutes. It issues one LIST + one WATCH and
   then rides the watch stream; it does NOT hammer LIST.
2. **Watch and consistency semantics.** When the aggexp Deployment
   scales to 0 and back to 1, ArgoCD logs noisily (expected for any
   informer) but recovers on its own when the AA returns. New
   synthetic UIDs on recovery may cause apparent full churn (per
   `FINDINGS/0004`), but ArgoCD should not crash.
3. **Resource modeling freedom.** Unrelated ArgoCD Applications
   syncing manifests from a Git repo (no aggexp resources) are
   unaffected by the state of the aggexp AA. The per-resource
   informer in ArgoCD's cluster cache is isolated from sync flows
   for Application kinds it actually manages.

## How to run

Dedicated cluster. Do not re-use `kind-aggexp`.

From repo root, with `kind`, `kubectl`, `docker`, `envsubst`
installed, and images already built from 0004 (`aggexp-repos:dev`,
`aggexp-policy:dev`):

```
./hack/gen-certs.sh      # deploy/certs/ for the AA serving cert
./experiments/0005-argocd-compat/run.sh
```

`run.sh` does:

1. Creates kind cluster `aggexp-argocd` (idempotent).
2. Creates namespace `aggexp-system` and applies base manifests
   from `deploy/manifests/`.
3. Applies 0004 overlay from
   `experiments/0004-github-driver-static-pat/manifests/` with
   `GITHUB_OWNER=kubernetes-sigs` and no PAT.
4. Loads `aggexp-repos:dev` and `aggexp-policy:dev` into the new
   cluster.
5. Installs ArgoCD: `kubectl create ns argocd && kubectl apply -n
   argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml`.
6. Waits for both sides to be Ready.
7. Applies the toy `Application` from
   `manifests/toy-application.yaml` pointing at a public Git repo
   of vanilla manifests.

Observation commands (run by hand):

```
# 1. ArgoCD saw our group in discovery
kubectl -n argocd logs deploy/argocd-application-controller \
  | grep -i aggexp

# 2. AA-side view of ArgoCD's watch traffic
kubectl -n aggexp-system logs deploy/aggexp | grep -i -E 'list|watch|argocd'

# 3. Scale-down / scale-up cycle
kubectl -n aggexp-system scale deploy/aggexp --replicas=0
sleep 60
kubectl -n aggexp-system scale deploy/aggexp --replicas=1
kubectl -n aggexp-system rollout status deploy/aggexp
sleep 30
kubectl -n argocd logs deploy/argocd-application-controller --since=2m \
  | grep -i -E 'aggexp|repos'

# 4. 10-minute quiet observation — count LIST vs WATCH hits
sleep 600
kubectl -n aggexp-system logs deploy/aggexp --tail=500

# 5. Toy application unaffected
kubectl -n argocd get applications
kubectl -n argocd describe application toy-guestbook
```

## Status

complete

<!-- See FINDINGS/0005-argocd-compat.md for results. -->

## Decisions made

- **Dedicated kind cluster `aggexp-argocd`.** Keep the `aggexp`
  cluster used by 0002/0003/0004 intact. Serving cert's CN
  (`aggexp.aggexp-system.svc`) is service-DNS-scoped, so the existing
  `hack/gen-certs.sh` output works unchanged on the new cluster.
- **`kubernetes-sigs` as GitHub owner, no PAT.** Public org, known
  to have ~200 repos, rate-limited but fine for a ≤30-minute test.
- **Toy Application sources vanilla guestbook manifests** from
  `https://github.com/argoproj/argocd-example-apps.git` path
  `guestbook` — ArgoCD's canonical smoke test. Anything with plain
  k8s manifests would do; this is the least-surprising choice.
- **Observation window: 10 minutes.** Long enough to observe a few
  polling cycles (our AA polls every 60s) and for ArgoCD's
  refresh/reconcile loops to tick a few times; short enough for an
  agent session.
- **Scale-down cycle: 60 seconds.** Long enough to guarantee the AA
  is gone from ArgoCD's perspective across at least one refresh
  tick; short enough that recovery is observable in the same session.
- **No UI interaction.** `argocd-server` UI is available via
  port-forward but not required for the findings; we read logs and
  API state directly.

## Prerequisites

- kind, kubectl, docker, envsubst installed.
- `aggexp-repos:dev` and `aggexp-policy:dev` images built from
  experiment 0004 and present in the local Docker daemon.
- `deploy/certs/` present (run `hack/gen-certs.sh` if not).
- Internet access for `argoproj/argo-cd` install manifest and for
  the GitHub REST API.

## What we're looking to learn

- **Watch and consistency semantics.** How does a long-lived
  controller-runtime-ish informer (ArgoCD's cluster cache uses
  client-go dynamic informers internally) behave against our
  polling-backed watch? Does it tolerate the apparent full-churn
  on pod restart, or hot-loop?
- **Wire protocol fidelity.** Discovery already worked for kubectl
  in 0001–0004. Does ArgoCD's discovery-driven auto-watch add any
  new demands we haven't met?
- **Resource modeling freedom.** Does the read-only nature of our
  resource confuse ArgoCD? (It shouldn't — Applications target
  specific GVKs, not every resource — but worth confirming.)
