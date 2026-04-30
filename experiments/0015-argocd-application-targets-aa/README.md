# Experiment 0015: argocd-application-targets-aa

Natural follow-up to `0005-argocd-compat` (ArgoCD observes but does
not write a read-only AA) and `0010-etcd-crd-facade-with-ssa` (writable
AA whose storage is a CRD on the host kube-apiserver, with SSA
`managedFields` preserved).

Here, an ArgoCD `Application` **targets** aggexp resources: its Git
source contains three `aggexp.io/v1` `Widget` manifests. ArgoCD's sync
loop issues server-side applies against our AA, watches for drift,
and prunes removed manifests. We observe whether each stage of
Argo's UX for a real, writable aggregated API behaves like a vanilla
Kubernetes CRD.

## Hypothesis

1. **Wire protocol fidelity (primary).** ArgoCD's sync pipeline
   (`argocd-application-controller` → gitops-engine → k8s.io/client-go
   dynamic client with server-side apply) treats our AA as a drop-in
   Kubernetes resource. Create, update-on-drift, and prune all work.
2. **Storage independence (primary).** Because the backing CRD
   (`widgetstorages.aggexpstorage.aggexp.io`) is stored in etcd on
   the host cluster, SSA `managedFields` must persist through the
   facade — ArgoCD's applies should leave a clean `managedFields`
   entry with `manager: argocd-controller` on the exposed Widget.
   The apiVersion/field-path rewrite the 0010 facade performs must
   hold up against a non-kubectl field manager.
3. **Per-request authorization (secondary).** 0005 found a single
   LIST 403 from an AA bricks gitops-engine's cluster cache. With
   0010's permissive `system:authenticated` ClusterRole on
   `widgets.aggexp.io`, the argocd SA should be fine. We also have
   to grant argocd read access on the backing CRD
   (`widgetstorages.aggexpstorage.aggexp.io`) because that CRD shows
   up in discovery too.

Health assessment (Argo's "Healthy/Progressing/Degraded" per-kind
check) has no custom plugin for `aggexp.io/v1.Widget`. The working
assumption is that Argo treats unknown kinds as Healthy by default.
This experiment documents what actually happens.

## How to run

Prerequisites:

- `kind`, `kubectl`, `docker`, `envsubst`, `jq` in PATH.
- Serving cert generated (`hack/gen-certs.sh`).
- `aggexp-widgets:dev` image built from experiment 0010:
  ```
  docker build -t aggexp-widgets:dev \
    -f experiments/0010-etcd-crd-facade-with-ssa/Dockerfile .
  ```

From the repo root:

```
./experiments/0015-argocd-application-targets-aa/run.sh
```

That script:

1. Creates kind cluster `aggexp-argo-app`.
2. Deploys base manifests + 0010's overlay (CRD + permissive RBAC +
   AA Deployment).
3. Installs ArgoCD from the stable manifests bundle.
4. Grants `argocd-application-controller` + `argocd-server` read
   access to the backing CRD so their cluster cache can list it.
5. Applies `manifests/00-git-server.yaml` — an in-cluster git
   daemon serving a bare repository seeded from a ConfigMap of
   Widget YAMLs.
6. Applies `manifests/10-application.yaml` — the ArgoCD Application
   pointing at `git://git-server.git-server.svc:9418/aggexp.git`
   with `ServerSideApply=true` and `automated.prune=true`.
7. Waits for the Application to report `sync=Synced` and prints
   the resulting widgets (both through the AA and through the
   backing CRD).

Scenarios (run after `run.sh`):

```
# Scenario A: edit alpha in Git. Expect Argo to detect drift and
# re-apply (counter 1 -> 42, new label+tag).
./experiments/0015-argocd-application-targets-aa/scenarios.sh v2
./experiments/0015-argocd-application-targets-aa/scenarios.sh refresh
./experiments/0015-argocd-application-targets-aa/scenarios.sh wait Synced 60

# Scenario B: remove charlie from Git. Expect Argo to prune the
# Widget.
./experiments/0015-argocd-application-targets-aa/scenarios.sh v3
./experiments/0015-argocd-application-targets-aa/scenarios.sh refresh
./experiments/0015-argocd-application-targets-aa/scenarios.sh wait Synced 60

# managedFields inspection (the 0010 probe under ArgoCD's SSA):
kubectl get --raw /apis/aggexp.io/v1/widgets/alpha | jq .metadata.managedFields

# Drift-from-cluster side (user edits the Widget; self-heal re-applies)
kubectl patch widget alpha --type=merge \
  -p '{"spec":{"description":"out-of-band edit"}}'
# ... expect Argo's self-heal to revert within the next reconcile cycle
```

Cleanup:

```
kind delete cluster --name aggexp-argo-app
```

## Status

complete

<!-- See FINDINGS/0015-argocd-application-targets-aa.md for results. -->

## Decisions made

- **Target AA: 0010 (Widget CRD facade).** Per the task brief,
  0010 is the interesting probe because its backing CRD persists
  `managedFields`; 0009 would re-observe the "SSA looks fine but
  managedFields vanish" already documented in its findings.
- **In-cluster git server using smart HTTP (apache +
  git-http-backend CGI).** First tried `git-daemon` over git://
  — `alpine/git` doesn't ship `git-daemon`. Second tried dumb
  HTTP (nginx autoindex) — ArgoCD's shallow-clone requires
  smart HTTP. Third try (debian-slim + apache2 + git-http-backend)
  works. Built from `git-server/Dockerfile`.
- **Git content driven by ConfigMap + `rollout restart`.** The
  entrypoint script rebuilds the bare repo from the `/content`
  mount each container start. Rewriting the ConfigMap + a
  rollout restart is how revisions are driven.
- **Branch name: `main`.** `git init -b main` in the seed script.
  ArgoCD's `targetRevision: HEAD` tracks it.
- **ArgoCD `Application` in `argocd` namespace.** Standard. The
  Application also carries the `resources-finalizer.argocd.argoproj.io`
  finalizer so the sync state is GC'd properly on Application delete.
- **`ServerSideApply=true` in syncOptions.** Forces Argo to use
  SSA even where client-side would normally be a default fallback.
  This is what we want to measure against 0010's managedFields path.
- **`CreateNamespace=false`.** Widgets are cluster-scoped.
- **`automated.prune=true, selfHeal=true`.** Needed to exercise
  the prune scenario and the drift-reconcile scenario respectively.
- **Argo SA RBAC on `widgetstorages.aggexpstorage.aggexp.io`**
  added defensively. The default ArgoCD install grants its SA
  `*` cluster-wide so this is redundant under stock argocd; a
  hardened (scope-limited) argocd install would need it.
- **Three widgets (alpha/bravo/charlie).** Enough to test
  partial-drift (one edited) and prune (one removed) while keeping
  a stable widget untouched across revisions as a control.
- **Context-explicit kubectl.** All scripts prefix kubectl calls
  with `--context kind-aggexp-argo-app` because parallel agents
  changing the global context is a known hazard (see SYNTHESIS
  Process observations #2).

## Prerequisites

- kind cluster `aggexp-argo-app`.
- Serving cert generated by `hack/gen-certs.sh`.
- Base manifests (`deploy/manifests/`) applied via `hack/deploy.sh`.
- 0010 overlay manifests applied.
- `aggexp-widgets:dev` docker image present locally.
- Internet access to pull ArgoCD's install manifest and the
  `alpine/git` image.

## What we're looking to learn

- **Wire protocol fidelity.** The first experiment in the repo where
  the ArgoCD sync pipeline *writes* to an aggregated API.
- **Storage independence.** ArgoCD-sourced SSA against 0010's CRD
  facade: does `managedFields` survive? Does kubectl's SSA from a
  different manager properly see Argo's fields as owned?
- **Per-request authorization.** Does the AA treat argocd's SA the
  same way the experiment expects, or does the "0005 brick-the-
  cluster-cache" failure mode reappear under a different (write-
  exercising) load?
