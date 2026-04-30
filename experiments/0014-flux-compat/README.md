# Experiment 0014: flux-compat

Sibling to 0005 (argocd-compat). Install Flux into a dedicated kind
cluster running the 0004 github-driver AA (`repos.aggexp.io/v1`,
read-only) and observe what Flux does when it discovers an aggregated
API it cannot modify. The headline question is whether Flux's
source-controller / kustomize-controller do cluster-wide
LIST-every-resource-type-they-can-RBAC discovery the way ArgoCD's
gitops-engine cluster cache does; and, if so, whether a single 403 on
one resource bricks unrelated syncs the same way it did for ArgoCD in
`FINDINGS/0005-argocd-compat.md`.

## Hypothesis

1. **Wire protocol fidelity.** Flux's controllers install cleanly next
   to our AA; discovery of `repos.aggexp.io/v1` does not make them
   crash-loop.
2. **Per-request authorization.** Either:
   - Flux does broad discovery like ArgoCD, in which case a
     default-deny policy against Flux's controller SAs produces
     the same operational pattern 0005 saw: unrelated Kustomization
     / GitRepository syncs brick because one LIST returned 403; OR
   - Flux's controllers only touch types they actually manage
     (GitRepository, Kustomization, HelmRelease, Bucket, …) plus the
     specific types their sync rendered. In that case a 403 on
     `repos.aggexp.io` never happens and our AA is invisible to
     Flux.
3. **Watch and consistency semantics.** An AA scale-to-0 / scale-to-1
   cycle does not crash Flux. Any Flux components watching our AA
   recover cleanly on return.

Fundamentals probed: **Wire protocol fidelity** (primary, with a
second non-kubectl sustained consumer) and **Per-request
authorization** (secondary — the 0005 gate question).

## How to run

Dedicated cluster. Do not reuse `kind-aggexp` or `kind-aggexp-argocd`.

From repo root, with `kind`, `kubectl`, `docker`, `envsubst`, and the
`flux` v2.8.6 CLI installed, and images already built from 0004
(`aggexp-repos:dev`, `aggexp-policy:dev`):

```
./hack/gen-certs.sh      # writes deploy/certs/
./experiments/0014-flux-compat/run.sh
```

`run.sh` is idempotent. It:

1. Creates kind cluster `aggexp-flux`.
2. Loads `aggexp-repos:dev` and `aggexp-policy:dev` into the cluster.
3. Applies base manifests + the 0004 overlay
   (`GITHUB_OWNER=kubernetes-sigs`, no PAT).
4. `flux install` with the default component set
   (source-controller, kustomize-controller, helm-controller,
   notification-controller).
5. Applies a trivial GitRepository + Kustomization pair from
   `manifests/` that targets a tiny public manifests repo (a
   Namespace + ConfigMap only — no controller concepts).

Observation commands (run by hand):

```
# Flux sees our AA group in discovery?
kubectl -n flux-system logs deploy/source-controller      | grep -i aggexp || true
kubectl -n flux-system logs deploy/kustomize-controller   | grep -i aggexp || true

# AA-side: what does Flux hit?
kubectl -n aggexp-system logs deploy/policy-service | grep -i flux-system

# Sync status of the toy Kustomization
kubectl -n flux-system get gitrepositories,kustomizations

# Scale-down / scale-up cycle
kubectl -n aggexp-system scale deploy/aggexp --replicas=0
sleep 60
kubectl -n aggexp-system scale deploy/aggexp --replicas=1
kubectl -n aggexp-system rollout status deploy/aggexp
sleep 30
kubectl -n flux-system get kustomizations
```

To reproduce the 0005 default-deny scenario, explicitly patch the
policy ConfigMap so Flux's controller SAs are NOT allow-listed (they
aren't by default in 0004's rules, so this is automatic in this
experiment).

## Status

complete

<!-- See FINDINGS/0014-flux-compat.md for results. -->

## Decisions made

- **Dedicated kind cluster `aggexp-flux`.** Matches the 0005 pattern.
- **Flux v2.8.6.** Latest stable at experiment time
  (https://github.com/fluxcd/flux2/releases/tag/v2.8.6).
- **Default component set** (source-controller, kustomize-controller,
  helm-controller, notification-controller) — what `flux install`
  gives with no flags.
- **Minimal toy manifests Git target.** A repo of a `Namespace` + a
  `ConfigMap` — the simplest thing Flux will reconcile. Kept in
  `fluxcd/flux2-kustomize-helm-example` via its production cluster
  overlay turned out too heavy (pulls HelmReleases); substituted
  `stefanprodan/podinfo` path `kustomize/` — a widely used Flux demo.
  Decided at runtime based on what actually rendered cleanly.
- **Observation window after sync: ~10 minutes**, matching 0005.
- **Scale-down window: 60 seconds**, matching 0005, so the
  comparison is apples-to-apples.
- **No Flux OCI repository or Helm probes.** This experiment is
  about compat of the two most commonly enabled controllers
  (source + kustomize) with our AA; not a full surface-area test.

## Prerequisites

- kind, kubectl, docker, envsubst, `flux` v2.8.6 installed.
- `aggexp-repos:dev` and `aggexp-policy:dev` images built from
  experiment 0004 and present in the local Docker daemon.
- `deploy/certs/` present (run `hack/gen-certs.sh` if not).
- Internet access for `ghcr.io/fluxcd/*` controller images and for
  the public Git source used by the toy GitRepository.

## What we're looking to learn

- **Wire protocol fidelity.** A second sustained non-kubectl consumer
  after 0005's ArgoCD. Does the Flux toolchain's discovery +
  informer behavior add any demand we haven't already met?
- **Per-request authorization.** The 0005 finding was
  gitops-engine-specific. Does Flux's controller set replicate the
  "one 403 on LIST bricks unrelated syncs" failure mode, or does it
  isolate per-resource-type?
