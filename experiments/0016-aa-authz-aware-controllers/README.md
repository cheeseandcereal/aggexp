# Experiment 0016: aa-authz-aware-controllers

Probe three concrete authorization patterns for an aggregated apiserver
whose authz would otherwise default-deny every cluster-controller
ServiceAccount — the operational hazard surfaced by `0005-argocd-compat`.

The three patterns:

- **A. Allow-list by SA name.** Policy rules explicitly allow each
  ecosystem-controller SA (`argocd-application-controller`,
  `argocd-applicationset-controller`, `argocd-server`, Flux's, kube-
  controller-manager) to `get|list|watch` on `repos.aggexp.io`. Humans
  still go through the per-user policy.
- **B. Blanket allow for any `system:serviceaccount:*` user.** Any SA
  gets `get|list|watch`. SAs can't write; humans still gated per-user.
- **C. Upstream-RBAC strict + AA refines.** kubectl RBAC is strict
  (no permissive ClusterRole for `system:authenticated`). Each
  controller SA is granted `get|list|watch` via standard Kubernetes
  RBAC. The AA's authorizer sees only identities RBAC let through;
  its job is to further constrain writes by identity.

## Hypothesis

**Primary fundamental: per-request authorization.** `0003` established
that an AA can make every authz decision against an external policy
service; `0005` showed that a default-deny posture bricks ArgoCD's
cluster-wide cache because gitops-engine treats one 403-on-LIST as
fatal for every Application. This experiment tests which of three
patterns actually accommodates a cluster controller without either
(a) turning the AA authz into a pass-through or (b) forcing per-
controller special-casing at the wrong layer.

**Secondary: wire protocol fidelity (controller-compat).** Each
pattern is validated against a real long-running consumer (ArgoCD's
cluster cache) and a toy Application whose sync state reflects
whether the cluster cache is healthy.

For each pattern we expect to observe:

1. Does ArgoCD's cluster cache complete initial LIST on
   `repos.aggexp.io`?
2. Does the toy `Application` reach `Synced, Healthy`?
3. Do human writes still land where they should (admin yes, alice no,
   mallory no)?
4. What does `kubectl auth can-i` say?
5. What does the policy-service / AA log look like?

## How to run

From the repo root of this worktree, with `kind`, `kubectl`, `docker`,
`envsubst` installed and images built (`aggexp-repos:dev`,
`aggexp-policy:dev` — both from experiment 0004):

```
./hack/gen-certs.sh                          # only if deploy/certs missing
./experiments/0016-aa-authz-aware-controllers/run.sh
```

`run.sh`:

1. Creates kind cluster `aggexp-authz` (idempotent).
2. Deploys base manifests + the aggexp overlay + policy-service.
3. Installs ArgoCD.
4. For each pattern (A, B, C) in sequence:
   - Applies the pattern's RBAC + rules ConfigMap.
   - Restarts the AA and the policy-service.
   - Creates/refreshes the toy Application.
   - Waits and captures: `repos` LIST status, ArgoCD Application
     state, policy-service logs, AA logs, a few `can-i` checks.
   - Attempts writes as admin / alice / mallory via kubectl impersonation.
5. Dumps all captures into `artifacts/<pattern>/`.

## Status

complete

<!-- See FINDINGS/0016-aa-authz-aware-controllers.md for results. -->

## Decisions made

- **Dedicated kind cluster `aggexp-authz`.** Don't mingle with the
  0005 cluster; parallel agents have clobbered shared clusters
  before (see SYNTHESIS "Process observations").
- **`kubernetes-sigs` as GitHub owner, no PAT.** Same choice as
  0005; matches read-only workload.
- **Reuse the 0003 policy-service image unchanged.** The rule DSL
  already supports prefix matching (`system:serviceaccount:*`) and
  alternation (`get|list|watch`), which is everything the three
  patterns need.
- **`aggexp-repos:dev` (0004 image) as the AA under test.** Read-
  only driver means ArgoCD's discover-and-LIST path exercises the
  authz surface cleanly without muddying with SSA / write paths.
- **Install Flux? No.** 0005 already observed that the ArgoCD cluster
  cache is the operationally interesting case. Flux's behavior is
  queued as `0014-flux-compat`. We name the flux SAs in Pattern A's
  allow-list as a documentation artifact only; they're not exercised
  live.
- **Pattern ordering A -> B -> C.** No semantic reason; A is the
  shape 0005's mitigation used (ad-hoc patch to add `argocd-
  application-controller` to the rules), so starting there mirrors
  prior observation.
- **Post-pattern settle time: 60s.** Long enough for ArgoCD to
  attempt a cluster-cache sync and for the toy Application to start
  reconciling; short enough that the whole experiment fits in one
  agent session.
- **Toy Application is ArgoCD's guestbook sample.** Same as 0005;
  unrelated to aggexp.io; its Synced state is the clean signal of
  "ArgoCD's sync flow is not blocked."
- **`kubectl auth can-i` checks recorded per pattern.** The reason
  can-i is-or-isn't meaningful under each pattern is itself a
  finding (see 0003: can-i lies when AA is the real gate).

## Prerequisites

- `aggexp-repos:dev` and `aggexp-policy:dev` built from experiment
  0004 and present in the local Docker daemon.
- `deploy/certs/` populated (run `hack/gen-certs.sh` if not).
- Internet access for the ArgoCD install manifest, argocd-example-apps,
  and GitHub REST.

## What we're looking to learn

- **Per-request authorization.** Which pattern best reconciles the
  AA's per-identity policy with cluster controllers' auto-discover-
  and-watch behavior? What are the tradeoffs in UX for human callers,
  operational overhead (per-controller maintenance), security
  posture (blast radius of an SA compromise), and implementation
  complexity?
- **Wire protocol fidelity.** Whether ArgoCD's cluster cache and
  toy Application behave identically under all three patterns once
  the reads-for-controllers question is solved, or whether any
  pattern has a subtle wire consequence beyond authz.
