# Findings — 0014 flux-compat

## What we were trying to learn

Sibling probe to `0005-argocd-compat`. ArgoCD's gitops-engine cluster
cache LISTed every discovered API group and treated one 403 on one
resource as fatal for the whole cluster cache, cascading into
`sync=Unknown` on unrelated Applications. The question for 0014:

1. Does Flux install cleanly next to our `repos.aggexp.io/v1` AA?
2. Does Flux's controller set (source-controller,
   kustomize-controller, helm-controller, notification-controller)
   do the same broad cluster-wide discovery that ArgoCD does?
3. If so, does a default-deny from the AA's policy for Flux's
   controller SAs brick unrelated syncs, the 0005 pattern?
4. Can a Flux GitRepository + Kustomization sync vanilla Kubernetes
   manifests (podinfo kustomize/) while the AA is present?

Fundamentals: **Wire protocol fidelity** (primary — second sustained
non-kubectl consumer after ArgoCD) and **Per-request authorization**
(secondary — the 0005 gate question).

## What we did

Dedicated kind cluster `aggexp-flux`. Base aggexp manifests + 0004
overlay deployed with `GITHUB_OWNER=kubernetes-sigs` and no PAT
(same posture as `0005-argocd-compat`). Installed Flux v2.8.6 via
`flux install` (default component set). Applied a trivial
`GitRepository` + `Kustomization` pair pointing at
`https://github.com/stefanprodan/podinfo` path `./kustomize` — a
vanilla Deployment + Service + HPA, no aggregated-API resources
involved. Observed:

- Initial install. Flux's four controllers ready within ~15s.
- Initial sync. Kustomization reached `Ready: True` with
  `ReconciliationSucceeded` in under two seconds of the GitRepository
  becoming ready.
- Steady state for ~10 minutes. Counted authz calls from every
  flux-system SA.
- Scale-down / scale-up of the AA (60s outage), same shape as the
  `0005` probe.
- Confirmed Flux's RBAC grants: `cluster-reconciler-flux-system`
  binds kustomize-controller and helm-controller to `cluster-admin`,
  so upstream RBAC does permit them to touch our API group. Our AA's
  policy has no rule for the flux-system SAs and defaults to deny —
  the 0005 shape exactly.

The `0005` default-deny-for-controller-SAs trap is therefore set up
identically. What's different is whether Flux steps on it.

## What we observed

### 1. Flux installed cleanly alongside our AA

```
✔ helm-controller: deployment ready
✔ kustomize-controller: deployment ready
✔ notification-controller: deployment ready
✔ source-controller: deployment ready
✔ install finished
```

The aggexp AA's `APIService v1.aggexp.io` reached `Available: True`
before Flux was applied. Flux installing after it had zero visible
effect on the AA side.

### 2. The podinfo Kustomization synced cleanly

```
NAME                                             URL                                       AGE   READY   STATUS
gitrepository.source.toolkit.fluxcd.io/podinfo   https://github.com/stefanprodan/podinfo   10m   True    stored artifact for revision 'master@sha1:9f4969c2c84338b026300dafb55698e05b1d6fbc'

NAME                                                AGE   READY   STATUS
kustomization.kustomize.toolkit.fluxcd.io/podinfo   10m   True    Applied revision: master@sha1:9f4969c2c84338b026300dafb55698e05b1d6fbc
```

A Service, Deployment, and HorizontalPodAutoscaler landed in the
`default` namespace. Both podinfo pods reached `Running` within
seconds. The reconcile loop continues every 60s with
`Reconciliation finished in ~90ms`.

### 3. Flux NEVER hits our AA. At all.

This is the headline finding.

Policy-service log, 10 minutes after start:

```
2026/04/30 05:32:01 loaded 6 rules from /etc/policy/rules.json (default: allow=false)
2026/04/30 05:32:01 policy-service listening on :8080 rules=/etc/policy/rules.json
2026/04/30 05:32:24 rule[0] matched -> allow=true user=kubernetes-admin verb=list resource=repos name=
```

Three lines total. The single matched rule is from my one
`kubectl get repos` smoke test as kubernetes-admin. **Zero authz
calls from any `system:serviceaccount:flux-system:*` SA across 10
minutes of sustained Flux operation.** The aggexp AA's ext-authz log
post-restart has zero entries (the counter starts from the new pod
process).

Compare to `0005`, where `argocd-application-controller` hit
`LIST repos` on first contact with the cluster.

Flux-controller log grep for aggexp mentions:

```
source-controller:      0 lines matching error/aggexp/warn
kustomize-controller:   0 lines matching error/aggexp/warn
helm-controller:        0 lines matching error/aggexp/warn
notification-controller:0 lines matching error/aggexp/warn
```

Not one reference, not one error, not one warning. Over 10+ minutes
including a full AA outage + recovery.

The mechanism is visible in the source-controller startup log: its
`EventSource` list enumerates only *Flux's own CRD kinds* —
GitRepository, Bucket, HelmChart, HelmRepository, OCIRepository —
plus `*v1.PartialObjectMetadata` in kustomize-controller for the
kinds referenced by the Kustomization it is reconciling. Nothing
Flux runs does a discovery-driven LIST of every API group it has
RBAC for. That's what distinguishes it from gitops-engine.

Source-controller startup:

```
Starting EventSource ... "gitrepository" ... source: "kind source: *v1.GitRepository"
Starting EventSource ... "helmchart"     ... source: "kind source: *v1.Bucket"
Starting EventSource ... "helmrepository"... source: "kind source: *v1.HelmRepository"
Starting EventSource ... "helmchart"     ... source: "kind source: *v1.HelmChart"
Starting EventSource ... "helmchart"     ... source: "kind source: *v1.HelmRepository"
Starting EventSource ... "ocirepository" ... source: "kind source: *v1.OCIRepository"
Starting EventSource ... "helmchart"     ... source: "kind source: *v1.GitRepository"
Starting EventSource ... "bucket"        ... source: "kind source: *v1.Bucket"
```

Kustomize-controller startup:

```
Starting EventSource ... "kustomization" ... source: "kind source: *v1.OCIRepository"
Starting EventSource ... "kustomization" ... source: "kind source: *v1.PartialObjectMetadata"
Starting EventSource ... "kustomization" ... source: "kind source: *v1.Kustomization"
Starting EventSource ... "kustomization" ... source: "kind source: *v1.Bucket"
Starting EventSource ... "kustomization" ... source: "kind source: *v1.GitRepository"
Starting EventSource ... "kustomization" ... source: "kind source: *v1.PartialObjectMetadata"
```

Two `PartialObjectMetadata` informers. These are the ones that could
in principle touch arbitrary API groups (they're used for garbage-
collection / inventory reconciliation against kinds the Kustomization
has rendered). Empirically: neither of them LISTed our
`repos.aggexp.io` group either. The inventory of applied objects
drives what PartialObjectMetadata shape the watcher registers, and
our toy Kustomization only applied `Deployment.apps`, `Service`, and
`HorizontalPodAutoscaler.autoscaling`. No aggexp resources appeared
in the Kustomization's inventory, so no informer got registered
for `repos.aggexp.io`.

### 4. AA outage is invisible to Flux

Scaled `deploy/aggexp` to 0 at 05:35:08; back to 1 at 05:36:16
(~68s outage). During and after the outage:

```
# source-controller
{"level":"info","ts":"2026-04-30T05:35:28.620Z","msg":"no changes since last reconcilation: observed revision '...'"}
{"level":"info","ts":"2026-04-30T05:36:26.632Z","msg":"no changes since last reconcilation: observed revision '...'"}

# kustomize-controller
{"level":"info","ts":"2026-04-30T05:35:31.337Z","msg":"server-side apply completed",...}
{"level":"info","ts":"2026-04-30T05:36:30.245Z","msg":"server-side apply completed",...}
```

Normal reconcile cadence throughout. Zero retry-watch-failure-style
errors of the sort that ArgoCD produced at ~1 Hz for 144 seconds
during its outage (0005). The AA is, from Flux's perspective,
something that exists in the cluster and that Flux has no reason
to interact with, so Flux doesn't notice when it goes away.

### 5. Consequent noise: GitHub rate-limit still bites

```
I0430 05:32:07.212234       1 storage.go:270] "github-list-failed" owner="kubernetes-sigs" err="github status 403"
I0430 05:33:07.256565       1 storage.go:270] "github-list-failed" owner="kubernetes-sigs" err="github status 403"
...
I0430 05:42:10.073714       1 storage.go:270] "github-list-failed" owner="kubernetes-sigs" err="github status 403"
```

Same consequent as 0004 and 0005: unauthenticated GitHub REST has
already hit the 60-req/hr ceiling by the time this experiment runs,
so our `repos` group is empty throughout. This does not change the
experiment's answer; from Flux's perspective the AA is there and
is not queried.

## Direct comparison to 0005 (ArgoCD)

Same cluster shape. Same AA deployment. Same read-only driver.
Same default-deny-for-everything-not-explicitly-allowed policy that
does not allow Flux's controller SAs (nor ArgoCD's). What differed:

| Probe                                           | ArgoCD (0005)                                                             | Flux (0014)                                      |
| ----------------------------------------------- | ------------------------------------------------------------------------- | ------------------------------------------------ |
| Eager discovery LIST of every API group         | **Yes** — gitops-engine cluster cache                                     | **No** — controllers only watch their own CRDs   |
| First 403 on our AA                             | On install                                                                | Never                                            |
| Unrelated sync bricked by 403 on `repos`        | **Yes** — toy-guestbook stuck at `sync=Unknown, ComparisonError`          | N/A — no 403 ever                                |
| Error rate during 60-80s AA outage              | **~1 Hz** ("Watch failed" × 145 over 144s)                                | **0 Hz** — no mention of AA anywhere             |
| Steady-state request cadence to AA              | ~1 LIST + 1 WATCH per 5 min per resource (reflector resync)              | 0                                                |
| Recovery on AA return                           | Immediate once AA comes ready                                             | Trivial (nothing to recover)                     |
| Toy sync succeeded after resolving the 403?     | Yes, after patching policy to allow `argocd-application-controller` SA  | Yes; no patch ever required                      |

Structurally: ArgoCD's cluster cache treats "the cluster" as a
uniform space it must catalog end-to-end; Flux's controllers are
narrowly scoped to the CRDs they own plus whatever kinds their
managed Kustomization actually renders. That's a design choice and
it is the entire delta.

## Fundamentals touched

**Wire protocol fidelity.** Second sustained non-kubectl consumer
after ArgoCD (0005); third if you count controller-runtime (0012).
Flux did not exercise the AA's wire surface at all in this scenario
because it does not do discovery-driven LIST. **Which means this
experiment provides almost no new evidence about our AA's wire
protocol handling.** That's itself the finding: "Flux-compat" with a
read-only AA in a side namespace is structurally an empty test. A
real wire-level probe of Flux against our AA would require a
Kustomization that *produces a Repo manifest as part of its apply
set* (MVP-example E4 direction), which is out of scope here.

**Per-request authorization.** The 0005 hazard (default-deny
controller-SAs → unrelated syncs brick) is **specific to discovery-
driven-LIST ecosystem tools**. Flux isn't one of those, so the
hazard does not apply. This sharpens the 0005 finding:
**the-operational-cost of AA default-deny is not "any cluster-wide
controller"; it is specifically "any cluster-wide controller that
auto-discovers-and-LISTs every API group it has RBAC for." ArgoCD
does this; Flux does not; kube-controller-manager does (seen in
0004 logs, inconsequentially). Each has to be assessed on its own.**

The sibling candidate `aa-authz-aware-controllers` in EXPERIMENTS.md
is still worth doing — but the threat model shrinks: it is
"gitops-engine-family tools and kube-controller-manager", not "any
controller."

**Watch and consistency semantics.** Not exercised. Flux's AA
non-engagement means there's no watch to observe.

## Consequents (implementation-dependent; do not generalize)

- **Flux v2.8.6 default component set.** The finding is about *this*
  set (source-controller + kustomize-controller + helm-controller +
  notification-controller). Future Flux versions could add a
  component with broader discovery surface; image-automation-
  controller and image-reflector-controller (not installed by
  default) were not probed and could have different discovery
  behavior.
- **Toy Kustomization's rendered surface is narrow.** Our podinfo
  kustomize path produces only `apps.Deployment`, `Service`,
  `HorizontalPodAutoscaler`. The PartialObjectMetadata informers
  kustomize-controller registers are driven by the inventory of
  applied objects; a Kustomization that rendered a Repo manifest
  would register a PartialObjectMetadata informer on
  `repos.aggexp.io` and the story changes. We did not test that
  case (see open questions).
- **Controllers bound to `cluster-admin`.** Our observation that
  Flux doesn't LIST our AA despite having RBAC to do so hinges on
  this binding. A maintainer who tightens Flux's RBAC doesn't change
  the finding; a maintainer who replaces kustomize-controller with a
  speculative "full cluster inventory" component would.
- **0004 GitHub rate-limit 403.** Re-observed. Unchanged consequent;
  didn't affect Flux's behavior.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**, the merge step should:

- **Per-request authorization.** Refine the 0005 finding: the
  default-deny-for-SAs hazard applies specifically to "cluster-wide
  controllers that auto-discover-and-LIST every API group they have
  RBAC for." ArgoCD's gitops-engine is one; kube-controller-manager
  (observed in 0004) is another, at least for the kinds it manages.
  Flux's controller set is **not** one, because it only registers
  informers for its own CRDs plus whatever kinds its managed
  workload renders. The `aa-authz-aware-controllers` candidate is
  still valuable, but the threat model shrinks.
- **Wire protocol fidelity.** Mention Flux as "second sustained non-
  kubectl consumer, but in the probed configuration did not engage
  with our AA at all; a future writable-Repo / E4-style experiment
  is what would actually exercise Flux's wire path against our AA."

For **EXPERIMENTS.md**, the merge step should:

- Mark `0014-flux-compat` complete.
- Retire the bare `flux-compat` candidate under wire-protocol
  fidelity (answered).
- Add / keep **`flux-applies-a-repo`** as a derived candidate: what
  happens when a Flux Kustomization's rendered inventory includes a
  `Repo` object (against a writable AA)? Depends on MVP-example E3
  prerequisites (writable Repo). This is the Flux analog of
  `argocd-application-targets-aa`.
- The `aa-authz-aware-controllers` candidate's scope narrows (see
  above); worth re-wording when that experiment is picked up.

## Open questions raised

- **Writable-AA / Flux-writes-a-Repo case.** If a Flux Kustomization
  rendered a `Repo` manifest as part of its apply set, would
  kustomize-controller start registering a PartialObjectMetadata
  informer on `repos.aggexp.io`? If so, its SA would start hitting
  the AA's authz and the 0005-shaped hazard would apply to that
  Kustomization's reconcile. Not probed; depends on a writable AA.
- **HelmRelease-driven apply surface.** helm-controller was not
  exercised in this experiment (no HelmRelease resource applied).
  Its discovery footprint could differ from kustomize-controller's.
- **image-reflector-controller / image-automation-controller.**
  These are Flux components not in the default install. Their
  discovery shape is untested.
- **Does `flux diff kustomization`** (CLI-time, not cluster-side)
  speak to our AA? The CLI's behavior against an AA discovery-exposed
  resource wasn't probed.

## Cluster cleanup

The `aggexp-flux` kind cluster was deleted after findings were
captured.
