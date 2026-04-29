# Findings — 0005 argocd-compat

## What we were trying to learn

How does ArgoCD behave when installed into a cluster that exposes an
aggregated API it cannot modify (`repos.aggexp.io/v1`, the read-only
0004 driver)? Three specific probes:

1. Does ArgoCD's cluster cache — which dynamically LIST+WATCHes every
   discovered resource — handle a polling-backed synthetic-watch AA
   cleanly over tens of minutes?
2. When the AA is unavailable (pod scaled to 0), what does ArgoCD
   log? Does it degrade silently, noisily, or crash?
3. Does ArgoCD's watch on `repos.aggexp.io` block sync flow for an
   unrelated Application whose target resources have nothing to do
   with aggexp.io?

## What we did

- Dedicated kind cluster `aggexp-argocd`. Deployed base manifests +
  0004 overlay with `GITHUB_OWNER=kubernetes-sigs` and no PAT.
- Installed ArgoCD from `argoproj/argo-cd/stable/manifests/install.yaml`.
  Server-side apply was required (the
  `applicationsets.argoproj.io` CRD's schema exceeds the 262144-byte
  `last-applied-configuration` annotation limit used by client-side
  apply — a general ArgoCD install quirk, not aggexp-specific).
- Applied a toy `Application` pointing at
  `argoproj/argocd-example-apps` path `guestbook`: plain Kubernetes
  manifests, no aggexp involvement.
- Observed: discovery, initial LIST/WATCH, steady-state request
  cadence, behavior during a 60-second AA scale-to-0 window,
  recovery on scale-back-to-1, and 10 minutes of subsequent quiet.

The 0004 policy-service default-denies every identity except admin,
alice, bob. **The `argocd-application-controller` ServiceAccount is
not in that list**, which turned into the experiment's most
load-bearing observation.

## What we observed

### A dense-and-immediate blocker: one 403 on one resource bricks the
### entire cluster cache

On first contact, `argocd-application-controller` hit `LIST repos`,
was denied by our policy-service ("no matching rule"), and logged:

```
Failed to sync cluster
  error: failed to load initial state of resource Repo.aggexp.io:
         failed to list resources: repos.aggexp.io is forbidden:
         User "system:serviceaccount:argocd:argocd-application-controller"
         cannot list resource "repos" ... no matching rule
```

The `toy-guestbook` Application sat at `sync=Unknown, health=Healthy`
(because nothing had been deployed) with a `ComparisonError`
condition whose message was the same 403 about repos.aggexp.io. The
guestbook pod was not created. The Git fetch worked
(`git_ms=502`); the target-state render was gated behind
`cluster cache is in sync`, and cluster cache refused to be in sync
with even one resource LIST returning 403.

After patching the policy ConfigMap (live, not in 0004 source) to
allow `system:serviceaccount:argocd:argocd-application-controller`
for get/list/watch, a hard-refresh of the Application produced:

- **1 LIST** + **1 WATCH** from the SA on `/apis/aggexp.io/v1/repos`,
  both allowed.
- Cluster cache sync completed within seconds.
- `toy-guestbook` transitioned to `Synced, Healthy`; the
  `guestbook-ui` Deployment appeared in the default namespace.

So: **a single 403 from an aggregated API on LIST propagates through
gitops-engine's cluster cache and stalls every Application, even
ones with no relation to the API group that 403'd.** Fundamental,
not consequent: this is a structural property of how ArgoCD's cluster
cache models "cluster state is ready to compare against".

### Steady-state request cadence is light

Over a 10-minute observation window post-recovery, the AA observed
exactly **4 authz calls** from the ArgoCD SA: 2 LISTs and 2 WATCHes.
Which implies one relist-and-rewatch every ~5 minutes per resource.
Our polling AA's 60-second poll loop was unaffected by this: ArgoCD
is not hammering LIST; it is riding the watch stream and relisting
only on scheduled reflector resync (the ~5-min cadence is
gitops-engine's default; not the same as client-go's default
`minWatchTimeout` of 5–10 min — close enough that it's probably that
mechanism).

No complaints about synthetic resourceVersion. No ResourceExpired
events observed. The watch stream stayed open across the full window.

### Scale-down: ~1 watch-retry per second, no back-off observed

Scaled aggexp to 0 replicas at 05:24:04. The first error appeared at
05:24:05:

```
{"msg":"Watch failed","error":"the server is currently unable to handle the request","time":"2026-04-29T05:24:05Z"}
```

Counted **145 identical "Watch failed" messages in 144 seconds**
(05:24:05 → 05:26:29), i.e. roughly 1 Hz. No discernible backoff.
Scaled back to 1 at ~05:26:30; the AA took about a minute to come
ready; first successful LIST+WATCH at 05:27:29. All errors ceased
the instant the AA returned.

The toy guestbook Application stayed `sync=Synced, health=Healthy`
throughout the outage and the recovery. Its reconciledAt timestamp
did tick forward during the outage (05:23:09 → 05:25:44 → 05:27:48),
meaning ArgoCD continued to attempt comparisons; it just couldn't
refresh the cluster cache view of repos.aggexp.io specifically, and
had enough cached state from before to keep reporting Synced for
resources unrelated to that kind.

**So — partial but interesting answer to probe #3:** during an AA
outage, Applications that have nothing to do with the AA are not
visibly blocked. The initial-sync-from-cold-cache case was blocked
(see above); the warm-cache degraded case is not.

### Discovery and kubectl view

`kubectl api-resources --api-group=aggexp.io` returned `repos`
cleanly. ArgoCD's UI cluster view (not inspected in this experiment
— we worked from logs) would presumably show Repo as a kind based
on this.

### Rate-limit bite re-observed

During the scale-back-to-1, the new AA pod's first poll hit GitHub's
unauthenticated rate limit (403). The cache stayed empty for the
remainder of the session — every minute: `"github-list-failed"
err="github status 403"`. `kubectl get repos` returned "No resources
found" from then on. ArgoCD's LIST against an empty (but successful)
repos endpoint still returned a valid empty list, so **from ArgoCD's
perspective the AA is healthy and the group is just empty**. This
is the 0004-predicted rate-limit coupling resurfacing; it does not
change the experiment's answer but it shaped what ArgoCD saw.

## Fundamentals touched

**Watch and consistency semantics.** A client-go-style reflector
(which is what gitops-engine's cluster cache uses under the hood)
handles our polling-backed synthetic-watch AA fine under steady
state. Initial LIST + long-lived WATCH + periodic (~5 min) relist
is the observed pattern. Synthetic monotonic `atomic.Uint64` RV and
no ResourceExpired machinery is sufficient for this consumer over a
15-minute window.

The **unavailability behavior** is the sharper finding. ArgoCD's
cluster cache retries at roughly 1 Hz with no visible backoff while
the AA is absent. 145 errors in 144 seconds for a ~2-minute outage
is noisy but not catastrophic; a long-term outage would fill logs
steadily. Recovery is clean: first successful LIST resumes the
informer immediately.

**Wire protocol fidelity.** No new demands beyond what 0001–0004
already probed. Discovery worked, LIST returned a valid list, WATCH
streamed. No kubectl-only pathways exercised (no explain, no apply)
because ArgoCD doesn't speak those against an aggregated API it
doesn't own.

**Per-request authorization.** The most operationally important
observation. The default-deny posture that 0003/0004 treat as
good defense-in-depth becomes a **cluster-wide availability
incident for ArgoCD** if the AA's policy doesn't explicitly allow
ArgoCD's controller SA. This is because:

- ArgoCD's cluster cache discovers every API group and attempts to
  LIST every resource it has RBAC for. Our permissive cluster RBAC
  (`aggexp-repos-permissive` in 0004) grants `*` to
  `system:authenticated`, which includes the ArgoCD SA.
- Upstream RBAC says yes; our AA's authorizer says no.
- Gitops-engine treats *any* LIST failure during cluster cache sync
  as a whole-cluster failure, not a per-resource degradation.

The boundary between "RBAC grants reach" and "AA's authorizer
decides" that was named in 0003 now has a concrete operational
cost: an AA whose authorizer denies the workload-controllers-in-
the-cluster's SAs by default will ruin that cluster's ArgoCD.
Cluster-wide consequence of a per-request authz decision.

**Resource modeling freedom.** No new boundary observed. The
read-only nature of our resource is not interesting to ArgoCD: it
never tried to write. If an Application had actually targeted a
`Repo`, this experiment would have been a different one; for now
the interesting surface is ArgoCD's observational footprint on our
API, not its management footprint.

## Consequents (implementation-dependent; do not generalize)

- **ArgoCD install manifest size.** The
  `applicationsets.argoproj.io` CRD's embedded schema blows past the
  262144-byte annotation limit for client-side apply. Use
  `kubectl apply --server-side` for the argoproj install YAML. ArgoCD-
  version-specific; not an aggregated-API property.
- **"Watch failed" log cadence: ~1 Hz.** Tied to gitops-engine's
  current watch-retry implementation; could change in a future
  release. The broader finding — ArgoCD retries watches at some
  rate rather than giving up — is fundamental; the specific
  frequency is not.
- **5-minute relist cadence** is gitops-engine's default; again, a
  future release could change it.
- **ClusterCache sync treats any resource-LIST failure as fatal.**
  This is how gitops-engine is written today; it is plausibly a
  tuning knob upstream could add. Until then, it's a concrete
  ArgoCD property operators need to know about.
- **Unauthenticated GitHub 403 on pod restart.** Already a known
  consequent from 0004.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**, the merge step should:

- Add to **Watch and consistency semantics**: first long-running
  real-informer observation against our AA confirms synthetic RV +
  periodic relist works for a consumer that isn't kubectl, over
  ~15 minutes, through one outage/recovery cycle. The ~5-min
  reflector-resync cadence is the first time we've measured a
  real client's relist-while-healthy behavior.
- Add to **Per-request authorization**: extend the existing finding
  about "AA's authorizer decides in real time" with the operational
  consequence that defaulting-to-deny affects **every
  cluster-wide controller that watches everything**, not just the
  human operator. Controller-SAs (ArgoCD's, likely Flux's, likely
  kube-controller-manager's — already observed in 0004) must be
  accounted for in policy. The SA-denial-by-default posture converts
  a "defense in depth" choice into a "breaks the cluster's ops
  tooling" consequence.
- Add to **Wire protocol fidelity** (open questions): ArgoCD's
  cluster cache was the first tested consumer that actually cares
  about the long-running watch semantics (previous experiments
  only probed kubectl). It survived; but that's one consumer. Flux
  is still untested.

For **EXPERIMENTS.md**, the merge step should:

- Mark 0005 complete.
- Retire nothing — the `flux-compat` candidate is still worth doing,
  and is now more interesting (does Flux react the same way to a
  403 on LIST from an unrelated group? Does its source-controller
  also do broad discovery?).
- Consider adding **`aa-authz-aware-controllers`**: what's the
  right pattern for AA authz to coexist with cluster controllers
  whose identity is not a policy-service-known user? Options:
  allow-list by group-prefix; allow get/list/watch for any
  `system:serviceaccount:*`; out-of-band "controller mode" in the
  authorizer. Derived from 0005.
- Consider adding **`argocd-application-targets-aa`**: what
  happens if an ArgoCD Application actually targets a `Repo`
  object (declarative Git → K8s state, with a write to an AA that
  refuses writes)? Follows MVP-example E4. Probes whether ArgoCD's
  error surface is proportional when writes are refused vs. now
  when reads are refused.

## Open questions raised

- Does ArgoCD's "Watch failed" loop have a built-in backoff in
  longer windows? Our 2-minute outage was too short to distinguish
  "no backoff" from "backoff with a long initial interval".
- Can gitops-engine be configured to treat a LIST failure on one
  resource as non-fatal for the cluster cache? (Upstream ArgoCD
  issue search would settle this; not done in this experiment.)
- What does the ArgoCD UI show for `repos.aggexp.io` when cluster
  cache is in sync? Not probed (logs-only observation session).
- Flux equivalent: does source-controller's watch pattern match
  gitops-engine's, or does it build its informer set differently
  (e.g. only what a Kustomization references, not every discovered
  group)?
- For a production AA: is the right policy-service default
  `allow-for-all-service-accounts get/list/watch` with strict rules
  on writes? Or is the right shape "RBAC upstream is strict; AA
  policy refines within what RBAC already permitted"? 0003's two
  working patterns hit a concrete tradeoff here.
