# Findings — 0016 aa-authz-aware-controllers

## What we were trying to learn

`0005-argocd-compat` surfaced a concrete operational hazard: an AA
whose `authorizer.Authorizer` default-denies every identity it doesn't
recognize bricks any cluster-wide ecosystem controller that auto-
discovers-and-watches every API group it has RBAC for — because
gitops-engine's cluster cache treats one LIST failure as fatal for
the *whole* cache, so an unrelated ArgoCD Application ends up stuck
at `sync=Unknown`.

Three concrete remediation shapes were floated. This experiment
builds all three, deploys each against the same AA + ArgoCD install,
and records the differences:

- **A. Allow-list by SA name.** Policy rules explicitly allow the
  ecosystem-controller SAs we care about (argocd-application-
  controller, argocd-applicationset-controller, argocd-server, flux
  source / kustomize, kube-controller-manager) to `get|list|watch`
  on `repos.aggexp.io`. Human users still gated per-identity.
- **B. Blanket allow for any `system:serviceaccount:*`.** Any SA
  gets reads; humans gated per-identity. One rule; no per-controller
  maintenance.
- **C. Upstream-RBAC strict + AA refines.** No permissive
  ClusterRole for `system:authenticated`; each controller SA gets
  `get|list|watch` via a standard Kubernetes ClusterRoleBinding.
  Requests that reach the AA have already cleared RBAC; the AA's
  policy refines writes by identity only.

The experiment is a single kind cluster `aggexp-authz` running
the 0004 github-driver AA (read-only `repos.aggexp.io/v1`), the 0003
policy-service, and an ArgoCD install syncing a toy `Application`
that points at the argoproj/argocd-example-apps `guestbook` sample
(unrelated to aggexp.io).

## What we did

- Scaffolded the experiment, three per-pattern rules ConfigMaps, a
  permissive and a strict RBAC file, and a `run.sh` harness.
- For each pattern in sequence: swap rules + RBAC, restart policy-
  service and AA, recreate the toy Application, restart the ArgoCD
  application-controller so it drops cluster-cache state, settle 120s,
  capture `admin-view`, `argocd-view`, `can-i` for four identity
  classes, explicit LIST attempts as four impersonated identities,
  CREATE attempts as admin / alice / mallory, plus policy-service,
  aggexp, and argocd-application-controller logs.
- Used an isolated KUBECONFIG per experiment (`kind get kubeconfig
  --name aggexp-authz > .kubeconfig-…`) because parallel worktrees
  clobbered the shared kubeconfig during this session (SYNTHESIS's
  "parallel agents on same kind cluster" process observation biting
  in a slightly different shape: parallel agents on *different*
  kind clusters clobber the shared kubeconfig's `current-context`).

Artifacts for each pattern live under `artifacts/{A,B,C}/`. They're
gitignored; the quotable pieces are below.

## What we observed

### All three patterns: ArgoCD is unblocked

The primary question 0005 raised is answered by all three patterns
equally well. Under each:

```
  ArgoCD app: sync=Synced health=Healthy
  guestbook-ui deployed: YES
  admin LIST repos: OK
```

ArgoCD's cluster cache logged `Cluster cache informer synced` on
the first attempt after its restart; no `Failed to sync cluster`
errors at the aggexp.io LIST path; the guestbook-ui Deployment
appeared in the `default` namespace within the 120s settle window.

The 0005 failure mode — one 403 on one resource bricks the whole
cluster cache and stalls unrelated Applications — does not recur
when *any* of A, B, or C is in place. So on the primary criterion
("can a cluster controller coexist with an identity-gated AA?")
there is no winner; the choice comes from the downstream
consequences below.

### The differences: where the 403 lives, and what can-i says

What changes across the three patterns is **which component 403s a
non-allowed caller**, and therefore **what `kubectl auth can-i`
reports**. Excerpts from `artifacts/{A,B,C}/can-i.txt` and
`list-attempts.txt`:

Pattern A (permissive RBAC, per-SA allow in policy):

| identity                     | can-i list | actual LIST                                    |
|-----------------------------|-----------|-----------------------------------------------|
| kubernetes-admin            | yes       | 200 OK (empty list)                            |
| alice                       | yes       | 200 OK (empty list)                            |
| mallory                     | yes       | **403 from AA** "mallory: deny"                |
| argocd-application-controller SA | yes  | 200 OK                                         |
| default:default SA          | yes       | **403 from AA** "no matching rule"             |

Pattern B (permissive RBAC, blanket-SA in policy):

| identity                     | can-i list | actual LIST                                    |
|-----------------------------|-----------|-----------------------------------------------|
| kubernetes-admin            | yes       | 200 OK                                         |
| alice                       | yes       | 200 OK                                         |
| mallory                     | yes       | **403 from AA** "mallory: deny"                |
| argocd-application-controller SA | yes  | 200 OK                                         |
| default:default SA          | yes       | **200 OK** (B grants reads to any SA)          |

Pattern C (strict RBAC, reads-pass-through in policy):

| identity                     | can-i list | actual LIST                                    |
|-----------------------------|-----------|-----------------------------------------------|
| kubernetes-admin (impersonated) | no    | **403 from kube-apiserver RBAC**               |
| alice                       | yes       | 200 OK                                         |
| mallory                     | yes       | 200 OK (bound in RBAC, refined to deny on writes) |
| argocd-application-controller SA | yes  | 200 OK                                         |
| default:default SA          | no        | **403 from kube-apiserver RBAC** (no suffix)   |

Two substantive pieces of this table:

1. **Under A and B, `can-i` is uniformly `yes`** (because upstream
   RBAC grants `get|list|watch` to `system:authenticated` as a
   blanket). That's the 0003-known "can-i lies when the AA is the
   real gate." Users querying `can-i list repos` see `yes` and then
   get 403 on the actual LIST for reasons the AA's policy controls.
2. **Under C, `can-i` is meaningful** (for reads; still lies for
   writes). A SA not in RBAC sees `no` from `can-i` and 403 from the
   actual LIST — both generated by kube-apiserver with no AA reason
   string.
3. The caveat in Pattern C is that `kubectl --as kubernetes-admin
   get repos` returns 403. Impersonation strips the real admin's
   group membership (`kubeadm:cluster-admins`), and strict RBAC
   doesn't bind user `kubernetes-admin`. The *real* admin (not
   impersonated) holds aggregated cluster-admin privileges and can
   LIST fine; this is an impersonation-erases-groups effect, not a
   Pattern C defect. Still, it changes the UX of "sudo-as"
   debugging.

### Denial messages differ in who speaks

Under A and B, denied SAs see a library-generated 403 with a reason
string authored by our policy-service:

```
repos.aggexp.io is forbidden: User "system:serviceaccount:default:default"
cannot list resource "repos" in API group "aggexp.io" at the cluster
scope: no matching rule
```

That trailing `: no matching rule` is our message, controlled by the
policy-service rule file. Useful for debugging; dangerous for
information leakage, as 0003 flagged.

Under C, denied SAs see a 403 from **kube-apiserver's RBAC
authorizer** with no AA-level reason:

```
repos.aggexp.io is forbidden: User "system:serviceaccount:default:default"
cannot list resource "repos" in API group "aggexp.io" at the cluster scope
```

No trailing reason. The policy-service logs show no entries for this
call; the request never reaches the AA. Operationally this is a
privacy and observability difference: under C, the AA has no idea
that a denied read was attempted. The signal lives in
kube-apiserver audit, not in policy-service logs.

### Cardinality of the policy-service rule set

The rule count drops as we move from A to C:

- Pattern A: 11 rules — admin, cluster-admins group, five controller-
  SA-and-verb rules, alice read + alice deny, mallory deny.
- Pattern B: 8 rules — admin, cluster-admins group, one `SA-prefix +
  reads` rule, one `kube-controller-manager + reads` rule, one
  `SA-prefix deny` rule for writes, alice read + alice deny, mallory
  deny.
- Pattern C: 5 rules — admin, cluster-admins group, one `verb=reads
  allow` rule, alice deny, mallory deny.

Each policy-service log begins with `loaded N rules from
/etc/policy/rules.json (default: allow=false)`. The rule count is a
rough proxy for maintenance burden: A grows linearly with
ecosystem-controller adoption; B and C are flat.

### Write-path behavior (secondary)

The 0004 base grants read-only RBAC (`verbs: ["get", "list", "watch"]`)
on `repos.aggexp.io`. CREATE as any identity (including admin) is
therefore 403'd by kube-apiserver's RBAC, never reaches the AA's
authorizer, under all three patterns. The compiled write-attempts
artifact shows:

```
Pattern A: repos.aggexp.io is forbidden: User "kubernetes-admin"
  cannot create resource "repos" …at the cluster scope
Pattern B: same (kube-apiserver)
Pattern C: same (kube-apiserver)
```

This is a consequent of using the 0004 read-only driver as the test
substrate; it's not informative about the patterns themselves.
However: **mallory's CREATE under A and B is 403'd with our policy
reason "mallory: deny"** on the GET-for-apply-merge subcall, which
is interesting — kubectl apply issues GET before CREATE, and mallory
has no GET permission either, so the policy-service message surfaces
in the apply flow:

```
repos.aggexp.io "alice-toy" is forbidden: User "mallory" cannot get
resource "repos" in API group "aggexp.io" at the cluster scope:
mallory: deny
```

Under C, mallory is bound in strict RBAC (for the deny-at-policy
test) and on the READ path is allowed to GET by the AA's pass-
through rule. On the CREATE path mallory is RBAC-gated (no
permissive write ClusterRole) and gets the no-reason kube-apiserver
message. This is a second instance of where-the-403-lives shifting
based on pattern choice.

### ArgoCD controller cluster cache — no wire-level difference

All three patterns produce a clean `Cluster cache informer synced`
message from `argocd-application-controller` within seconds of its
restart. The policy-service logs for the controller SA:

- Pattern A: `rule[2] matched -> allow=true … system:serviceaccount:
  argocd:argocd-application-controller verb=list … reason=A:
  argocd app controller`.
- Pattern B: same, via `rule[2] … reason=B: any SA may read`.
- Pattern C: `rule[2] … reason=C: reads allowed for anyone RBAC
  let through` — because RBAC already vetted the caller before it
  reached the AA.

In all three cases the actual decision is `allow` and the LIST
succeeds. Wire-protocol fidelity is unchanged across the patterns:
same kubectl discovery shape, same ArgoCD sync flow, same cluster-
cache informer lifecycle.

## Comparison

A side-by-side, from the captured data:

| axis                        | A (allow-list)                 | B (blanket SA)                 | C (upstream strict)           |
|-----------------------------|-------------------------------|-------------------------------|-------------------------------|
| ArgoCD cluster cache        | syncs                         | syncs                         | syncs                         |
| toy Application             | Synced/Healthy                | Synced/Healthy                | Synced/Healthy                |
| rule count                  | 11                            | 8                             | 5                             |
| adding a new controller     | edit rules ConfigMap, per-SA  | no change                     | add a standard RBAC binding   |
| where deny lives (unallowed SA) | AA authorizer (403 + reason) | allowed on reads              | kube-apiserver RBAC (403 no reason) |
| `can-i` accuracy            | lies (permissive RBAC)        | lies (permissive RBAC)        | accurate for reads; lies for writes |
| observability (denied attempts) | in policy-service logs     | mostly allowed; writes in policy logs | in kube-apiserver audit only  |
| blast radius of SA compromise | named SAs only               | **all SAs in cluster**        | only SAs with explicit RBAC   |
| impersonating admin         | works (authz allows)          | works                         | **fails** (impersonation strips groups; strict RBAC doesn't bind user kubernetes-admin) |
| internal consistency with RBAC | "RBAC is a decoration" (all auth is at AA) | same | RBAC is the coarse gate; AA refines |

## Recommendation

**Pattern C (upstream-RBAC strict + AA refines).**

Rationale, explicit tradeoffs included:

1. **Smallest blast radius under a compromised SA.** In B, any
   workload in the cluster that can mint a projected service-account
   token can `LIST repos.aggexp.io`. In A, an omitted controller SA
   breaks silently (its LISTs 403 and, per 0005, bricks the
   consumer's cluster cache). In C, the set of SAs that can even
   reach the AA is explicitly enumerated in RBAC — the same place
   operators already look for "who can do what."
2. **Zero per-controller maintenance in the policy-service.** Adding
   Crossplane or Kyverno under A requires editing the policy rules;
   under C it requires adding a ClusterRoleBinding, which is the
   standard Kubernetes dance and fits existing RBAC tooling and
   GitOps patterns. B is also zero-maintenance but at the cost of
   posture.
3. **`kubectl auth can-i` is meaningful for reads.** Users running
   `can-i list repos` get a truthful answer: yes means RBAC grants
   it; no means it will 403. This is a real UX gain. Writes still
   lie (0003), but they always will when the AA gates them — that's
   wire-level.
4. **Rule file is smallest and most auditable** — five rules,
   all about human identity and write refinement. The reader does
   not have to keep a mental model of "which ecosystem controllers
   are currently deployed and which ones have RBAC to our group."

The tradeoffs Pattern C explicitly pays:

- **Denied reads produce no AA-side observability.** The request
  is 403'd by kube-apiserver before reaching the AA; the policy-
  service has no entry. If you want "why was this caller denied"
  from the AA's side, you lose that under C. The signal lives in
  kube-apiserver audit.
- **Denied reads produce a reasonless 403 message.** The caller
  sees the library's template with no suffix. Less helpful for
  debugging than A/B.
- **Impersonation changes outcomes.** `kubectl --as <user>` erases
  the impersonator's group memberships, so impersonating a user
  not explicitly RBAC-bound to the read role will 403 under C,
  even if the impersonator is cluster-admin. This surfaced for
  `kubectl --as kubernetes-admin` in our test. Workaround:
  explicitly `--as-group` or bind cluster-admin. Observed, not
  fatal.
- **Adding a new human-readable group (e.g. `team-foo` maps to
  read-access) requires RBAC work *and* possibly policy work**
  if the AA needs to refine writes for that group. Under A and B
  all policy lives in the policy-service.

If the AA's threat model prioritizes "denied attempts must be
observable from the AA side" (e.g. an audit requirement that the
AA must log every access attempt), Pattern A is the right answer
and the operational cost of maintaining a per-controller allow-list
is the price. Under any other posture, C is better.

**Rejected: Pattern B.** Its simplicity is tempting, but granting
every ServiceAccount in the cluster read-access to an API that is
deliberately backed by identity-aware authorization undercuts the
reason for having the AA in the first place. A single pod in any
namespace can LIST our whole resource surface. For a lab that's
fine; for anything with real identity separation it's not.

## Fundamentals touched

**Per-request authorization (primary).** The 0005 operational hazard
has three concrete resolution shapes, differentiated by where the
"who can reach this AA at all" decision is taken:

- A: at the AA, per-identity, with a policy rule per allowed
  controller SA.
- B: at the AA, wildcard-matched on the `system:serviceaccount:*`
  user prefix.
- C: at kube-apiserver via standard RBAC; the AA only refines among
  identities RBAC has already vetted.

C cleanly separates coarse reachability (RBAC) from policy refinement
(AA). This also means the AA's authorizer becomes more honest about
what it opines on — in C it opines only on writes, because reads
pass through from anyone-RBAC-let-in. The `can-i`-lies consequence
from 0003 is **partially corrected under C** and **preserved under
A and B**.

**Wire protocol fidelity (secondary, via controller-compat).** None
of the patterns changed the wire shape seen by ArgoCD's cluster
cache. All three produced `Cluster cache informer synced` and a
Synced/Healthy Application. The patterns are indistinguishable at
the protocol level; they differ only in who gets to see the
protocol.

## Consequents (do not generalize)

- `kubectl --as kubernetes-admin` erasing groups is kubectl's
  impersonation behavior. It would erase groups for any
  impersonation target; Pattern C is not uniquely affected. The
  reason we noticed it here is that strict RBAC doesn't bind
  user-name `kubernetes-admin`, only group `kubeadm:cluster-admins`.
- ArgoCD's cluster cache sync time (~seconds after controller
  restart) is ArgoCD-specific. Flux will probably differ; queued
  as `0014-flux-compat`.
- The `policy-service` rule DSL's `prefix*` matching makes Pattern
  B one rule; without that, it would be many. That's a lab-local
  choice without bearing on the fundamental.
- CREATE failing at RBAC upstream for everyone (because the 0004
  ClusterRole grants only reads) is a property of using the read-
  only GitHub driver as the AA under test. A writable AA would
  route more of the CREATE path through the policy-service and
  expose more per-pattern differences on writes. Not done here;
  writes are not the operational question 0005 raised.
- `kubectl auth can-i` warning about "resource 'repos' is not
  namespace scoped" is kubectl output noise; it does not affect
  the yes/no decision.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS** (Per-request authorization section):

- The 0005 operational-hazard paragraph can now reference three
  concrete remediation shapes with explicit tradeoffs. The
  recommendation is Pattern C (upstream-RBAC strict + AA refines)
  unless AA-side observability of denied attempts is a hard
  requirement, in which case Pattern A.
- The long-standing `kubectl auth can-i` lies observation from
  0003 is refinable: **it lies for everything the AA is the sole
  gate for**, which under Pattern C is only writes. Reads under C
  are RBAC-gated and `can-i` tells the truth about them.
- A new adjacency named: **where the 403 lives**, between kube-
  apiserver RBAC and AA authz, is a design knob with observable
  UX consequences (message shape, reason string presence, log
  locality). Under C the AA's policy rules are materially simpler
  because the AA's scope has narrowed.

For **EXPERIMENTS.md**:

- Mark 0016 complete.
- Retire the `aa-authz-aware-controllers` candidate under Per-
  request authorization (answered here).
- The `argocd-application-targets-aa` candidate (ArgoCD writes
  to a `Repo`) is sharper now: it depends on a writable AA and
  Pattern C's RBAC model. Under C, granting ArgoCD SA write RBAC
  to repos.aggexp.io is a standard operator move; the AA's
  policy then refines which names / shapes it accepts.
- `flux-compat` (0014 candidate) should re-run all three patterns
  to confirm Flux's discovery / watch behavior is Pattern-
  independent the same way ArgoCD's is, or surface a difference.

## Open questions raised

- **Pattern C + `--as` debugging UX.** Operators who routinely
  impersonate for debugging (`kubectl --as kubernetes-admin`) will
  see surprising 403s under C. Does a convention emerge of binding
  cluster-admin role to the user-name as well as the group?
- **AA-side audit under Pattern C.** We have no AA-side log of
  denied reads under C. Does kube-apiserver audit provide
  sufficient coverage for AA operators' needs? Or is a second-layer
  audit (AA logs every request it sees) still wanted?
- **Flux behavior across patterns.** ArgoCD's cluster cache was
  pattern-agnostic. Is Flux's source-controller / kustomize-
  controller discovery also pattern-agnostic? If so, the
  recommendation stands; if not, surface it.
- **What about `kubectl api-resources` discovery for denied SAs?**
  We didn't probe whether a default-SA can even SEE the
  `repos.aggexp.io` resource via discovery under C. If discovery
  is RBAC-gated, a controller with no binding might not even know
  the resource exists — which may be an additional benefit of C.
