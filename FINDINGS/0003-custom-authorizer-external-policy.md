# Findings — 0003 custom-authorizer-external-policy

## What we were trying to learn

Can an aggregated apiserver make per-request authorization decisions
against an external policy service, based on caller identity plus
request attributes — and what does that look like to standard
tooling?

Four concrete hypotheses going in:

1. A custom `authorizer.Authorizer` wired into
   `serverConfig.Config.Authorization.Authorizer` via `union.New`
   is actually consulted on every relevant request.
2. With permissive upstream RBAC, the AA's authorizer is the
   effective gate.
3. Denials surface to kubectl as HTTP 403 with our reason string
   embedded in the message.
4. `kubectl auth can-i` is answered by kube-apiserver RBAC, not by
   the AA — so `can-i` reports incorrect allows against the
   permissive ClusterRole.

## What we did

Forked 0002 into `experiments/0003-custom-authorizer-external-policy`
(ethos says duplicate rather than abstract; 0002 is the only other
library-backed AA so there's nothing yet to share). Added:

- `pkg/authz/authorizer.go` — an `authorizer.Authorizer` that POSTs
  a JSON payload to a configurable URL. Only opines on the
  `aggexp.io` group; returns `NoOpinion` for everything else so the
  library's privileged-groups / health-path / SAR chain still
  handles them. Fails-open-to-NoOpinion on transport errors.
- `policy-service/` — a stdlib-only HTTP service backed by a JSON
  rules file. Rule matching supports `*`, prefix globs like
  `bob-*`, and `a|b|c` alternation.
- `manifests/` — a permissive ClusterRole/Binding that grants
  `system:authenticated` full CRUD on `hellos.aggexp.io` (so upstream
  RBAC does not pre-deny our test identities), the rules ConfigMap,
  and the policy-service Deployment + Service.

Two rebuilds during the experiment:

- First wiring of `union.New(existing, ext)` produced no authz calls
  because the delegating SAR authorizer in the existing chain
  returns `Allow` when permissive RBAC grants the verb, short-
  circuiting our authorizer. Swapped to `union.New(ext, existing)`
  so ours runs first; then it fires on every aggexp.io request.
- Policy service's `matchField` originally only handled `*` and
  `a|b|c`; did not support prefix globs. Added `prefix*` handling
  to make `bob-*` match `bob-hi`.

Deployed and tested with `kubectl --as alice`, `kubectl --as bob`,
`kubectl --as mallory`, and admin. Ran the compat scoreboard.

## What we observed

### The authorizer *is* consulted on every aggexp.io request

Policy-service logs show one decision per authz check. kubectl
`apply` on a new resource produces several: a GET to fetch current
state (for 3-way merge), a CREATE to submit the new object, and in
SSA paths a PATCH. Each is individually authorized.

Admin requests flow through with `rule[0]` matching every time;
`alice` hits `rule[2]` for reads and `rule[3]` for writes;
`bob` hits `rule[4]` for `bob-*` names and `rule[5]` as a default
deny otherwise; `mallory` falls to `default: allow=false`.

### Chain order matters sharply — library chain first causes silent no-op

With `union.New(existing, ours)`, our authorizer was never called,
despite the module loading correctly. The delegated SAR authorizer,
consulting kube-apiserver's RBAC that we deliberately made
permissive, returned `Allow` for every aggexp.io request and
short-circuited the rest of the chain.

Swapping to `union.New(ours, existing)` put our authorizer in front:
it opines on aggexp.io resource requests and returns `NoOpinion` for
everything else (health paths, metrics, non-aggexp.io groups, non-
resource paths). Non-resource and cross-group requests flow through
the existing chain unchanged — the `system:masters` bypass and
always-allow health-path behavior still work for them.

### Denials carry the reason string verbatim

Kubectl prints:

> `Error from server (Forbidden): hellos.aggexp.io is forbidden:
>  User "alice" cannot create resource "hellos" in API group
>  "aggexp.io" at the cluster scope: alice: writes denied`

The suffix `: alice: writes denied` is the `reason` we returned
from the authorizer. The prefix is the library's standard
`metav1.Status.Message` template; the cluster-scoped / namespaced
distinction, verb, resource, and group are all library-filled.

This means a policy service controls the UX of a denial. Useful
for operators debugging why a specific caller was denied; also a
caveat — any text the policy service emits is visible to the
caller, so don't leak sensitive reasoning.

### `kubectl auth can-i` reports incorrect allows (confirmed)

`kubectl --as alice auth can-i create hellos.aggexp.io` returns
**yes** even though the actual create is denied. `can-i` issues a
`SubjectAccessReview` against kube-apiserver, which answers based on
RBAC — and our RBAC is permissive. The SAR never reaches the AA.

This is a wire-level property of SAR, not something an extension
apiserver can fix. If identity-based authz in the AA is the real
policy, `can-i` is actively misleading.

Two pragmatic responses if an operator's UX cares:

1. Keep RBAC strict upstream as the coarse gate and have the AA
   refine within that. `can-i` stays meaningful but the AA's role
   shrinks.
2. Teach users that `can-i` is advisory and the AA is authoritative.
   This is the direction most of the interesting use cases for
   aggregated APIs will push.

### CREATE carries no resource name in Attributes

When kubectl does `kubectl --as bob apply` on a new `Hello` named
`bob-hi`, the authz check fires with `verb=create, name=""`. The
name lives in the request body, not in the URL; the authorization
filter doesn't populate `Attributes.GetName()` for CREATE.

Practical consequence: name-based rules cannot be enforced at
CREATE time through the authorizer. Our rule "bob can create
Hellos with `name: bob-*`" silently fell through to the
`bob default deny` rule because `name=""` does not match
`bob-*`.

This is a fundamental separation-of-concerns in Kubernetes:
**authorization is about who-can-do-what on the resource URL**;
shape-of-the-payload decisions belong to **admission** (validating
admission webhooks / CEL policies). An extension apiserver that
wants name-based create policies needs admission logic, not
authorizer logic.

### Latency is not perceptible at lab scale

Serial `kubectl get hellos` calls through the full aggregation
layer + policy-service HTTP round trip measured 63–67 ms
consistently. Most of that is kube-apiserver's aggregation-layer
proxy latency, not our authorizer. No observable degradation vs.
0002 which had no external authz call.

### Impersonation works; identity arrives intact

`kubectl --as alice` successfully reached the AA. The REST storage's
mutation log recorded `user=alice groups=[system:authenticated]`.
The policy service received the full `user.Info` in its request
body. Identity handoff continues to work exactly as in 0001 / 0002;
the authorizer's extra consumption of it is clean.

We did not probe impersonation of `Extra` fields or UID — later
experiments may do so.

### Fails-open-to-NoOpinion is safe here because 403 is the default

We chose `NoOpinion` on transport errors reaching the policy service
rather than `err != nil` (which would surface as HTTP 500). Because
our authorizer runs first and the request then falls through the
library chain — where SAR will say `Allow` only if RBAC permits —
the net effect of a policy-service outage is: requests succeed iff
upstream RBAC permits them. That's **not** what we want for a
sensitive production setup, but it's consistent for this lab:
permissive RBAC means a policy-service outage would mean everyone
can do everything. Worth documenting as a consequent; a production
design would fail closed (return `Deny` or `err`) and accept a
noisier denial shape.

### Compat scoreboard still 7/7 as admin

Admin is allow-all in our rules, so no compat check regressed.
`FINDINGS/compat/2026-04-29-02.md` shows the same 7/7 as 0002.
Under a denying identity the numbers would drop, but they would
reflect the policy, not the AA's technical capability. The
scoreboard continues to be an admin-identity test.

## Fundamentals touched

**Per-request authorization.** First-class confirmation. An AA can
make every authorization decision based on runtime identity +
request attributes against an external system. The library
cooperates cleanly via `union.New`.

Four concrete findings:

- The **chain-order requirement** is sharp: with permissive
  upstream RBAC, a custom authorizer must run *before* the
  delegated SAR authorizer. Otherwise SAR returns Allow and
  short-circuits.
- Denials from the AA carry our **reason string verbatim** in the
  HTTP 403 body. This is UX surface area.
- `kubectl auth can-i` is answered by kube-apiserver RBAC, so
  **can-i lies** when the AA is the real gate. This is a wire-
  level property, not fixable from the extension.
- **CREATE attributes don't carry a resource name**; name-based
  creation policy must live in admission (webhook / CEL), not
  authz.

**Identity handoff.** Observed end-to-end through the authz
pipeline: `user.Info` arriving at the authorizer matches what
the REST storage later sees; `kubectl --as` impersonation works
straight through. No surprises. The JSON payload the AA posts to
the policy service is an interesting surface area — it carries
name, groups, UID, extras, plus request attributes — and a
production-grade broker could be built over it.

**Wire protocol fidelity.** Unchanged by this experiment; 7/7
compat checks still pass. Per-request authz does not interact
with the wire contract beyond the HTTP 403 path, which was
already exercised.

## Consequents (implementation-dependent; do not generalize)

- `union.New` chain-order sensitivity is specific to the 1.32
  apiserver. The underlying semantics (Deny short-circuits, Allow
  short-circuits, NoOpinion continues) is stable, but which
  authorizers are in the default chain depends on which
  `DelegatingAuthorizationOptions` defaults the library ships —
  those defaults have been stable but aren't frozen.
- Message template for Forbidden (cluster-scope vs namespaced
  phrasing) comes from `apierrors.NewForbidden`; a future library
  rewrite could change it.
- Latency of 63–67 ms is a combination of the local kind cluster,
  the kube-apiserver aggregation-layer proxy timeout defaults, and
  our TTL-less policy service. Different in different clusters.
- Our policy service's tiny DSL (`*`, `prefix*`, `a|b|c`) is a
  lab-local choice with no bearing on the fundamentals.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**:

- `Per-request authorization` gets a concrete datapoint. The
  permissive-RBAC-plus-AA-authorizer pattern works; chain-order
  rules are documented; the `can-i` limitation is captured.
- Add a nuance: **authorization and admission are distinct
  Kubernetes concerns**, and certain policy shapes (name-based
  CREATE) require admission, not the authorizer interface. This
  was not in the pre-experimental hypothesis; it's a real
  boundary to record.

For **EXPERIMENTS.md**:

- `custom-authorizer-external-policy` — marked complete.
- Retain `authorizer-cel` as a candidate: using CEL in the AA's
  authorizer would remove the external-service round trip at the
  cost of adding an evaluator.
- `sar-delegation-compare` — now pointed-at: with 0003 we have
  the "permissive RBAC + AA decides" pattern. A companion
  experiment could flip to "strict RBAC + AA refines" to observe
  what changes for `can-i` and `kubectl auth reconcile` UX.
- New candidate suggested: **`name-aware-admission`** — a
  validating admission hook in the AA that enforces the
  name-based policy we could not enforce in the authorizer.
  Probes the authz-vs-admission distinction directly.

## Open questions raised

- How does a controller-runtime informer behave when its identity
  is denied by the AA's authorizer? Does the reflector hot-loop?
  Log the deny and back off? Crash? Not yet probed.
- Does SSA (`kubectl apply --server-side`) produce a different
  sequence of authz checks than client-side apply? We saw both
  work under admin; denials under alice/bob hit the authz path
  either way, but the exact verb sequence under SSA is worth
  logging.
- Can we safely cache the policy-service answer for
  `(user, verb, resource, namespace, name)` for, say, 1 second?
  The library already caches the webhook SAR; adding a cache to
  our authorizer is trivial. How much does it change latency
  under sustained load?
- What happens when the policy service itself is evicted or
  down? We fail-open-to-NoOpinion; the next authorizer (SAR)
  then answers based on RBAC, which in our case is permissive.
  That's arguably wrong for production; a fail-closed probe
  would document the shape of that denial.
- Can we pass custom `user.Info.Extra` fields through `kubectl
  --as --as-user-extra` (1.35+) and see them arrive at the
  policy service? Interesting for the identity-handoff
  fundamental.
