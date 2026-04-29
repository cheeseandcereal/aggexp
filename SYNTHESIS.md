# Synthesis

This file reflects the current best understanding of the **problem
space** — aggregated Kubernetes APIs and their boundaries. It is
silent about code structure (that's `ARCHITECTURE.md`'s job) and
silent about individual experiments' specifics (those live in
`FINDINGS/`).

This file is **rewritten**, not appended to. When the author's mental
model of the problem space shifts meaningfully, the relevant sections
are rewritten. History lives in git.

Organized around the six named fundamentals. An entry under each
fundamental says what is currently believed and which FINDINGS
provide the evidence.

---

## Current state

Informed by `FINDINGS/0001-raw-http-aggregation` (a hand-rolled Go
stdlib probe against kube 1.32 / kubectl 1.35 in kind). Everything
else remains pre-experimental hypothesis. Claims without a
`FINDINGS/*` reference are unvalidated.

## Wire protocol fidelity

**The bar for `kubectl get` is lower than commonly assumed.** A stdlib-
only HTTP handler (no `k8s.io/apiserver`) that returns correct
`APIResourceList` discovery, a `HelloList` with `TypeMeta` +
`ObjectMeta`, a `meta.k8s.io/v1 Table` via content negotiation, and
`/livez` / `/readyz` is sufficient for `kubectl api-resources`,
`kubectl get <resource>`, and `kubectl get <resource> -o yaml` to
pass [`0001`]. Short names and minimal OpenAPI v2 / v3 stubs do not
cause rejection. The aggregation layer tolerates legacy discovery
shape (`APIResourceList`) when newer `APIGroupDiscoveryList` is
requested via Accept headers — it falls back gracefully [`0001`].

**The first break is `kubectl explain`.** Structurally-valid minimal
OpenAPI v2/v3 documents are not enough; kubectl looks up the GVR in
the schema index and fails with `GVR ... not found in OpenAPI schema`
[`0001`]. The schema needs the right extensions (`x-kubernetes-group-
version-kind`), path/component structure, or both. This suggests a
binary distinction worth probing: either the OpenAPI is "real enough"
(and both `explain` *and* server-side-apply start working) or it
isn't.

**Watch at wire level is also low-bar** — chunked NDJSON with
`ADDED` + `BOOKMARK` events is accepted by kubectl [`0001`]. What
kubectl does with them in `-w` mode is its own story (table rendering
mutates between renders on the stream — consequent, implementation-
specific). We have not yet probed the harder watch cases:
`MODIFIED`/`DELETED` events, stale-RV 410-Gone reconnect, bookmark-
driven RV advance, long-lived informer relist.

Open questions:

- What exact OpenAPI v3 shape does `kubectl explain` require? (Likely
  `hello-aggregated` or `ssa-probe` will answer.)
- How tolerant is the ecosystem (ArgoCD, Flux, controller-runtime) of
  a server that honors the protocol approximately vs. exactly?
- Does server-side apply work if we emit the right OpenAPI, or does
  it need independent machinery (fieldmanager, managedFields)?

## Identity handoff

**Baseline: the aggregation layer forwards more identity metadata than
just user + groups.** The probe received `X-Remote-Extra-
Authentication.kubernetes.io%2Fcredential-Id` with an X.509 SHA256
fingerprint, with no additional configuration on kube-apiserver or
the AA side [`0001`]. Whatever extras kube-apiserver's authenticators
populate into `user.Info.Extra` flow through as `X-Remote-Extra-*`
headers with `/` escaped as `%2F`.

This upgrades the Extra-forwarding question from "is it possible?" to
"it is happening by default; what are the consequences?" The security
model must assume that extras already surface in the AA. Anything
sensitive that a future authenticator stashes in extras will be
visible to the AA.

**Bearer tokens are stripped — confirmed.** No `Authorization` header
reached the probe. Identity → credential exchange for downstream
backends must happen at the AA, not through forwarding.

**Internal control-plane traffic is mixed in.** The AA receives
requests with `X-Remote-User=[system:kube-aggregator]` and
`X-Remote-Group=[system:masters]` during discovery / openapi refresh
[`0001`]. Worth filtering when analyzing identity-based behavior.

Open questions:

- What is the complete set of extras populated by each authenticator
  type (client cert, SA token, OIDC)? The credential-id was a
  surprise; what else is already in there?
- What's the cleanest pattern for "do X on behalf of the caller" when
  the caller is Kubernetes-native and the target speaks a different
  identity? (Broker pattern, workload-identity federation.)

## Storage independence

Hypothesis (unchanged, unvalidated beyond the stateless probe
existing): an aggregated apiserver does not require etcd. The probe
`0001` proved the stateless-on-the-way-out shape trivially (no backend
at all). Real storage-independence with CRUD semantics is untested.
The synthetic-watch pattern (poll + broadcast + monotonic RV) remains
hypothetical until an experiment implements it.

Open questions:

- Where does the polling-driven synthetic-watch pattern break at
  scale?
- What is the smallest viable resourceVersion scheme that satisfies
  client-go's relist semantics without backend state to derive from?

## Per-request authorization

**Refinement: kube-apiserver RBAC is a gate the request must pass
before the AA ever sees it.** `kubectl --as alice get hellos` was
rejected by kube-apiserver with `Forbidden` before reaching the probe
[`0001`]. This means a custom authorizer in the AA is **additive** —
it can only restrict further, not expand. For custom authz to matter
for a given user, that user must first have RBAC that permits the
verb/resource.

Two patterns emerge:
1. Grant permissive RBAC upstream (e.g. `system:authenticated` gets
   `get`/`list`/`watch` on the group) and make the AA's authorizer
   the real gate. Makes the AA's authorizer the security-relevant
   decision point.
2. Keep RBAC strict upstream and use the AA's authorizer only for
   finer-grained decisions *within* what RBAC already allows.

The pattern choice has meaningful UX implications: with pattern 1,
`kubectl auth can-i` is meaningless (it asks RBAC, not the AA). With
pattern 2, `can-i` remains useful but the AA's authorizer is only
ever consulted for the subset of users/verbs that RBAC pre-approves.

Open questions:

- Does `SubjectAccessReview` from the AA back to kube-apiserver
  preserve the interaction pattern, or is it orthogonal?
- What is the performance budget for a per-request authz call to an
  external policy service?
- How does pattern 1 interact with standard tooling that consults
  `can-i` / SelfSubjectRulesReview before attempting an action?

## Resource modeling freedom

Untested beyond the trivial `Hello` resource in `0001`. Hypothesis
(unchanged): anything with an addressable identity and a schema can
be projected as a Kubernetes resource. The interesting boundaries —
backends without stable names, without list operations, without
deletion primitives — remain unprobed.

## Watch and consistency semantics

**Watch mechanically works at the wire level** [`0001`]. What kubectl
does client-side with a minimally-spec'd watch stream is complicated
and partially surprising (table schema changing between renders in
watch mode) — but those surprises are currently consequent and
plausibly resolvable with richer event emissions (`MODIFIED`,
`DELETED`).

The harder questions — stale-RV 410-Gone handling, bookmark-driven
RV advance in informers, controller-runtime informers past the
relist boundary — are all untested because the probe's
`resourceVersion`s are static.

Open questions:

- What watch behavior does a long-lived controller-runtime informer
  actually require? Bookmarks? Strict RV monotonicity? Precisely
  correct event ordering?
- What happens when the AA's serving cert rotates mid-watch? Does the
  client reconnect cleanly?
- If the backend emits its own change events, can we skip polling
  entirely?

---

## Process observations

One early observation worth noting: the start-of-task reading ritual
in `AGENTS.md` is only useful if FINDINGS are dense enough to be
worth reading. This first FINDINGS file leans long because `0001`
produced a lot of signal; a thin probe with a thin findings file
would not benefit future agents much. The rule of thumb is forming:
**findings files should be proportional to the signal produced, not
to the effort expended.**

No rewrite of ETHOS/AGENTS is needed yet. If a pattern emerges of
agents over- or under-writing FINDINGS, revisit.
