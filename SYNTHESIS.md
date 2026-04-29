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

Informed by three experiments:
- `FINDINGS/0001-raw-http-aggregation` — hand-rolled Go stdlib probe.
- `FINDINGS/0002-hello-aggregated` — library-backed (`k8s.io/apiserver`)
  stateless AA with read/write/watch + SSA.
- `FINDINGS/0003-custom-authorizer-external-policy` — per-request
  identity-based authorization via an external HTTP policy service.

Remaining claims below without a `FINDINGS/*` reference are
unvalidated.

## Wire protocol fidelity

**The minimum wire contract for `kubectl get` is well below the
library threshold** [`0001`]. A stdlib-only HTTP handler returning
correct `APIResourceList` discovery, `TypeMeta`+`ObjectMeta`-shaped
responses, a `meta.k8s.io/v1 Table` on content negotiation, and
`/livez` / `/readyz` passes `kubectl api-resources`, `kubectl get`,
`kubectl get -o yaml`. No library required.

**The sharp edge is at `kubectl explain`.** Hand-rolled
structurally-valid OpenAPI is not sufficient: kubectl's schema index
keys off the `x-kubernetes-group-version-kind` extension on schema
components. `openapi-gen` emits those extensions automatically via
`openapi.NewDefinitionNamer(Scheme)`. **A library-backed AA with
generated OpenAPI passes `kubectl explain` with zero additional
work** [`0002`]. Without generated OpenAPI, you can approximate
everything except GVK indexing; that specific bit is non-negotiable
for explain.

**Full modern-tooling compatibility costs roughly 400 hand-written
lines plus code generation** [`0002`]. That covers `kubectl get`,
`kubectl explain`, `kubectl apply` (merge-patch), `kubectl apply
--server-side` (with managedFields), and `kubectl get -w`. Server-
side apply is free — implement `rest.Patcher` (= Getter + Updater)
and supply generated OpenAPI; the library's generic PATCH path
handles structured-merge-diff, managedFields tracking, and conflict
detection.

**The internal version is not optional** [`0002`]. The generic PATCH
machinery converts incoming versioned objects to the group's
internal hub version before applying merges. Registering only the
external `v1` fails SSA and strategic-merge patches with `no kind
... is registered for the internal version`. Even when internal and
external types are byte-identical, both must be present in the
scheme with 1:1 conversion funcs.

Open questions:

- How tolerant is ArgoCD / Flux / controller-runtime of a watch
  with synthetic resourceVersion under sustained load?
- How does our stateless AA behave when the pod restarts mid-watch?
- What fraction of the generated OpenAPI (~130KB for a single type)
  can be trimmed while preserving kubectl behavior?

## Identity handoff

**Baseline: the aggregation layer forwards more identity metadata
than just user + groups** [`0001`, `0002`]. `X-Remote-User`,
`X-Remote-Group`, and `X-Remote-Extra-*` (with `/` escaped as `%2F`)
arrive at the AA with kube-apiserver's mTLS aggregator client cert
validating the handoff. In kube 1.32, client-cert authenticators
populate `X-Remote-Extra-Authentication.kubernetes.io%2FCredential-Id`
with the X.509 SHA256 fingerprint automatically — no opt-in.

The security model must assume: **whatever kube-apiserver's
authenticators populate into `user.Info.Extra` will reach the AA.**
The library's `DelegatingAuthenticator` honors the
`extension-apiserver-authentication` configmap protocol transparently;
we call `UserFrom(ctx)` and get the identity.

**Bearer tokens do not forward.** An AA that needs a downstream
credential must do identity → credential exchange itself. This is
architectural, not a bug.

Open questions:

- Complete enumeration of what each authenticator type populates
  into `user.Info.Extra`. Credential-Id was the surprise; what
  else is already in there?
- Cleanest pattern for "do X on behalf of the caller" to systems
  that don't speak Kubernetes identity (GitHub, AWS, etc.).

## Storage independence

**Confirmed: an aggregated apiserver does not require etcd** [`0002`].
Replacing `RecommendedOptions.Etcd` with a bespoke Options struct
(the pattern metrics-server uses) is clean. The library does not
resist the etcd-less path; it simply doesn't advertise it. The cost
is bounded:

- ~250 lines for the `rest.Storage` implementation itself (Get,
  List, Create, Update, Delete, Watch, TableConvertor, plus identity
  markers).
- `watch.NewBroadcaster` handles watch fan-out.
- A single `atomic.Uint64` stringified as decimal is an acceptable
  resourceVersion scheme for kubectl and basic watches; stricter
  informers remain untested.

**The library is built around generic-store assumptions that the
etcd-less path must replicate manually.** `NewDefaultAPIGroupInfo` +
`VersionedResourcesStorageMap` is generic over `rest.Storage`, so
the plug-in point is clean. Everything after that is your code.

Open questions:

- Where does the polling-driven synthetic-watch pattern break at
  scale? Not yet probed.
- What is the smallest viable resourceVersion scheme that still
  satisfies client-go's reflector under long-lived informers?
- Pod-restart behavior: what do clients see when our in-memory
  state vanishes?

## Per-request authorization

**Confirmed end-to-end** [`0003`]. A custom `authorizer.Authorizer`
can make every authorization decision for a given API group based on
runtime identity + request attributes against an external system.
The library cooperates cleanly via `union.New`. Four concrete
sub-findings:

1. **Chain order is sharp.** With permissive upstream RBAC, the
   default library chain's delegated SAR authorizer returns `Allow`
   for our group and short-circuits whatever comes after it.
   `union.New(custom, existing)` — custom first — is required; the
   custom authorizer must return `NoOpinion` for everything outside
   its scope so the library's privileged-groups / always-allow-paths
   behavior still works.

2. **Denials carry the reason string verbatim** to clients. The
   HTTP 403 body is `metav1.Status.Message = "User ... cannot
   <verb> resource ... : <your reason>"`. This is a UX surface:
   helpful for operators debugging, dangerous if the policy
   service leaks sensitive reasoning.

3. **`kubectl auth can-i` lies** when the AA is the real gate.
   SAR is answered by kube-apiserver's RBAC, not the AA. With the
   permissive-RBAC-plus-AA-authorizer pattern, `can-i` reports
   allows that aren't. This is a wire-level property, not fixable
   from the extension. The operator UX choice is either "keep RBAC
   strict upstream and refine in the AA" (can-i stays meaningful)
   or "teach users can-i is advisory" (the AA becomes authoritative
   but asynchronously discoverable).

4. **CREATE has no `name` in authorizer Attributes.** Kubernetes
   puts the resource name in the request body on CREATE, not the
   URL. Name-based creation policies cannot be enforced in the
   authorizer interface; they belong in **admission** (validating
   admission webhook / CEL). This is the first observed case of
   authz and admission being distinct concerns with distinct
   capabilities in the AA world.

**Latency is not the limiting factor at lab scale** [`0003`]. One
external HTTP round trip per authz check added ~0ms perceptible
(measured ~65ms per kubectl call end-to-end, dominated by the
aggregation-layer hop). Real production pressure would want a TTL
cache; the library caches SAR answers but not our custom authz.

**The AA's role is still additive, not replacing RBAC** [`0001`,
`0003`]. Two working patterns:
- Permissive RBAC upstream; AA authorizer is the real gate.
  `can-i` breaks; AA is authoritative. Used in `0003`.
- Strict RBAC upstream; AA authorizer refines. `can-i` remains
  meaningful; AA can only narrow, not expand. Not yet probed.

Open questions:

- Does `SubjectAccessReview` from AA back to kube-apiserver
  preserve the interaction pattern, or is it orthogonal?
- What is the right caching strategy? TTL per-(user, verb, name)?
  How stale is acceptable?
- How do controller-runtime informers behave when their identity
  is denied by the AA? Hot-loop, back off, crash?

## Resource modeling freedom

Still untested beyond the trivial `Hello` resource. Hypothesis
(unchanged): anything with an addressable identity and a schema can
be projected as a Kubernetes resource. Interesting boundaries —
backends without stable names, without list operations, without
deletion primitives, with inconsistent schema — all unprobed.

**An adjacent boundary is now named**: Kubernetes separates
authorization from admission. The authorizer sees the request URL
attributes (user, verb, group, resource, namespace, name on
non-CREATE); admission sees the object body. Any policy that
depends on fields inside the object at CREATE time is an admission
concern, not an authz concern [`0003`]. This shapes what a future
"driver" interface needs to surface: a hook for validating/
mutating admission is probably required alongside the authz hook.

Drivers on the menu: `fs-driver`, `github-driver-static-pat`,
`http-driver`.

## Watch and consistency semantics

**Watch mechanically works at both levels** [`0001`, `0002`]. stdlib
chunked-NDJSON with hand-built events; `watch.NewBroadcaster` +
`WatchWithPrefix` in the library-backed path. kubectl renders both.

**A single monotonic `atomic.Uint64` is a workable synthetic
resourceVersion** [`0002`]. kubectl does not complain about it.
client-go's reflector semantics (relist on 410-Gone) suggest that
under real informer load we should `ResourceExpired` any old-RV
watch we cannot replay — our current implementation does that.

Still untested:

- Long-lived controller-runtime informers past the relist boundary.
- Cert rotation mid-watch.
- Real backend-change-driven watch (vs. polling or synthetic).

---

## Process observations

Two experiments in; one meta observation so far: **findings files
should be proportional to the signal produced, not the effort
expended.** `0001` produced dense signal (first real contact with
the aggregation layer); its findings is long. `0002` produced
focused signal (four specific hypotheses tested, three confirmed);
its findings is also long but could have been shorter had the
experiment been less productive.

No rewrite of ETHOS/AGENTS is warranted yet. If a pattern emerges
of agents over- or under-writing FINDINGS, revisit.
