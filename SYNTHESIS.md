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

Informed by nine experiments:

- `FINDINGS/0001-raw-http-aggregation` — hand-rolled Go stdlib probe.
- `FINDINGS/0002-hello-aggregated` — library-backed (`k8s.io/apiserver`)
  stateless AA with read/write/watch + SSA.
- `FINDINGS/0003-custom-authorizer-external-policy` — per-request
  identity-based authorization via an external HTTP policy service.
- `FINDINGS/0004-github-driver-static-pat` — GitHub repos projected
  as a read-only aggregated-API resource via a polling client.
- `FINDINGS/0005-argocd-compat` — ArgoCD deployed against an AA that
  exposes `repos.aggexp.io/v1`.
- `FINDINGS/0006-identity-broker-github-app` — broker-mediated
  identity-to-backend token exchange (mock broker + mock GitHub).
- `FINDINGS/0007-runtime-fs-driver` — third backend consuming the
  extracted `runtime/` substrate; files on disk as `files.aggexp.io/v1`.
- `FINDINGS/0008-long-lived-informer` — client-go SharedInformer
  sustained against a synthetic-RV AA over four probe scenarios.
- `FINDINGS/0009-ack-aggregated-s3` — ACK-pattern inversion: AWS S3
  Bucket as aggregated API with no local state; live reads, live
  writes; watch via poll-diff.

MVP-lab and MVP-example (GitHub repos end-to-end) are both complete;
see `FINDINGS/example-e1-github-repos.md`.

`runtime/` substrate exists and is consumed by experiments 0007 and
0009 today. See `ARCHITECTURE.md`.

Remaining claims below without a `FINDINGS/*` reference are
unvalidated.

---

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
lines plus code generation — now substantially less via the
substrate.** Consumers of `runtime/` need ~260 lines of server+
storage wiring plus types+codegen for a new resource [`0007`]. The
library's generic PATCH path handles SSA merging, managedFields
tracking, and conflict detection for free when `rest.Patcher` is
implemented and OpenAPI v3 is registered.

**The internal version is not optional** [`0002`]. The generic PATCH
machinery converts incoming versioned objects to the group's
internal hub version before applying merges. Even when internal and
external types are byte-identical, both must be present in the
scheme with 1:1 conversion funcs.

**The first non-kubectl consumer (ArgoCD) survived** [`0005`]. The
gitops-engine cluster cache discovered our aggregated resource,
issued LIST+WATCH via a dynamic informer, and operated at ~0.003 Hz
steady state (one LIST+WATCH every ~5 min reflector resync). The
wire-level behavior passed; failures occurred at the authorization
boundary, not the protocol (see Per-request authorization below).

Open questions:

- How does Flux's source-controller / kustomize-controller behave?
  `0005` only covered ArgoCD.
- How does controller-runtime's manager layer behave on top of
  informers that `0008` established work? The manager has caches
  and reconcile loops that may add new assumptions.
- `WatchListClient` (1.32 client-go default-on, server default-off)
  is a different wire path than classic list-then-watch. Untested
  against our AA.
- What fraction of the generated OpenAPI (~130KB for a single type)
  can be trimmed while preserving kubectl behavior?

---

## Identity handoff

**Baseline: the aggregation layer forwards more identity metadata
than just user + groups** [`0001`, `0002`]. `X-Remote-User`,
`X-Remote-Group`, and `X-Remote-Extra-*` (with `/` escaped as `%2F`)
arrive at the AA with kube-apiserver's mTLS aggregator client cert
validating the handoff. In kube 1.32, client-cert authenticators
populate `X-Remote-Extra-Authentication.kubernetes.io%2FCredential-Id`
with the X.509 SHA256 fingerprint automatically — no opt-in.

**Impersonation erases Extras** [`0006`]. `kubectl --as alice`
arrives with `user.Name=alice`, `groups=[system:authenticated]`, and
`extras={}`. The credential-id from the real caller does not carry
through. This is fundamental to the impersonation wire contract;
brokers or authorizers that depend on the original caller's
credential strength cannot use impersonated identities as input.

**Bearer tokens do not forward.** An AA that needs a downstream
credential must do identity → credential exchange itself. Confirmed
by every experiment that needed backend access.

**The broker-mediated "on behalf of the caller" pattern works
end-to-end** [`0006`]. Plumbing `user.Info` from request context
into `rest.Storage` methods, then through a `TokenProvider`
interface into a broker that returns a caller-scoped token, slots
cleanly into the library with no new framework support.
Observations:

- Latency of the broker exchange was invisible (~300µs) under the
  aggregation-layer floor (~65 ms). A production broker doing real
  JWT signing and installation-token minting would flip this
  balance and demand caching.
- A fully stateless AA (no cache; per-request fetch) was ~200
  lines shorter than the polling variant (0004) and removed
  pod-restart amnesia as a category — each request is its own
  round trip.
- Broker denial and authorizer denial are different UX shapes: the
  authorizer path produces a loud HTTP 403 with our reason in the
  `metav1.Status.Message`; a broker that returns "no token"
  produces a quiet empty list (or 500 if we leaked the dial
  error). Both are valid; they serve different threat models.

**Internal control-plane traffic is mixed in.** The AA receives
requests with `X-Remote-User=[system:kube-aggregator]` and
`X-Remote-Group=[system:masters]` during discovery / openapi
refresh. Worth filtering when analyzing identity-based behavior.

Open questions:

- Does `kubectl --as --as-user-extra` (1.35+) populate Extras that
  reach the AA, or does the same impersonation-erases-extras
  boundary apply?
- `X-Remote-Extra-*` header-name escaping: what characters beyond
  `/` get escaped? Not documented authoritatively anywhere found.
- Real GitHub App integration (mint installation tokens from a
  private key, honor GitHub's token scopes) — untested; queued as
  MVP-example E2.

---

## Storage independence

**Confirmed end-to-end against four backends** [`0002`, `0004`,
`0007`, `0009`]. The library does not require etcd: replacing
`RecommendedOptions.Etcd` with a bespoke Options struct is clean.
For in-memory state, ~250 lines of `rest.Storage` implementation plus
standard broadcaster wiring. For external state (GitHub, AWS S3), the
incremental cost is a small API client + a polling loop. For disk
(fs files), the same pattern adapts to a different source of truth.

**The substrate makes this a promoted pattern** [`0007`, `0009`]. A
`Backend` interface plus an adapter in `runtime/storage` means new
experiments write a few hundred lines of backend-specific code and
inherit the rest.Storage interfaces, watch fan-out, RV generation,
and Table rendering. `0007`'s fs backend was ~36% shorter than
0004's hand-rolled shape; `0009`'s S3 backend is comparable.

**Three stateful-vs-stateless costs observed** [`0004`, `0006`,
`0008`]:

1. **Pod-restart amnesia.** Cache and synthetic UIDs are
   process-local to the polling variant. On AA restart, UIDs
   regenerate; consumers keyed on UID see apparent full churn
   [`0004`]. client-go reflectors handle this cleanly by
   synthesizing DeleteFuncs for the prior-store items, then
   AddFuncs for the new ones [`0008`]. No hot-loops, no crashes.
2. **Fully stateless AA eliminates the amnesia category** at the
   cost of per-call latency and no watch semantics [`0006`]. A
   request that wants live events must open a long-running watch
   HTTP stream the AA itself synthesizes.
3. **Rate-limit coupling.** Poll interval × page count × call
   cost is a joint decision with the backend's rate limit, not
   independent knobs [`0004`].

**Library features that assume persistence do not survive a
stateless AA** [`0009`]. Three named casualties from the
ACK-inversion experiment:

1. **SSA's field ownership tracking** is library-layer state.
   `kubectl apply --server-side` succeeds on the wire but
   `managedFields` is absent from subsequent GETs because the AA
   has nowhere to persist them. A conflict-from-second-manager
   scenario has no prior ownership record to conflict against.
   Three remediations: abandon SSA (awkward — library enables it
   by default); encode managedFields into the backend (reintroduces
   state, usually lossy); shadow-persist in etcd (resurrects the
   controller model this inversion set out to avoid).
2. **ObjectMeta bookkeeping**: labels, annotations, finalizers,
   and ownerReferences have no natural home. kubectl's
   `last-applied-configuration` annotation triggers a warning on
   every apply but functionally works (kubectl re-patches it each
   time). Finalizers and ownerReferences would need backend
   modeling that doesn't exist naturally in most backends.
3. **Sync-vs-async backend boundary.** The inverted model fits
   synchronous backends (S3 Bucket, SNS Topic) cleanly. For
   asynchronous provisioning (EKS cluster creation, IAM
   propagation, RDS), the AA's Create-equivalent would need to
   block the HTTP request for minutes — not workable. ACK's
   controller pattern earns its complexity specifically by
   handling async. An AA covering the full AWS surface would
   need state for async operations, which defeats the
   inversion's premise for those resources.

The inversion pays off for the synchronous / simple-lifecycle
subset of backends. For those, the stateless AA is a better fit
than CRD+controller: no drift, no reconciler backoff, no stuck
finalizers, no two-process coordination. For complex backends,
the controller pattern's complexity is load-bearing.

**Synthetic resourceVersion suffices for real informers** [`0002`,
`0004`, `0008`]. A single `atomic.Uint64` stringified as decimal
is accepted. Returning `410 Gone` on any resume request with an RV
other than current makes reflectors relist — though they more often
reach that state via a 503-on-connection-refused than via an actual
410 on resume.

Open questions:

- Can SSA's ownership tracking be reconstructed by encoding
  managedFields into backend-specific metadata (S3 tags, GitHub
  repo description fields, etc.) without forcing a general etcd
  shadow store? Queued as `ssa-managedfields-in-backend`.
- What does "async backend" actually cost when you try to model
  one? Queued as `async-backend-sim`.
- ETag-aware polling: our GitHub and S3 clients don't honor
  ETags. How much rate-limit headroom does adding them buy?
- Webhook-driven backends (GitHub pushes events; AWS has
  CloudTrail and EventBridge) could skip polling entirely.
  Untested.
- Deterministic UIDs (hash of backend's stable ID) would preserve
  identity across AA restarts. Not implemented.

---

## Per-request authorization

**Confirmed end-to-end** [`0003`]. A custom `authorizer.Authorizer`
can make every authorization decision for a given API group based
on runtime identity + request attributes against an external system.
The library cooperates cleanly via `union.New`. The pattern is now
a substrate-level helper in `runtime/authz` [`0007`].

Four concrete sub-findings from `0003`:

1. **Chain order is sharp.** With permissive upstream RBAC, the
   default library chain's delegated SAR authorizer returns `Allow`
   for our group and short-circuits anything after it.
   `union.New(custom, existing)` — custom first — is required; the
   custom authorizer must return `NoOpinion` for everything outside
   its scope so the library's privileged-groups / always-allow-paths
   behavior still works.
2. **Denials carry the reason string verbatim** to clients. The
   HTTP 403 body is `metav1.Status.Message = "User ... cannot
   <verb> resource ... : <your reason>"`. UX surface: helpful for
   debugging, dangerous if the policy service leaks sensitive
   reasoning.
3. **`kubectl auth can-i` lies** when the AA is the real gate. SAR
   is answered by kube-apiserver's RBAC, not the AA. This is a
   wire-level property, not fixable from the extension.
4. **CREATE has no `name` in authorizer Attributes.** Name-based
   creation policies cannot be enforced in the authorizer
   interface; they belong in **admission** (validating admission
   webhook / CEL).

**The operational hazard** [`0005`]: an AA whose default-deny
policy applies to every unknown identity will **brick any
cluster-wide controller that auto-discovers-and-watches every API
group its RBAC permits**. ArgoCD's gitops-engine cluster cache
treats one LIST failure as fatal for the *whole* cache, so an
unrelated ArgoCD Application stays stuck at `sync=Unknown` because
our AA 403'd the `argocd-application-controller` SA. Controller
SAs must be explicitly allow-listed in the policy, or the policy
must allow broad `get/list/watch` for any `system:serviceaccount:*`,
or the architectural split must be "strict RBAC upstream + AA
refines" rather than "permissive RBAC + AA is the real gate."

**Authorizer-as-gate and broker-as-gate are different positions**
[`0006`]. Both are valid. The authorizer runs before the handler
and its denial is loud (403 + reason). A broker runs during the
handler and its "no token" outcome can manifest as an empty list,
a 500 with a dial-error message, or an explicit denial — depending
on how the `rest.Storage` translates it. Both positions compose
cleanly; a future experiment will run them together.

**Latency is not the limiting factor at lab scale** [`0003`]. One
external HTTP round trip per authz check adds ~0 ms perceptible
(measured ~65 ms per kubectl call end-to-end, dominated by the
aggregation-layer hop). Real production pressure would want a TTL
cache.

Open questions:

- Does `SubjectAccessReview` from AA back to kube-apiserver preserve
  the interaction pattern, or is it orthogonal?
- What's the right caching strategy? TTL per-(user, verb, name)?
  How stale is acceptable?
- Can the authorizer gracefully accommodate cluster-controller SAs
  without hand-maintaining an allow-list?

---

## Resource modeling freedom

**Confirmed across four real backends** [`0004`, `0006`, `0007`,
`0009`]. Mapping an external system's state to Kubernetes resources
is clean when the system has stable identifiers and a describable
schema:

- GitHub repos: `<owner>.<name>` worked for 206 real repos; spec
  fields mapped 1:1 from JSON. [`0004`]
- GitHub repos again, via mock broker + mock backend: the pattern
  did not change when authentication shifted. [`0006`]
- Files on disk: filename as resource name; path, size, mode as
  spec. [`0007`]
- AWS S3 buckets: global-unique name as resource name; region +
  tags as spec. [`0009`]

**Two adjacent boundaries now named**:

- **Authorization vs. admission** [`0003`]. The authorizer sees
  request URL attributes; the object body belongs to admission.
  Policies depending on fields inside the object at CREATE time
  need admission logic. The substrate does not yet surface an
  admission hook.
- **Synchronous vs. asynchronous backend operations** [`0009`].
  The stateless-AA model fits sync backends (S3 create returns in
  milliseconds) cleanly. For async provisioning (EKS clusters,
  RDS, IAM role propagation), the model breaks down: the AA's
  Create-equivalent would block on minute-scale operations. An
  AA replacement for ACK would need state to handle async —
  which is the controller pattern rebuilt.

Untested shape-boundaries: backends with inconsistent schema,
without stable names, without list operations, without deletion
primitives, with names containing characters Kubernetes rejects.

Drivers worth building: `http-driver` (arbitrary HTTP endpoints as
resources), `grpc-as-resource`, `virtual-composition` (projecting
a join of two underlying resources).

---

## Watch and consistency semantics

**Watch works at the wire level across three implementations**
[`0001`, `0002`, `0004`, `0007`]: hand-rolled chunked-NDJSON,
library broadcaster with in-memory source, library broadcaster
with polling external source. kubectl renders all of them.

**A single monotonic `atomic.Uint64` resourceVersion is accepted
by client-go reflectors across all tested scenarios** [`0008`].
Baseline cadence, AA outages, cert rotation, slow handlers — the
reflector does not complain, hot-loop, or lose objects. Specifics:

1. **Steady-state cadence is light.** Real consumers (ArgoCD's
   gitops-engine, client-go SharedInformer) issue one LIST+WATCH
   every ~5 min via the reflector-level resync [`0005`, `0008`].
   Our polling-driven synthetic watch is not exercised harder than
   kubectl already did.
2. **Pod restart surfaces as synthesized DeleteFuncs + fresh
   AddFuncs** to a reflector [`0008`]. Consumers keyed on name
   are mostly OK; consumers keyed on UID see churn. The server
   side's pod-restart amnesia from `0004` is what the client sees
   through this lens.
3. **The 410-on-resume path is defensible but rarely exercised**
   [`0008`]. The AA's server-side 410 code is live (confirmed by
   `kubectl get --raw`), but a real reflector's recovery goes
   through a fresh LIST after the server becomes reachable again,
   skipping the 410 path entirely.
4. **Cert rotation is a non-event** [`0008`] as long as the AA's
   dynamic cert controller reloads in-process (it does) and the
   APIService's `caBundle` stays consistent. Existing TLS
   connections survive. This is the "same-CA, rotated cert" case;
   CA rotation is untested.
5. **Slow user handlers produce zero server-side pressure**
   [`0008`]. client-go's DeltaFIFO decouples the wire from user
   callbacks; the broadcaster's `DropIfChannelFull` is effectively
   unreachable from a slow user handler.

**ArgoCD's gitops-engine cluster cache was the first sustained
non-kubectl consumer** [`0005`]. It discovered the group, cached
LIST+WATCH, and operated without wire-protocol complaint over 15
minutes through one AA outage. The only breakage was at the authz
boundary (see Per-request authorization above).

Still unmeasured:

- Controller-runtime manager (not just the raw reflector).
- CA rotation with simultaneous `APIService.caBundle` rotation.
- `WatchListClient` feature gate behavior.
- Hours-long informers that outlive multiple backend poll cycles
  and several AA restarts.

---

## Process observations

Five observations after nine experiments and one substrate
promotion:

1. **Findings proportional to signal** holds. Dense experiments
   (0001, 0002, 0003, 0006, 0008, 0009) produced long FINDINGS;
   lean ones (0005, 0007) produced tighter ones. Agents have not
   been padding, which was the risk the ethos was guarding
   against.
2. **Parallel agents on the same kind cluster clobbered each
   other's state** during the 0005/0008 arcs. Each agent created
   its own `aggexp-<slug>` cluster after the first collision.
   Worth noting in AGENTS.md next rewrite: `kubectl config
   use-context` is process-global; parallel agents need isolated
   clusters.
3. **Substrate extraction was deliberate and worked**. The
   two-driver precondition (0002 + 0004) produced a natural
   `Backend` interface that survived its second consumer (0007)
   and its third (0009) with only minor seam issues (OpenAPI
   still copy-pasted into experiments; `WritableBackend.Update`
   pre-fetch-then-mutate did not fit the S3 delete-then-create
   case but did fit update-in-place). Promotion discipline —
   tests, docs, thought-through interface — was honored.
4. **The six fundamentals frame has held** across nine
   experiments. No new fundamental has emerged. Three adjacent
   concerns have been named and fit cleanly under existing
   fundamentals without demanding a rewrite of the list:
   authz-vs-admission, substrate-promotion triggers, and
   sync-vs-async backend operations.
5. **The inversion thought experiments (0006 broker, 0009
   ACK-AA) were disproportionately productive.** They exposed
   specific library-layer features that assume persistence
   (SSA managedFields; labels/annotations survival; finalizer
   semantics) — findings a positive "what works" probe would
   have missed. Worth repeating: inversions surface assumptions
   that direct probes don't.

The ethos itself needs no changes yet. If a pattern emerges of
experiments going longer than they need to, or of SYNTHESIS
falling out of sync with FINDINGS, revisit.
