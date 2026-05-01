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

Informed by twenty-three experiments and two substrate promotions:

- `FINDINGS/0001-raw-http-aggregation` through `FINDINGS/0021-runtime-component-parity`
  — see earlier listing.
- `FINDINGS/0022-stateful-middleware-thesis` — arc kickoff for the
  stateful-middleware-refinement arc. Captured design commitments
  (state required; shared metadata CRD invisible to exposed-
  resource discovery; no generic backend state API; multiplex
  with dynamic reconciler; declarative admission; swappable
  transport; 4 watch capability levels; `runtime/component/v2/`
  as new package). Thesis package compiles but has no runtime.
- `FINDINGS/0023-schema-source-exploration` — empirical probe of
  three OpenAPI-source paths. All three produce kubectl-identical
  behavior. Recommendation: **Track B (middleware synthesizes
  full Kubernetes OpenAPI from a plain JSON Schema the backend
  supplies)** is the default for the arc. Keeps zero K8s-concepts
  on the backend-author side, matches mainstream JSON-Schema
  tooling across languages, minimal middleware delta.

MVP-lab and MVP-example complete.

Substrate `runtime/` has two promotions:
1. `runtime/{server, group, authz, storage}` — the library pattern
   (consumers: 0002, 0004, 0007, 0009, 0010, 0011).
2. `runtime/component/{proto, scheme, openapi, grpcbackend}` — the
   component-server pattern (consumers: 0013, 0017, 0018, 0019,
   0020, 0021).

A third promotion (`runtime/component/v2/`) is queued for 0030
incorporating the stateful-middleware arc's findings.

See `ARCHITECTURE.md`.

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

**controller-runtime's manager layer works on top of the raw
reflector that `0008` validated** [`0012`]. Leader election via
Leases in the host cluster works at ms-scale; reconcile fires
within 1s of object creation; reflector backoff on AA unavailability
is ~1-3-3-13s, no hot-loop. The manager-startup race (mgr.Start()
runs before our APIService is ready) produces a ~20s warning burst
but self-heals. Two concrete gaps surfaced at the manager level:
(a) `client.Options.Cache.Unstructured: true` is opt-in; default-off
silently bypasses the manager cache for unstructured reads and
turns the AA into the bottleneck. (b) Pod-restart UID regeneration
propagates to controllers as a synthesized delete-add pair per
object — a controller doing real work in Reconcile redoes it on
every AA restart at O(objects) cost. Promotes the deterministic-UID
candidate from "nice to have" to "load-bearing at scale."

**A generic schema-dynamic component server can honor the full
wire contract with no per-resource Go types for CRUD, but SSA and
rich explain require a typed seam** [`0013`, `0017`, `0018`].
0013 registered `*unstructured.Unstructured` against a dynamic
Scheme (GVK received over gRPC at startup); CRUD + watch +
discovery + degraded explain all passed. SSA failed at
`managedfields.NewTypeConverter` with "unstructured object has
no kind"; per-field explain collapsed to a catch-all description.
0017 closed both: threading the backend's OpenAPI into the defs
map (keyed at the Scheme's sample-object canonical name) enables
rich explain; registering a typed Go **wrapper** under the GVK
(content stays untyped) enables SSA end-to-end including conflict
detection and force-conflicts. The library's SSA path has TWO
typed-Scheme checkpoints: `NewTypeConverter` construction (closed
by shipping real OpenAPI) and `Scheme.New(gvk)` empty-object
creation (closed only by a typed Scheme entry). These are
architecturally independent; 0013's initial reading conflated
them. 0018 confirmed at the S3 scale: the component + gRPC
backend pattern has user-facing parity with a library-linked AA;
backend-side translation is the same size; the scheme/codegen/
apiserver wiring is amortized into the (reusable) component
server.

**The component-server shape is language-agnostic on the backend
side** [`0019`]. 0017's component-server binary was reused
unchanged against a **python** note-backend. CRUD + watch + rich
explain + SSA (including conflict detection and force-conflicts)
all pass end-to-end; the component server cannot tell the
backend's language apart. Python semantic LOC is ~30% shorter
than Go; latency is indistinguishable (~71 ms mean `kubectl get`
at lab scale, dominated by the aggregation-layer hop).
Image-size is the real cost (python 159 MB vs distroless-Go 12.3 MB
~13×), not runtime performance. The JSON-bytes payload decision
in the proto (from 0013) is load-bearing for language
portability: the backend never decodes into a typed language
object, so no per-type codegen is required on its side.

**The component-server substrate is now a promoted package**
[`0021`]. `runtime/component/{proto, scheme, openapi, grpcbackend}`
encapsulates the 0017 approach (typed wrapper for SSA, OpenAPI
threading for explain). First post-promotion consumer is a
~40-line `cmd/note-aa/main.go` plus the same note-backend
0017/0019 used, producing ~0.27× the handwritten Go of 0017. The
substrate extraction held under this first library-mode consumer
with no per-consumer patches. A new `--use-typed-wrapper=true`
default flipped from 0017's opt-in, because SSA working is now
the expected baseline.

**The OpenAPI source can be middleware-synthesized from plain
JSON Schema without any backend-side Kubernetes tooling** [`0023`].
0023 tested three paths: (A) backend ships full Kubernetes
OpenAPI, (B) backend ships plain JSON Schema and middleware
lifts, (C) OpenAPI lives in a host-cluster `APIDefinition` CRD.
All three produce kubectl-identical behavior including SSA and
per-field `explain`. The lift in (B) is 127 lines of purely
mechanical boilerplate: synthesize ObjectMeta wrapper, List
wrapper, stamp the GVK extension, insert
`x-kubernetes-preserve-unknown-fields` where the JSON Schema is
silent. Mainstream JSON-Schema tooling (pydantic, schemars,
zod-to-json-schema) emits exactly what the middleware consumes;
the backend author writes zero K8s-specific code to describe
their type. The arc standardizes on (B) as the default with (C)
retained as an escape hatch for dynamic multi-AA deployment.

**The stateless-AA + CRD-facade pattern supports real gitops
controllers writing to our resources** [`0015`]. ArgoCD
Application can target a `Widget` (0010's CRD-facade AA), do SSA
via its own field manager, detect drift, re-apply, prune, and
cascade-delete. The 0010 apiVersion + field-path rewrite on
managedFields survives a non-kubectl field manager. Effectively
closes the `ssa-managedfields-in-backend` candidate for the
CRD-facade case. **New facade-level obligation**: tracking
annotations stamped by ecosystem controllers (argocd's
`tracking-id`) cause double-tracking when the facade passes
annotations through to the backing CRD verbatim. ArgoCD sees both
the exposed Widget and the backing WidgetStorage as managed and
auto-prune breaks. A facade needs an annotation allowlist, not
just a field-path rewrite.

**Flux's default controller set does NOT exercise our AA unless
a Kustomization inventory targets our resource** [`0014`]. The
"one LIST failure bricks cluster cache" pattern from 0005
applies specifically to ArgoCD's gitops-engine and to
kube-controller-manager; Flux's source / kustomize / helm /
notification controllers start EventSources only on their own
CRDs. The authz-aware-controllers threat model is narrower than
0005 implied.

controller-runtime's manager layer and the substrate's watch
behavior under real reflectors are covered in the Watch and
consistency semantics section below.

Open questions:

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

**Confirmed end-to-end against six backends across four storage
axes** [`0002`, `0004`, `0007`, `0009`, `0010`, `0011`, `0013`]:

1. **In-memory direct.** `runtime/storage.Backend` + a `sync.Map`.
   `0002` (Hello), `0007` (fs), `0013` component-server's note-
   backend. ~250 lines of backend code.
2. **External API as source of truth, polling cache for watch.**
   `0004` (GitHub), `0009` (S3), `0011` (async mock). Polling
   loop diffs against the last observation and publishes events.
3. **CRD facade on the host cluster.** `0010`. The AA is
   stateless; persistence lives in the host kube-apiserver's etcd,
   reached via `dynamic.Interface`. Recovers library features
   stateless variants lose (SSA managedFields, finalizers,
   ownerReferences, real resourceVersions).
4. **Component-server / thin-backend over gRPC.** `0013`. Storage
   is whatever the backend chooses; the component server is
   stateless. The backend can be in any language.

The library does not require etcd: replacing `RecommendedOptions.Etcd`
with a bespoke Options struct is clean. The substrate's `Backend`
interface has now survived four experiment-level consumers plus one
component-server adapter, with no material interface changes.

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
stateless AA** [`0009`, `0013`]. Three named casualties from the
ACK-inversion experiment:

1. **SSA's field ownership tracking** is library-layer state.
   `kubectl apply --server-side` succeeds on the wire but
   `managedFields` is absent from subsequent GETs because the AA
   has nowhere to persist them. A conflict-from-second-manager
   scenario has no prior ownership record to conflict against.
   Three remediations: abandon SSA (awkward — library enables it
   by default); encode managedFields into the backend; use a CRD
   facade where the host kube-apiserver persists them.
2. **ObjectMeta bookkeeping**: labels, annotations, finalizers,
   and ownerReferences have no natural home. kubectl's
   `last-applied-configuration` annotation triggers a warning on
   every apply but functionally works (kubectl re-patches it each
   time). Finalizers and ownerReferences would need backend
   modeling that doesn't exist naturally in most backends.
3. **Sync-vs-async backend boundary** — now nuanced by `0011`.
   The naive read was that async backends force state back in
   because Create would have to block. In practice a Create that
   returns immediately with `phase=Provisioning` and lets watch
   stream the transition reproduces the controller-model
   status-evolves-over-time behavior without any backend state
   in the AA. The thesis survives async. What breaks is specific
   ecosystem idioms (see below), not the stateless posture.

**The CRD-facade option recovers all three casualties**
[`0010`]. A stateless AA whose `Get/List/Create/Update/Delete/Watch`
call through to a CRD on the host kube-apiserver via the dynamic
client inherits managedFields persistence, finalizer semantics,
ownerReferences + GC, and real (non-synthetic) resourceVersions
from the host — at the cost of one extra hop per request (~0 ms
perceptible at lab scale). This is a **third storage axis**
alongside (in-memory) and (external-API-as-truth): persistence
lives in the host cluster's etcd but **the AA itself is still
stateless**. A gotcha: if the facade renames fields between the
exposed and backing schemas, `managedFields` entries' `apiVersion`
and `fieldsV1` keys must be rewritten symmetrically — the library
silently drops mismatched entries, and SSA *appears* to work with
zero ownership tracked. This is a facade-level finding, not a
general one.

**The component-server / thin-backend shape is a fourth storage
axis** [`0013`]. Storage lives entirely on the backend side of a
gRPC boundary; the component server is stateless (other than watch
broadcaster caching). The backend can use anything for
persistence — in-memory, a database, another external API. The
component server doesn't care and has no compile-time knowledge
of the resource type.

**Async backends cost two specific ecosystem idioms** [`0011`].
`kubectl wait --for=jsonpath=...` fails against our AA because
the substrate doesn't emit the `k8s.io/initial-events-end` bookmark
that WatchList-aware clients (1.31+) hard-require. `kubectl delete
--wait=true` (the default) hangs past the deprovision window;
`--wait=false` works cleanly. The former is a substrate-level gap
worth closing; the latter appears to be a cache-staleness issue on
reconnect. Neither breaks the inversion's thesis; both are
addressable.

The inversion pays off for the synchronous / simple-lifecycle
subset of backends AND for async backends that model
phase-evolution through status. For complex backends that need
persisted intent distinct from observed state (retry queues,
desired-state reconciliation across partial failures), the
controller pattern's complexity is load-bearing.

**Synthetic resourceVersion suffices for real informers** [`0002`,
`0004`, `0008`]. A single `atomic.Uint64` stringified as decimal
is accepted. Returning `410 Gone` on any resume request with an RV
other than current makes reflectors relist — though they more often
reach that state via a 503-on-connection-refused than via an actual
410 on resume. When the backing store is a CRD [`0010`] the AA
inherits real host-kube-apiserver resourceVersions, which removes
the synthesis question entirely for that axis.

Open questions:

- Can SSA's ownership tracking be reconstructed by encoding
  managedFields into backend-specific metadata (S3 tags, GitHub
  repo description fields, etc.) without forcing a general etcd
  shadow store? Resolved for the CRD-backed case [`0010`]; still
  open for non-CRD backends (S3 tags is the obvious target).
- The `initial-events-end` bookmark gap [`0011`] is a substrate-
  level fix queued as a candidate. Small in scope; high in
  operational value.
- ETag-aware polling: our GitHub and S3 clients don't honor
  ETags. How much rate-limit headroom does adding them buy?
- Webhook-driven backends (GitHub pushes events; AWS has
  CloudTrail and EventBridge) could skip polling entirely.
  Untested.
- Deterministic UIDs (hash of backend's stable ID) would preserve
  identity across AA restarts. Not implemented. Promoted to
  load-bearing-at-scale by `0012`'s phantom-reconcile finding.

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
   webhook / CEL). **Closed for the component-server architecture
   by `0020`**: adding `Validate` + `Mutate` RPCs to the Backend
   proto and running them in the component server's request path
   (mutate-then-validate ordering, matching standard Kubernetes
   admission) produces errors byte-wire-identical to
   ValidatingAdmissionWebhook; the name-based CREATE case from
   `0003` enforces cleanly as an admission rule. Differences from
   ValidatingAdmissionWebhook: gRPC instead of HTTPS; position is
   after kube-apiserver proxied to us (a host-cluster VAW still
   runs as an outer layer); opt-in via schema flags, not a
   cluster-level config object; fails closed on transport error.

**The operational hazard** [`0005`]: an AA whose default-deny
policy applies to every unknown identity will **brick any
cluster-wide controller that auto-discovers-and-watches every API
group its RBAC permits**. ArgoCD's gitops-engine cluster cache
treats one LIST failure as fatal for the *whole* cache, so an
unrelated ArgoCD Application stays stuck at `sync=Unknown` because
our AA 403'd the `argocd-application-controller` SA. The hazard is
**narrower than 0005 first implied** [`0014`]: Flux's default
controllers do not discovery-LIST our resources, and
kube-controller-manager has its own story. The population of
"cluster controllers that auto-discover-and-LIST every API group
they have RBAC for" is small. Still, the pattern matters for any
cluster that runs ArgoCD.

**Three mitigation patterns were tested head-to-head** [`0016`]:
Pattern A (allow-list controller SAs by name in the policy),
Pattern B (blanket `system:serviceaccount:*` allow for reads),
Pattern C (strict upstream RBAC with no permissive
`system:authenticated` binding — only RBAC-bound SAs reach the
AA, which refines further). All three unblock ArgoCD; they
differ in where denials originate, what the caller sees, what
`kubectl auth can-i` reports, and maintenance overhead. **Pattern
C (strict-RBAC + AA-refines) is the recommended default**:
smallest blast radius under compromised SA, smallest rule set,
`can-i` truthful for reads (gated by RBAC) though still lying for
writes (refined by AA), new controllers handled by standard
ClusterRoleBindings. Tradeoff: no AA-side observability of
denied read attempts (audit moves to kube-apiserver). Pattern B
is rejected as it negates identity-aware authz; Pattern A is the
fallback if AA-side audit of denied reads is required.

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
- **Synchronous vs. asynchronous backend operations** [`0009`,
  `0011`]. Refined from the `0009` reading: the stateless-AA
  model handles async backends cleanly **if** Create returns
  immediately with `phase=Provisioning` and watch streams
  subsequent status transitions. What breaks is specific
  ecosystem idioms like `kubectl wait --for=jsonpath` (see
  Storage independence for the substrate fix queued).
- **Typed vs. unstructured resource registration** [`0013`]. A
  generic component server registering `*unstructured.Unstructured`
  can honor the full CRUD+watch+discovery wire contract without
  compile-time knowledge of any resource type. What it cannot do
  is support SSA (the library's managedFields typed-converter
  requires a typed scheme) or rich per-field `kubectl explain`.
  This is the line between wire-protocol-level features (portable
  to unstructured) and library-typed-model features (aren't).
- **Backend implementation language is orthogonal** [`0019`].
  The component server's ignorance of the resource extends to
  ignorance of the backend's language. A Python backend behind an
  unchanged Go component-server image serves Notes with
  indistinguishable user-facing behavior — CRUD, watch, rich
  explain, SSA (conflict detection + force-conflicts included).
  The 0013 decision to put JSON bytes in the proto payload
  (rather than per-resource protobuf messages) is what makes
  this hold: JSON is ambient in every language and imports no
  Go-specific codegen assumptions. Python backend is ~30%
  shorter than the Go equivalent (254 vs 374 semantic lines);
  single-caller `kubectl get` latency is indistinguishable (the
  aggregation-layer hop dominates). Image size is the real cost
  (159 MB python vs 12.3 MB distroless Go).

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

**controller-runtime's manager layer also works** [`0012`], with
four specific observations beyond what raw reflectors expose:
(a) manager startup vs. APIService-ready is racy and surfaces a
~20s warning burst; (b) `client.Options.Cache.Unstructured: true`
is opt-in and its default-off setting silently bypasses the manager
cache; (c) pod-restart UID regeneration amplifies into one
synthesized delete+add reconcile pair per object, so a controller
doing real work in Reconcile redoes it on every AA restart at
O(objects) cost; (d) leader election via Leases in the host
cluster (since our AA doesn't serve Leases) works cleanly.

**The `initial-events-end` bookmark is missing from the substrate**
[`0011`]. WatchList-aware clients (1.31+ with
`InitialEventsListBlueprintAnnotationKey`) hard-require it;
`kubectl wait --for=jsonpath` times out rather than triggering on
phase transitions. Fix is substrate-level: emit a bookmark event
at the tail of initial-state replay. Queued as
`watch-initial-events-end-bookmark`.

Still unmeasured:

- CA rotation with simultaneous `APIService.caBundle` rotation.
- `WatchListClient` feature gate behavior.
- Hours-long informers that outlive multiple backend poll cycles
  and several AA restarts.

---

## Process observations

Eight observations after twenty-one experiments and two substrate
promotions:

1. **Findings proportional to signal** holds. Dense experiments
   (0001, 0002, 0003, 0006, 0008, 0009, 0010, 0011, 0013, 0015,
   0016, 0017, 0018, 0019, 0020) produced long FINDINGS; lean
   ones (0005, 0007, 0012, 0014, 0021) produced tighter ones.
   Agents have not been padding.
2. **Parallel agents on the same kind cluster clobbered each
   other's state** during the 0005/0008 arcs. Each agent created
   its own `aggexp-<slug>` cluster after the first collision.
   Worth noting in AGENTS.md next rewrite: `kubectl config
   use-context` is process-global; parallel agents need isolated
   clusters.
3. **Substrate extraction was deliberate and worked — twice.** The
   two-driver precondition (0002 + 0004) produced the first
   `Backend` interface promotion (`runtime/{server,group,authz,
   storage}`) which survived six experiment-level consumers
   (0007, 0009, 0010, 0011, 0021) with only minor seam issues.
   The three-consumer KRM precondition (0013 + 0017 + 0018)
   produced the second promotion (`runtime/component/{proto,
   scheme, openapi, grpcbackend}`), which survived its first
   post-promotion consumer (0021) with zero per-consumer
   patches. Promotion discipline — tests, docs, thought-through
   interface, wait for two or three consumers — was honored in
   both cases.
4. **The six fundamentals frame has held** across twenty-one
   experiments. No new fundamental has emerged. Five adjacent
   concerns have been named and fit cleanly under existing
   fundamentals without demanding a rewrite of the list:
   authz-vs-admission [`0003`] (closed for the component
   architecture by `0020`), substrate-promotion triggers,
   sync-vs-async backend operations (nuanced by `0011`),
   typed-vs-unstructured resource registration (resolved by
   `0017`'s typed wrapper), and language-agnostic backends
   (resolved by `0019`).
5. **The inversion thought experiments (0006 broker, 0009
   ACK-AA, 0013 KRM component) were disproportionately
   productive.** They exposed specific library-layer features
   that assume persistence (SSA managedFields; finalizer
   semantics) or typed Go models (SSA typed-converter; rich
   explain) — findings a positive "what works" probe would have
   missed. Worth repeating: inversions surface assumptions that
   direct probes don't.
6. **Sub-agent task interruption is recoverable when the
   worktree + commit convention is followed.** In Wave 1 of the
   ten-experiment arc, the 0013 sub-agent was interrupted before
   committing its working tree. Recovery: pick up the untracked
   files in the worktree, fix a trivial missing-import bug, run
   the test scenarios manually, write FINDINGS, commit under the
   original branch name. No rework of the agent's actual
   implementation was necessary. The convention held; the
   recovery was well-defined. Worth noting in AGENTS.md's
   parallel-dispatch section: "if a sub-agent is interrupted,
   resume from its worktree rather than re-dispatching."
7. **Parallel agents across waves occasionally cross kubeconfig
   contexts.** Observed in both the Wave 1 and Wave 2 arcs. One
   agent's kubectl operations silently retargeted another
   agent's cluster. Mitigation in the moment: each agent sets a
   per-experiment `KUBECONFIG` env var. Worth promoting to an
   AGENTS.md rule for the next parallel-dispatch session.
8. **Three waves of parallel dispatch (4+5+3 experiments) over a
   single arc produced 12 new experiments + 2 substrate
   promotions.** SYNTHESIS was rewritten at each wave boundary;
   EXPERIMENTS.md merge conflicts were the norm but trivially
   resolved. The wave structure let each subsequent wave's
   agents read the prior wave's findings before dispatching
   their own tasks — this shows up concretely in 0017's use of
   0013's findings, 0018's use of 0013+0017, 0019's use of
   0017, and 0021's use of 0013+0017+0018. Dispatching all 12
   at once would have prevented this feedback loop.

The ethos itself needs no changes yet. If a pattern emerges of
experiments going longer than they need to, or of SYNTHESIS
falling out of sync with FINDINGS, revisit.
