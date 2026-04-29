# Findings — 0002 hello-aggregated

## What we were trying to learn

Where `0001` hand-rolled stdlib HTTP, this experiment builds on
`k8s.io/apiserver` — the library. Four concrete hypotheses going in:

1. `kubectl explain` fails on hand-rolled OpenAPI because the schema
   components lack `x-kubernetes-group-version-kind`; generated
   OpenAPI from `openapi-gen` + `openapi.NewDefinitionNamer(Scheme)`
   should fix it.
2. `watch.NewBroadcaster` plus a monotonic `atomic.Uint64`
   resourceVersion counter satisfies kubectl / client-go watch.
3. `kubectl apply` (merge-patch) and `kubectl apply --server-side`
   both work without us writing any field-management code, provided
   we satisfy `rest.Patcher` (= Getter + Updater).
4. Dropping `EtcdOptions` by rolling a bespoke Options struct (a la
   metrics-server) gives a clean, stateless, in-memory AA.

## What we did

Wrote `experiments/0002-hello-aggregated` — a Go AA in
`~400` lines of hand code plus generated deepcopy and OpenAPI.
Components:

- `pkg/apis/aggexp/types.go` — internal types (`aggexp.Hello`,
  `aggexp.HelloList`).
- `pkg/apis/aggexp/v1/types.go` — external versioned types with
  `+k8s:deepcopy-gen` and `+k8s:openapi-gen` markers.
- `pkg/apis/aggexp/v1/conversion.go` — hand-rolled 1:1 converters
  between internal and external.
- `pkg/apis/aggexp/install/install.go` — registers both versions in
  a shared scheme.
- `pkg/apiserver/apiserver.go` — the shared `Scheme` + `Codecs` +
  unversioned types (`metav1.Status`, `APIVersions`, `APIGroupList`,
  …) required by kube-apiserver's discovery path.
- `pkg/registry/hello/storage.go` — in-memory `rest.Storage`
  implementation backed by `sync.Map`. Satisfies `Getter`, `Lister`,
  `Creater`, `Updater` (and thus `Patcher`), `GracefulDeleter`,
  `Watcher`, `TableConvertor`, `Scoper`, `KindProvider`,
  `SingularNameProvider`. Compile-time interface assertions fail the
  build if any are missing.
- `pkg/server/server.go` — `Options` struct composing
  `SecureServing`, `DelegatingAuthentication`, `DelegatingAuthorization`,
  `Audit`, `Features`, `CoreAPI`. No `Etcd`, no `RecommendedOptions`.
- `cmd/aggexp-hello/main.go` — cobra + `cli.Run` wiring.

Generated files:

- `pkg/apis/aggexp/v1/zz_generated.deepcopy.go` via
  `kube_codegen.sh` from `k8s.io/code-generator@v0.32.3`.
- `pkg/generated/openapi/zz_generated.openapi.go` via the same.

Deployed into the kind cluster, swapping the Deployment image from
the 0001 probe to the library-backed `aggexp-hello:dev`.

## What we observed

### The `kubectl explain` hypothesis holds precisely

After redeploying with generated OpenAPI, `kubectl explain hello`
worked immediately, printing the type docstring, the apiVersion/kind
metadata, and FIELDS (`metadata`, `spec`, `status`). `kubectl explain
hello.spec` drilled down and printed the per-field doc
(`Greeting is the string the server echoes back. Arbitrary.`).

The thing the 0001 probe was missing is that openapi-gen synthesizes
`x-kubernetes-group-version-kind` extensions onto each schema
component automatically — via the `openapi.NewDefinitionNamer(Scheme)`
called in `genericapiserver.DefaultOpenAPIV3Config`. kubectl's
schema index keys off that extension. Absent it, the server serves a
structurally-valid OpenAPI that kubectl cannot index by GVR.

This is now a sharp, reproducible finding: **the generated OpenAPI
pipeline handles `kubectl explain` correctly with zero additional
work from the author; the hand-rolled minimal OpenAPI does not,
because GVK extensions are the discriminator.**

### `kubectl apply` works — merge-patch, no fieldmanager required

Plain `kubectl apply -f -` creates and updates cleanly with the
`kubectl.kubernetes.io/last-applied-configuration` annotation
persisted on the object. Strategic-merge patches are not involved
(we have no strategic-merge metadata) but JSON-merge-patch works
by default for aggregated APIs.

### `kubectl apply --server-side` works — managedFields appear

This was the headline surprise from the probe's perspective. Once
the internal version was registered and `rest.Patcher` was
satisfied, `kubectl apply --server-side`:

1. Successfully creates the object.
2. Populates `metadata.managedFields` with the correct
   `fieldsV1.f:spec.f:greeting` entry owned by the `kubectl` manager.
3. A second apply from a different field manager (via
   `--field-manager=other-mgr --force-conflicts`) correctly swaps
   ownership of the conflicted field and updates
   `metadata.managedFields` accordingly.

We wrote zero SSA code. The generic PATCH path, handed an
`rest.Patcher` and an OpenAPI v3 schema rich enough to include our
types, does the structured-merge-diff work itself — conflict
detection included.

This is a significant finding for storage-independent AAs: an
in-memory `sync.Map` resource can fully participate in the modern
SSA-based tooling ecosystem without bespoke field management logic.

### The internal version is not optional for SSA

First attempt at SSA failed with:

> `failed to convert to unversioned (...): no kind "Hello" is
>  registered for the internal version of group "aggexp.io" in
>  scheme`

The generic PATCH machinery converts the incoming versioned object
to the group's internal version before applying the merge. With only
`v1` registered in our scheme, the conversion had nowhere to go.
Adding `pkg/apis/aggexp/types.go` (the same types re-declared as the
internal version) plus hand-written 1:1 conversion funcs resolved
it. This is forced by the library even when — as in our case — the
internal and external types are byte-identical, because a future
multi-version group would need internal types as the hub.

Noted as a **fundamental** of library-backed AAs: if you want SSA /
strategic-merge-patch / anything that routes through the internal
hub, you register an internal version, full stop.

### Watch with `watch.NewBroadcaster` and a monotonic RV — works

`watch.NewBroadcaster(100, DropIfChannelFull)` plus
`watch.WatchWithPrefix([]watch.Event{…initial ADDEDs…})` produced a
watchable stream that `kubectl get -w` consumed correctly. Manual
test sequence — `apply watch-test` → `delete watch-test` —
triggered visible ADDED and a rv bump, though kubectl's tabular
rendering of `DELETED` events in `-w` mode is (as in 0001)
inconsistent-looking even when the events are in fact flowing.

Our synthetic resourceVersion — a single `atomic.Uint64` stringified
as decimal — was accepted by kubectl without protest. We did not
stress-test a long-lived client-go informer past the relist
boundary; that experiment remains queued (`long-lived-informer`).

### Impersonation still stops at kube-apiserver RBAC

Confirmed from 0001: `kubectl --as alice get hellos` returns
`Forbidden` from kube-apiserver before the request reaches our
server. Our custom logging never sees user `alice`. This is
consistent and reaffirms: **per-request authz in the AA is strictly
additive to RBAC upstream.**

### APF needs RBAC we don't have — disabled outright

First deploy crashed with `priority and fairness requires a core
Kubernetes client`. Fixing that (passing the kubernetes clientset
into `Features.ApplyTo`) made APF initialize — and then immediately
fail its readyz probe because our ServiceAccount lacks `list`
permission on `flowschemas.flowcontrol.apiserver.k8s.io` and
`prioritylevelconfigurations.flowcontrol.apiserver.k8s.io`.

Pragmatic choice for the lab: passed `--enable-priority-and-fairness=false`.
A single-replica AA in a kind cluster does not need APF; the max-in-flight
handler is sufficient.

Documenting both the workaround and the upstream defaults as a
consequent: the default flag value for `--enable-priority-and-fairness`
is `true`, and that pulls in RBAC requirements most experimental AAs
won't want. If we ever need APF in a substrate AA, we add the RBAC
bindings for those two resources and move on.

## Fundamentals touched

**Wire protocol fidelity.** `kubectl explain` is fixed by generated
OpenAPI with GVK extensions. Plain `kubectl apply` works via
JSON-merge-patch when `rest.Patcher` is satisfied. Server-side
apply works — including managedFields tracking and conflict
handling — with no bespoke field-management code. The minimum
obligation on a real AA for full tooling compatibility is "register
your types, generate OpenAPI, implement Patcher." Everything else
follows.

**Storage independence.** An in-memory `sync.Map` with a synthetic
resourceVersion satisfies every Kubernetes contract we exercised —
including SSA. The cost is real but bounded: drop `EtcdOptions`,
write a hand-rolled `rest.Storage` in place of `genericregistry.Store`,
invent a resourceVersion scheme, wire watch fan-out. Roughly 250
lines of hand code for the storage itself. The library does not
resist the etcd-less path; it merely doesn't advertise it.

**Watch and consistency semantics.** `watch.NewBroadcaster` + a
prefix-seeded `WatchWithPrefix` fans out events to multiple
consumers with minimal ceremony. `kubectl get -w` accepts our
stream. The harder tests — long-lived reflectors past the relist
boundary, controller-runtime informers, 410-Gone reconnect — are
queued.

**Identity handoff.** Unchanged from 0001. Our REST storage's log
line shows every mutation's `user`, `groups`, and RV, proving the
identity flowed through the DelegatingAuthenticator and into
`genericapirequest.UserFrom(ctx)`. No surprise here; confirms the
library's plumbing matches the 0001 hand-rolled observation.

## Consequents (implementation-dependent; do not generalize)

- **APF is on by default and needs RBAC our SA lacks.** Consequent
  of the apiserver library's 1.32 defaults. Other library versions
  may differ; a production AA in a customer cluster can choose to
  grant APF RBAC. We turned it off.
- **`EffectiveVersion` must be set before `Complete()`** in 1.32.
  `serverConfig.EffectiveVersion = utilversion.DefaultKubeEffectiveVersion()`
  (import path `k8s.io/component-base/version`, **not**
  `k8s.io/apiserver/pkg/util/version` — the latter doesn't exist
  in this release). Consequent of the KEP-4330 rollout.
- **Kube-openapi pseudo-version** (`v0.0.0-20241105132330-…`) is the
  one the 1.32 staging uses. Newer pseudo-versions will conflict;
  older ones miss methods. Consequent of kube-openapi's no-semver
  convention.
- **`kube_codegen.sh`** is the current code-generator entry point
  (replaces the pre-1.28 `generate-groups.sh` / `generate-internal-groups.sh`
  pattern). The old `hack/update-codegen.sh` from sample-apiserver
  pre-2023 does not apply.
- **`go mod tidy` bumps the `go` directive** to the host's Go
  version (1.26 on this machine). That broke `docker build`
  against `golang:1.24-alpine`. Pinned the directive to `go 1.24`
  manually. Consequent of Go's toolchain-pinning behavior.
- **`rest.Watcher`'s `WatchWithPrefix` returns `watch.Interface`**
  whose events use `watch.EventType` which is a string (`ADDED`,
  `MODIFIED`, `DELETED`, `BOOKMARK`, `ERROR`) — not the `k8s.io/api/*`
  package types. Minor but easy to trip on.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**:

- `Wire protocol fidelity`: upgrade from "minimum for `kubectl get`
  is achievable stdlib-only" to "minimum for full modern tooling
  compat (explain + SSA + managedFields) is achievable with the
  library in ~400 hand-written lines plus codegen." The sharp
  edge at `kubectl explain` is now characterized: the GVK
  extension in generated OpenAPI is the specific bit that matters.
- `Storage independence`: confirmed. The etcd-less path is clean
  enough that future experiments can duplicate this template
  without ceremony. First real datapoint: ~250 lines for the
  storage, ~400 total for the AA shell, generated openapi
  ~130KB — not trivial but entirely approachable.
- `Watch and consistency semantics`: confirmed the broadcaster
  pattern works; `long-lived-informer` remains the important
  unknown.
- `Per-request authorization`: unchanged structurally; still
  additive to RBAC. Our custom authorizer experiment queued as
  `custom-authorizer-external-policy` is the real test.

For **EXPERIMENTS.md**:

- `hello-aggregated` moves from candidate to complete.
- `openapi-explain-minimum` can be retired: this experiment
  answered the question (generated OpenAPI with GVK extensions
  suffices; hand-rolled without them does not).
- `extract-runtime` is closer — we now have one library-backed AA.
  The pattern is not yet repeated; refactoring is premature. Wait
  for a second driver (fs or github) to demand shared code.
- New candidate: **`apf-rbac-investigation`** — what minimum RBAC
  lets an AA enable APF cleanly? Consequent, but operationally
  useful.

## Open questions raised

- Can we produce a smaller OpenAPI (not the full 130KB the
  generator emits) that still satisfies `kubectl explain` and SSA?
  Is there value in trimming it, or is the aggregated-apiserver
  openapi merge cost already paid?
- Does a Go client built from this AA's types (via `client-gen`)
  work against this AA? We did not generate clientsets; future
  experiments that depend on that should answer.
- What's the behavior of `kubectl get -w` when our server restarts
  mid-watch? Does the client reconnect gracefully? (Expected yes,
  but unobserved.)
- Does a controller-runtime manager pointed at this AA function
  normally? Experiment queued.
- Our `Watch()` returns `ResourceExpired` when asked for an RV
  older than current; is that the correct response shape, or
  should we buffer a sliding window of events? Pragmatic answer:
  let client-go's reflector relist; measure the cost when we
  have a real informer under load.
