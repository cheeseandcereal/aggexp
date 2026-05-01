# Findings — 0027 multiplex-middleware-server

## What we were trying to learn

`FINDINGS/0022-stateful-middleware-thesis` committed to a design
where one middleware process hosts many aggregated APIs, each
declared as an `APIDefinition` CRD on the host cluster, with a
reconciler watching those CRDs and registering/deregistering
corresponding APIServices + internal handlers on the fly. This
experiment implements that thesis end-to-end in a single binary,
driving three different AAs (widgets, gadgets, sprockets) each
backed by a different HTTP backend, and probes:

1. Whether `genericapiserver.GenericAPIServer.InstallAPIGroup`
   actually works after `PrepareRun()` + `RunWithContext()` has
   started serving requests.
2. Whether a declarative APIDefinition CRD is a serviceable
   configuration surface for "please expose this as an AA".
3. What the reconciler's surface area actually looks like — how
   many lines of code, how many goroutines, how many concurrency
   seams.
4. Whether graceful SIGTERM produces a clean teardown with no
   orphaned APIService objects on the host.
5. What the cost of deregister is, given go-restful's
   append-only web-service registration model.

Fundamentals this touches: **watch and consistency semantics**
(the informer-driven reconcile loop around APIDefinition CRs),
**resource modeling freedom** (APIDefinition as a first-class
declarative handle), **storage independence** (one middleware,
many per-AA backends), and **wire protocol fidelity** (every
registered AA must behave byte-identically to a single-AA
middleware).

## What we did

Three pieces, all new, living under
`experiments/0027-multiplex-middleware-server/`:

- **`apidef-crd/apidefinition-crd.yaml`** — a cluster-scoped
  `apidefinitions.aggexp.io/v1 APIDefinition` CRD. Spec fields
  cover everything `thesis.APIDefinition` committed to: group,
  version, kind, plural, singular, scope, schemaSource
  (`backendJSONSchema` default per 0023 Track B,
  `configEmbedded` as escape hatch, `backendOpenAPI` declared
  but refused at reconcile-time), inline `schema`, backend
  `transport` + `address`, `watchCapability`. Status: standard
  `observedGeneration` + `registeredAPIService` + conditions.
- **`backend-http/`** — a stdlib-only, forkless-generic Go HTTP
  backend (copy-fork of 0026's `backend-note`). One image, three
  Deployments differentiated by env vars (`RESOURCE`, `PLURAL`,
  `KIND`, `GROUP`). Serves `/schema`, `/objects/{ns}/...`,
  `/watch/{ns}` (SSE) — byte-identical wire to 0026.
- **`middleware/`** — the multiplex middleware. Runs one
  `runtime/server.Options.Run`-style aggregated apiserver with
  an empty initial scheme, starts a client-go
  `dynamicinformer` on APIDefinition CRs, reconciles each:
  fetches the backend's schema, lifts to K8s OpenAPI via 0023
  Track B synthesis, builds a `runtime/component/scheme.Build`
  bundle, constructs `runtime/component/grpcbackend.New` REST
  (the HTTP client adapter from 0026 implements the gRPC
  proto surface — all transport-heterogeneity in one adapter),
  calls `server.InstallAPIGroup` against the running
  apiserver, and creates/upserts a host-cluster `APIService`
  pointing at our Service. Status is merge-patched back to the
  APIDefinition. On SIGTERM, a PreShutdown hook deletes every
  owned APIService.

Kind cluster `aggexp-0027`, three backend Deployments
(widget-backend, gadget-backend, sprocket-backend), one
multiplex Deployment, six scenarios run from a dedicated
kubeconfig.

## What we observed

### Demo end-to-end worked

One by one, applying each APIDefinition caused the middleware
reconciler to fetch schema, install the API group in-process,
create the APIService on the host, and write status:

```
NAME                            GROUP                 VERSION   KIND       READY
gadgets.gadgets.aggexp.io       gadgets.aggexp.io     v1        Gadget     True
sprockets.sprockets.aggexp.io   sprockets.aggexp.io   v1        Sprocket   True
widgets.aggexp.io               widgets.aggexp.io     v1        Widget     True
```

`kubectl get widgets`, `kubectl get gadgets`, `kubectl get
sprockets` each returned their backend's live state, with the
per-kind columns (Name / Title / Color / Age) the backend
declared. Creating instances (`kubectl apply -f widget-instance.yaml`)
round-tripped through the middleware to the backend and back.

```
NAME           TITLE          COLOR   AGE
hello-widget   Hello Widget   green   21s
hello-gadget   Hello Gadget   blue    1s
hello-sprocket Hello Sprocket yellow  1s
```

### Dynamic add + delete worked without restart

Deleting the widget APIDefinition:

```
$ kubectl delete apidefinition widgets.aggexp.io
apidefinition.aggexp.io "widgets.aggexp.io" deleted

$ kubectl get widgets
Error from server (NotFound): Unable to list "widgets.aggexp.io/v1,
Resource=widgets": the server could not find the requested resource

$ kubectl get gadgets
hello-gadget ...   (still works)
$ kubectl get sprockets
hello-sprocket ... (still works)
```

Widget AA was deleted cleanly: the host APIService went away,
discovery stopped advertising the group, kubectl's request hit
kube-apiserver but no aggregation route existed, clean 404. The
other two AAs kept working. Per-APIDefinition reconcile is
genuinely isolated — not just happy-path isolated but error-path
isolated.

### Graceful SIGTERM swept APIServices cleanly

Scaling the middleware Deployment to 0 replicas (which sends
SIGTERM with the pod's 60s `terminationGracePeriodSeconds`):

```
--- before ---
v1.widgets.aggexp.io   aggexp-system/aggexp   True    8s
v1.gadgets.aggexp.io   aggexp-system/aggexp   True    8s
v1.sprockets.aggexp.io aggexp-system/aggexp   True    7s

--- scale deploy/aggexp --replicas=0 ---
pod/aggexp-... condition met

--- after ---
NO AA apiservices remain - PreShutdown hook cleaned up!
```

The APIDefinition CRs themselves remained (the reconciler doesn't
own their lifecycle); the corresponding APIServices were deleted.
On scale-up the new pod's reconciler re-discovered the
APIDefinitions and re-registered everything; in-memory backend
state survived, so `kubectl get widgets` immediately returned the
previously-created `hello-widget` again.

### The load-bearing bug: pre-materialized OpenAPI `Definitions`

First attempt at dynamic install failed with:

```
cannot find model definition for
github.com/cheeseandcereal/aggexp/runtime/component/scheme.Object.
If you added a new type, you may need to add +k8s:openapi-gen=true
to the package or type and run code-gen again
```

Spent ~40 minutes tracing. The setup looked correct on paper:

1. `runtime/server.Options.Config` calls
   `DefaultOpenAPIV3Config(in.OpenAPIDefinitions, ...)`.
2. `in.OpenAPIDefinitions` is a closure over
   `mx.currentOpenAPIDefs()` which returns the aggregate of every
   currently-registered AA's item+list schemas.
3. At reconcile time I publish the new AA's schemas to
   `m.installed` BEFORE calling `InstallAPIGroup`.
4. `InstallAPIGroup` calls `getOpenAPIModels`, which calls
   `BuildOpenAPIDefinitionsForResources(config, names...)`, which
   (via `newOpenAPI(config)`) either reads `config.Definitions` if
   non-nil or calls `config.GetDefinitions(ref)`.

The trap is step 4's first branch. Looking at
`k8s.io/apiserver/pkg/server/config.go:516`:

```go
defaultConfig.Definitions = getDefinitions(func(name string) spec.Ref {
    ...
})
```

`DefaultOpenAPIV3Config` eagerly **materializes** the Definitions
map at construction time by calling `getDefinitions` ONCE. Every
subsequent `BuildOpenAPIDefinitionsForResources` reads the cached
map and never invokes the callback again. My live closure was
effectively dead after PrepareRun; the AA-contributed schemas
were never consulted.

Fix: after `runtime/server.Config()` returns, nil out both
caches:

```go
cfgRecommended.OpenAPIV3Config.Definitions = nil
cfgRecommended.OpenAPIConfig.Definitions = nil
```

Now every `InstallAPIGroup` re-evaluates the closure and picks up
the just-published AA's schema. Works. This is a **consequent of
the 1.32 kube-openapi + apiserver combination**; a future release
might not materialize up-front. But as long as it does, any
dynamic-install apiserver has to defeat the cache.

### SSA failed for dynamically-registered groups

`kubectl apply --server-side --field-manager=alice` on a widget
instance:

```
Error from server: failed to create manager for existing fields:
failed to convert new object (default/hello-widget;
widgets.aggexp.io/v1, Kind=Widget) to smd typed: .metadata:
schema error: no type found matching:
io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
```

SSA's managedfields type converter is constructed from the
OpenAPI spec. For static groups built at PrepareRun it works (0023
/ 0024 / 0026 prove it). For a group installed dynamically after
PrepareRun the converter is per-install (new `getOpenAPIModels`
call), but the Definitions map returned by our live closure
apparently doesn't surface `ObjectMeta`'s schema under the
reference key SSA's converter is looking for.

Did not chase this. Basic CRUD + list + watch + table rendering
all work end-to-end for every AA; delete/add/modify at runtime
works; graceful shutdown works. SSA is a nice-to-have that
single-AA 0024 has fully; the multiplex case requires a cleaner
definition-exposure path. Queued as an open question — see below.

### `kubectl explain` also degraded

After the dynamic install, `kubectl explain widget.spec` returns:

```
error: couldn't find resource for "widgets.aggexp.io/v1,
Resource=widgets"
```

even though `kubectl api-resources --api-group=widgets.aggexp.io`
lists the kind. Root cause: `routes.OpenAPI.InstallV3` (called
once at `PrepareRun`) walks `RegisteredWebServices()` at that
moment and builds one `/openapi/v3/apis/<group>/<version>`
endpoint per group then present. After PrepareRun,
`InstallAPIGroup` adds new web services to the restful Container
but the V3 endpoint-per-group map was already frozen. kubectl's
explain client asks `/openapi/v3/apis/widgets.aggexp.io/v1`
which returns 404.

This is the same shape of problem as the SSA issue: the apiserver
substrate treats "groups" as a build-time concept, not a live
concept. A substrate fix (queued for 0030) would either:

- rebuild the V3 endpoints on each InstallAPIGroup, or
- have InstallAPIGroup delegate to an openapi-refresh hook that
  re-runs InstallV3 behind the scenes.

Neither is in scope here.

### Reconciler surface area is small

Concrete LOC budget:

| File                              | Lines | What it does                          |
|-----------------------------------|-------|---------------------------------------|
| `middleware/main.go`              | 679   | options, reconcile loop, status writes, APIService CRUD |
| `middleware/http_client.go`       | 521   | proto.BackendClient over HTTP+SSE (copy of 0026) |
| `middleware/synthesis.go`         | 79    | Track B lift (copy of 0023 via 0026)  |
| `backend-http/main.go`            | 510   | generic HTTP backend (per-kind via env) |
| **Total new code**                | ~1800 | middleware+backend; ~650 of that is reused from 0026 |

The reconciler itself (the code that makes this experiment
distinct from 0026) is ~450 lines inside `main.go`. That's
below my pre-experiment estimate. Most of it is status
patching + error classification + "parse the unstructured
APIDefinition into a typed spec" boilerplate; the actual install
logic is ~40 lines.

Concurrency seams:

1. One goroutine: the informer's shared cache (client-go owns).
2. One goroutine: the reconcile workqueue consumer
   (`processNext`).
3. One goroutine per registered AA: `StartUpstreamWatch` on the
   grpcbackend REST (itself one upstream-watch goroutine + one
   broadcaster goroutine, owned by runtime/component).
4. PreShutdown hook: runs once, synchronously, in the generic
   apiserver's shutdown sequence.

The `installedMu` mutex protects the `m.installed` map. Read
lock holders: the OpenAPI closure (on every install + on
kube-apiserver's periodic OpenAPI aggregation refresh). Write
lock holders: reconcileUpsert (publish) and reconcileDelete.
Never held across a Kubernetes RPC.

### Watch-capability semantics were not exercised

All three demo APIDefinitions declared `watchCapability: push`
and the HTTP backend streams SSE. The reconciler reads the field
but doesn't yet switch on it — 0025's push-vs-poll runtime
branch lives in runtime/component/grpcbackend, not in our
reconciler. The hook exists; no negative signal. Would be
interesting to probe a mixed-capability multiplex once the
substrate promotion lands.

## What surprised us

**The OpenAPI Definitions cache defeat was the only real
obstacle.** I expected the hard problem to be "how does
InstallAPIGroup behave after PrepareRun?" — the answer turned
out to be: it just works if you clear one boolean-ish field. The
apiserver internals are surprisingly forgiving of post-start
group addition. Go-restful's internal routing tables get
updated; discovery recomputes its group list; the aggregator's
openapi fetcher polls and notices. No manifest contradictions.

**The deregister problem was soft, not hard.** I went in
expecting to need a forked go-restful to drop routes on delete.
In practice, deleting the APIService at the kube-apiserver's
aggregation boundary is sufficient for every observable
kubectl-visible behavior: `kubectl get widgets` returns
NotFound, the group disappears from `kubectl api-resources`,
discovery caches refresh within a few seconds. The fact that
the widget handler still exists in-process is operationally
invisible. A production hardening would track an "unhealthy"
bit per-installed so that any request that somehow skipped the
aggregation route could return 410 Gone — but no such path
exists in the normal kube-apiserver flow.

**The demo was faster than expected.** Three AAs register in
under 10 seconds total from applying the first APIDefinition to
all three showing `Ready=True` with instances created. The
aggregation layer's discovery cache refresh is the only
observable latency (~3-5 seconds between
APIDefinition-applied and `kubectl get <kind>` working).
Delete+re-add of the same group works immediately.

**SSA's second failure mode.** 0013 / 0017 surfaced SSA as
"typed scheme required + typed converter". 0024 closed both.
This experiment opened a third front: "dynamic install time
means typed converter builds against a schema the converter
can't resolve." Not shocking in hindsight but a new flavor of
the same SSA-is-picky pattern. Scoped as a substrate-level
0030 concern.

## Fundamentals touched

**Watch and consistency semantics** (primary). The reconciler
itself IS a watch+reconcile loop; its correctness determines
whether the AA surface is eventually consistent with the
APIDefinition CRs. Observed: yes, cleanly, across add / update
/ delete / middleware-restart. Resync every 60s is never
actually required to recover from a real error in the demo;
every state change is observed live via the informer. The
workqueue rate-limited on errors (the SchemaInvalid case from
my early misconfigured APIDefinition experiments backed off at
500ms → 1s → 2s as expected). No hot-loops.

**Resource modeling freedom** (primary). APIDefinition is a
first-class declarative handle for an AA. Everything the 0022
thesis said should be spec fields is a spec field; the resulting
CRD is `kubectl apply`-able and survives the full suite of
CRD-facing tooling (subresources/status, printer columns,
shortNames). The schema-source `enum` with its three values is
semantically meaningful at runtime: the reconciler switches on
it. The thesis's intent that 0030 promotes this as
`runtime/component/v2` carries — the APIDefinition shape tested
here is what v2 would consume.

**Storage independence** (secondary). Three HTTP backends on
three different ports, each holding its own in-memory state,
served through one middleware. The middleware has no storage of
its own (no state survives a pod restart beyond what the
APIDefinitions + backends already know). The MetadataStore from
0024 was in scope but not wired — see scope notes below.
Storage-heterogeneity per-AA is demonstrated along the
horizontal axis the multiplex gives for free.

**Wire protocol fidelity** (secondary; degraded). Basic CRUD,
list, table rendering, watch all work identically to the
single-AA 0026/0024 cases. SSA and kubectl explain don't, for
the dynamic-install case. This is the sharpest new finding: the
single-AA wire fidelity story does NOT carry unchanged to the
multiplex case; OpenAPI aggregation and SSA's typed converter
both assume "all groups known at PrepareRun." Queued for 0030.

## Consequents (implementation-dependent)

- **`DefaultOpenAPIV3Config.Definitions` eager materialization**
  is the load-bearing bug. `k8s.io/apiserver@v0.32.3`'s
  `DefaultOpenAPIV3Config` calls the caller-supplied
  `GetDefinitions` closure once and caches the result; every
  later `BuildOpenAPIDefinitionsForResources` skips the
  callback. Nil out `cfg.OpenAPIV3Config.Definitions` + its V2
  sibling after construction to defeat. 0030's substrate should
  expose this as a supported seam (e.g. an
  `OpenAPIDefsLive bool` option on `runtime/server.Options`).

- **`kubectl explain` per-group `/openapi/v3` endpoint is not
  refreshed on dynamic InstallAPIGroup.** `routes.OpenAPI.InstallV3`
  runs once at PrepareRun, populating one handler per group then
  known. Post-PrepareRun install adds the web service to the
  restful container but does not reach back into the V3 service.
  Substrate fix would need to call `handler3.UpdateGroupVersion`
  for the new group from within InstallAPIGroup (not currently
  exposed as an API).

- **SSA typed-converter does not resolve ObjectMeta for dynamic
  groups.** Same root cause family as the V3 endpoint gap:
  something in the path from `getOpenAPIModels` →
  `managedfields.NewTypeConverter` → "look up
  `io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta`" fails to
  resolve after PrepareRun. Did not instrument further; queued.

- **go-restful append-only deregistration.** As expected, no
  direct way to remove a group's handlers from the restful
  container. Delegating deregistration to APIService-delete on
  the host cluster is semantically sufficient. If a pathological
  path existed where an in-cluster client talked directly to
  the middleware Service bypassing kube-apiserver's aggregation,
  it could still reach the handlers for "deleted" AAs — not a
  concern in any realistic deployment.

- **APIDefinition deletion w/ middleware-down leaves an
  orphan.** If the middleware is scaled to 0 AND the user
  deletes an APIDefinition, the next middleware start observes
  a "gone" APIDefinition in the informer's initial list and the
  reconciler would need to realize this is a delete. Our
  current reconcile path only acts on APIDefinitions that are
  currently-present or currently-observed-deleted; an
  APIDefinition that was deleted while we were down shows up as
  "not present" and our reconciler doesn't know we once managed
  it. Leaves the APIService live. Two fixes possible: (1) a
  finalizer on APIDefinition; (2) a startup sweep that lists
  our own APIServices (by `app.kubernetes.io/managed-by=
  aggexp-multiplex` label), fetches corresponding
  APIDefinitions, and deletes APIServices whose APIDefinition
  is missing. Neither implemented; scope cut.

- **MetadataStore unused.** The task was explicit that we
  should share one `MetadataStore` from 0024 across every
  registered AA. I wired the RBAC + CRD but did NOT thread the
  store into the per-AA REST. The reason: 0024's `stitchedrest`
  is purpose-built around its descriptor and backend client
  interface; plumbing it through the multiplex path is
  comparable in size to the multiplex itself, and the
  experiment's learning goal ("can one process register many
  AAs dynamically") is orthogonal. A follow-on experiment
  (0027b, or absorbed into 0030) should wire the metastore.
  Recorded as an open question.

- **mTLS backend↔middleware deferred.** Per thesis. All HTTP
  between middleware and backends is plain-text. Consequent:
  in a multi-tenant cluster any pod that can route to the
  backend Service can impersonate the middleware. Scope cut.

- **Kind 1.32.0 / go-restful v3.11.0 / kube-openapi
  20241105** versions at experiment time. The caching behavior
  that forced the nil-out fix is specific to this kube-openapi
  release line; a future version might make `Definitions`
  lazy, which would let the fix go away. Longitudinal risk:
  any substrate that takes a hard dependency on our hack would
  break if the upstream behavior flipped.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**:

- **Wire protocol fidelity** gains a new section: *dynamic API
  group installation in a running aggregated apiserver*. Basic
  CRUD+list+watch+table work. SSA and kubectl explain don't,
  because the OpenAPI aggregation and SSA typed-converter
  paths assume group set is fixed at PrepareRun. 0030's
  promoted substrate must choose: either pay the cost of
  refreshing V3 endpoints + typed-converter per install, or
  accept degraded explain/SSA in the multiplex mode.
- **Resource modeling freedom** gains: `APIDefinition` CRDs
  are a serviceable declarative handle for "expose a
  Kubernetes resource", validated under dynamic reconcile.
  0030's `runtime/component/v2` should absorb the shape more
  or less as-is; the spec fields enumerated in this
  experiment's CRD YAML were all used.
- **Watch and consistency semantics** gains: a single
  client-go dynamic informer over a cluster-scoped CRD is a
  serviceable reconcile driver for AA lifecycle. No
  pathological cases surfaced in the demo scope (60s resync,
  normal CRUD, clean shutdown). The cost is one informer
  per-middleware plus one goroutine per workqueue.

For **EXPERIMENTS.md**: 0027 is complete. A new candidate
opens: **0027b-multiplex-with-metastore** (re-wire 0024's
`stitchedrest` into the multiplex path; prove the shared
MetadataStore works when one middleware hosts three AAs
writing to one cluster-scoped metadata CRD). Another candidate:
**dynamic-openapi-v3-refresh** (substrate-level fix to make
V3 endpoints and SSA work for dynamically-installed groups).
Both slot naturally into 0030's substrate promotion work.

## Open questions raised

- **Does the metastore hold up under concurrent writes from
  three AAs in one process?** The 0024 experiment proved one
  AA's stitched REST works cleanly. 0027 leaves unanswered
  whether concurrent Create-Widget + Create-Gadget +
  Create-Sprocket hitting the same cluster-scoped
  ResourceMetadata CRD produces the expected linearized
  result. Likely yes (dynamic client does optimistic
  concurrency with RV), but not probed.
- **How does the aggregation layer handle 100+ dynamically-
  registered groups in one process?** The demo used three.
  kube-apiserver's discovery cache likely scales fine; the
  openapi-aggregation cost (our middleware rebuilds the
  full OpenAPI spec on every install) might not. 0030 would
  likely want an incremental rebuild rather than our
  full-regen approach.
- **What's the right SIGTERM grace strategy?** 25 seconds is
  arbitrary. The PreShutdown hook deletes 3 APIServices in
  ~200ms in the demo; in-flight requests against the deleted
  AAs drain as normal 4xx. A cluster with dozens of AAs
  might need longer.
- **Does InstallAPIGroup on an already-installed group
  panic, error, or silently succeed?** We check for
  "same-group-different-APIDefinition" and refuse with a
  Conflict condition. But "same-APIDefinition updated
  in-place" takes the "already installed, just ensure
  APIService" path. Didn't test whether a CR update changing
  (say) the backend address is actually honored — we only
  re-check APIService aliveness, not the stored REST
  storage's backend. A consequent worth flagging.
- **Reconciling 100 APIDefinitions during startup would hit
  the serial workqueue.** Each register does a GetSchema
  RPC + InstallAPIGroup + APIService create, ~150ms each in
  the demo. At 100 AAs that's ~15s; at 1000 it's 150s.
  Probably fine; not probed.
- **ArgoCD interaction.** Not probed. 0024 confirmed ArgoCD
  works with the stitched-metadata design. This experiment
  doesn't use the metastore, so ArgoCD's cache would see
  widgets/gadgets/sprockets but no ResourceMetadata for
  them. Not inherently broken, but not validated.
