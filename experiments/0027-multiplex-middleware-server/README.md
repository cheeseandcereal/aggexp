# Experiment 0027: multiplex-middleware-server

One middleware process, many AAs. The middleware watches
`APIDefinition` CRDs on the host cluster and dynamically registers
/ deregisters aggregated API groups as the CRDs are created,
modified, and deleted. A single `MetadataStore` (the 0024 CRD-backed
store) is shared across every AA served by the middleware.

## Hypothesis

- **Wire protocol fidelity + watch/consistency semantics** (primary
  fundamentals). `kubectl` (+ optional ArgoCD-class consumers) see
  each registered AA identically to the single-AA case from 0024,
  regardless of which sibling AAs are registered in the same
  middleware, and regardless of whether those AAs were registered
  at startup or hot-added later.
- **Storage independence** (secondary). Three AAs with three
  different backends (three different HTTP ports, three different
  JSON-Schema-described resource types) can share one
  `MetadataStore`. The stitched metadata CRD is a shared substrate,
  not per-AA.
- **Resource modeling freedom** (tertiary). The `APIDefinition` CRD
  is a declarative handle for "expose a Kubernetes resource";
  everything the task's thesis commits to (group, version, kind,
  plural, singular, scope, schema source, backend transport + addr,
  watch capability) is expressible as CR fields.

## What this experiment tests (in order)

1. Middleware starts with zero AAs registered. `kubectl api-resources`
   in the `aggexp.io` supergroup surface returns nothing.
2. Apply `APIDefinition/widgets.aggexp.io`. Within ~seconds:
   - middleware has fetched the widget backend's JSON Schema,
   - lifted it to full K8s OpenAPI (Track B per 0023),
   - installed the `aggexp.io/v1` API group against our running
     aggregated apiserver via `InstallAPIGroup`,
   - created a host-cluster `APIService` pointing at us,
   - written `status.conditions=[Ready=True, Available=True]` and
     `status.registeredAPIService=v1.aggexp.io` back to the CRD.
   `kubectl get widgets` works.
3. Apply a second `APIDefinition/gadgets.gadgets.aggexp.io` against
   a **different** backend (different HTTP port, different schema).
   Middleware installs a new group, new APIService, status flips to
   Ready=True. `kubectl get gadgets` works while `kubectl get widgets`
   still works.
4. Apply a third `APIDefinition/sprockets.sprockets.aggexp.io`
   against a third backend. Three AAs live in one middleware process.
5. Delete `APIDefinition/widgets.aggexp.io`. The middleware deletes
   the `v1.aggexp.io` `APIService`, drains in-flight widget requests,
   and `kubectl get widgets` fails cleanly (no APIService → no
   aggregation route). `kubectl get gadgets` and `kubectl get
   sprockets` continue to work.
6. Send SIGTERM to the middleware pod. Graceful shutdown path: the
   middleware deletes every APIService it owns, waits for in-flight
   requests to drain, then exits 0. No orphaned APIService objects
   remain on the host cluster.

## Architecture

```
 host cluster
 ├── APIDefinition CRDs (apidefinitions.aggexp.io/v1)   <-- written by user
 ├── APIService objects       <-- created/deleted by middleware
 ├── ResourceMetadata CRD     <-- 0024's shared-CRD metadata store
 │
 └── middleware pod (one process)
     ├── runs an aggregated apiserver on :8443
     │   - PrepareRun()+Run() at startup with zero API groups
     │   - dynamic InstallAPIGroup() on reconcile
     ├── watches APIDefinitions with a client-go informer
     ├── reconciler goroutine (workqueue) acts on add/update/delete
     ├── one MetadataStore instance, passed into every AA's REST
     ├── per-AA HTTP clients to the backends (transport=http, 0026)
     └── SIGTERM handler: deletes owned APIServices, exits cleanly
```

## APIDefinition shape

Cluster-scoped CRD: `apidefinitions.aggexp.io/v1 APIDefinition`.

Spec fields (exactly matching the task's requirements + 0022 thesis):
- `group`, `version`, `kind`, `plural`, `singular`
- `scope`: `Namespaced` | `Cluster`
- `schemaSource`: `backendOpenAPI` | `backendJSONSchema` (default)
  | `configEmbedded`
- `schema`: free-form JSON; used when `schemaSource=configEmbedded`
- `backend.transport`: `http` | `grpc` (only `http` implemented in
  this experiment; gRPC path would reuse 0024's grpc code)
- `backend.address`: hostport (e.g. `widget-backend.aggexp-system.svc:8080`)
- `watchCapability`: `poll` | `push` | `both`

Status fields (written by the middleware):
- `observedGeneration`: last `metadata.generation` the reconciler acted on
- `registeredAPIService`: name of the APIService object on the host
  (e.g. `v1.aggexp.io`)
- `conditions[]`: standard-shape `Ready`, `Available`, with reasons:
  - `Ready=True, Reason=Reconciled`
  - `Ready=False, Reason=BackendUnreachable`
  - `Ready=False, Reason=SchemaInvalid`
  - `Available=True, Reason=APIServiceRegistered`

## How to run

```bash
# 1. bring up the dedicated kind cluster
./hack/make-kind.sh aggexp-0027

# 2. mint serving certs
./hack/gen-certs.sh

# 3. base manifests (ServiceAccount, RBAC, Service, aggexp Deployment
#    placeholder — we override the container image below)
./hack/deploy.sh deploy/manifests

# 4. build images
docker build -t aggexp-multiplex:dev \
  -f experiments/0027-multiplex-middleware-server/middleware/Dockerfile .
docker build -t aggexp-widget-backend:dev \
  -f experiments/0027-multiplex-middleware-server/backend-http/Dockerfile \
  --build-arg RESOURCE=widget \
  experiments/0027-multiplex-middleware-server/backend-http
docker build -t aggexp-gadget-backend:dev \
  -f experiments/0027-multiplex-middleware-server/backend-http/Dockerfile \
  --build-arg RESOURCE=gadget \
  experiments/0027-multiplex-middleware-server/backend-http
docker build -t aggexp-sprocket-backend:dev \
  -f experiments/0027-multiplex-middleware-server/backend-http/Dockerfile \
  --build-arg RESOURCE=sprocket \
  experiments/0027-multiplex-middleware-server/backend-http

kind load docker-image aggexp-multiplex:dev --name aggexp-0027
kind load docker-image aggexp-widget-backend:dev --name aggexp-0027
kind load docker-image aggexp-gadget-backend:dev --name aggexp-0027
kind load docker-image aggexp-sprocket-backend:dev --name aggexp-0027

# 5. install the APIDefinition CRD + ResourceMetadata CRD + permissive RBAC
kubectl apply -f experiments/0027-multiplex-middleware-server/apidef-crd/
kubectl apply -f experiments/0024-metadata-crd-store/metadata-crd/

# 6. deploy the three backends + the multiplex middleware override
AGGEXP_IMAGE=aggexp-multiplex:dev \
  ./hack/deploy.sh experiments/0027-multiplex-middleware-server/manifests
kubectl -n aggexp-system rollout status deploy/aggexp --timeout=120s
kubectl -n aggexp-system rollout status deploy/widget-backend --timeout=60s
kubectl -n aggexp-system rollout status deploy/gadget-backend --timeout=60s
kubectl -n aggexp-system rollout status deploy/sprocket-backend --timeout=60s

# 7. register AAs one at a time, observe reconcile
kubectl apply -f experiments/0027-multiplex-middleware-server/samples/apidef-widget.yaml
kubectl wait --for=condition=Ready apidefinition/widgets.aggexp.io --timeout=60s
kubectl get widgets
kubectl apply -f experiments/0027-multiplex-middleware-server/samples/widget-instance.yaml
kubectl get widgets

kubectl apply -f experiments/0027-multiplex-middleware-server/samples/apidef-gadget.yaml
kubectl wait --for=condition=Ready apidefinition/gadgets.gadgets.aggexp.io --timeout=60s
kubectl get gadgets

kubectl apply -f experiments/0027-multiplex-middleware-server/samples/apidef-sprocket.yaml
kubectl wait --for=condition=Ready apidefinition/sprockets.sprockets.aggexp.io --timeout=60s
kubectl get sprockets

# 8. deregister one, confirm clean teardown
kubectl delete apidefinition widgets.aggexp.io
sleep 3
kubectl get widgets   # expected: an error about the API group being gone
kubectl get gadgets   # still works
kubectl get sprockets # still works

# 9. SIGTERM the middleware, check for orphaned APIServices
kubectl -n aggexp-system delete pod -l app=aggexp --grace-period=30
kubectl get apiservices | grep aggexp || echo "no orphans"
```

## Status

complete

<!-- See FINDINGS/0027-multiplex-middleware-server.md for the full write-up. -->

## Decisions made

- **Kind cluster name `aggexp-0027`**, dedicated. No sharing with
  other in-flight experiments.
- **Backends use HTTP transport only** (per 0026's recommendation).
  One code base, three images, differ only in the baked-in
  resource-name + schema. Cheapest path to "three different AAs on
  different backends" without implementing two transport paths.
- **Multiplex middleware runs a SINGLE aggregated apiserver process**
  with `InstallAPIGroup` called once per registered APIDefinition
  at reconcile time. No mTLS. No metrics. No admission.
- **One APIService per group/version** (standard Kubernetes
  semantics). If two APIDefinitions share a group/version, the
  second is rejected with a status condition; they MUST differ in
  (group, version).
- **Deregistration strategy**: delete the APIService on the host
  cluster. The internal REST handler stays alive in our middleware
  (go-restful has no clean unregister path), but aggregation no
  longer reaches it. `kubectl get widgets` fails cleanly because
  discovery no longer advertises the group. This is a documented
  consequent; truly removing the internal handler would require
  either restarting the middleware or forking a custom
  go-restful. Trade-off taken for experiment-level scope.
- **Reconcile rate-limit**: `workqueue.DefaultTypedControllerRateLimiter`
  (5ms min, 1000s max). Arbitrary; we never retry hot enough for
  this to matter.
- **Resync period**: 60s. Same arbitrary choice as 0024.
- **APIDefinition deletion: no finalizer.** The reconciler observes
  the DELETE event directly and cleans up. A finalizer would be
  more robust for production (guaranteed cleanup even if the
  middleware is down) but is explicit scope-cut for this
  experiment. A consequent: if the middleware is down at the
  moment the APIDefinition is deleted, the APIService stays until
  the middleware restarts and its startup-reconcile notices the
  orphan.
- **Graceful shutdown grace period**: 25 seconds. Enough to: send
  delete for each APIService, wait for kube-apiserver to finish
  in-flight requests against us (generic apiserver's default drain
  is ~20s).
- **Schema source default is `backendJSONSchema`** (Track B from
  0023). `configEmbedded` is supported (reads `spec.schema`
  verbatim with Track B lift); `backendOpenAPI` is not supported in
  this experiment (would require grpc transport coupling).
- **Field manager for ResourceMetadata writes: `aggexp-multiplex`**.
  Distinct from 0024's `aggexp-middleware` so the two experiments
  don't fight if run on the same cluster (they shouldn't, but
  conservative).
- **OpenAPI served per-group at startup time**: we register one
  `OpenAPIDefinitions` callback spanning all currently-registered
  groups. When a new group arrives, the aggregation layer refreshes
  via its own OpenAPI aggregator (polls /openapi/v2 and /openapi/v3
  on our pod). The middleware rebuilds the openapi-definitions map
  on each reconcile.
- **APIService `caBundle` is stable** across all registered groups
  — same serving cert for all AAs from this middleware. Kube
  Aggregation supports per-APIService CA, but we don't need per-
  group isolation.
- **Nil-out pre-materialized OpenAPI `Definitions` cache at
  startup.** `genericapiserver.DefaultOpenAPIV3Config` eagerly
  materializes the `Definitions` map from `GetDefinitions` at
  construction time. Every subsequent
  `BuildOpenAPIDefinitionsForResources` inside `InstallAPIGroup`
  checks `if Definitions != nil` and bypasses the callback. For the
  multiplex's dynamic reconcile case this means new AAs' item
  schemas are never consulted and install fails with `cannot find
  model definition for .../scheme.Object`. Fix: immediately after
  `runtime/server.Config()` returns, we set
  `cfg.OpenAPIV3Config.Definitions = nil` and
  `cfg.OpenAPIConfig.Definitions = nil` so the closure is invoked
  every install. The cost is an extra full-map allocation per
  install; negligible at 1-100 AAs.

## Prerequisites

- kind cluster `aggexp-0027` (created via `hack/make-kind.sh
  aggexp-0027`).
- Serving cert at `deploy/certs/` (from `hack/gen-certs.sh`).
- The `ResourceMetadata` CRD from 0024 (re-applied here).

## What we're looking to learn

1. Does `genericapiserver.GenericAPIServer.InstallAPIGroup` work
   after `PrepareRun()` + `RunWithContext()` has started? If yes,
   is there observable latency or disruption?
2. What does the `kubectl get <kind>` latency look like when the
   middleware hosts 1, 2, 3 groups? (Expected: essentially
   identical — the groups are independent; this is a sanity
   check.)
3. What's the shape of the reconciler? How many goroutines? How
   big is the failure surface? Concrete LOC budget.
4. Does SIGTERM produce a clean teardown, or do we leak
   APIServices / in-flight requests?
5. Does deleting one APIDefinition break the other AAs' discovery
   or kubectl experience?
