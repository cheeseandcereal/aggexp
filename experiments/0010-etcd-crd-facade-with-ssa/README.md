# Experiment 0010: etcd-crd-facade-with-ssa

An aggregated API server that serves `widgets.aggexp.io/v1` by
**forwarding every storage operation to a CRD** on the host
kube-apiserver. The AA holds no state of its own; the CRD row in the
host cluster's etcd carries all ObjectMeta bookkeeping that a
fully-stateless AA (see `0009-ack-aggregated-s3`) cannot hold:
managedFields, finalizers, ownerReferences, labels, annotations.

Combines two ideas:

1. **No etcd in the AA.** The AA process has no etcd client, no
   `genericregistry.Store`. Storage is a `dynamic.Interface` against
   a CRD served on the host cluster.
2. **A facade with real persistence.** Because the CRD naturally
   stores ObjectMeta, the library-layer features `0009` lost
   (SSA ownership tracking, finalizers, owner references) work
   again — at the cost of one extra kube-apiserver hop per request.

The facade also demonstrates two transformations to prove the
boundary is real:

- **Field rename.** Exposed `spec.counter` ⇄ storage `spec.storedCounter`.
- **Identity-aware filter.** When the caller's user name starts with
  `alice-`, `spec.tags` on the response is filtered to only include
  keys starting with `alice-`. Proves identity reaches the backend.

## Hypothesis

- **Storage independence (primary).** A CRD on the host cluster is a
  viable "backing store" for an aggregated API. Every library feature
  that assumes persistence — SSA, finalizers, owner references —
  should work because the CRD is itself stored in etcd via the host
  kube-apiserver.
- **Per-request authorization (secondary).** The identity-aware filter
  exercises `user.Info` availability inside a `runtime/storage.Backend`
  and demonstrates that facade transformations can depend on request
  identity.
- **Watch fan-out.** A dynamic watch on the CRD, forwarded through
  `runtime/storage.Publisher`, should drive the AA's watch stream.
  Editing the underlying `WidgetStorage` directly (bypassing the AA)
  must surface as a `MODIFIED` event through the AA's watch.

## What this is

- `pkg/apis/aggexp/{types.go,install/install.go,v1/...}` — internal
  and v1 Widget types + conversion + scheme install.
- `pkg/crdbackend/backend.go` — `runtime/storage.WritableBackend`
  whose Get/List/Create/Update/Delete/Watch all go through
  `k8s.io/client-go/dynamic` against
  `widgetstorages.aggexpstorage.aggexp.io/v1`.
- `pkg/server/server.go`, `pkg/apiserver/apiserver.go`,
  `cmd/aggexp-widgets/main.go` — substrate wiring matching the 0007
  / 0009 pattern.
- `manifests/`:
  - `00-permissive-rbac.yaml` — ClusterRole on widgets.aggexp.io for
    system:authenticated, plus CRUD on widgetstorages.aggexpstorage.aggexp.io
    for the AA's ServiceAccount.
  - `05-crd.yaml` — CRD definition for WidgetStorage.
  - `30-aggexp-deployment-override.yaml` — Deployment for the AA.
  - `widget.yaml` — sample Widget used in the test scenarios.

## How to run

From the repo root:

```
./hack/gen-certs.sh
kind create cluster --name aggexp-etcd-crd
kubectl --context kind-aggexp-etcd-crd create namespace aggexp-system

# Base manifests (SA + auth-delegator + APIService + Service + base Deployment)
kubectl config use-context kind-aggexp-etcd-crd
./hack/deploy.sh deploy/manifests

# Build & load the experiment image (build context MUST be repo root)
docker build -t aggexp-widgets:dev \
  -f experiments/0010-etcd-crd-facade-with-ssa/Dockerfile .
kind load docker-image aggexp-widgets:dev --name aggexp-etcd-crd

# Experiment-specific manifests: CRD + permissive RBAC + Deployment override
AGGEXP_IMAGE=aggexp-widgets:dev \
  ./hack/deploy.sh experiments/0010-etcd-crd-facade-with-ssa/manifests

kubectl -n aggexp-system rollout status deploy/aggexp
rm -rf ~/.kube/cache/discovery/

# Scenarios:

# 1. Create via kubectl apply; confirm a WidgetStorage appears.
kubectl apply -f experiments/0010-etcd-crd-facade-with-ssa/manifests/widget.yaml
kubectl get widgets
kubectl get widgetstorages.aggexpstorage.aggexp.io

# 2. Server-side apply; managedFields should populate.
kubectl apply --server-side \
  --field-manager=lab-manager \
  -f experiments/0010-etcd-crd-facade-with-ssa/manifests/widget.yaml
kubectl get --raw /apis/aggexp.io/v1/widgets/sample | jq .metadata.managedFields

# 3. Finalizer persistence across GETs + blocked delete.
kubectl patch widget sample --type=merge \
  -p '{"metadata":{"finalizers":["lab.aggexp.io/test"]}}'
kubectl get widget sample -o json | jq '.metadata.finalizers'
kubectl delete widget sample --wait=false   # enters pending state
kubectl get widget sample -o json | jq '.metadata.deletionTimestamp'
kubectl patch widget sample --type=merge -p '{"metadata":{"finalizers":[]}}'

# 4. Owner reference survives round trip.
kubectl apply -f experiments/0010-etcd-crd-facade-with-ssa/manifests/widget.yaml
CM_UID=$(kubectl -n default create cm refme --dry-run=client -o json | jq -r .metadata.uid || true)
kubectl -n default apply -f - <<YAML
apiVersion: v1
kind: ConfigMap
metadata: {name: refme}
YAML
CM_UID=$(kubectl -n default get cm refme -o jsonpath='{.metadata.uid}')
kubectl patch widget sample --type=merge -p "{\"metadata\":{\"ownerReferences\":[{\"apiVersion\":\"v1\",\"kind\":\"ConfigMap\",\"name\":\"refme\",\"uid\":\"${CM_UID}\"}]}}"
kubectl get widget sample -o json | jq '.metadata.ownerReferences'

# 5. Identity-aware filter.
kubectl get widget sample -o json | jq '.spec.tags'            # all tags
kubectl --as alice-lab get widget sample -o json | jq '.spec.tags'  # filtered

# 6. Watch fan-out via dynamic watch on the CRD.
kubectl get widgets -w &
WATCH_PID=$!
sleep 2
kubectl patch widgetstorage sample --type=merge -p '{"spec":{"description":"poked via the backing CRD"}}'
sleep 3
kill $WATCH_PID

# Cleanup
kind delete cluster --name aggexp-etcd-crd
```

## Status

complete

<!-- See FINDINGS/0010-etcd-crd-facade-with-ssa.md for results. -->

## Decisions made

- **Different API group for storage.** The backing CRD lives under
  `aggexpstorage.aggexp.io/v1` (plural `widgetstorages`) so it doesn't
  collide with the AA's exposed `widgets.aggexp.io/v1`. Both groups
  run on the same host kube-apiserver.
- **Cluster-scoped Widget.** Matches every prior experiment in this
  repo; namespace scoping is a separate probe.
- **ObjectMeta round-trips via `runtime.DefaultUnstructuredConverter`.**
  Rather than hand-mapping managedFields/finalizers/ownerReferences
  one field at a time, we serialize the whole ObjectMeta through the
  unstructured converter. This is the shortest path to "every meta
  field survives the trip" and the whole point of the experiment.
- **No status subresource on the CRD.** The AA writes the full object
  in one update. Adding `/status` is a later experiment.
- **Watch retry backoff: 2s fixed.** Arbitrary; a production facade
  would want exponential backoff + jitter.
- **Field-rename demo chose `storedCounter`.** Chosen to be slightly
  ugly so no one mistakes it for the exposed name.
- **Identity filter is alice-prefix.** Arbitrary lab demonstration;
  the point is "identity reaches the backend", not a realistic policy.
- **Dynamic client built off loopback or CoreAPI client config.**
  `runtime/server.Options.Config` already wires both; we try
  CoreAPI's ClientConfig first (populated by delegated authn/authz
  setup) and fall back to LoopbackClientConfig. In-cluster the
  loopback config reaches the host apiserver via the AA's SA token.

## Prerequisites

- kind cluster `aggexp-etcd-crd`.
- Serving cert generated by `hack/gen-certs.sh`.
- Base manifests applied via `hack/deploy.sh deploy/manifests`.

## What we're looking to learn

- **Storage independence.** Can a CRD-on-host act as the backing
  store for an aggregated API without introducing a local etcd?
- **Does SSA managedFields survive?** This is the specific gap
  `0009` surfaced. If yes, "CRD-as-storage" is a pragmatic middle
  ground between "no etcd, SSA broken" and "full etcd in the AA."
- **Do finalizers work?** Library behavior around
  `metadata.finalizers` expects a persistence layer that holds a
  row in a pending-delete state. The CRD's row naturally does that
  via kube-apiserver's own finalizer machinery.
- **Per-request latency cost.** Every op is now one extra hop
  (AA → kube-apiserver → etcd) versus (AA → etcd). Measurable.
- **Does the dynamic-watch fan-out work end-to-end?** Editing the
  backing CRD directly should surface through the AA's watch.
