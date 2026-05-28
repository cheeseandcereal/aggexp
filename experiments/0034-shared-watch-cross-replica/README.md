# Experiment 0034: shared-watch-cross-replica

A multi-replica library-mode aggregated apiserver where every replica
watches a shared CRD store on the host kube-apiserver via an informer,
and re-broadcasts events to its own watch clients. Replicas don't
communicate directly — kube-apiserver's etcd is the synchronization
point. The hypothesis is that **load-balanced kubectl watch streams
see all events even when writes hit a different replica than the one
serving the watch**.

## Hypothesis

- **Watch and consistency semantics (primary).** A multi-replica AA
  can serve consistent watch streams without any in-AA coordination,
  by letting all replicas observe the same etcd-assigned
  resourceVersion stream from a shared backing CRD via informers.
  Watch event ordering is preserved because etcd assigns a monotonic
  RV to every write.
- **Storage independence (secondary).** Reuses the
  `0010-etcd-crd-facade-with-ssa` "CRD-as-storage" pattern but with
  N replicas of the AA all pointing at the same CRD via
  shared informers (see `FINDINGS/0024` for the informer-on-CRD
  pattern).

## Hard load-bearing decision: RV authority

Per replica, the substrate's `runtime/storage.adapter.go` stamps a
local `atomic.Uint64` as resourceVersion on every emitted event.
That counter starts at 1 on each replica's process boot — so:

- replica A: write -> RV=42 (its own counter)
- replica B: same write observed via informer -> RV=17 (B's counter)

A client whose watch was registered against replica A and then
reconnects against replica B with `?resourceVersion=42` would either
get a 410 Gone or, worse, silently skip events because B's counter
is "behind".

This experiment **abandons the per-replica counter** and uses the
host CRD's RV — which is a single etcd-assigned monotonic stream
every replica observes via its informer — as the RV authority.
Implementation: `pkg/shared/rest.go` is a parallel `rest.Storage`
implementation that does NOT call `stampRV` on emitted events; the
RV stamped by kube-apiserver's CRD machinery flows through unchanged.

## Architecture

```
                         host kube-apiserver
                                |
                  widgetstorages.aggexpstorage.aggexp.io/v1
                                |
            +-------------------+--------------------+
            |                   |                    |
       informer A           informer B           informer C
            |                   |                    |
       broadcaster A      broadcaster B        broadcaster C
            |                   |                    |
            v                   v                    v
       AA pod 0            AA pod 1             AA pod 2
        |                   |                    |
        +---- Service "aggexp" (load-balanced) ---+
                                |
                          v1.aggexp.io APIService
                                |
                          kube-apiserver aggregator
                                |
                              kubectl
```

Plus per-pod Services (`aggexp-0`, `aggexp-1`, `aggexp-2`) so we can
re-point the APIService at a single replica for replica-pinning
during test scenarios.

## What this is

- `pkg/apis/aggexp/{types.go,install/install.go,v1/...}` — Widget
  internal + v1 types. `WidgetSpec{Color, Size}` — minimal shape;
  this experiment is about watch, not facade transformations.
- `pkg/crdbackend/backend.go` — `runtime/storage.WritableBackend`
  whose Get/List read from a shared dynamic informer cache and whose
  writes go to the host CRD. The informer event handler forwards to
  an `EventSink` that bypasses the substrate's RV stamping.
- `pkg/shared/rest.go` — custom `rest.Storage` that uses host-CRD
  RVs instead of a per-replica counter. The Watch/List/Get/CRUD path
  mirrors `runtime/storage/adapter.go` but without `stampRV`.
- `pkg/server/server.go`, `cmd/aggexp-widgets/main.go` — substrate
  wiring matching the 0010 pattern.
- `manifests/`:
  - `00-permissive-rbac.yaml` — RBAC on widgets + on backing CRD.
  - `02-namespace.yaml` — `aggexp-widgets` namespace where Widget
    resources (and their CRD rows) live.
  - `05-crd.yaml` — namespace-scoped WidgetStorage CRD.
  - `30-aggexp-statefulset.yaml` — StatefulSet (3 replicas) +
    headless Service (`aggexp-pod`) + per-pod Services
    (`aggexp-0` / `aggexp-1` / `aggexp-2`).
  - `widget.yaml` — sample Widget.
- `hack/deploy-multi.sh` — convenience deploy: applies base
  manifests, deletes the Deployment from the default base manifests
  (so the StatefulSet can claim the name), builds and loads the
  image, applies experiment manifests, waits for rollout.
- `hack/pin-replica.sh` — flip the `v1.aggexp.io` APIService to
  route all kubectl traffic at a specific replica's per-pod Service
  (or the load-balanced `aggexp` Service).

## How to run

From the repo root:

```
./hack/gen-certs.sh
kind create cluster --name aggexp-0034
kubectl --context kind-aggexp-0034 create namespace aggexp-system

kubectl config use-context kind-aggexp-0034

# Deploy base + experiment manifests, build & load image, wait for
# rollout.
./experiments/0034-shared-watch-cross-replica/hack/deploy-multi.sh
```

### Scenario 1 — basic CRUD across the load-balanced Service

```
kubectl apply -f experiments/0034-shared-watch-cross-replica/manifests/widget.yaml
kubectl get widgets -n aggexp-widgets
kubectl get widgetstorages -n aggexp-widgets
```

Confirm a `WidgetStorage` row exists on the host CRD; confirm the
exposed `Widget` reads back identical content.

### Scenario 2 — cross-replica watch propagation

In one shell, pin to replica 0 and start a watch:

```
./experiments/0034-shared-watch-cross-replica/hack/pin-replica.sh aggexp-0
kubectl get widgets -n aggexp-widgets -w -o yaml
```

In a second shell, pin to replica 1 and create a widget:

```
./experiments/0034-shared-watch-cross-replica/hack/pin-replica.sh aggexp-1
kubectl create -n aggexp-widgets -f - <<YAML
apiVersion: aggexp.io/v1
kind: Widget
metadata: {name: cross-1}
spec: {color: red, size: 3}
YAML
```

The pin in shell 2 affects ALL kubectl traffic (the APIService now
points at aggexp-1's Service); the watch in shell 1 was opened
against aggexp-0 and is still streaming via that pre-existing
connection through kube-apiserver. Confirm shell 1 sees the ADDED
event.

End-to-end latency: the timestamp delta from `kubectl create`
returning to the watcher receiving the event in shell 1.

### Scenario 3 — watch stream survives replica restart

```
./experiments/0034-shared-watch-cross-replica/hack/pin-replica.sh lb
kubectl get widgets -n aggexp-widgets -w &  # any replica
WPID=$!
kubectl -n aggexp-system delete pod aggexp-1
# wait for the pod to come back up (the StatefulSet recreates it)
kubectl -n aggexp-system rollout status statefulset/aggexp
kill ${WPID}
```

Observe: does kubectl reconnect cleanly? Does it 410 (forcing a
re-list)?

### Scenario 4 — resume against different replica with explicit RV

```
RV=$(kubectl get widgets -n aggexp-widgets -o jsonpath='{.metadata.resourceVersion}')
./experiments/0034-shared-watch-cross-replica/hack/pin-replica.sh aggexp-0
kubectl get --raw "/apis/aggexp.io/v1/namespaces/aggexp-widgets/widgets?watch=true&resourceVersion=${RV}" &
WPID=$!
sleep 2
./experiments/0034-shared-watch-cross-replica/hack/pin-replica.sh aggexp-2
kubectl create -n aggexp-widgets -f - <<YAML
apiVersion: aggexp.io/v1
kind: Widget
metadata: {name: resume-test}
spec: {color: green, size: 5}
YAML
sleep 3
kill ${WPID}
```

Probes whether RV from one replica resumes cleanly against a
different replica.

### Scenario 5 — list+watch consistency

```
kubectl get widgets -n aggexp-widgets
kubectl get widgets -n aggexp-widgets -w &
WPID=$!
sleep 2
kill ${WPID}
```

Watch's initial state replay must include the same widgets List
returned (no duplicates, no missing).

### Scenario 6 — concurrent writes

In two shells:

```
# shell A
./experiments/0034-shared-watch-cross-replica/hack/pin-replica.sh aggexp-0
for i in $(seq 1 10); do
  kubectl create -n aggexp-widgets -f - <<YAML
apiVersion: aggexp.io/v1
kind: Widget
metadata: {name: a-${i}}
spec: {color: red, size: ${i}}
YAML
done
```

```
# shell B
./experiments/0034-shared-watch-cross-replica/hack/pin-replica.sh aggexp-2
for i in $(seq 1 10); do
  kubectl create -n aggexp-widgets -f - <<YAML
apiVersion: aggexp.io/v1
kind: Widget
metadata: {name: b-${i}}
spec: {color: blue, size: ${i}}
YAML
done
```

A watch on any replica during this window must see all 20 events.

### Inspect logs

```
kubectl -n aggexp-system logs aggexp-0 | grep -E 'create|update|delete|informer-event' | head -30
kubectl -n aggexp-system logs aggexp-1 | grep -E 'create|update|delete|informer-event' | head -30
kubectl -n aggexp-system logs aggexp-2 | grep -E 'create|update|delete|informer-event' | head -30
```

The `replica=` field on each log line ties events to specific pods.

### Cleanup

```
kind delete cluster --name aggexp-0034
```

## Status

complete

<!-- See FINDINGS/0034-shared-watch-cross-replica.md for results. -->

## Decisions made

- **RV authority = host CRD RV.** The single load-bearing decision.
  See `pkg/shared/rest.go`'s package doc for the contract. We do
  NOT 410-Gone on resume with an unknown RV; we replay the current
  list-state instead. (Trade-off: a stale resume produces extra
  ADDED events but never silently misses events.)
- **Storage CRD lives in `aggexpstorage.aggexp.io/v1`**, distinct
  from `aggexp.io/v1` because an APIService claims an entire
  (group, version) so the AA's exposed group cannot also host a
  CRD. The task brief specified `widgetstorages.aggexp.io/v1` but
  that conflicts with the AA's APIService claim on `aggexp.io/v1`;
  this experiment follows 0010's convention instead.
- **Namespace-scoped Widget**, namespace `aggexp-widgets`. The task
  brief specifies namespace-scoped storage; this exposes the
  exposed resource consistently.
- **3 replicas** (StatefulSet). Stable pod names enable per-pod
  Services for the replica-pinning test approach.
- **Per-pod Services + flip-the-APIService approach** for
  replica-pinning during scenarios. APIServices can only point at
  one Service, so to pin we re-target it; switch back to `aggexp`
  to re-enable load-balancing. `hack/pin-replica.sh` automates the
  flip.
- **Informer resync period: 30s**, arbitrary. A resync re-fires
  every cached object as a MODIFIED event and is useful for
  noticing missed events from the host kube-apiserver, but adds
  CPU. Adjust by editing `pkg/server/server.go`.
- **Broadcaster size: 100** (default; matches 0002/0010).
- **Each replica's informer is independent** (each replica runs
  its own `dynamicinformer.NewFilteredDynamicSharedInformerFactory`).
  They share nothing process-to-process; their only synchronization
  is the host CRD's etcd RV stream.
- **No locking** — CRD-level optimistic concurrency (kube-apiserver
  rejects updates with stale resourceVersion) handles concurrent
  writes from different replicas. 0032/0033 separately probe
  Lease- and CRD-CAS-based locking; this experiment deliberately
  doesn't compose with them.
- **No initial-events-end BOOKMARK** — that's 0040's scope. The v1
  library mode doesn't emit it (per 0011/0025); whatever the v1
  gap is here is what 0034 inherits.

## Prerequisites

- kind cluster `aggexp-0034`. Do **not** touch `aggexp` (the default
  cluster used by other experiments).
- Serving cert generated by `hack/gen-certs.sh`.
- Base manifests applied via `hack/deploy.sh deploy/manifests`.
  The experiment-specific deploy script handles this for you.

## What we're looking to learn

- **Watch and consistency semantics.** Cross-replica event
  propagation: does it work? With what latency? RV semantics
  across replicas: do they preserve client-side resume contracts?
- **Storage independence (secondary).** Does the
  shared-informer-on-shared-CRD pattern compose cleanly with the
  library mode, or does it require substrate-level changes?
