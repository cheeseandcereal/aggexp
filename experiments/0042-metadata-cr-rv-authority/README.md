# Experiment 0042: metadata-cr-rv-authority

Unify the resourceVersion authority on the host metadata CR for the
0024 **stitched** storage model — KRM metadata on a cluster-scoped CR,
business body on the backend — and confirm that the host etcd's RV on
that CR is a sound single RV authority for the *stitched* object across
Get, List, and Watch, in a multi-replica deployment.

This is the foundational experiment of the multi-replica library
composition arc. 0043, 0044, and 0045 each build on the metadata-CR
core this experiment establishes.

## Status

complete

## Prior findings this builds on

- `FINDINGS/0024-metadata-crd-store.md` — the stitched store: business
  data (spec+status) on the backend; KRM metadata
  (uid/resourceVersion/managedFields/finalizers/ownerReferences/
  labels/annotations/deletionTimestamp) on a shared cluster-scoped
  `resourcemetadatas.aggexpmeta.aggexp.io` CRD, stitched onto every
  response.
- `FINDINGS/0025-push-backed-watch.md` — surfaced the
  resourceVersion-authority **split**: Get/List returned
  backend-supplied RVs, Watch returned middleware-counter RVs, which
  makes reflector relist-with-RV semantically inconsistent.
- `FINDINGS/0034-shared-watch-cross-replica.md` — established host-CRD
  RV as the cross-replica authority, but over a **whole-object**
  storage CRD (the entire object lives in the CRD), not the 0024
  stitched store where only metadata lives in the CR.
- The `rv-authority-unification` candidate (EXPERIMENTS.md, Watch and
  consistency semantics) — named this question but never closed it.

## Hypothesis

- **Watch and consistency semantics (primary).** In the 0024 stitched
  model, the host etcd resourceVersion of the per-object metadata CR
  can serve as the single RV authority for the whole stitched object.
  Stamped uniformly on Get/List/Watch, it resolves the 0025 split and
  preserves client-side resume-by-RV across replicas, because every
  replica observes the same monotonic etcd RV stream via its informer
  on the metadata CRD.
- **Storage independence (secondary).** The 0024 stitch (metadata in
  the CR, body on a separate backend) composes with the 0034
  cross-replica informer pattern without requiring the whole object to
  live in the CR.

## Hard load-bearing decision: RV authority

Every served object's `metadata.resourceVersion` is the host etcd RV of
its metadata CR — never a backend-supplied RV, never a per-replica
`atomic.Uint64` counter (the posture 0034 abandoned for whole-object
storage and this experiment abandons for the stitched store).

The stitch path overlays the metadata CR's RV onto the body returned by
the backend before the response leaves the adapter. List stamps
`ListMeta.resourceVersion` from the metadata store's high-water RV.
Watch events carry the metadata CR's RV at emission time. Per 0034, an
unknown RV on resume replays current list-state rather than returning
410 (extra ADDED events, never silently-missed events).

## Architecture

```
                       host kube-apiserver  ◄──► etcd
                                │
        resourcemetadatas.<metagroup>/v1  (cluster-scoped; metadata only)
                                │
            +-------------------+--------------------+
            |                   |                    |
       informer A           informer B           informer C
            |                   |                    |
       stitch + RV         stitch + RV          stitch + RV
            |                   |                    |
        AA pod 0            AA pod 1             AA pod 2
            │                   │                    │
            │  Backend.Get/List (business body; no RV, no metadata)
            ▼                   ▼                    ▼
                 in-memory Widget backend (per replica)
            |                   |                    |
            +---- Service "aggexp" (load-balanced) ---+
                                │
                       v1.<group> APIService → kube-apiserver aggregator → kubectl
```

Plus per-pod Services (`aggexp-0/1/2`) so the APIService can be pinned
at a single replica during cross-replica scenarios (the 0034 approach).

## What this is (files to create)

- `pkg/apis/<group>/{types.go,v1/...,install/install.go}` — a minimal
  `Widget` type (`WidgetSpec{Color,Size}`, `WidgetStatus{Phase}`). The
  shape is incidental; this experiment is about RV authority.
- `pkg/metastore/store.go` — the stitched metadata-CR store: dynamic
  client + dynamic informer on the metadata CRD; Get/List/Create/
  Update/Delete of the metadata Record; stitch(metadata, body); host-RV
  stamping. Adapt from `runtime/library/crdstore.go` (CRD-backed
  storage) and `experiments/0034-shared-watch-cross-replica/pkg/shared/
  rest.go` (host-RV, no counter). Duplicated into this experiment, not
  imported — the stitched (metadata-only) shape diverges from
  crdstore's whole-object converter. The substrate stays frozen.
- `pkg/backend/inmem.go` — an in-memory `Widget` body store (spec+status
  only; never sees metadata or RV).
- `pkg/server/server.go`, `cmd/aggexp-widgets/main.go` — wiring on
  `runtime/server` + `runtime/group`.
- `metadata-crd/crd.yaml` — the cluster-scoped metadata CRD (own group,
  see Decisions).
- `manifests/` — namespace, permissive RBAC (widgets + metadata CRD),
  StatefulSet (3 replicas) + headless Service + per-pod Services,
  APIService, a sample `widget.yaml`.
- `go.mod` (with `replace github.com/cheeseandcereal/aggexp => ../..`),
  `Dockerfile` (build context = repo root, per 0034), `hack/deploy.sh`,
  `hack/pin-replica.sh`.

## How to run

From the repo root:

```
./hack/gen-certs.sh
kind create cluster --name aggexp-0042
kubectl --context kind-aggexp-0042 create namespace aggexp-system
kubectl config use-context kind-aggexp-0042

# Build + load image, apply metadata CRD + base + experiment manifests,
# wait for the StatefulSet rollout.
./experiments/0042-metadata-cr-rv-authority/hack/deploy.sh
```

### Scenario 1 — RV is the metadata-CR RV on Get/List/Watch

```
kubectl apply -f experiments/0042-metadata-cr-rv-authority/manifests/widget.yaml
RV_GET=$(kubectl get widget w1 -o jsonpath='{.metadata.resourceVersion}')
CR=$(kubectl get resourcemetadatas -o name | grep w1)
RV_CR=$(kubectl get "$CR" -o jsonpath='{.metadata.resourceVersion}')
# Expect RV_GET == RV_CR (the stitched object's RV is the metadata CR's RV).
```

Confirm List's `ListMeta.resourceVersion` and Watch event RVs also
match the metadata-CR RV stream (no backend RVs, no counter values).

### Scenario 2 — cross-replica resume by RV

Open a watch pinned to replica 0; capture an RV; write via replica 1;
resume the watch against replica 1 with the captured RV. Confirm the
resume is honored (or replays current state per the 0034 contract — no
silent gaps). Use `hack/pin-replica.sh` to flip the APIService.

### Scenario 3 — multi-replica consistency

Write via one replica; confirm the object (with identical stitched RV)
is read back via every replica's informer cache.

### Scenario 4 — stitch overhead and propagation latency

Measure per-Get stitch overhead against the direct metadata-CR Get
baseline (0024 measured ~3–5 ms), and the cross-replica propagation
delay from `kubectl apply` returning to a watcher on another replica
observing the event (0034 measured single-digit ms).

### Cleanup

```
kind delete cluster --name aggexp-0042
```

## Decisions made

- **Served group:** `aggexp.io/v1`, resource `widgets` (namespace-scoped
  `Widget`, `WidgetSpec{Color,Size}` + `WidgetStatus{Phase}`).
- **Metadata CR group:** `widgetmeta.aggexp.io/v1`, cluster-scoped
  `ResourceMetadata` (plural `resourcemetadatas`). Distinct from the
  served group because an APIService claims an entire group/version.
- Metadata CR is **cluster-scoped**; the served object's namespace is a
  field on `spec.resourceRef` (per 0024). Record name format:
  `<group-with-dashes>.<resource>.<namespace-or-cluster>.<name>` with a
  sha256 fallback for over-length names.
- **DEVIATION from the original sketch — body is a SECOND shared CRD,
  not a per-replica in-memory map.** The README's architecture diagram
  showed an "in-memory Widget backend (per replica)". That does not
  compose with cross-replica reads: a write that lands on replica 0
  leaves replicas 1/2 with the metadata CR (via informer) but no body,
  so Get on a non-writer replica 404s. Confirmed empirically before
  the change. Resolution (chosen interactively): the body lives on a
  separate cluster-scoped CRD `widgetbodies.widgetbody.aggexp.io/v1`,
  read by every replica via its own informer. The body store is
  RV-BLIND: the body CR's resourceVersion is read but DISCARDED; only
  the metadata CR's RV is ever surfaced. This keeps the 0024 split
  (metadata in one CR, body in another, stitched on read) while making
  the body cross-replica consistent.
- **RV authority:** every served Widget's `metadata.resourceVersion`
  is the host etcd RV of its metadata CR. List stamps
  `ListMeta.resourceVersion` from the metastore high-water mark
  (informer-observed). Watch events carry the metadata CR's RV.
- **No 410 on unknown resume RV** — replay current list-state (the
  0034 contract). Verified: a watch resumed on replica 2 with an RV
  minted on replica 0 returns no 410 and delivers a write made through
  replica 1.
- Informer resync 30s; broadcaster size 100 (0002/0010/0034 defaults).
- Watch events are driven ONLY by the metadata-CR informer (that is
  where the RV authority lives). The body CRD's informer exists purely
  to make Get/List cross-replica consistent; it does NOT fan out watch
  events.
- **APIService `insecureSkipTLSVerify: true`.** When pinned to a
  per-pod Service (`aggexp-0/1/2`), the aggregation layer dials
  `aggexp-N.aggexp-system.svc`, which is not in the lab serving cert's
  SAN list (only `aggexp.aggexp-system.svc`). Rather than regenerate
  certs with per-pod SANs (the 0034 approach), 0042 skips TLS
  verification on the aggregation hop. Lab convenience, not a security
  posture.
- Body CR name format: `body.<namespace-or-cluster>.<name>` with a
  sha256 fallback.


## Prerequisites

- kind cluster `aggexp-0042`. Do **not** touch the default `aggexp`
  cluster used by other experiments.
- Serving cert from `hack/gen-certs.sh`.
- No external systems or secrets (in-memory backend).

## What we're looking to learn

- **Watch and consistency semantics.** Is the host metadata-CR RV a
  sound single authority for the *stitched* object? Does it hold the
  cross-replica resume-by-RV contract that the 0025 split broke?
- **Storage independence.** Does the 0024 stitch compose with the 0034
  cross-replica informer pattern without pulling the body into the CR?

## Expected FINDINGS shape

- **Fundamental:** whether RV-authority unification on the metadata CR
  generalizes to the stitched model (this closes, or reframes, the
  `rv-authority-unification` candidate and the 0025 split).
- **Consequent:** measured stitch overhead and propagation latency,
  informer-resync behavior, and any kube-apiserver-version-specific
  resume quirks — real but tied to this lab's environment.
