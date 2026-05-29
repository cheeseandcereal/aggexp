# Experiment 0045: read-path-reconcile-amplification

Treat the backend as the **source of truth for object existence**:
reconcile the 0024 metadata store against the backend *inline* on every
Get and List (adopt unknown backend objects, collect records whose
backend object is gone), rather than only on 0028's periodic sweep.
Then measure the read amplification this causes.

Builds on the metadata-CR core from 0042.

## Status

in-progress

<!-- valid values: in-progress, complete, abandoned -->
<!-- Scaffolded brief: hypothesis + run plan written; implementation
     pending. Copy the 0042 metastore core into this experiment first. -->

## Prior findings this builds on

- `FINDINGS/0024-metadata-crd-store.md` — the stitched store.
- `FINDINGS/0028-metadata-store-gc.md` — the GC reconciler: a periodic
  sweep diffs metadata records against `Backend.List` and deletes
  orphans, with a `minAge` grace window. It also surfaced the
  **tolerant-Get sharp edge**: a finalizer-protected record whose
  backend object is gone makes the stitched Get 404 through the absent
  backend, so operators had to edit the metadata CR directly.
- Experiment 0042 — the metadata-CR core.

## Hypothesis

- **Storage independence (primary); resource modeling freedom
  (secondary).** If the backend is the source of truth for existence,
  the read paths reconcile inline:
  - **Get:** always query the backend. Backend has it, no record →
    synthesize a record (adopt) and return the stitched object. Backend
    404s, record exists → collect the record (GC, subject to a `minAge`
    grace window) and return 404. No "tolerant-Get": a backend 404 is a
    404 regardless of finalizers, removing the 0028 sharp edge.
  - **List:** `Backend.List` is authoritative; adopt unknown backend
    objects and collect orphan records inline (on the initial page),
    then stitch.

  The cost is **read amplification**: every Get reaches the backend
  (no store-miss short-circuit 404), and a flood of Gets for
  non-existent names becomes a flood of backend calls. The question is
  whether this is acceptable, or whether a negative / short-TTL
  existence cache is needed. A secondary cost is **adoption noise** on
  a shared backend (every foreign backend object surfaces as a
  resource) — mitigated by an adoption toggle and/or backend-side
  filtering in `List`.

## Hard load-bearing decision

Reads observe the backend directly; the store is an overlay, not an
independent inventory. Adoption and GC are independently toggleable
(both default on) and run inline on Get/List as well as on the periodic
sweep. There is no tolerant-Get path.

## Architecture

```
Get(ns,name):
   record := store.Get(ref)         # a miss is NOT a 404 by itself
   obj, err := Backend.Get(ns,name) # backend is source of truth
   ├─ found, record present → stitch + return
   ├─ found, no record      → adopt (synthesize record) → stitch + return
   └─ 404                    → collect record (minAge guard) → return 404

List(ns):
   backendObjs := Backend.List(ns)  # authoritative set
   records     := store.List(...)
   reconcile (initial page): adopt (backendObjs − records),
                             collect (records − backendObjs, minAge)
   snapshot store RV after reconcile → sort → page → stitch
```

## What this is (files to create)

- Copy the 0042 `pkg/metastore`, `pkg/apis`, `pkg/server`, `cmd/`,
  metadata CRD, and manifests.
- `pkg/backend/inmem.go` — an in-memory backend whose object set can be
  mutated **out of band** (objects created/deleted without going
  through the AA), so adoption and collection on read are observable.
- Read-path reconcile in the Get/List adapter: inline adopt/collect
  with a `minAge` guard; adoption and GC toggles.
- A periodic sweep too (the 0028 shape), to confirm the inline path and
  the sweep agree.
- Instrumentation: backend call counters (Get/List), adopt/collect
  counters, to measure amplification.
- Optional: a negative/short-TTL existence cache behind a flag, to
  measure its effect on 404-heavy load.
- `manifests/` — single replica is fine here (this is about read
  paths, not cross-replica); a shared-backend scenario uses a second
  writer.

## How to run

```
./hack/gen-certs.sh
kind create cluster --name aggexp-0045
kubectl --context kind-aggexp-0045 create namespace aggexp-system
kubectl config use-context kind-aggexp-0045
./experiments/0045-read-path-reconcile-amplification/hack/deploy.sh
```

### Scenario 1 — adopt on read

Create an object directly in the backend (out of band). `kubectl get`
it: confirm a metadata record is synthesized inline and the stitched
object is returned. Confirm it then appears in `kubectl get` list.

### Scenario 2 — collect on read

Delete a backend object out of band while its metadata record remains.
`kubectl get` it: confirm a 404 and that the orphan record is collected
inline (subject to `minAge`). Confirm a record younger than `minAge` is
**not** collected (guards the freshly-created race).

### Scenario 3 — tolerant-Get is gone

Give a record a finalizer, then remove its backend object out of band.
Confirm Get returns 404 (not a record-only object) and the record is
collected — finalizers do not block collection once the backend
confirms absence. (Contrast with the 0028 sharp edge.)

### Scenario 4 — read amplification

Drive a flood of Gets for random non-existent names and record backend
`Get` call count (1:1 with requests by default). Enable the negative
cache and re-measure. Drive high-QPS Gets for existing names and record
backend load.

### Scenario 5 — shared-backend adoption noise

With adoption **on**, create many foreign objects in the backend;
confirm they all surface as resources (noise). With adoption **off**,
confirm Get returns 404 for unknown backend objects and List omits
them; confirm backend-side `List` filtering as the alternative.

### Cleanup

```
kind delete cluster --name aggexp-0045
```

## Decisions made

- `minAge` grace window 30s (0028 default).
- Adoption and GC both default on, each independently toggleable; the
  toggle applies to BOTH the inline read path and the periodic sweep.
- Negative-cache is behind a flag, default off, so the un-cached
  amplification cost is measured first.
- Adopted objects with no namespace land in `default` (a convention to
  record if it bites).

## Prerequisites

- kind cluster `aggexp-0045` (not the default `aggexp`).
- Serving cert from `hack/gen-certs.sh`.
- The 0042 metastore core copied in. No external secrets.

## What we're looking to learn

- **Storage independence.** Does treating the backend as the source of
  truth for existence (inline reconcile on read) cleanly remove the
  0028 tolerant-Get sharp edge? What is the read-amplification cost, and
  is a negative/short-TTL cache warranted? How noisy is adoption on a
  shared backend, and is the toggle + backend-side filtering enough?

## Expected FINDINGS shape

- **Fundamental:** whether backend-as-source-of-truth-for-existence is
  a clean model (it resolves the 0028 sharp edge) or trades one sharp
  edge for an amplification problem.
- **Consequent:** measured backend-call amplification under 404-heavy
  and high-QPS load, and the negative-cache's effect — tied to this
  backend and environment.
