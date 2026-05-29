# Experiment 0048: library-multireplica-vertical-slice

Capstone of the multi-replica library composition arc. Compose every
mechanism the arc validated in isolation — 0042's host-CR RV authority,
0043's embedded lock + emission filtering, 0044's per-watcher watch,
0045's read-path reconcile, and 0046's generated types — into a single
multi-replica library-mode aggregated apiserver on an in-memory
backend, and exercise it against the real Kubernetes ecosystem.

The integration question: do the fragments compose without mutual
interference? (The same question 0031 and 0041 asked of their arcs.)

## Status

complete

<!-- valid values: in-progress, complete, abandoned -->
<!-- Composed 0042-0045 onto the 0046-generated widgets.aggexp.io/v1
     Widget; ran all six scenarios against a 3-replica StatefulSet on
     kind aggexp-0048; ran the compat scoreboard at the phase boundary.
     See FINDINGS/0048. -->

## Prior findings this builds on

- `FINDINGS/0041-library-promotion.md` — the prior composition: nine
  single-replica capabilities consolidated into `runtime/library`
  without mutual interference. This experiment is the multi-replica
  analogue.
- `FINDINGS/0034-shared-watch-cross-replica.md` — the multi-replica
  deployment shape (StatefulSet, per-pod Services, shared informer).
- Experiments 0042–0046 — the mechanisms being composed.

## Hypothesis

- **Wire protocol fidelity (primary).** A single multi-replica
  library-mode AA can compose: the host-CR RV authority (0042), the
  embedded lock with emission filtering and pre-acquire OCC (0043),
  per-watcher identity-carrying watch (0044), backend-as-source-of-
  truth read-path reconcile (0045), and OpenAPI-first generated types
  (0046) — and present a resource indistinguishable from a built-in to
  kubectl, client-go, and controller-runtime, with no mutual
  interference between the mechanisms.

## Hard load-bearing decision

This is an integration experiment; it introduces no new mechanism. It
copies/wires the 0042–0045 code and consumes the 0046-generated types,
on an in-memory backend, in a 3-replica deployment. It is a **phase
boundary**: on completion, run `hack/test-compat.sh` and commit the
dated `FINDINGS/compat/` record (per AGENTS.md / ETHOS).

## Architecture

```
generated Widget types (0046)
        │
shared body CRD backend ── per-watcher watch (0044)
        │                     read-path reconcile (0045)
metadata-CR store (0042) ── embedded lock + emission filter + OCC (0043)
        │
3-replica StatefulSet, host metadata CRD = RV authority (0034 shape)
        │
v1.widgets.aggexp.io APIService → kube-apiserver aggregator
        │
   kubectl  │  client-go reflector/informer  │  controller-runtime manager
```

The body lives on a SHARED cluster-scoped CRD
(`widgetbodies.widgetbody.aggexp.io`), not an in-memory map: 0042
established that a per-replica in-memory body breaks cross-replica reads
(a write on one replica is invisible on the others). "In-memory backend"
in the scaffold brief was superseded by that finding.


## What this is (files to create)

- A composed AA binary wiring 0042 (`metastore`) + 0043 (`locking` +
  emission filter) + 0044 (`watch/perwatcher`) + 0045 (read-path
  reconcile) on `runtime/server` + `runtime/group`, over an in-memory
  backend, using the **0046-generated** `Widget` types and OpenAPI.
- `cmd/aggexp-widgets/main.go`, `pkg/server/server.go`.
- `manifests/` — namespace, RBAC (widgets + metadata CRD), 3-replica
  StatefulSet + Service + per-pod Services, APIService, sample widget.
- `client-go-probe/` — a small reflector/informer program.
- `controller-runtime-probe/` — a tiny manager + reconcile loop over
  the served resource (controller-runtime is in-scope here because
  this experiment specifically probes its compatibility).
- `hack/deploy.sh`, `go.mod`, `Dockerfile`.

## How to run

```
./hack/gen-certs.sh
kind create cluster --name aggexp-0048
kubectl --context kind-aggexp-0048 create namespace aggexp-system
kubectl config use-context kind-aggexp-0048
./experiments/0048-library-multireplica-vertical-slice/hack/deploy.sh
```

### Scenario 1 — kubectl round-trip

`kubectl api-resources` lists the group; `get`, `get -o yaml`,
`create`, `apply`, `apply --server-side`, `explain`, `get -w`,
`wait --for=jsonpath`, `delete` all behave as for a built-in.

### Scenario 2 — multi-replica writes (lock)

Concurrent writes to one object across replicas: the embedded CAS lock
serializes them; losers get 409; watchers see one MODIFIED per logical
change (emission filtering holds under real load).

### Scenario 3 — pod restart under watch

Open `kubectl get widgets -w`; delete a replica pod; confirm the watch
reconnects via the Service and UIDs/RVs persist (no spurious
delete/add; the metadata CR persists identity across restarts).

### Scenario 4 — client-go reflector/informer

Run the reflector probe; confirm list+watch, resync, and resume-by-RV
behave (the 0042 RV authority + 0044 per-watcher emission).

### Scenario 5 — controller-runtime

Run the manager probe with a reconcile loop and a finalizer; confirm
caches, reconciles, and finalizer lifecycle work against the AA.

### Scenario 6 — compat scoreboard (phase boundary)

```
./hack/test-compat.sh --group <group> --version v1 --resource widgets
# commit the resulting FINDINGS/compat/<date>.md
```

### Cleanup

```
kind delete cluster --name aggexp-0048
```

## Decisions made

- **Served group is `widgets.aggexp.io`** (the 0046-generated group),
  distinct from the metadata group (`widgetmeta.aggexp.io`) and the
  body group (`widgetbody.aggexp.io`). An APIService claims a whole
  group/version, so the two backing CRDs must live in their own groups.
- **The Widget types are the 0046-GENERATED package**, copied verbatim
  into `pkg/apis/widgets/v1/` (the openapi-def keys rewritten to this
  module's import path so `NewDefinitionNamer` matches). No kin-openapi
  dependency is pulled in — only the generated output, which has none.
- **Watch mode: push** (per 0044). The shared body informer is the one
  upstream stream fanned out per-watcher (the internal multiplex), so
  push is cheap here. `--shared-poll` and `--watch-mode=poll` remain
  available.
- **Read-path reconcile (0045) stays ON** (`--adopt=true --gc=true`).
  It coexists with the lock and per-watcher watch; measured
  `getAmplification = 1.0` (every served Get is one backend
  authoritative read), confirming 0045's 1:1 amplification holds under
  the full composition.
- **Owner is a backend-only authz tag.** The 0046-generated Widget has
  no owner field, so the per-user authz owner (0044) is server-stamped
  onto the body CR and never surfaced on the served Widget — the
  capstone keeps the generated type pristine.
- **`spec.coordinates` ($ref nested object) is omitted from the sample.**
  The generated bare-`$ref` schema breaks the create strict decoder and
  the SSA typed-converter under this composition
  (`.spec.coordinates.true: field not declared in schema`) — see the
  FINDINGS. Scalar/enum/map/array fields compose cleanly.
- **Lease 15s, renewal Lease/3 ≈ 5s** (0032/0033/0043 inherited
  defaults). Dynamic-client QPS raised to 200/burst 400 (the default 5
  throttles the 2-CR-writes-per-served-write + read-path host reads).
- **`insecureSkipTLSVerify: true`** on the APIService (lab convenience
  for per-pod-Service replica pinning; the serving cert only SANs the
  load-balanced `aggexp` Service).
- **3 replicas** (0034 shape) so the lock and cross-replica RV are
  exercised, not bypassed.
- **controller-runtime v0.20.4** pinned (matches k8s 0.32); built with
  `GOTOOLCHAIN=local` and `go 1.24.0` (FINDINGS/0046 toolchain trap).


## Prerequisites

- kind cluster `aggexp-0048` (not the default `aggexp`).
- Serving cert from `hack/gen-certs.sh`.
- 0042–0045 code composed in and 0046's generated types consumed. No
  external secrets.

## What we're looking to learn

- **Wire protocol fidelity.** Do the arc's mechanisms compose into one
  multi-replica AA that is indistinguishable from a built-in to
  kubectl, client-go, and controller-runtime — without the mechanisms
  interfering (e.g., does emission filtering survive real lock
  contention; does per-watcher watch hold the RV contract; does
  read-path reconcile coexist with the lock)?

## Expected FINDINGS shape

- **Fundamental:** whether the multi-replica composition holds end to
  end (the arc's central thesis), and any interference discovered only
  under composition that the isolated experiments missed.
- **Consequent:** the compat-scoreboard results for this lab's kubectl
  / client-go / controller-runtime versions, recorded in
  `FINDINGS/compat/`.
