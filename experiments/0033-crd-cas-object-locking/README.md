# Experiment 0033: crd-cas-object-locking

A multi-replica library-mode aggregated apiserver where every write
acquires a per-object (Track A) or per-resource (Track B) lock via
CAS on a custom CR. Compares ergonomics, latency, and failure modes
to 0032's Lease-based approach.

This is the third of three Phase-0 experiments in the
production-library-readiness arc (0032/0033/0034). The arc explores
what a production-grade generic AA library needs beyond what
`runtime/storage` currently provides; 0033 specifically probes
**lock state as its own storage axis**: can we get correct
multi-writer semantics without depending on the Lease API's
opinionated lifecycle (renew, transfer, identity, etc.)?

## Hypothesis

A `lockedBy` field + `resourceVersion`-based CAS on a custom CR can
provide per-object write ownership without depending on the Lease
API's semantics. The expected costs: more retries under contention
because every CAS-loss is "do the whole read-then-write again", and
GC of stale lock CRs is the operator's problem (Lease has
leaseDuration; ours has a `spec.lockExpires` we must honor).

Fundamentals touched (named per ETHOS.md):

- **Storage independence** (primary). Lock state is itself a
  storage axis, distinct from business-data persistence and from
  KRM-metadata persistence (the fifth axis from 0024). 0033 is the
  first experiment to call lock state out as its own axis.
- **Watch and consistency semantics** (secondary). The CAS pattern
  uses optimistic concurrency on the CRD's own resourceVersion;
  this is the same primitive client-go uses for ResourceVersion
  conflict detection and is the natural building block for 0039.
- **Per-request authorization** (tertiary, not probed but
  observed). The locking layer runs as the AA's identity; the
  user's identity is not propagated to the lock CR. A future
  experiment could probe per-identity lock ownership.

## Tracks

- **Track A — per-object**: One `objectlocks.aggexp.io/v1
  ObjectLock` CR per (group, resource, namespace, name) tuple.
  Concurrent writes to *different* objects do not contend.
- **Track B — per-resource**: One `resourcelocks.aggexp.io/v1
  ResourceLock` CR per (group, resource) pair. All writes to that
  resource type serialize through one CR — a coarse-grained guard
  intended for write-rare resources or for lab debugging.

Both modes are selectable at runtime via `--lock-mode`.

## Architecture

```
                  ┌───────────────────────────────┐
                  │    kubectl / clients          │
                  └──────────────┬────────────────┘
                                 │ kube-apiserver
                                 │ aggregation
                                 ▼
                  ┌─────────────────────────────────┐
                  │  AA replica (POD_NAME=aa-N)      │
                  │  ┌───────────────────────────┐   │
                  │  │ runtime/server + group    │   │
                  │  │ runtime/storage adapter   │   │
                  │  └───────────┬───────────────┘   │
                  │              │ Backend           │
                  │              ▼                   │
                  │  ┌───────────────────────────┐   │
                  │  │ locking.WritableBackend   │◄──┼── (lock CR CAS)
                  │  │ (wraps memory backend)    │   │
                  │  └───────────┬───────────────┘   │
                  │              ▼                   │
                  │  ┌───────────────────────────┐   │
                  │  │ memory.Backend (gizmos)   │   │
                  │  └───────────────────────────┘   │
                  └──────────────────────────────────┘
                                 │ dynamic client
                                 ▼
                  ┌──────────────────────────────────┐
                  │ host kube-apiserver:             │
                  │ - objectlocks.aggexp.io/v1       │
                  │ - resourcelocks.aggexp.io/v1     │
                  └──────────────────────────────────┘
```

## CAS algorithm

Per the experiment spec; pseudocode below describes the
acquire path. `now` is wall-clock at the AA. `ourID` is
`POD_NAME` (or hostname).

```
loop (max retries = 8):
    cur, err := dyn.Get(name)
    case NotFound:
        new := { lockedBy: ourID, lockExpires: now + TTL, ref: ref }
        _, err := dyn.Create(new)
        if Conflict { continue }    # someone else created concurrently
        if err != nil { return err }
        return acquired
    case ok:
        if cur.lockedBy == ourID:
            if cur.lockExpires - now < TTL/3:
                # renew
                cur.lockExpires = now + TTL
                _, err := dyn.Update(cur)   # CAS on RV
                if Conflict { continue }
            return acquired
        if cur.lockExpires < now:
            # expired; steal
            cur.lockedBy = ourID
            cur.lockExpires = now + TTL
            _, err := dyn.Update(cur)
            if Conflict { continue }
            return acquired
        # someone else owns it and it's fresh
        return Conflict
```

Release is best-effort: on success, set `lockedBy=""` and
`lockExpires=now-1s`; on failure, just leave it — expiry handles
recovery.

## Status

complete

<!-- See FINDINGS/0033-crd-cas-object-locking.md for the writeup. -->

## Decisions made

- **Lock TTL = 15s**, arbitrary. Long enough that a normal write
  RTT (~10–80 ms in this lab) doesn't expire mid-write; short
  enough that crash recovery is bounded. Recorded actual behavior
  in FINDINGS.
- **CAS retry policy: 8 attempts, fixed 25 ms sleep between
  retries.** No exponential backoff. Justification: the CAS-loss
  signal in this design is "another replica raced you"; backoff
  would just delay the contender. The retries are bounded so a
  pathological storm returns a 409 to the user instead of looping
  forever.
- **Lock acquire timeout: matches the request context.** No
  separate timeout — if the user's `--request-timeout` expires the
  acquire is aborted.
- **Lock CR naming:** deterministic from the resource ref.
  - Per-object: `<group-with-dashes>--<resource>--<ns-or-cluster>--<name>`,
    fall back to `objlock-<sha256[:24]>` if length > 253 or invalid
    DNS-1123. Double-dash separators because `-` is a valid name
    character and we want segments visible at a glance.
  - Per-resource: `<group-with-dashes>--<resource>` with the same
    fallback.
- **Lock CRs are cluster-scoped.** Avoids the namespace-chosen-by
  question. ObjectLocks for namespaced resources still encode the
  namespace into the CR's name.
- **Replica identity** = `os.Getenv("POD_NAME")` (Downward API),
  falling back to `os.Hostname()`. Pod name is what the lock CR's
  `spec.lockedBy` ends up containing.
- **Replica pinning approach for the demo**: distinct
  port-forwards to specific pods. We expose each pod via its own
  `Service` selector trick: a Service per replica that selects on
  `statefulset.kubernetes.io/pod-name`. A StatefulSet (rather than
  a Deployment) makes the per-pod selector cleanly possible.
  kubectl uses one specific replica per shell.
- **Storage**: in-memory (`sync.Map`-equivalent), per-replica.
  Diverges between replicas — that's fine; the experiment is about
  locking, not consistency. (0034's problem.)
- **No GC of stale locks.** The expiry mechanism resolves the
  correctness question; long-term cleanup is deferred to a future
  experiment that would compose 0028's GC pattern with this CRD.
- **No fancy backoff under heavy contention.** Fixed retry
  interval + bounded count. Recording actual retry-count
  distribution under a probe storm is one of the data points.
- **Resource type: `gizmos.aggexp.io/v1 Gizmo`** — distinct from
  any other experiment's name to avoid confusion when running
  side by side.
- **One AA module / one binary**: library-mode. No component-server
  split (that's the v2 path; the arc is v1).
- **Lock CRDs under `locks.aggexp.io`, not `aggexp.io`.** Discovered
  during deployment that the APIService for `v1.aggexp.io` captures
  all resources in that group, so lock CRD requests would route back
  to our AA recursively. Separate group avoids the trap.
- **`--enable-priority-and-fairness=false`** to avoid needing RBAC
  for FlowSchema/PriorityLevelConfiguration (standard lab shortcut).
- **`--lock-kubeconfig` renamed from `--kubeconfig`** to avoid flag
  collision with the substrate's own `--kubeconfig` registration.

## Prerequisites

- kind, kubectl, docker, go (>=1.24).
- `kind create cluster --name aggexp-0033` (NOT the shared
  `aggexp` cluster).
- Self-signed CA + serving cert via `hack/gen-certs.sh`.
- This experiment installs the locking CRDs into the host cluster.

## How to run

From repo root, on a fresh checkout:

```
# 1. cluster
kind create cluster --name aggexp-0033
kubectl config use-context kind-aggexp-0033

# 2. base AA infra (namespace, RBAC delegation, serving cert)
./hack/gen-certs.sh
kubectl create namespace aggexp-system
./hack/deploy.sh deploy/manifests       # noisy: APIService NotReady until pod ready

# 3. install lock CRDs
kubectl apply -f experiments/0033-crd-cas-object-locking/manifests/00-lock-crds.yaml

# 4. build & load image
docker build -t aggexp-0033:dev \
  -f experiments/0033-crd-cas-object-locking/Dockerfile .
kind load docker-image aggexp-0033:dev --name aggexp-0033

# 5. install permissive RBAC + 3-replica StatefulSet override
AGGEXP_IMAGE=aggexp-0033:dev \
  ./hack/deploy.sh experiments/0033-crd-cas-object-locking/manifests

kubectl -n aggexp-system rollout status statefulset/aggexp

# 6. drive scenarios
./experiments/0033-crd-cas-object-locking/hack/run-scenarios.sh
```

To target a specific replica via port-forward:

```
# Replica-0 listens on 8443, Replica-1 on 8444, etc.
kubectl -n aggexp-system port-forward pod/aggexp-0 8443:8443 &
KUBECONFIG_R0=$(./experiments/0033-crd-cas-object-locking/hack/kubeconfig.sh aggexp-0 8443)

KUBECONFIG=$KUBECONFIG_R0 kubectl get gizmos
```

Tear down:

```
kind delete cluster --name aggexp-0033
```

## What we're looking to learn

- **Storage independence (primary).** Is lock state genuinely a
  storage axis on its own? It composes with backends but is
  orthogonal to them.
- **CAS retry cost vs. Lease.** Under a contention storm, what
  does the retry-count distribution look like for per-object vs.
  per-resource locking? How does that compare to what we'd expect
  from Lease (where holder identity must be checked, but no CAS
  loop is involved at the API layer because the holder doesn't
  contend on its own renewals)?
- **Failure-mode comparison.** When a holder crashes, both Lease
  and CAS-CR designs recover via expiry. The difference is who
  observes the expiry: Lease has a built-in `holderIdentity` +
  `leaseDuration` semantic; ours has us reading lockExpires and
  comparing to wall clock. What goes wrong.
- **Track A vs. Track B granularity ergonomics.** Per-resource
  feels grossly serialized; per-object feels right. Does that
  intuition survive the demo?

## Scope cuts

- No tests.
- No metrics endpoint.
- No state-consistency between replicas (0034's problem).
- No GC of stale lock CRs (one of 0028's patterns would apply;
  open question).
- No fancy backoff. Fixed retry interval, bounded count.
- No per-identity lock ownership (locks are AA-pod-identity, not
  user-identity).
