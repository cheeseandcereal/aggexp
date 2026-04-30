# Findings — 0012 controller-runtime-manager-compat

## What we were trying to learn

`0008-long-lived-informer` established that a client-go
`SharedInformer` survives most realistic perturbations when pointed
at a synthetic-RV aggregated apiserver: AA restarts, cert rotation,
slow user handlers. That only probed the *raw reflector*. One
layer up sits `sigs.k8s.io/controller-runtime`'s Manager — caches,
reconcile loops, finalizer lifecycle, owner-reference handling, and
leader election. These add their own assumptions to the ones
`0008` already cleared:

- A manager-level **cache** that wraps informers and serves GETs
  for read-side path.
- A **reconcile loop** that expects `Get` against a namespaced-or-
  cluster-scoped name to return the same object the watch just
  delivered.
- An SSA-capable **`client.Client`** that expects `client.Apply`
  to produce a real `managedFields` record on the wire.
- The **delete/finalizer dance**: a `DELETE` call is expected to
  set `deletionTimestamp`, not to actually remove the object
  (until finalizers clear).
- Garbage-collection via **ownerReferences** — a separate
  controller in the kube-apiserver's control plane that cascades
  deletes.
- **Lease-based leader election** via a resource lock the manager
  runs against the host cluster.

Does any of this work when the target API is our AA? How much
of it is a consequence of our fs backend being read-only, vs. a
fundamental mismatch between a stateless/AA model and the
ObjectMeta conventions controller-runtime is built around?

## What we did

Built a small controller-runtime manager (`controller/main.go`,
~270 lines) that:

- Watches `aggexp.io/v1 File` via `unstructured.Unstructured`.
- On every reconcile, attempts: (a) an SSA patch to add
  `aggexp.io/last-reconciled=<rfc3339>` with field manager
  `aggexp-file-controller`; (b) an UPDATE-based finalizer add of
  `aggexp.io/example-finalizer`; (c) an UPDATE to set an
  ownerReference to a host-cluster ConfigMap
  `aggexp-system/aggexp-files-parent` which the controller
  creates if missing; (d) on a delete path, clears the finalizer.
- Logs every step with outcome-carrying keys
  (`ssa-patch-result`, `finalizer-add-result`, `ownerref-set-result`,
  `reconcile-not-found`).
- Enables leader election against `coordination.k8s.io/leases`
  in `aggexp-system` (host-cluster, not our AA).
- Uses `client.Options.Cache.Unstructured: true` so the manager
  cache serves `Get` for unstructured File objects.

Deployed to a fresh kind cluster `aggexp-ctrl` alongside 0007's
AA (image `aggexp-files:dev`, read-only, fs-backed, three sample
Files). Ran 7 scenarios.

### Process note: global-context bleed-over

During bring-up the controller's Deployment applied against the
wrong cluster (`kind-aggexp-async`, a parallel experiment's
cluster) because `kubectl config current-context` is a
process-global setting; setting it in this worktree overwrote the
other worktree's target. I caught this when pods with image
`aggexp-widgets:dev` landed in this run's deploy; undid the damage
by redeploying the async experiment's manifests. From there, this
experiment used `KUBECONFIG=deploy/certs/kubeconfig-ctrl` (a
file-scoped kubeconfig extracted via `kind export kubeconfig
--name aggexp-ctrl --kubeconfig ...`) to avoid the shared file.
This matches the issue SYNTHESIS.md's Process observations §2
already flagged; worth promoting the `KUBECONFIG=<file>` pattern
to the default in AGENTS.md the next time that file is rewritten.

## What we observed

### Scenario 1 — manager starts; caches eventually sync

The manager came up cleanly on the first try. Leader election won
the lease in milliseconds:

```
I0430 04:12:39.353712       1 main.go:304] manager-start
I0430 04:12:39.353982       1 leaderelection.go:257] attempting to acquire leader lease aggexp-system/aggexp-file-controller...
I0430 04:12:39.359579       1 leaderelection.go:271] successfully acquired lease aggexp-system/aggexp-file-controller
I0430 04:12:39.359692       1 main.go:298] "cache-sync-ok" gvk="aggexp.io/v1, Kind=File"
```

**But the source-informer startup raced the APIService becoming
available.** Two attempts failed before a third succeeded:

```
2026-04-30T04:12:39Z INFO Starting EventSource source="kind source: *unstructured.Unstructured"
2026-04-30T04:12:39Z ERROR controller-runtime.source.EventHandler failed to get informer from cache
  error="unable to retrieve the complete list of server APIs: aggexp.io/v1: no matches for aggexp.io/v1, Resource="
2026-04-30T04:12:49Z ERROR controller-runtime.source.EventHandler failed to get informer from cache
  error="failed to get API group resources: ... aggexp.io/v1: the server is currently unable to handle the request"
2026-04-30T04:12:59Z INFO Starting Controller controllerGroup=aggexp.io controllerKind=File
2026-04-30T04:12:59Z INFO Starting workers worker count=1
```

Two observations here:

1. **The "cache-sync-ok" I logged from the manager at t=0 is
   misleading.** My runnable called `mgr.GetCache().WaitForCacheSync`
   but at that moment there were *no informers registered yet* —
   the `For(newFile())` builder's informer is lazy-initialized when
   `Starting EventSource` fires. `WaitForCacheSync` over zero
   informers returns immediately. A controller author who takes
   that log line at face value will be surprised when reconciles
   don't fire for another ~20s. This is a consequent — specific
   to controller-runtime v0.20.2's source.Kind initialization
   order — and I'm flagging it so a future reader knows
   `cache-sync-ok` in the log stream above is a false positive.
2. **The error message flips shape as discovery progresses.**
   First: "no matches for aggexp.io/v1, Resource=" (the REST
   mapper's discovery hasn't seen the group yet). Ten seconds
   later: "the server is currently unable to handle the request"
   (discovery saw the group but the APIService endpoints aren't
   healthy yet). Then silently succeeds. This is the same
   503-from-kube-apiserver shape `0008` observed, rendered one
   layer up the stack.

**Time to first reconcile:** ~20s from `manager-start` to
`Starting workers`. In that window, `controller-runtime.source.EventHandler`
retries with what looks like a 10s backoff. No hot loop.

### Scenario 2 — reconcile fires on new Files

Kubectl can't create Files (the backend is read-only; see
Scenario 3 for the 405 on writes). Instead we created a new File
by adding `newcomer.txt: "hi\n"` to the source ConfigMap
(`aggexp-sample-files`) mounted in the AA pod.

- **Mounted-CM propagation** to kubelet tmpfs: ~28-30s.
  Environmental; not an AA property.
- **AA polling** (fs-driver 5s ticker) then picked up the new file
  within one cycle.
- **Controller saw the watch ADD and reconciled in the same second:**

```
2026-04-30T04:19:18Z INFO reconcile-start File={"name":"newcomer.txt"} at="2026-04-30T04:19:18.872436391Z"
2026-04-30T04:19:18Z INFO reconcile-observed File={"name":"newcomer.txt"} rv="7" uid="8afa855d-4a35-4225-83c0-f40d5f529577" finalizers=[] deletionTimestamp=<nil> annotations=null
```

Total latency from `kubectl apply` (of the updated ConfigMap) to
first `reconcile-start`: ~33s, virtually all of it kubelet's
ConfigMap remount delay. The controller-runtime path added
effectively zero perceptible latency over the raw informer path.
**Reconcile firing is not a boundary here; the 0008 result
lifts cleanly.**

Note that `reconcile-observed.rv="7"` — a non-empty RV reaches
the controller's observation. This differs from what we see on
the *initial sync*, where `rv: ""` arrives because the 0007
backend doesn't stamp a per-object RV (only a single global RV on
watch events). The watch ADD event carries the RV; the informer
stores it; our `Get` retrieves it. On a fresh relist the
per-object RV field in the object body is empty (0007 never
populates `metadata.resourceVersion`), so reconciles from
relist-seeded stores see `rv=""`.

### Scenario 3 — SSA patch: fails at the wire with 405

Every reconcile attempted:

```go
r.c.Patch(ctx, applyObj, client.Apply, client.FieldOwner("aggexp-file-controller"), client.ForceOwnership)
```

Result, reproducibly:

```
2026-04-30T04:12:59Z ERROR ssa-patch-result File={"name":"haiku.txt"} outcome="error"
  error="update is not supported on resources of kind \"files.aggexp.io\""
```

The wire-level confirmation, via raw kubectl on the host:

```
$ kubectl apply --server-side --field-manager=kubectl-probe -f -
apiVersion: aggexp.io/v1
kind: File
metadata:
  name: haiku.txt
  annotations:
    aggexp.io/probe: manual
Error from server (MethodNotAllowed): update is not supported on resources of kind "files.aggexp.io"

$ kubectl patch files readme.txt --type=merge -p '{"metadata":{"annotations":{"probe":"x"}}}' -v7
... Response status="405 Method Not Allowed" milliseconds=2
"code": 405
```

**This is stronger than 0009's SSA finding** (which was "SSA
succeeds on the wire, managedFields not persisted"). With a
backend that doesn't implement `WritableBackend` at all, SSA's
generic PATCH path can't even dispatch — the substrate's
`rest.Storage` adapter (see
`runtime/storage/adapter.go:241`) returns `NewMethodNotSupported`
for Update, and since SSA PATCH is routed through the library's
Update machinery, the whole SSA round trip becomes a 405. No
`managedFields` block is ever produced.

Consequence: **on a read-only AA, SSA is not just `managedFields`-
lossy; it is wholesale unavailable.** `managedFields` analysis is
premature — you can't get to the code path that would write them.

Observed managedFields state, taken from a GET after the
controller had been running for 7+ minutes:

```
$ kubectl get files readme.txt -o yaml
apiVersion: aggexp.io/v1
kind: File
metadata:
  creationTimestamp: "2026-04-30T04:14:03Z"
  name: readme.txt
  uid: 441931c5-68f7-415c-a3e9-182749039e51
spec:
  mode: 511
  path: /etc/aggexp-sample-files/readme.txt
  size: 17
status:
  observedAt: "2026-04-30T04:14:03Z"
```

No `metadata.managedFields`, no annotations, no finalizers, no
ownerRefs. The backend is the sole source of truth and the
backend does not model any of those fields.

### Scenario 4 — finalizers: can't be added and the delete path is single-phase

The controller attempted `client.Update` after appending the
finalizer to a deep-copy of the observed object:

```
2026-04-30T04:12:59Z ERROR finalizer-add-result File={"name":"haiku.txt"} outcome="error"
  error="update is not supported on resources of kind \"files.aggexp.io\""
```

Same 405 as the SSA attempt.

**More interesting finding:** what happens on delete when the
finalizer *doesn't exist* is that the delete is single-phase. We
triggered a "delete" by removing `haiku.txt` from the source
ConfigMap; after the mount propagation (~60s) and the AA's next
poll, the AA fired a watch DELETE event:

```
2026-04-30T04:17:29Z INFO reconcile-observed File={"name":"haiku.txt"} rv="" uid="b41a94aa-..." finalizers=[] deletionTimestamp=<nil>
2026-04-30T04:17:29Z ERROR ssa-patch-result outcome=error ...           # this was the last reconcile BEFORE deletion
2026-04-30T04:17:53Z INFO reconcile-start File={"name":"haiku.txt"}     # first reconcile AFTER the watch DELETE
2026-04-30T04:17:53Z INFO reconcile-not-found msg="file vanished from cache; nothing to do"
2026-04-30T04:17:59Z INFO reconcile-start File={"name":"haiku.txt"}     # one more pass from the 30s RequeueAfter
2026-04-30T04:17:59Z INFO reconcile-not-found
```

The watch DELETE immediately evicted the object from the
controller cache; subsequent `Get`s returned NotFound; our
reconciler's `IsNotFound` branch took over. **There was no
`deletionTimestamp`-set phase between "object present" and
"object gone".**

This is fundamental to a backend-is-source-of-truth AA: the
object exists iff the backend reports it. Two-phase delete is a
library-layer affordance that exists to coordinate multiple
controllers via finalizers before the etcd row goes away. With
no etcd row to linger, there is no phase-1 to block on.
Controllers that depend on finalizers — for pre-delete cleanup,
for reference-counting, for "refuse to delete while dependent
objects exist" — **cannot work on this shape of AA at all.**
This extends 0009's "labels/annotations don't survive" finding:
*finalizers don't exist in the first place*.

A writable backend that models metadata (e.g. an RDS
`last-reconciled` tag) could synthesize a pseudo-finalizer
(`delete rejected until backend-tag X cleared`), but that's a
re-implementation of the library's state machinery at the
backend layer. 0009 flagged this trade-off conceptually; 0012
confirmed it concretely.

### Scenario 5 — ownerReferences: can't be set; cascading delete N/A

Same 405 as Scenario 4 on the UPDATE. The parent ConfigMap lives
in the host cluster (`aggexp-system/aggexp-files-parent`) and
*was* created successfully by the controller:

```
2026-04-30T04:12:59Z INFO parent-configmap-created File={"name":"haiku.txt"} uid="5d5adff8-4c98-41a6-9dcd-70104e3cfe8c"
```

Then we tested cascade: `kubectl delete cm aggexp-files-parent`.
The Files were unaffected because (a) the ownerRefs never made
it onto them, and (b) even had they, kube-apiserver's
garbage-collector (the one that does cascade-on-owner-delete)
only knows about objects it stores in etcd. Our AA's objects
aren't in etcd; the GC controller doesn't know they exist.

The final state, 10s after the CM delete:

```
$ kubectl get files
NAME         PATH                                  SIZE   MODE   AGE
haiku.txt    /etc/aggexp-sample-files/haiku.txt    16     0777   1m
notes.md     /etc/aggexp-sample-files/notes.md     15     0777   1m
readme.txt   /etc/aggexp-sample-files/readme.txt   17     0777   1m
```

No cascade. The controller recreated the parent CM 24s later on
its next reconcile cycle.

**Fundamental, not consequent**: cascading delete via
ownerReferences is a garbage-collection property of kube-apiserver,
running against etcd-backed objects. An AA with an external
source of truth is fundamentally outside the reach of that GC.
Cross-apiserver cascade deletion doesn't exist in the Kubernetes
model and this experiment is consistent with that absence.

### Scenario 6 — leader election works

Leader election targeted leases in `aggexp-system` (host
cluster). It worked on the first try:

```
I0430 04:12:39.353982       1 leaderelection.go:257] attempting to acquire leader lease aggexp-system/aggexp-file-controller...
I0430 04:12:39.359579       1 leaderelection.go:271] successfully acquired lease aggexp-system/aggexp-file-controller
```

Inspection of the Lease object (host cluster, not AA):

```
spec:
  acquireTime: "2026-04-30T04:12:39.353988Z"
  holderIdentity: aggexp-file-controller-79b84789b6-7brrh_db686b19-bbd9-47c5-95ca-22ee44c83c42
  leaseDurationSeconds: 15
  leaseTransitions: 0
  renewTime: "2026-04-30T04:20:58.812817Z"
```

Scaled the Deployment to 2 replicas to confirm mutual exclusion:

```
$ kubectl -n aggexp-system get pods -l app=aggexp-file-controller
NAME                                      READY   STATUS    RESTARTS   AGE
aggexp-file-controller-79b84789b6-7brrh   1/1     Running   0          8m22s
aggexp-file-controller-79b84789b6-q7qhc   1/1     Running   0          30s

# Replica 2's log:
I0430 04:20:30.588229 1 leaderelection.go:257] attempting to acquire leader lease aggexp-system/aggexp-file-controller...

# Lease:
holderIdentity: aggexp-file-controller-79b84789b6-7brrh_...
leaseTransitions: 0
```

Replica 2 blocks cleanly waiting for the lease; only replica 1
performs reconciles. Scaled back to 1.

**This is the unsurprising result**: leader election is a
host-cluster concern, doesn't touch the AA at all, and composes
with the usual controller-runtime manager exactly as it would
for any other controller. The explicit finding is that
**`LeaderElectionResourceLock: "leases"` plus
`LeaderElectionNamespace: "aggexp-system"` is the correct
configuration pattern** for an AA-targeting controller. If
anyone naively left leader election at its defaults and happened
to have an AA that doesn't serve leases (0007 doesn't), the
manager would fail to start.

### Scenario 7 — AA restart mid-reconcile

Scaled AA `Deployment` 1 → 0 → 1 while the controller was running
(no restart of the controller itself).

Timing:

- t=0: `kubectl scale deploy/aggexp --replicas=0`.
- t≈0+: existing reconciles ongoing (last observed RV for
  haiku.txt was UID `8bb2fafa-...`).
- t+24s: first reflector error surfaces:
  ```
  E0430 04:13:43.041097 1 reflector.go:166] "Unhandled Error" err=
    "... Failed to watch aggexp.io/v1, Kind=File: the server is currently unable to handle the request"
  ```
- t+24..24s: backoff-retry lines with a cadence of roughly
  1s, 3s, 3s, 13s (line timestamps):
  ```
  E0430 04:13:43.041097 ... failed to watch
  E0430 04:13:44.509141 ... failed to list
  E0430 04:13:47.071028 ... failed to list
  E0430 04:13:50.503198 ... failed to list
  E0430 04:14:03.296110 ... failed to list
  ```
  (No hot loop; backoff widens. Matches the 2-3-7-10s pattern
  `0008` characterized for a raw reflector.)
- t+~20s: `kubectl scale --replicas=1`.
- t+~45s: AA Ready again.
- t+~45s: reconciles resume at 04:14:24 with a **new UID**:
  ```
  2026-04-30T04:14:24Z INFO reconcile-observed File={"name":"haiku.txt"} rv="" uid="b41a94aa-db20-4bd0-b4df-75bd85fd99de"
  ```

UID continuity check across restart (all three sample files):

| File | UID before restart | UID after restart |
| --- | --- | --- |
| haiku.txt | `8bb2fafa-e811-4831-8465-58fdcb488573` | `b41a94aa-db20-4bd0-b4df-75bd85fd99de` |
| notes.md | `8afe8411-d0f0-4243-97ad-ab0c0255c9f9` | `976660b2-ad6b-4184-a16d-b840fa62b885` |
| readme.txt | `ce4d96d1-8bd3-4c78-a5e4-1db25d025c1e` | `441931c5-68f7-415c-a3e9-182749039e51` |

All three UIDs changed — 0007 generates UIDs via `github.com/
google/uuid` on each scan, with no persistence. This reproduces
the pod-restart amnesia pattern from `0004` / `0008`.

The interesting part is what **controller-runtime** did with it.
Controller-runtime's cache wraps the same client-go reflector
`0008` probed, so the store-level behavior (synthesized
DeleteFuncs for the old UIDs, then fresh AddFuncs for the new
ones) should be identical. For the **reconcile loop** above
that, each object key (`name=haiku.txt`) is enqueued once per
store transition. We observed exactly three reconciles per file
in the 04:14:24-29 window — consistent with delete-enqueue
followed by add-enqueue for each object, the delete path taking
the `reconcile-not-found` branch (since the cache briefly has no
object), and the add path taking the full reconcile branch
(logged as `reconcile-observed` with the new UID).

The reconcile-loop side effect of 0007's pod-restart amnesia is
**a deluge of "phantom" reconciles** — one pair (delete + add)
per object per restart. A production controller doing real work
on each reconcile (charging a credit card, submitting a job to
Kafka, etc.) would see every object reappear as a fresh `Add`
and re-perform that work. **This makes deterministic UIDs from
`EXPERIMENTS.md#repo-uid-stability` significantly more
attractive; in our experiment it's 3 extra reconciles per
restart, but a 10k-object AA restart would be a 10k-reconcile
storm.**

Worth noting that this is distinct from SYNTHESIS's previous
framing, which was "consumers keyed on UID see churn; consumers
keyed on name are mostly OK". The controller-runtime reconcile
loop is *keyed on name* — but its reconcile semantics (the
Reconcile() function's expected idempotence) don't help if the
work that needs to be done is UID-scoped.

## Fundamentals touched

### Watch and consistency semantics — primary

The core 0008 result composes cleanly one layer up:
controller-runtime's cache is a thin wrapper over the same
reflector. Manager start survives `APIService not-yet-Available`
(automatic backoff + retry, ~20s convergence window in our run).
Manager mid-flight survives AA scale 1→0→1 (no crash, no hot
loop, resumes reconciles ~25s after AA Ready).

**New observation not in 0008:** the reconcile queue amplifies
pod-restart amnesia into phantom reconciles. For a 3-object AA
this is invisible; for a 10k-object AA it could dominate work.
This strengthens the case for deterministic UIDs on any
writable-at-scale AA.

**New observation not in 0008:** controller-runtime's initial
informer startup races the APIService's discovery reaching Ready
(`AvailableConditions` in kube-apiserver). The error shapes
observed in the controller log during that race are:

- `"no matches for aggexp.io/v1, Resource="` — REST mapper hadn't
  seen the group yet.
- `"failed to get API group resources: ... the server is
  currently unable to handle the request"` — 503 from
  kube-apiserver while the AA endpoints aren't accepting.

Both transient, both resolved by the 10s retry loop inside
`source.Kind.Start`.

### Wire protocol fidelity — secondary

Positive result at the edges: GET, LIST, WATCH behave exactly as
0008 established; nothing about controller-runtime's usage of
those verbs introduced new fidelity demands.

Hard negative result on the write edge, but not a surprise given
0007's design: an AA whose backend doesn't implement
`WritableBackend` (i.e., the substrate's `rest.Storage` adapter
refuses to register `rest.Updater` and `rest.Creater`) returns
HTTP 405 to the entire SSA/UPDATE/DELETE family. controller-
runtime handles this honestly — it surfaces the 405 as an error
to the reconciler, the reconciler fails the Reconcile() call, and
the controller backs off and retries. No silent corruption, no
unexpected success.

The consequent-vs-fundamental split here matters:

- **Fundamental**: any AA without a write-path cannot participate
  in SSA, finalizers, or ownerReferences. controller-runtime
  makes this loud, not quiet.
- **Consequent**: the specific error message format ("update is
  not supported on resources of kind \"files.aggexp.io\"") comes
  from `apierrors.NewMethodNotSupported(groupResource, "update")`
  in our substrate; a different library could format differently.

### Resource modeling freedom — tertiary

`ObjectMeta`'s finalizer slot, `deletionTimestamp` slot, and
`ownerReferences` slot are resource-modeling commitments baked
into the Kubernetes API shape. 0012 concretely demonstrates
that they come packaged with *behavioral* commitments enforced
*above* the AA:

- Finalizers expect a two-phase delete. A stateless-AA model where
  deletes are "the backend row disappeared" has only one phase.
- ownerReferences expect a garbage-collector that knows about
  both owner and owned. Cross-apiserver GC doesn't exist. Our AA
  is on the wrong side of that boundary.
- `deletionTimestamp` is a library-layer convenience that the
  generic `rest.Storage` sets during `Delete` before calling
  into the storage backend. With no `Delete` path on the
  backend, the timestamp is never set.

A writable AA with a backend that models its own
metadata-storage (S3 tags, RDS columns, …) could bring back
finalizer semantics by *convention* — e.g. "delete the object
only when its `aggexp.io/finalizer` tag is empty." But that's a
re-implementation at the backend layer, not an inheritance from
the aggregated-apiserver library.

This generalizes the 0009 "managedFields, labels, annotations
don't persist" finding. The ObjectMeta affordances break along
a consistent seam: **anything the library stores before calling
into the backend** is what our AAs fail to persist. That's a
library-layer state invariant — fine to name as such,
appropriate to design around, not fixable within the library.

## Consequents (do not generalize)

- **`controller-runtime` v0.20.2** pairs with `client-go` v0.32.3.
  Earlier versions (v0.19 and below) pre-date the
  `TypedBuilder[request]` generics rewrite and accept slightly
  different builder signatures; the reconciler logic would
  transfer unchanged.
- **`client.Options.Cache.Unstructured = true` is required** for
  the manager's cache-backed client to serve reads of
  `unstructured.Unstructured` objects. Default is false, which
  silently routes reads to a live API call and hides the cache
  layer from the controller — a real footgun if the AA is the
  only bottleneck. Consequent of controller-runtime's default.
- **`LeaderElectionResourceLock: "leases"` is explicit by
  default** in v0.20.2 but the manager will also work without it
  (the library falls through to leases). Consequent of library
  version; pin it explicitly so readers of the code don't need
  to look up the default.
- **My custom `cache-sync-ok` log line fires before any informer
  is actually registered.** This is controller-runtime's lazy
  source initialization showing through. Future scaffolds
  should either add the Source-level readiness signal explicitly
  (via `source.Kind.Start` completion) or drop the custom log.
- **0007's fs-driver generates UIDs via `uuid.NewRandom()` with
  no persistence**, so UIDs regenerate on every pod restart.
  `0004` does the same; this is consistent with the substrate's
  current opinions, not a library choice.
- **Mounted-ConfigMap propagation latency** (~28-30s in kind) is
  a kubelet property, not an AA property. It shaped the Scenario
  2 timeline but doesn't inform anything about the AA model.
- **The reflector backoff cadence during AA downtime** (roughly
  1-3-3-13s) observed here is comparable to but not identical to
  `0008`'s 2-3-7-10s. Different client-go code paths (list vs.
  watch failure; SSL session reuse or not) produce slightly
  different intervals. Not interesting; consequent of the
  specific error branches hit.
- **Our "reconcile-not-found" path being a clean terminal branch**
  depends on `client.IgnoreNotFound`-equivalent handling inside
  the reconciler. A naïve controller that returns the NotFound
  error (instead of short-circuiting on it) would produce a
  `Reconciler error` log and backoff, still correct but noisier.
  Our experiment opts for clean handling; a poorly-written
  controller would surface the amnesia as error-log noise.
- **On a relist (but not on a watch ADD), our `reconcile-observed`
  logs show `rv=""`.** 0007's fs-backend doesn't stamp
  `metadata.resourceVersion` on individual File objects — there's
  a single global RV counter and watch events carry it out-of-
  band, but LIST responses don't embed it per-item. The library-
  layer `rest.Storage` adapter could do that; the substrate
  doesn't today. Queue as a possible substrate polish item.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS** — `Watch and consistency semantics` section:

- The 0008 result holds one level up. Controller-runtime's
  Manager composes cleanly on top of an AA whose wire behavior
  passes the 0008 scenarios.
- Add explicit note: **manager startup races APIService
  availability**; controller-runtime's ~10s retry loop masks this
  for lab latencies but could bite production controllers that
  restart frequently. Not a new fundamental — same APIService
  readiness that kubectl already sees.
- Strengthen the "pod-restart amnesia is observable as synthesized
  Delete/Add" claim with a downstream observation:
  **reconcile loops amplify pod-restart amnesia into phantom
  reconciles at rate proportional to object count.** Deterministic
  UIDs become substantially more attractive at scale.

For **SYNTHESIS** — `Wire protocol fidelity`:

- The "controller-runtime has not been measured" bullet can be
  removed. The positive result: read path + watch path + reconcile
  firing are all fine. The negative result: write path is a
  faithful 405 when the backend doesn't implement writes.

For **SYNTHESIS** — `Resource modeling freedom`:

- Add: **a stateless/AA model that doesn't mediate deletion
  through a phase-1 step cannot support finalizers at all**,
  because finalizers are a two-phase-delete contract. This is
  upstream of "finalizers don't persist" — it's "finalizers
  have no phase to live in."
- Add: **ownerReference-driven cascade delete is a cross-apiserver
  operation the Kubernetes GC doesn't perform.** ObjectMeta
  carrying ownerReferences into an AA resource is a meaningless
  gesture absent backend-layer plumbing.

For **EXPERIMENTS.md**:

- Mark `0012-controller-runtime-manager-compat` **complete**.
- Retire the `controller-runtime-manager-compat` candidate bullet.
- **New candidate** (worth its own slot under
  Watch/consistency): **`controller-runtime-on-writable-aa`**.
  0012 was limited by 0007's read-only backend; the interesting
  half of the SSA/finalizer story (managedFields persistence,
  backend-modeled finalizers) needs a writable AA to probe. The
  natural target is `0009-ack-aggregated-s3` or a bespoke
  writable fs-driver variant.
- **New candidate**: **`controller-runtime-dynamic-client-phantom-reconciles`**
  — measure the rate at which pod-restart amnesia produces
  phantom reconciles in a 10k+ object AA. If the cost scales as
  expected, deterministic UIDs stop being a nice-to-have.

## Open questions raised

- **With a writable AA, does `client.Apply` (SSA) correctly carry
  the `managedFields` update on the wire, and does the server
  return the synthesized field manager record to the client, even
  when the backend can't persist it?** The substrate's Update
  path would accept the patch; the library would synthesize
  managedFields; the backend would be asked to store an object
  that carries those managedFields; the backend would probably
  drop them (S3, fs, etc.). The client-side view (what
  `client.Apply` + `Get` returns within one reconcile loop)
  might show them transiently. Worth probing.
- **What does a writable AA that re-reads from the backend *after*
  an SSA patch look like to the controller?** If the backend
  drops managedFields, does client.Apply produce conflict-detect
  errors on the next reconcile because the prior field manager
  record is gone? A controller that retries on "conflict" might
  hot-loop.
- **Can finalizer semantics be simulated by a writable AA that
  intercepts `DELETE` at the backend layer, queues a phase-1
  "deletionTimestamp-equivalent" in its own store, and only
  removes the backend row once client-reported finalizers
  clear?** This is a design experiment — the ACK model has a
  shape like this via its reconciler loop.
- **What happens when controller-runtime's manager runs against
  an AA that's served from multiple replicas with a load-balancer
  in front?** Our AA is 1-replica. A 2-replica AA would produce
  independently-progressing RV counters (each process holds its
  own `atomic.Uint64`) and watch streams that disagree on
  current state. Worth probing as an extension of 0008/0012.
- **The manager-startup-vs-APIService-readiness race** could be
  papered over with an explicit pre-start hook that waits for
  the APIService status. Worth encoding in the runtime
  substrate if we start writing more controllers against AA.
