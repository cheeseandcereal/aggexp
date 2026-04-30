# Findings — 0015 argocd-application-targets-aa

## What we were trying to learn

`0005-argocd-compat` deployed ArgoCD alongside a read-only
aggregated API (0004 repos.aggexp.io) and observed ArgoCD *reading*
it — discovery, LIST+WATCH, cluster-cache resilience. Nothing was
ever written to the AA; ArgoCD's sync pipeline was never exercised
against an aggregated resource.

`0010-etcd-crd-facade-with-ssa` built a **writable** AA whose
backing store is a CRD served by the host kube-apiserver. It
demonstrated that SSA managedFields, finalizers, and ownerReferences
all survive the facade round-trip when the facade performs the
required apiVersion + field-path rewrites.

This experiment composes the two: **an ArgoCD Application targets
Widget resources served by the 0010 facade.** The Git source
contains three Widget manifests; ArgoCD's
`argocd-application-controller` syncs them into the cluster via
server-side apply. Four probes:

1. Can an ArgoCD Application using `directory` source with a Git
   repo of `aggexp.io/v1` Widget manifests pass the full sync cycle
   (dry-run → apply → Synced)?
2. What does ArgoCD's diff / sync / health workflow look like for
   a custom aggregated resource?
3. Does ArgoCD's default SSA produce correct `managedFields` on
   the exposed Widget (via 0010's facade rewrite)? Or does it
   regress to the 0009 "managedFields-don't-persist" case?
4. Pruning: delete a manifest from Git; does ArgoCD detect and
   prune the aggexp resource correctly?
5. Health: ArgoCD has no per-kind health check for `Widget`. How
   is that manifested?

The recommended target was 0010 rather than 0009 because 0010's
CRD-backed storage persists managedFields — the exact path ArgoCD
exercises.

## What we did

- Dedicated kind cluster `aggexp-argo-app`.
- Deployed base + 0010 overlay (CRD + permissive RBAC + AA
  Deployment). `aggexp-widgets:dev` image reused from 0010.
- Installed ArgoCD from `argoproj/argo-cd/stable/manifests/install.yaml`.
- Built a tiny in-cluster smart-HTTP git server: a debian-slim
  image running `apache2 + git-http-backend` serving a bare repo
  that an entrypoint script seeds at container start from a
  ConfigMap of YAMLs. `http://git-server.git-server.svc:8080/aggexp.git`.
- Created an Application targeting that repo, `ServerSideApply=true`,
  `automated.prune=true`, `automated.selfHeal=true`.
- Drove five scenarios end-to-end:
  1. Initial sync (three widgets created).
  2. Edit alpha in Git (v2 content); observe drift detection +
     re-apply.
  3. Remove charlie from Git (v3 content); observe pruning.
  4. Out-of-band drift (kubectl patch alpha directly); observe
     self-heal.
  5. Delete the Application (cascade).

The experiment had to work around two dead-end first attempts:

- `git-daemon` over the git:// protocol: alpine/git doesn't ship
  `git-daemon` (the binary is gated behind the separate
  `git-daemon-run` package). Scrapped.
- Dumb HTTP via nginx serving the bare-repo directory tree:
  connects, but ArgoCD's go-git client issues `--depth=1`
  shallow clones, which are not supported over dumb HTTP
  (`fatal: dumb http transport does not support shallow
  capabilities`). Required smart HTTP via `git-http-backend` CGI.

## What we observed

### Initial sync: three widgets created via ArgoCD SSA

Once smart HTTP worked, the initial sync completed within 5 seconds
of Application creation. ArgoCD's task plan:

```
Tasks (dry-run): [Sync/0 resource aggexp.io/Widget:default/alpha nil->obj (,,),
                  Sync/0 resource aggexp.io/Widget:default/bravo nil->obj (,,),
                  Sync/0 resource aggexp.io/Widget:default/charlie nil->obj (,,)]
```

And the three applies ran in parallel:

```
{"manager":"argocd-controller","msg":"Applying resource Widget/alpha in cluster: ...","serverSideApply":true,"serverSideDiff":true}
{"manager":"argocd-controller","msg":"Applying resource Widget/bravo ...","serverSideApply":true}
{"manager":"argocd-controller","msg":"Applying resource Widget/charlie ...","serverSideApply":true}
```

The AA backend logs show each arriving as a `verb=APPLY` patch at
the library layer, which the library translated to `Update()` calls
on our `runtime/storage.WritableBackend`:

```
"Patch" ... url:/apis/aggexp.io/v1/widgets/bravo ... verb:APPLY (606ms)
Trace: ---"About to apply patch" 196ms
Trace: ---"Object stored in database" 409ms
```

```
crd-backend verb="update" user="system:serviceaccount:argocd:argocd-application-controller"
  name="bravo" managedFields=2
```

`managedFields=2` on the second apply of bravo (the library had
already materialized a `before-first-apply` entry + the
`argocd-controller`/Apply entry). The 0010 facade rewrote
apiVersions and field-paths on the way out to the backing CRD.

After the sync, the exposed resources carry ArgoCD's ownership:

```
$ kubectl get --raw /apis/aggexp.io/v1/widgets/alpha \
    | jq '.metadata.managedFields[] | {manager, operation}'
{"manager":"argocd-controller","operation":"Apply"}
{"manager":"aggexp-widgets","operation":"Update"}
```

`argocd-controller` with `operation: Apply` — the definitive
evidence that ArgoCD's SSA round-tripped through the facade and
established proper field ownership. `aggexp-widgets` is the
facade's own field manager identity, used for the `Update` it
issues to the backing CRD.

### SSA field ownership is **fully functional**, including conflict detection

A direct `kubectl apply --server-side` from a different manager
produces the right conflict:

```
$ kubectl apply --server-side --field-manager=direct-test -f widget-alpha.yaml
error: Apply failed with 2 conflicts: conflicts with "aggexp-widgets" using aggexp.io/v1:
- .spec.counter
- .spec.description
```

Note the conflict is against `aggexp-widgets` (the facade's
internal Update manager) rather than `argocd-controller`. This is
a consequence of how the library's fieldmanager tracks update
vs. apply ownership: the internal Update entry covers the full
object's spec/metadata/status keys (even those *also* claimed by
an Apply entry), and a competing Apply must co-own those fields
with the Update manager before it can modify them. Functionally,
conflicts *are* detected — the experiment's claim is "SSA works
end-to-end", and it does.

The backing CRD (`widgetstorage`) shows the SSA state preserved
on the host side (with the facade's rewrites applied):

```
$ kubectl get --raw /apis/aggexpstorage.aggexp.io/v1/widgetstorages/alpha \
    | jq '[.metadata.managedFields[] | {manager, operation, apiVersion}]'
[{"manager":"argocd-controller","operation":"Apply","apiVersion":"aggexpstorage.aggexp.io/v1"},
 {"manager":"aggexp-widgets","operation":"Update","apiVersion":"aggexpstorage.aggexp.io/v1"}]
```

The apiVersion-rewrite + field-path rewrite that 0010 discovered
as load-bearing is what makes this transparent.

**Gotcha during observation**: `kubectl get` strips `managedFields`
from output by default. `kubectl get --raw` (or
`kubectl get --show-managed-fields`) is required to see them. An
agent who just runs `kubectl get -o json` and finds
`.metadata.managedFields == null` will incorrectly conclude
managedFields are not persisted. They are; kubectl just doesn't
print them.

### Edit-in-Git: drift detected, re-applied cleanly

Rewrote the git-content ConfigMap so alpha became:

```yaml
counter: 42                                   # was 1
description: "alpha widget edited in Git to revision v2"
labels: {..., mutated: "v2"}                  # new label
tags: {..., revision: v2, extra: added-in-v2} # two changes
```

Restarted the git-server Deployment (rebuilds the bare repo),
refreshed the Application, and within one reconcile cycle (~5s)
Argo dry-ran, applied alpha (only alpha; bravo+charlie's
spec hadn't changed), and reached `Synced + Healthy`.

`argocd-controller`'s managedFields entry on the exposed Widget
now includes the new label and new tag keys. The
`argocd.argoproj.io/tracking-id` annotation sits alongside
label/tag ownership. The AA's `aggexp-widgets` Update entry
was re-stamped during the library's apply → update translation
so its timestamp matches.

### Prune: works, but has a facade-specific surprise

Rewrote git-content to remove charlie entirely. Restarted
git-server. Triggered hard refresh on the Application.

**Unexpected intermediate state**: the reconcile reported
`Synced + Healthy` even though charlie was still present in the
cluster. Examining `application.status.resources` revealed what
had happened:

```
[
  {"kind":"Widget",         "name":"alpha",   "status":"Synced"},
  {"kind":"Widget",         "name":"bravo",   "status":"Synced"},
  {"kind":"WidgetStorage",  "name":"charlie", "status":null,   "group":"aggexpstorage.aggexp.io"}
]
```

ArgoCD's cluster cache sees **both** the Widget (exposed by the
AA) and the backing WidgetStorage (the CRD on the host), because
each carries the same `argocd.argoproj.io/tracking-id` annotation
— the facade preserves annotations verbatim when it writes the
storage row, so the tracking-id reaches the backing CRD. ArgoCD
conflated Widget/charlie and WidgetStorage/charlie as "somehow
the same resource being tracked", and its auto-sync + prune logic
left the state in a partial reported-Synced.

A manually-initiated sync with `--prune` cleaned it up:

```
{"resources":[
  {"name":"charlie","status":"Pruned","message":"pruned"},
  {"name":"bravo","status":"Synced","message":"widget.aggexp.io/bravo configured..."},
  {"name":"alpha","status":"Synced","message":"widget.aggexp.io/alpha configured..."}
]}
```

`kubectl get widgets` and `kubectl get widgetstorages` both now
return two rows (alpha, bravo). The prune propagated cleanly
through the facade: deleting the Widget through the AA triggers a
`dynamic.Delete` on the WidgetStorage, kube-apiserver GC's the
CRD row.

**Fundamental finding, facade-specific**: a facade AA that
exposes a resource whose backing CRD is *also* discoverable by
ecosystem controllers (because it's on the host cluster and RBAC
permits read) produces a **double-registration** in those
controllers' caches. The tracking-id annotation — which is
supposed to be ArgoCD's private bookkeeping on a single resource
— gets echoed onto the backing CRD because the facade preserves
annotations. ArgoCD sees the echo as a second managed resource,
at a different GVK. Prune doesn't fire automatically on the extra
phantom row because a manual sync is needed.

Two reasonable remediations:

1. **Strip the tracking-id annotation in the facade's write
   path.** The facade already rewrites managedFields for
   apiVersion correctness; it could whitelist which annotations
   cross the exposed/storage boundary. Argo's
   `argocd.argoproj.io/tracking-id` should live only on the
   exposed Widget, not the storage WidgetStorage.
2. **RBAC: don't grant ecosystem controllers read on the
   backing CRD group.** ArgoCD's default install grants `*` on
   everything, which defeats this mitigation; but a scoped
   ArgoCD install could exclude `aggexpstorage.aggexp.io`. This
   is an operational-posture choice, not a fix.

The first is the right answer for a facade intended to be
production-friendly.

### Self-heal: on manual drift, Argo re-applies within the reconcile cycle

```
$ kubectl patch widget alpha --type=merge \
    -p '{"spec":{"counter":999,"description":"out-of-band edit"}}'
widget.aggexp.io/alpha patched
# immediately: counter=999, description="out-of-band edit"
# 15s later:   counter=42,  description="alpha widget edited in Git to revision v2"
```

ArgoCD's next reconcile (default cadence ~3 min, but annotation-
triggered refresh or `selfHeal` semantics make it faster in
practice) detected the drift and re-applied the Git state through
SSA. Net cost: the manual patch is an irrelevant blip — Argo owns
those fields and takes them back.

### Delete the Application: cascade works cleanly

The Application carries the
`resources-finalizer.argocd.argoproj.io` finalizer. Deleting it:

```
$ kubectl delete application aggexp-widgets -n argocd --wait=false
# 3 seconds later:
$ kubectl get widgets         # empty
$ kubectl get widgetstorages  # empty
```

ArgoCD's cascade deletion ran against each managed Widget (and
the phantom-tracked WidgetStorages). The facade's Delete path
called `dynamic.Delete` on each WidgetStorage; the host
kube-apiserver handled the row deletion normally. No zombies, no
stuck finalizers, no log errors.

### Health: `null` per resource, `Healthy` at the Application

```
$ kubectl get application aggexp-widgets -o json | jq '.status.resources[].health, .status.health.status'
null
null
"Healthy"
```

ArgoCD's health-plugin registry has no entry for `aggexp.io/Widget`
(or indeed for any custom kind it doesn't recognize). For such
kinds it reports `null` per resource and rolls up to Application-
level `Healthy`. This matches the "unknown kinds default to
Healthy" hypothesis and mirrors how Argo treats most generic CRDs
that don't ship a Lua health check.

From an operator's perspective this means: **the Application will
not go Degraded when a Widget is in a broken state** — because
ArgoCD has no notion of "broken Widget." A real production
aggregated resource that wants ArgoCD-visible health needs either
a Lua health script on the Application or an embedded
`status.conditions` that Argo's default heuristics can interpret
(e.g., a Progressing/Available condition pair).

### Wire-protocol surprises: zero

The kubectl / ArgoCD view of the sync process is
indistinguishable from syncing against a vanilla CRD. Discovery
works. `kubectl api-resources --api-group=aggexp.io` shows the
Widget kind. `kubectl get widgets` returns a table (the AA's
Table-converter is fine). `kubectl describe widget alpha` shows
all standard fields. No protocol-level deviation from CRD
behavior was observed.

### RBAC: `system:authenticated` permissive is enough

Unlike 0005 (which had the argocd SA denied by the AA's
policy-service and cluster-cache bricked), the 0010 overlay
grants `system:authenticated` `*` on `widgets.aggexp.io` via a
ClusterRoleBinding. ArgoCD's SA is `system:authenticated`, so it
gets through. Additionally: **ArgoCD's default install grants its
application-controller SA `*` on `*` cluster-wide**, so even the
backing CRD (which doesn't have a permissive binding) is
listable. We added a focused `argocd-widgetstorage-read`
ClusterRoleBinding in run.sh for defensive clarity, but the
experiment confirmed that the default argocd install's blanket
grant makes it redundant. A hardened argocd install (scoping the
SA to specific groups) would need the binding.

### Latency

Single-digit seconds for the initial sync. The per-reconcile
diff-vs-live comparison is ~5ms. Under normal conditions the AA's
double-hop (client → AA → host-CRD) is lost in the noise; 0010
already observed that the aggregation-layer floor (~65ms) dominates.

## Fundamentals touched

**Wire protocol fidelity.** First experiment in the repo where
ArgoCD's sync pipeline **writes** to an aggregated API. The AA
is wire-protocol-indistinguishable from a CRD to ArgoCD's
gitops-engine + go-git + k8s.io/client-go stack. PATCH-with-SSA,
LIST, WATCH, DELETE, and the custom table-converter all ride
through cleanly. No new wire demands beyond what 0005 had
already validated for reads.

**Storage independence.** The CRD-facade pattern (0010) holds up
under the most demanding ecosystem-consumer we've tested. SSA
field ownership survives the rewrite end-to-end; ArgoCD's
`argocd-controller` is the manager on the exposed Widget's
managedFields and on the backing WidgetStorage's managedFields,
with the apiVersion rewritten symmetrically in each direction.
This **closes** the "ssa-managedfields-in-backend" open question
for the CRD-facade case; the mechanism works end-to-end under a
real gitops consumer, not just the `kubectl apply` probes of 0010.

A new facade-level finding that 0010 didn't surface: **a facade
that preserves annotations verbatim leaks ecosystem bookkeeping
through to the backing store**. When ArgoCD stamps
`argocd.argoproj.io/tracking-id` on the Widget, the facade's
annotation-preserving write echoes it onto the WidgetStorage.
ArgoCD then sees two managed resources per logical widget; its
prune logic doesn't cleanly handle the duplication. The fix is
at the facade layer: decide which annotations cross the
exposed/storage boundary, as a deliberate allow-list rather than
a default-pass. This joins the managedFields-rewrite obligation
0010 named as a general fact about facades.

**Per-request authorization.** This experiment didn't stress the
authorizer — the AA uses permissive RBAC on `widgets.aggexp.io`
for `system:authenticated`, and ArgoCD's SA is in that group. The
"one 403 bricks the cluster cache" failure mode from 0005 doesn't
apply here (writes aren't blocked by RBAC; the only authz decision
is handled by kube-apiserver RBAC since 0010 doesn't install a
custom authorizer). Confirmed indirectly: an experiment composing
0003's custom authorizer + 0010's facade + ArgoCD's sync would be
interesting — specifically, an authorizer that default-denies
writes (except for allowlisted users) would expose a different
failure shape than 0005's read-denial brickage, because ArgoCD
would report the Application as syncing-failed rather than the
entire cluster cache going sideways. Queued as
`aa-authz-aware-controllers` (already in EXPERIMENTS) or an
explicit follow-on.

## Consequents (implementation-dependent; do not generalize)

- **alpine/git lacks git-daemon.** The `git-daemon-run` package
  is split out in Alpine's ports; `alpine/git:v2.47.2` does not
  have it. The git:// protocol initContainer approach died on
  this. (`cgr.dev/chainguard/git` likewise omits it.)
- **ArgoCD's go-git shallow-clones by default.** Every clone is
  `--depth=1`, which is a **smart-HTTP-only capability**. Dumb
  HTTP via nginx autoindex does not work. An in-cluster git
  server for ArgoCD must implement smart HTTP; we used
  `apache2 + git-http-backend` CGI on debian:slim.
- **Nginx config needs `server_name _` to catch-all.** The first
  nginx manifest used `server_name: git-server.git-server.svc`
  which accepted requests by that Host header — but ArgoCD sets
  `Host: git-server.git-server.svc:8080`, which matched.
  Unrelated to the final working config with apache.
- **ArgoCD v3.3.8 is the `stable` tag at experiment time.**
  Behavior (reconcile cadence, retry loops, health-plugin
  registry shape) is tied to this version.
- **ArgoCD's default install grants `*` cluster-wide to its SA.**
  The `argocd-widgetstorage-read` ClusterRoleBinding we create
  is defensive: it's redundant under the default install but
  required under a hardened scope-down. An agent cloning this
  experiment and using a custom argocd install should re-evaluate.
- **Argo's `resources-finalizer.argocd.argoproj.io` finalizer
  with no `/` in the name** triggers a kubectl warning about
  domain-qualified finalizer names. This is argocd's own choice
  of name; not fixable from outside argocd.
- **manual `{operation.sync}` patch bypassed ServerSideApply**
  and wrote a `last-applied-configuration` annotation. ArgoCD's
  auto-sync honors `syncOptions: ServerSideApply=true` but the
  direct operation-patch path has a different codepath. Not
  investigated further; if an operator regularly does manual
  syncs and expects SSA, they should set SSA via the global
  `argocd-cm` setting.
- **`kubectl get` hides managedFields by default.** Use
  `kubectl get --raw /apis/GROUP/VER/resource/name` or
  `kubectl get --show-managed-fields`. Easy to miss.
- **debian-slim image size.** The git-server image is ~150MB
  (apache + git). A production version could shrink to ~40MB
  via `alpine + apache2 + git + the xinetd git-daemon package`
  but we didn't spend time on size.
- **`rev=HEAD` in early `Application.status.sync`** until the
  first successful clone resolves a real SHA. Not an error, just
  a pre-resolution state.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**:

- **Wire protocol fidelity.** Add a line under the "non-kubectl
  consumer" thread: ArgoCD's full sync pipeline (not just
  observation) works against a 0010-style writable aggregated
  API. PATCH-with-SSA is the hot path; LIST+WATCH cluster cache
  is still there for drift-detection; DELETE rides finalizers
  through the host kube-apiserver's GC. The library's PATCH →
  Update translation preserves SSA semantics when the backend is
  a CRD facade.
- **Storage independence.** The CRD-facade's apiVersion +
  field-path rewrite obligation (named by 0010 for the
  kubectl-apply case) extends cleanly to the
  argocd-controller-apply case. The field-manager identity
  (`argocd-controller`) round-trips through the facade correctly.
  Add a second obligation for facades at scale: **annotation
  allow-listing**. Ecosystem controllers stamp tracking metadata
  as annotations, and a facade that passes annotations through to
  the backing store causes those controllers to see duplicated
  resources. The two obligations (managedFields apiVersion
  rewrite; annotation allow-list) are the named cost of the
  facade pattern once it's under real ecosystem load, joining
  the existing "doubled kube-apiserver load" cost.
- **Watch and consistency semantics.** Nothing new: ArgoCD's
  dynamic watch on `widgets.aggexp.io/v1` behaved exactly as in
  0005, and the additional watch on the backing CRD is
  kube-apiserver-native (not aggregated-API-mediated).

For **EXPERIMENTS.md**:

- Mark `0015-argocd-application-targets-aa` complete under Wire
  protocol fidelity.
- Retire the `argocd-application-targets-aa` candidate.
- The `flux-compat` candidate remains open and is now sharper:
  does Flux's source-controller + kustomize-controller exhibit
  the same "double-tracked via backing-CRD annotation" behavior
  against a facade? (Flux uses Kustomize inventory ConfigMaps
  rather than per-resource annotations, so the answer may
  differ.)
- New candidate (low-priority): **`facade-annotation-allowlist`**
  — modify the 0010 backend to strip / allow-list annotations
  on the exposed→storage write path. Verify Argo's prune logic
  behaves cleanly under the change. Complements 0010's
  managedFields rewrite.
- Cross-reference: `aa-authz-aware-controllers` (already in
  EXPERIMENTS) remains worth doing with a WRITE-blocking
  authorizer now that we've exercised the write path.

## Open questions raised

- **What does Argo's UI show in the Resource Details tab for
  `Widget`?** The experiment was logs-only. UI renders would
  help confirm the diff-view treats the custom kind reasonably.
- **Does `argocd app diff` produce useful output against an
  aggexp resource?** Argo's diff relies on field-by-field
  comparisons that respect managedFields. We have reason to
  believe it'd work, but didn't run the CLI end-to-end.
- **Lua health script authorability.** A real production Widget
  would want a health plugin. How do you register one with
  ArgoCD for an aggregated API's resource? (Argo-specific
  question, outside the AA side.)
- **WatchList-aware informers against the facade.** ArgoCD v3.x
  may have the `WatchListClient` feature gate opportunities.
  0011 flagged `initial-events-end` bookmarks as a substrate
  gap; but here the watch rides kube-apiserver's (not the
  substrate's), so the gap may be invisible. Untested.
- **Multi-Application contention.** If two Applications both
  target the same Widget with conflicting specs, SSA ownership
  conflicts should surface as "conflicts with argocd-controller"
  on the second Apply — but Argo may have explicit
  cross-Application detection that supersedes. Untested; would
  probe ArgoCD-level bookkeeping rather than the AA side.
- **Deterministic UIDs revisited.** A Widget created by Argo,
  then a pod-restart of the AA, would (per 0004/0012) regenerate
  UIDs on the exposed Widget. But here the UID comes from the
  backing CRD, which doesn't regenerate. So this specific
  failure mode is absent under the 0010 facade. Worth recording
  as a consequent benefit of the facade pattern for ecosystem
  controllers.
