# Findings — 0008 long-lived-informer

## What we were trying to learn

The 0002 and 0004 findings kept flagging the same untested corner of
**watch and consistency semantics**: a hand-rolled synthetic-RV
aggregated apiserver with a `watch.NewBroadcaster` + `DropIfChannelFull`
stream and a permissive `ResourceExpired` (410 Gone) policy on
resume. The 0002 and 0004 AAs both satisfy `kubectl get -w`, but
neither had ever been driven by a real client-go reflector across
the edges we know exist: relists, 410s, pod restart, cert rotation,
broadcaster pressure. This experiment does that.

Concrete questions going in:

1. How does a client-go `SharedInformer` handle a 410 Gone on watch
   resume — clean relist, crash, or hot-loop?
2. What does AA pod restart look like from the informer's side?
   DELETE+ADD, silent UPDATE, or something weirder?
3. Does serving-cert rotation perturb an in-flight watch at all?
4. Can a slow user-level event handler drive the broadcaster's
   `DropIfChannelFull` path, and what does that look like
   downstream?

## What we did

Built a small client-go program (`experiments/0008-long-lived-informer/cmd/watcher`,
~240 lines) that:

- Uses `dynamicinformer.DynamicSharedInformerFactory` pointed at
  `aggexp.io/v1 hellos` (0002's resource type).
- Logs every AddFunc / UpdateFunc / DeleteFunc callback in plain
  `ts=... event=add name=... rv=... uid=...` lines on stdout.
- Exposes `/status` for a summary of adds / updates / deletes /
  last-rv / store-items.
- Logs a heartbeat every 30s so silence-because-healthy is
  distinguishable from silence-because-dead.
- Installs a `WatchErrorHandler` so reflector-level errors
  (including `ResourceExpired`, TLS failures, and 503s) also
  land in the plain log stream instead of only klog.
- Has a `WATCHER_SLEEP_MS` env var that inserts a sleep inside
  each event callback, for scenario 4.

Ran it in a dedicated `aggexp-informer` kind cluster against a
fresh 0002 AA deployment. Drove scenarios 1-4 by hand.

## What we observed

### Scenario 1: baseline event flow

Normal CRUD produces exactly what you'd expect. A create/patch/delete
sequence:

```
event=add    name=alpha rv=2 uid=752a2b05-77fb-4764-ab7f-04a55e884671 initial=false
event=add    name=beta  rv=3 uid=99a79290-8d4a-4843-8ff3-3822ecfe0636 initial=false
event=update name=alpha rv=4 uid=752a2b05-77fb-4764-ab7f-04a55e884671 oldrv=2 olduid=752a2b05-... uidchanged=false
event=delete name=alpha rv=5 uid=752a2b05-77fb-4764-ab7f-04a55e884671
```

`resourceVersion` increments monotonically one per mutation (0002's
global `atomic.Uint64`). `uid` is stable across the update. No
heartbeat lies: `store_items` tracks the informer's view, `last_rv`
is the highest object RV delivered to a callback.

### Scenario 2: 410 Gone on resume — not what we expected

We spent most of the experiment looking for an observable 410 Gone
in the reflector's logs. It's hard to provoke, because **client-go's
reflector almost always relists before watching**, not after a
disconnect. A 410 on watch-resume is a narrow window.

The wire-level 410 path on 0002 is real. A hand-crafted request
confirms it:

```
$ kubectl get --raw '/apis/aggexp.io/v1/hellos?watch=true&resourceVersion=1&timeoutSeconds=1' -v6
... "Response" verb="GET" url="...?resourceVersion=1&timeoutSeconds=1&watch=true" status="410 Gone" milliseconds=7
server response object: Error from server (Expired): too old resource version: 1 (current 201)
```

But the reflector never surfaced a 410 during our scenarios. What
we saw instead, scaling the AA from 1 → 0 → 1 replicas while the
informer was running:

```
# before: lastRV=10 in our watcher, AA RV=10.
# scale to 0.
event=watch-error err="the server is currently unable to handle the request"
event=watch-error err="failed to list aggexp.io/v1, Resource=hellos: the server is currently unable to handle the request"
event=watch-error err="failed to list aggexp.io/v1, Resource=hellos: the server is currently unable to handle the request"
# ... retries back off (2s, 3s, 7s, 10s intervals; ~8 errors over 60s)
event=heartbeat adds=12 updates=1 deletes=3 store_items=9 last_rv=10 uptime=6m30s
# scale to 1. new AA RV starts at 1.
# ~25s after AA is ready, reflector's relist succeeds; it diffs against its store and fires deletes.
event=delete name=bulk-1 rv=3 uid=...
event=delete name=bulk-2 rv=4 uid=...
event=delete name=bulk-3 rv=5 uid=...
event=delete name=bulk-4 rv=6 uid=...
event=delete name=bulk-5 rv=7 uid=...
event=delete name=bulk-6 rv=8 uid=...
event=delete name=bulk-7 rv=9 uid=...
event=delete name=bulk-8 rv=10 uid=...
event=delete name=rv-probe rv=2 uid=...
# heartbeat confirms the store is empty, lastRV synthetically preserved from last delivery
event=heartbeat adds=12 updates=1 deletes=12 store_items=0 last_rv=10 uptime=7m0s
```

The "watch-error" lines here carry kube-apiserver's aggregation-layer
error string (`the server is currently unable to handle the
request`), not a 410. That makes sense: when the AA pod is down,
kube-apiserver cannot reach it and returns 503 to the reflector
before our AA's 410 policy is ever evaluated. Once the AA comes
back, the reflector goes through a fresh **list** before any
watch, and the fresh list reports the empty cluster at a low RV
with no need for the server to emit 410.

Synthesizing 9 deletes from a single successful relist is a real
property of client-go's reflector: its store holds the prior state;
the relist gives it the new state; the delta is converted to
delete callbacks. The reflector does **not** care that the new
state's RVs are lower than the old ones.

**This means our 0002 AA's 410-on-resume code path is rarely
exercised by a real client.** The path exists for clients that
bypass list (relatively rare) or clients whose watch survives
longer than the AA's RV window (not possible with our AA: the
window is effectively the server's lifetime). Our 410 behavior is
defensible — it makes the rare-path client relist — but in
practice the list path dominates so thoroughly that 410 never
appeared spontaneously across ~15 minutes of deliberate
AA-restart scenarios.

### Scenario 3: cert rotation mid-watch

Rotating certs on a **live AA** (overwriting the Secret without
restarting the pod) produced zero disruption to the informer.

The AA's `DynamicServingCertificateController` picked up the new
cert file on the mounted volume:

```
# in AA logs, roughly at the time we overwrote the Secret:
I0429 05:28:54.920810 dynamic_serving_content.go:195] "Failed to remove file watch, it may have been deleted" file="/etc/aggexp/certs/tls.crt" err="fsnotify: can't remove non-existent watch: ..."
I0429 05:28:54.921070 dynamic_serving_content.go:116] "Loaded a new cert/key pair" name="serving-cert::/etc/aggexp/certs/tls.crt::/etc/aggexp/certs/tls.key"
I0429 05:28:54.921216 tlsconfig.go:181] "Loaded client CA" index=1 ...
I0429 05:28:54.921380 tlsconfig.go:203] "Loaded serving cert" ... "aggexp.aggexp-system.svc" (...05:23:00 to 2036-04-26 05:23:00...)
```

Across that reload, the informer's heartbeat continued
uninterrupted. No `watch-error`, no relist, no gap in event
counts. The existing TLS connections are kept alive; the new
cert only gets handed to new incoming handshakes. The informer's
watch is a long-lived HTTP connection; it doesn't get re-
handshaken just because the server loaded a new keypair.

The **cert rotation boundary is empirically invisible to the
client** as long as the new chain is still trusted by
kube-apiserver's aggregator. If the rotation also changes the CA,
the `APIService.spec.caBundle` must be updated in the same
transaction — kube-apiserver will refuse to proxy to a cert that
doesn't chain to the advertised caBundle, at which point the
next scenario (AA visible to clients as "ServiceUnavailable")
applies.

### Scenario 4: slow watcher → no broadcaster drops observable

We set `WATCHER_SLEEP_MS=2000` (2s sleep inside every event
callback) and then dumped 200 `kubectl apply` creates at the AA
in rapid succession. The hypothesis was that the AA's broadcaster
(`watch.NewBroadcaster(100, DropIfChannelFull)`) would observe
downstream backpressure, drop events, and close the watcher —
surfacing as a watch-error or forcing the informer to relist.

That didn't happen:

```
# counts in the heartbeat stream over 10 minutes
event=heartbeat adds=22  updates=0 deletes=0 store_items=200 last_rv=23  uptime=1m0s
event=heartbeat adds=37  updates=0 deletes=0 store_items=200 last_rv=38  uptime=1m30s
event=heartbeat adds=127 updates=0 deletes=0 store_items=200 last_rv=128 uptime=4m30s
event=heartbeat adds=200 updates=0 deletes=0 store_items=200 last_rv=201 uptime=8m30s
# watch-errors: 0 across the entire scenario
```

The crucial observation is `store_items=200` from the first heartbeat,
while our `adds` counter is only 22. **The informer's internal
store was already full** while the user-level callback loop was
still at event #22. client-go's shared-informer architecture
decouples the wire (reflector goroutine draining HTTP) from the
handler (processLoop delivering to user callbacks). A slow user
handler backs up the **DeltaFIFO**, not the watch stream. The
reflector keeps reading and keeps the channel drained; the AA's
broadcaster sees a fast consumer.

**Server-side `DropIfChannelFull` is effectively unreachable from
user-handler slowness.** To trigger it, you would need either:
- Multiple watchers sharing the same broadcaster where *the
  broadcaster's own send to one consumer's channel* blocks
  (happens if that consumer's TCP connection is slow); or
- A sustained wire rate exceeding the reflector's HTTP read
  throughput (hard at lab scale).

### Bonus observation — informer `initial=true` delivers ADD twice

With a fresh informer connecting to an AA that already has 200
Hellos:

```
event=synced store_items=200
# ... 200 event=add ... initial=true ...
# immediately after, same timestamp:
# ... 200 event=update ... uidchanged=false, oldrv==rv ...
```

The reflector delivers each object as an **ADD** (initial list)
followed immediately by an **UPDATE** with identical old/new RV
and UID. This appears to be a side-effect of the
`InOrderInformersBatchProcess` feature gate (default-on in
client-go v0.32) doing a post-sync re-enqueue. Consumers that
assume "add → update means the object changed" will see spurious
updates on every informer startup. Consumers that dedupe on
`oldRV == newRV` (e.g. a controller with a proper reconcile loop)
are fine. We did not dig further; flagged as a consequent below.

### What "pod-restart amnesia" looks like from the consumer side

0004 predicted that a restarted AA regenerating UIDs would look
like "full churn" to a consumer. The actual observation is more
specific and somewhat less alarming:

- Old store: 9 items, each with some UID U_old and some RV R_old.
- AA restart; new state: the same 9 items do not exist (AA is
  stateless — 0004's actual backend would re-discover them, but
  0002 does not).
- Reflector relists; empty list; diffs against store; **9
  DeleteFunc callbacks fire**, each carrying the old (U_old, R_old).
- If the backend were real (as in 0004), the subsequent poll would
  create 9 fresh objects with (U_new, R_new); reflector sees
  those as new ADDs.

So a consumer that keys on name sees: 9 deletes, then (later) 9
adds with same names. A consumer that keys on UID sees: 9
different objects. A consumer that uses the name as primary key
with a small debounce window will **not** produce a visible
glitch at all if the delete → re-add window is shorter than the
debounce; with 0004's 60s poll interval it's not, so the glitch
would be visible there.

This is a real cost, but it's consequent on the AA's UID-generation
strategy. A deterministic UID (hash of backend's stable ID) would
eliminate the UID churn. The name-based delete/re-add churn would
remain but compress into a single window.

## Fundamentals touched

**Watch and consistency semantics.** Several concrete claims
strengthen or refine:

- The 410 Gone resume path on a synthetic-RV AA is **rarely**
  exercised by a real client-go reflector. Reflectors relist
  first; watch-only-from-RV reconnects are only attempted after a
  clean disconnect on a healthy server. When the AA disappears,
  the reflector sees kube-apiserver's 503, backs off, and
  eventually relists once the AA is back. The AA's 410 policy is
  correct and defensive, but does not dominate observed behavior.
- Relist-based recovery is **faithful**: the reflector synthesizes
  DeleteFunc callbacks for every object that existed in the prior
  store but not in the fresh list, regardless of whether RVs
  moved backwards. We saw 9 synthesized deletes from a single
  successful relist after an AA restart.
- The reflector does **not** crash, does **not** hot-loop on
  watch-errors during AA downtime. Backoff is visible — roughly 2s,
  then 3s, then 7s, then 10s — spaced widely enough that log
  volume remains manageable.
- **Cert rotation mid-watch is invisible** to the informer as long
  as the CA chain is kept consistent with the APIService.caBundle
  and existing TLS connections stay up. `DynamicServingCertificateController`
  reloads in-process; no pod restart, no connection reset.
- **DropIfChannelFull on the server broadcaster cannot be triggered
  by a slow user-level event handler.** client-go's DeltaFIFO
  decouples the wire from the handler; the reflector keeps
  draining the stream fast. Scenario 4 saw no drops and no
  watch-errors.
- An informer reconnecting to a fresh AA with lower RVs than it
  last saw does not error out — the reflector's relist path
  doesn't check "new RV must be ≥ old". This is critical for
  synthetic-RV AAs that restart.

**Storage independence.** Reinforces 0004: AA pod restarts are
observable from the client side as "all objects apparently
deleted, then re-added as fresh UIDs." Deterministic UIDs
(suggested as `repo-uid-stability` in EXPERIMENTS) would remove
the UID churn half of this; the name-based delete/re-add would
remain and is fundamental to a stateless rebuild-from-source AA.

## Consequents (do not generalize)

- **`InOrderInformersBatchProcess` duplicates initial list items as
  ADD-then-UPDATE.** This is a client-go v0.32 feature-gated
  behavior. Older clients will not show this; future clients may
  change it. A consumer depending on "never see spurious updates
  at startup" would want to dedupe on `oldRV == newRV`.
- **kube-apiserver returns 503 "server is currently unable to
  handle the request" when the AA is unreachable**, not a more
  specific error. The reflector's `WatchErrorHandler` receives
  only the human-readable string; parsing the status code from
  it is fragile.
- **Reflector backoff schedule observed** (roughly 2-3-7-10s, then
  plateauing) is from `k8s.io/client-go@v0.32.3`'s
  `reflector.go`. Other versions differ.
- **Multiple kind clusters on one host produce context bleed-over.**
  Running `aggexp-informer`, `aggexp-argocd`, `aggexp-runtime`
  simultaneously had `kubectl config use-context` persisting
  globally while `hack/deploy.sh` used the current context; it
  was easy to accidentally apply one cluster's overlay into
  another. Not an aggregated-API finding — a workflow trap.
  Mitigation: `KUBECONFIG=dedicated-file` or explicit
  `--context` on every invocation.
- **Raw wire-level 410 is trivial to provoke** with
  `kubectl get --raw '?watch=true&resourceVersion=1'` when the
  server's current RV is higher. Confirms the 0002 storage.go
  path is live; just not hit by real clients.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS** — `Watch and consistency semantics` should be
updated to reflect:

- The long-lived informer boundary is now measured, not just
  hypothesized. client-go v0.32 reflectors survive AA pod
  restarts, AA-unreachable periods, and cert rotations without
  crashing, hot-looping, or silently losing objects.
- The 410-on-resume path on a synthetic-RV AA is defensible but
  rarely exercised. Most informer recovery goes through relist.
- Cert rotation is a non-event as long as the AA's dynamic cert
  controller and the APIService.caBundle stay consistent.
- "Pod-restart amnesia" (0004) surfaces to a reflector as
  synthesized DeleteFuncs followed (on a backed backend like
  GitHub) by fresh AddFuncs with new UIDs. Consumers keyed on
  name are mostly OK; consumers keyed on UID see churn.
- Client-go's reflector decouples the wire from user handlers;
  slow handlers do not produce server-side broadcaster pressure.

For **EXPERIMENTS.md**:

- Mark `0008-long-lived-informer` **complete**.
- Retire the `long-lived-informer` candidate bullet under Watch
  and consistency semantics.
- New candidate: **`controller-runtime-manager-compat`** —
  controller-runtime layered on top of this reflector behavior
  is still unmeasured. That has caches, informers, and reconcile
  loops with their own assumptions. Logical next step for "does
  the ecosystem at large tolerate a synthetic-RV AA?"
- New candidate: **`watch-list-feature-gate`** — the
  `WatchListClient` feature gate (default-off in 1.32 server-side
  but default-on in 1.32 client-go) is a different wire path.
  Its behavior against our AA is untested; it may relist differently
  or not at all.
- `cert-rotation-under-watch` — this experiment partially
  answered it but we only probed the "same-CA, same-cert-name"
  rotation. "CA rotation with APIService caBundle rotation"
  remains a more operationally interesting case.

## Open questions raised

- Can we provoke the AA's 410-Gone path from a *real* client-go
  reflector? Possibly by disabling the relist-on-disconnect
  optimization, or by using `TryReconnectWithLastKnownResourceVersion`
  if such a knob exists. If it can't be provoked, does the 410
  response matter operationally at all?
- The extra UPDATE-after-ADD at startup (`InOrderInformersBatchProcess`)
  — is it universally applied to every handler, or only when
  certain feature gates line up? Worth confirming before a
  consumer depends on dedupe-on-equal-RV.
- What does controller-runtime do on top of this? Its cache wraps
  the informer and does its own reconcile-triggering; we haven't
  measured whether the 200 spurious updates at startup translate
  to 200 reconcile events or get deduped.
- Our `WatchErrorHandler` sees a string error message; is there a
  way to get the original HTTP status code out? For
  telemetry/alerting that would matter; for the experiment we
  had to infer from the string ("the server is currently unable
  to handle the request" ≈ 503; "too old resource version" ≈ 410).
