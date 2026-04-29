# Experiment 0008: long-lived-informer

Stand up a small client-go `SharedInformer` against the 0002
`hellos.aggexp.io/v1` aggregated apiserver, let it run, and poke
at it to characterise the **Watch and consistency semantics**
fundamental's untested edges:

- How often does a reflector relist vs. reconnect watch, and what
  `resourceVersion` does it present on reconnect?
- How does it handle `410 Gone` (`ResourceExpired`) — clean relist,
  crash, or hot-loop?
- Does a serving-cert rotation mid-watch reconnect cleanly?
- After an AA restart (UIDs regenerate per 0004) does the informer
  see ADDED for every object, or DELETE+ADD, or silent update?
- When the broadcaster drops events under `DropIfChannelFull`
  pressure, what does that look like downstream — silent loss, 410,
  or something else?

## Hypothesis

The library handles all four scenarios "correctly" in the
hand-wavy sense: reflector recovers cleanly, with a relist on 410;
cert rotation triggers TLS re-handshake + reconnect; UID churn is
surfaced as MODIFIED with a changed `.metadata.uid` (the reflector
doesn't treat UID as identity); dropped events eventually cause
the server-side broadcaster to close the watcher which makes the
reflector retry. I expect at least one of these to be subtly wrong
in a way worth writing down.

## What we're looking to learn

Primary fundamental: **watch and consistency semantics**. The 0002
and 0004 findings left a concrete question: what does a reflector
actually do across the boundaries we know exist (pod restart, 410,
cert rotation) over a multi-hour window? We do not need hours —
minutes suffice to drive every event — but the program is shaped
so the user can leave it running.

Secondary: confirms/contradicts 0004's "pod-restart amnesia" claim
from the *consumer* side. 0004 saw the server regenerate UIDs; 0008
observes what the reflector does with that.

## How to run

Prereq: `kind`, `kubectl`, `docker`, `go`. A fresh kind cluster is
used (`aggexp-informer`) so this experiment does not step on any
parallel experiments running against the default `aggexp` cluster.

```sh
# From repo root.

# 1. Make the dedicated cluster.
kind create cluster --name aggexp-informer
kubectl --context kind-aggexp-informer create namespace aggexp-system

# 2. Generate serving cert (shared deploy/certs/ with the default cluster).
./hack/gen-certs.sh

# 3. Build and load the 0002 AA image into the informer cluster.
docker build -t aggexp-hello:dev experiments/0002-hello-aggregated/
kind load docker-image aggexp-hello:dev --name aggexp-informer

# 4. Apply base + 0002 overlay against the informer cluster.
KUBECONFIG=~/.kube/config kubectl config use-context kind-aggexp-informer
./hack/deploy.sh deploy/manifests
AGGEXP_IMAGE=aggexp-hello:dev ./hack/deploy.sh experiments/0002-hello-aggregated/manifests
kubectl -n aggexp-system rollout status deploy/aggexp

# 5. Build and load the watcher image.
docker build -t aggexp-watcher:dev experiments/0008-long-lived-informer/
kind load docker-image aggexp-watcher:dev --name aggexp-informer

# 6. Deploy the watcher.
kubectl apply -f experiments/0008-long-lived-informer/manifests/
kubectl -n aggexp-system rollout status deploy/aggexp-watcher

# 7. Tail logs and drive scenarios.
kubectl -n aggexp-system logs -f deploy/aggexp-watcher &
# Scenario 1: baseline — just wait.
# Scenario 2: 410 — `kubectl -n aggexp-system scale deploy/aggexp --replicas=0`
#   and back to 1.
# Scenario 3: cert rotation —
#   ./hack/gen-certs.sh --force
#   kubectl -n aggexp-system delete secret aggexp-serving-cert
#   ./hack/deploy.sh deploy/manifests
#   kubectl -n aggexp-system rollout restart deploy/aggexp
# Scenario 4: slow watcher — set WATCHER_SLEEP_MS=5000 on the Deployment
#   and create/update/delete Hellos in a tight loop from `kubectl`.

# 8. Query the status endpoint.
kubectl -n aggexp-system port-forward svc/aggexp-watcher 8080:8080 &
curl localhost:8080/status
```

`experiments/0008-long-lived-informer/hack/` holds optional helper
scripts for scenarios 2-4.

## Status

complete

<!-- See FINDINGS/0008-long-lived-informer.md. -->

## Decisions made

- **Target: 0002 (`hellos.aggexp.io/v1`)**, not 0004. Hellos are
  hand-drivable from the CLI (`kubectl create/apply/delete hello
  foo`), which makes scenario 2 (410 trigger via AA restart) and
  scenario 4 (slow watcher) much easier to drive deterministically.
  0004's polling loop adds a 60s latency floor between every
  observable state change.
- **Dedicated kind cluster `aggexp-informer`.** Experiment isolation.
  Avoids stepping on any parallel experiment running in `aggexp`.
- **client-go `dynamicinformer.DynamicSharedInformerFactory`** rather
  than a typed informer. Zero codegen; works against any GVR. The
  point of the experiment is the reflector's wire behavior, not the
  type system.
- **Resync period: 30 minutes.** Nonzero so we observe a natural
  resync cycle if a run goes that long. Shorter would pollute the
  signal.
- **Status endpoint on :8080, plain HTTP.** No TLS, no auth. This
  is a lab; the service is cluster-scoped.
- **Heartbeat every 30s** to disambiguate silence-because-nothing-happened
  from silence-because-watcher-is-dead.
- **`WATCHER_SLEEP_MS` env var** to artificially block the event
  handler. Default 0; set nonzero to exercise scenario 4.
- **Logs plain `key=value` lines on stdout**, not klog structured —
  keeps the findings file readable when we paste snippets.

## Prerequisites

- kind cluster `aggexp-informer` (create via
  `kind create cluster --name aggexp-informer`).
- Serving cert from `hack/gen-certs.sh`.
- Base manifests + 0002 overlay deployed to the informer cluster.

## What we're looking to learn

**Watch and consistency semantics.** Concretely, for a client-go
`SharedInformer` sustained against a synthetic-RV AA:

1. Relist cadence — what drives it? (default config, explicit resync,
   reconnect, 410?)
2. What RV does the reflector present after a watch disconnect?
3. What does it do when that RV is unsatisfiable (our 0002 AA
   returns 410)?
4. Does TLS cert rotation visible at the aggregation layer surface
   to the client at all?
5. What happens on AA pod restart — apparent delete+add churn, or
   MODIFIED-in-place with a new UID, or something else?
6. Can the broadcaster's `DropIfChannelFull` be observed from the
   client side, and how?
