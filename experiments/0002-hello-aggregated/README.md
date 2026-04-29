# Experiment 0002: hello-aggregated

The smallest *real* aggregated API server. Built on `k8s.io/apiserver`
(not stdlib this time), etcd-less, stateless, with an in-memory
`sync.Map` backing a `Hello` resource. CRUD + watch + bookmarks + real
OpenAPI v3 via `openapi-gen`.

Where `0001-raw-http-aggregation` probed what you can get away with
by hand-rolling, this experiment establishes the opposite pole: what
the library-backed baseline looks like, and what it costs.

## Hypothesis

1. `kubectl explain` can be fixed by generating a proper OpenAPI v3
   with GVK extensions — `0001` observed this failing because the
   hand-rolled schema did not carry `x-kubernetes-group-version-kind`
   markers on schema components.
2. A Go AA with `watch.NewBroadcaster` + a monotonic uint64
   resourceVersion counter satisfies a long-lived client-go informer
   without needing etcd.
3. `kubectl apply` (merge-patch) and `kubectl apply --server-side` both
   work without implementing any field-management logic of our own —
   the library's generic PATCH path handles the merge when we satisfy
   `rest.Patcher` (= Getter + Updater).
4. Dropping `RecommendedOptions.Etcd` does not break startup if we
   substitute a custom Options struct that composes the other
   generic options (SecureServing, DelegatingAuth*, CoreAPI, Audit,
   Features) a la metrics-server.

## What we're looking to learn

- **Wire protocol fidelity.** What does a "real" library-backed AA
  pass/fail on the compat scoreboard vs. `0001`? Particularly
  `kubectl explain`, `kubectl apply`, `kubectl apply --server-side`.
- **Storage independence.** Is a stateless AA with in-memory
  `sync.Map` enough to satisfy client-go's informer contract in
  practice?
- **Watch and consistency semantics.** Does a broadcaster-backed
  watch with a global monotonic RV work cleanly? How do bookmarks
  interact with `AllowWatchBookmarks`?

Later experiments (custom authorizer, github driver) layer on top of
this AA; this experiment itself does none of that. Identity is observed
in a per-request log line but not used for authz.

## How to run

From repo root:

```
./hack/gen-certs.sh
./hack/make-kind.sh
./hack/deploy.sh deploy/manifests

# Build and load the image
docker build -t aggexp-hello:dev experiments/0002-hello-aggregated/
kind load docker-image aggexp-hello:dev --name aggexp

# Apply this experiment's deployment overlay
AGGEXP_IMAGE=aggexp-hello:dev \
  ./hack/deploy.sh experiments/0002-hello-aggregated/manifests

kubectl -n aggexp-system rollout status deploy/aggexp
kubectl api-resources | grep hellos
kubectl get hellos
kubectl apply -f - <<'YAML'
apiVersion: aggexp.io/v1
kind: Hello
metadata:
  name: world
spec:
  greeting: Hello from kubectl apply
YAML
kubectl get hello world -o yaml
kubectl get hellos -w &
WATCH_PID=$!
kubectl apply -f - <<'YAML'
apiVersion: aggexp.io/v1
kind: Hello
metadata:
  name: friend
spec:
  greeting: Hey friend
YAML
sleep 3
kill $WATCH_PID 2>/dev/null
kubectl delete hello world
kubectl explain hello

./hack/test-compat.sh
```

### Regenerating generated code

`zz_generated.deepcopy.go` and `pkg/generated/openapi/zz_generated.openapi.go`
are checked in. Regenerate only when `pkg/apis/aggexp/v1/types.go` or
its dependencies change:

```
cd experiments/0002-hello-aggregated
./hack/update-codegen.sh
```

The script shells out to `kube_codegen.sh` shipped with
`k8s.io/code-generator`. `go` and a writable `$GOPATH` are the only
hard requirements.

## Status

complete

<!-- See FINDINGS/0002-hello-aggregated.md for results. -->


## Decisions made

- **Module path**: `github.com/cheeseandcereal/aggexp/experiments/0002-hello-aggregated`.
  Opts into its own `go.mod`, per AGENTS.md: experiments that depend
  on heavy libraries (`k8s.io/apiserver@v0.32`) pin their own deps so
  other experiments can continue to use stdlib-only. The root repo
  module remains dependency-free.
- **Group/version**: `aggexp.io/v1`. Same group as `0001`, so they
  cannot run simultaneously — intentional: we only run one
  experiment at a time against the kind cluster.
- **ResourceVersion scheme**: global `atomic.Uint64`, decimal
  stringified. Mimics etcd's modRevision. On `?resourceVersion=X`
  watch requests, if X is older than our current RV and we don't
  have events to replay, we return `410 Gone (ResourceExpired)`
  so the reflector relists cleanly.
- **Watch event buffer**: `watch.NewBroadcaster(100, DropIfChannelFull)`.
  Arbitrary; if a slow watcher shows up, it gets dropped rather than
  blocking the writer.
- **Bookmark interval**: 10s, emitted unconditionally. Note that this
  violates `AllowWatchBookmarks=false` semantics — we do not filter
  per-watcher. Documented as an intentional cut.
- **No admission plugins compiled in.** If later experiments need
  admission, they wire it themselves.
- **No clientset / lister / informer generation.** This AA is the
  server; we don't need a Go client for this experiment. Later
  experiments that want a client can add the generation.
- **k8s.io/apiserver pinned at v0.32.3** matching the kind cluster
  version (v1.32). These staging modules must always move together.
- **No field management / managedFields tracking of our own.** The
  generic PATCH path handles SSA merging via `UpdatedObjectInfo`;
  we persist the merged object. We trust the library's field manager.
- **Single replica, single-binary.** No leader election, no HA.

## Prerequisites

- kind cluster `aggexp` created via `hack/make-kind.sh`.
- Serving cert generated by `hack/gen-certs.sh`.
- Base manifests applied via `hack/deploy.sh deploy/manifests`.
