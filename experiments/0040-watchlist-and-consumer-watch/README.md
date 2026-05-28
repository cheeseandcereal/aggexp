# Experiment 0040: watchlist-and-consumer-watch

Closes the `kubectl wait --for=jsonpath` gap identified in FINDINGS/0011 for
the v1 library-mode path by emitting the `k8s.io/initial-events-end` BOOKMARK
at the tail of the Watch prefix. Additionally demonstrates push vs poll
consumer-side watch ergonomics: one resource uses direct push (the backend
calls Publisher methods), another uses a poll wrapper (the library calls
List periodically and diffs).

## Hypothesis

1. Emitting the `k8s.io/initial-events-end` BOOKMARK at the tail of the watch
   prefix closes `kubectl wait --for=jsonpath` for the v1 library mode.
2. Respecting `allowWatchBookmarks=false` (suppress BOOKMARK when the client
   does not opt in) is correct protocol behavior.
3. The library's consumer-facing watch interface can cleanly support both push
   (implementer calls Publisher methods) and poll (library polls List + diffs).

Fundamentals touched:
- **Watch and consistency semantics** (primary).
- **Storage independence** (secondary, via poll wrapper).

## How to run

```bash
# 1. Create kind cluster
kind create cluster --name aggexp-0040

# 2. Build
cd experiments/0040-watchlist-and-consumer-watch
CGO_ENABLED=0 GOOS=linux go build -o widget-aa ./cmd/widget-aa

# 3. Docker image + load
docker build -t aggexp-0040:latest .
kind load docker-image aggexp-0040:latest --name aggexp-0040

# 4. Generate certs and create secret
export KUBECONFIG=$(kind get kubeconfig --name aggexp-0040)
../../hack/gen-certs.sh --force
kubectl create namespace aggexp-system
kubectl -n aggexp-system create secret tls aggexp-certs \
  --cert=deploy/certs/tls.crt --key=deploy/certs/tls.key

# 5. Deploy
kubectl apply -f manifests/

# 6. Wait
kubectl -n aggexp-system wait --for=condition=Ready pod -l app=aggexp --timeout=90s

# 7. Test (see scenarios below)
```

## Status

complete

## Decisions made

- Reuse 0032's Widget type and OpenAPI verbatim; add Gadget as the poll-mode
  resource.
- Poll interval for Gadget: 5s. Arbitrary; short enough to observe diffs in
  a demo, long enough to not spam.
- BOOKMARK emitted only when `opts.AllowWatchBookmarks` is true per WatchList
  protocol spec.
- Poll wrapper diffs by (name -> JSON-serialized-spec); adds are ADDED,
  removals are DELETED, value changes are MODIFIED.

## Prerequisites

- cluster: kind cluster `aggexp-0040`
- tools: kind, kubectl, go, docker

## What we're looking to learn

- Does the v1 library path's Watch handler, once BOOKMARK-enhanced, satisfy
  `kubectl wait --for=jsonpath`?
- Is `allowWatchBookmarks=false` correctly suppressing the BOOKMARK?
- Can a poll-mode wrapper (~50-100 LOC) transparently provide watch semantics
  from a read-only List backend?
