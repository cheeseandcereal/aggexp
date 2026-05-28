# Experiment 0036: pagination-limit-continue

Library-mode aggregated apiserver implementing cursor-based pagination
(`limit` + `continue` token) in the storage adapter layer, without any
backend support for pagination.

## Hypothesis

Cursor-based pagination (`limit` + `continue` token) can be implemented
in the library layer without backend support, by truncating the full
List result and encoding a continuation token that references a
point-in-time snapshot.

Fundamentals touched:
- **Wire protocol fidelity** (primary). `limit` and `continue` are part
  of the Kubernetes List wire contract that `kubectl` uses under
  `--chunk-size`.

## How to run

```bash
# 1. Create kind cluster
kind create cluster --name aggexp-0036

# 2. Build binary
cd experiments/0036-pagination-limit-continue
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o widget-aa ./cmd/widget-aa

# 3. Build and load image
docker build -t aggexp-0036:latest .
kind load docker-image aggexp-0036:latest --name aggexp-0036

# 4. Generate certs and deploy
cd ../..
hack/gen-certs.sh --force
AGGEXP_IMAGE=aggexp-0036:latest hack/deploy.sh experiments/0036-pagination-limit-continue/manifests

# 5. Wait for pod
kubectl wait --for=condition=Ready pod -l app=aggexp -n aggexp-system --timeout=120s

# 6. Demo pagination
kubectl get widgets.widgets.aggexp.io -n default --chunk-size=5
```

## Status

complete

## Decisions made

- Continue token format: base64(`rv:offset`). Simple; the rv is the
  adapter's current resourceVersion at list time, offset is the item index.
- Stale-RV detection: if the RV in the continue token != current RV,
  return 410 ResourceExpired. This is conservative; a smarter scheme
  could detect whether items actually changed.
- 20 widgets pre-populated at startup via a PostStart hook (widget-00
  through widget-19) to give enough items for pagination demos.
- Pagination implemented as a wrapper around `runtime/storage.REST`
  rather than modifying the substrate. The wrapper intercepts `List()`
  and wraps it with pagination logic.
- Items sorted by name for deterministic pagination ordering.
- ConvertToTable override required: the substrate's adapter doesn't
  populate `Object` on TableRows or propagate `Continue`/
  `RemainingItemCount` to the Table's ListMeta. The wrapper fixes both.
  This is a substrate gap, not a pagination-specific issue.

## Prerequisites

- cluster: kind cluster `aggexp-0036`

## What we're looking to learn

- Can pagination be added purely in the adapter/library layer, with
  no backend awareness of page boundaries?
- Does `kubectl --chunk-size=N` work end-to-end against our paginated
  List implementation?
- What are the trade-offs of loading all items into memory on each
  paginated request?
