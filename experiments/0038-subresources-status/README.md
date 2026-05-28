# Experiment 0038: subresources-status

A library-mode AA for `widgets.aggexp.io/v1 Widget` that registers both
a main resource storage and a `/status` subresource storage in the same
`Resources` map. The main resource's Update preserves status; the status
subresource's Update preserves spec.

## Hypothesis

The `/status` subresource pattern (separate Update paths for spec vs
status) can be supported in the library via registering a second
`rest.Storage` at `"widgets/status"` in the Resources map. The main
resource's Update rejects status changes; the status subresource's Update
rejects spec changes. SSA field ownership will track subresource
ownership separately out of the box because the apiserver routes
`/status` updates through a different storage path.

## How to run

```bash
# From repo root
kind create cluster --name aggexp-0038

# Build
cd experiments/0038-subresources-status
CGO_ENABLED=0 GOOS=linux go build -o widget-aa ./cmd/widget-aa/

# Load into kind
docker build -t aggexp-0038:latest .
kind load docker-image aggexp-0038:latest --name aggexp-0038

# Deploy
kubectl apply -f manifests/all.yaml
kubectl -n aggexp-system rollout status deployment/aggexp --timeout=60s

# Test main resource
kubectl apply -f manifests/widget-sample.yaml
kubectl get widgets.widgets.aggexp.io foo -o yaml

# Test status subresource
kubectl patch widgets.widgets.aggexp.io foo --type=merge \
  --subresource=status -p '{"status":{"phase":"Active","message":"running"}}'
kubectl get widgets.widgets.aggexp.io foo -o yaml

# SSA test
kubectl apply --server-side --field-manager=spec-mgr -f manifests/widget-sample.yaml
kubectl apply --server-side --field-manager=status-mgr --subresource=status \
  -f manifests/widget-status.yaml
kubectl get widgets.widgets.aggexp.io foo -o yaml | grep -A30 managedFields
```

## Status

complete

## Decisions made

- Chose `Deployment` with 1 replica; multi-replica is not relevant here.
- Reuse the 0032 library-mode pattern (runtime/server + runtime/group + runtime/storage).
- Status subresource storage implements only rest.Getter + rest.Updater (no Creater/Deleter/Lister/Watcher).
- Namespace-scoped widgets for consistency with 0032.
- The main resource Update preserves existing status on every write. The status subresource Update preserves existing spec on every write.
- OpenAPI must include `apiVersion` and `kind` as top-level properties for SSA typed-converter to work.
- ObjectMeta `$ref` must use the `ref` callback (not a hardcoded `#/definitions/` path) for SSA schema resolution.
- `Dependencies` field on `OpenAPIDefinition` required for proper ref resolution.
- `TableRow.Object` must be set for table rendering to work (empty `RawExtension{}` triggers "object does not implement the Object interfaces").

## Prerequisites

- cluster: kind cluster `aggexp-0038`
- Go 1.24+, docker, kind, kubectl

## What we're looking to learn

**Resource modeling freedom**: Can the library-mode AA support the
standard Kubernetes spec/status split via subresource registration?
Does the genericapiserver machinery route `/status` requests to the
separate storage automatically? Does SSA track field ownership per
subresource out of the box?

**Wire protocol fidelity**: Does `kubectl api-resources` show the
status subresource? Does `kubectl patch --subresource=status` work?
What OpenAPI metadata is needed to declare the subresource?
