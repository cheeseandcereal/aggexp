# Experiment 0037: field-selectors

Library-mode aggregated apiserver implementing resource-specific field
selectors in the adapter layer. Extends the `runtime/storage` pattern
with field-selector parsing, validation (reject unknown fields with
422), and defensive filtering on both List and Watch paths.

## Hypothesis

Resource-specific field selectors can be implemented in the library
layer via: (1) a `SelectableFields() []string` method on a new
interface the Backend optionally implements, (2) the adapter parses the
field selector from `metainternalversion.ListOptions.FieldSelector`,
validates against declared fields, and applies defensive filtering
after the backend returns. Kubernetes always supports `metadata.name`
and `metadata.namespace` implicitly. This is a library-level concern
(resource modeling freedom) with wire-protocol implications (field
selectors are part of the standard kubectl/client-go List contract).

Fundamentals touched:
- **Resource modeling freedom** (primary). Field selectors define which
  fields are queryable for a given resource type.
- **Wire protocol fidelity** (secondary). `--field-selector` is a
  standard kubectl parameter that kube-apiserver passes through to
  aggregated APIs in the ListOptions.

## How to run

```bash
# 1. Create kind cluster
kind create cluster --name aggexp-0037

# 2. Build binary
cd experiments/0037-field-selectors
CGO_ENABLED=0 GOOS=linux go build -o widget-aa ./cmd/widget-aa/

# 3. Build and load image
docker build -t aggexp-0037:latest .
kind load docker-image aggexp-0037:latest --name aggexp-0037

# 4. Generate certs and create secret
cd ../..
hack/gen-certs.sh --force
kubectl --context kind-aggexp-0037 create namespace aggexp-system
kubectl --context kind-aggexp-0037 -n aggexp-system create secret tls aggexp-certs \
  --cert=deploy/certs/tls.crt --key=deploy/certs/tls.key

# 5. Deploy
kubectl --context kind-aggexp-0037 apply -f experiments/0037-field-selectors/manifests/

# 6. Wait for pod ready
kubectl --context kind-aggexp-0037 -n aggexp-system wait --for=condition=Ready pod -l app=aggexp --timeout=60s

# 7. Test field selectors
kubectl --context kind-aggexp-0037 get widgets --field-selector spec.color=red
kubectl --context kind-aggexp-0037 get widgets --field-selector metadata.name=widget-03
kubectl --context kind-aggexp-0037 get widgets --field-selector spec.color=red,spec.size=large
kubectl --context kind-aggexp-0037 get widgets --field-selector spec.bogus=x  # expect error
kubectl --context kind-aggexp-0037 get widgets -l app=demo --field-selector spec.color=blue
kubectl --context kind-aggexp-0037 get widgets -w --field-selector spec.color=red
```

## Status

complete

## Decisions made

- Widget spec fields: color, size, priority. Three fields gives enough
  surface for intersection queries.
- Pre-populate 10 widgets at startup with varying field values to avoid
  needing manual creates for basic demos.
- Field accessor uses a simple switch-case on known paths rather than a
  generic dotted-path walker. Experiment code; no need for generality.
- Selectable fields: spec.color, spec.size, spec.priority (declared via
  SelectableFields interface). metadata.name and metadata.namespace are
  always implicitly supported.
- Unknown field returns the library's built-in BadRequest via
  AddFieldLabelConversionFunc; the library intercepts before our List
  handler runs.
- Namespace-scoped widgets in the "default" namespace for simplicity.
- Single-replica Deployment (not StatefulSet); no locking needed for
  this experiment.
- Forked the storage adapter locally (pkg/storage/) rather than
  modifying the substrate. Minimal diff: added field selector parsing,
  validation, and filtering to List/Watch; added ConvertToTable with
  per-row Object references; added setListItemsReflect fallback.
- AddFieldLabelConversionFunc is the required mechanism to declare
  selectable fields to the apiserver library; without it, the library
  rejects all non-metadata field selectors before reaching the List
  handler.
- Used fields.Selector.Requirements() for manual matching rather than
  fields.Set.Matches() because we need per-field accessor dispatch.

## Prerequisites

- cluster: kind cluster `aggexp-0037`

## What we're looking to learn

1. Can field selectors be implemented cleanly in the adapter layer
   without modifying the substrate?
2. What does the accessor function complexity look like?
3. Does watch filtering with field selectors compose correctly with
   label selectors?
4. What error shape does kube-apiserver/kubectl expect for unknown
   field paths?
5. How does the field selector arrive at the AA (via
   metainternalversion.ListOptions.FieldSelector)?
