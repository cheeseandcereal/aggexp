# Experiment 0039: optimistic-concurrency

Library-mode aggregated apiserver for `widgets.aggexp.io/v1` that
implements optimistic concurrency control (resourceVersion conflict
detection) in the storage adapter's Update path. Forked from 0032's
working library-mode pattern, stripped of Lease-based locking, and
augmented with a local OCC wrapper that compares incoming RVs against
stored RVs before allowing writes.

## Hypothesis

Rejecting writes with stale resourceVersions (optimistic concurrency
control) is implementable in the library layer by comparing the
incoming object's RV against the current stored RV before calling
Backend.Update. This is the standard Kubernetes behavior: Update with
a stale `metadata.resourceVersion` should return 409 Conflict.

Fundamentals touched:
- **Watch and consistency semantics** (primary). resourceVersion is
  the foundation of watch correctness; OCC enforces its semantic
  meaning on the write path.
- **Storage independence** (secondary). OCC is a library-layer
  concern, independent of the backend's storage mechanism.

## How to run

```bash
# 1. Create kind cluster
kind create cluster --name aggexp-0039

# 2. Build the binary
cd experiments/0039-optimistic-concurrency
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o widget-aa ./cmd/widget-aa/

# 3. Build and load Docker image
docker build -t aggexp-0039:latest .
kind load docker-image aggexp-0039:latest --name aggexp-0039

# 4. Generate and install certs
CERT_DIR=$(mktemp -d)
openssl genrsa -out "$CERT_DIR/ca.key" 2048
openssl req -x509 -new -nodes -key "$CERT_DIR/ca.key" -subj "/CN=aggexp-ca" -days 3650 -out "$CERT_DIR/ca.crt"
openssl genrsa -out "$CERT_DIR/tls.key" 2048
openssl req -new -key "$CERT_DIR/tls.key" -subj "/CN=aggexp.aggexp-system.svc" \
  -addext "subjectAltName=DNS:aggexp.aggexp-system.svc,DNS:aggexp.aggexp-system.svc.cluster.local,DNS:localhost,IP:127.0.0.1" \
  -out "$CERT_DIR/tls.csr"
openssl x509 -req -in "$CERT_DIR/tls.csr" -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" \
  -CAcreateserial -out "$CERT_DIR/tls.crt" -days 3650 \
  -extfile <(printf "subjectAltName=DNS:aggexp.aggexp-system.svc,DNS:aggexp.aggexp-system.svc.cluster.local,DNS:localhost,IP:127.0.0.1")
kubectl --context kind-aggexp-0039 create namespace aggexp-system
kubectl --context kind-aggexp-0039 -n aggexp-system create secret tls aggexp-certs \
  --cert="$CERT_DIR/tls.crt" --key="$CERT_DIR/tls.key"

# 5. Deploy
kubectl --context kind-aggexp-0039 apply -f manifests/

# 6. Wait for pod
kubectl --context kind-aggexp-0039 -n aggexp-system wait --for=condition=Ready pod -l app=aggexp --timeout=60s

# 7. Demo scenarios
# See Demo scenarios section below.
```

## Demo scenarios

### Create (no RV) always succeeds
```bash
kubectl --context kind-aggexp-0039 apply -f - <<'EOF'
apiVersion: widgets.aggexp.io/v1
kind: Widget
metadata:
  name: test-create
spec:
  color: blue
  size: large
EOF
# Expect: 201 Created
```

### Correct-RV update succeeds
```bash
# Get current RV
RV=$(kubectl --context kind-aggexp-0039 get widget test-create -o jsonpath='{.metadata.resourceVersion}')
# Update with correct RV
kubectl --context kind-aggexp-0039 apply -f - <<EOF
apiVersion: widgets.aggexp.io/v1
kind: Widget
metadata:
  name: test-create
  resourceVersion: "$RV"
spec:
  color: red
  size: large
EOF
# Expect: 200 OK
```

### Stale-RV update returns 409
```bash
# Get current RV
RV=$(kubectl --context kind-aggexp-0039 get widget test-create -o jsonpath='{.metadata.resourceVersion}')
# Do a successful update (bumps RV)
kubectl --context kind-aggexp-0039 patch widget test-create --type=merge -p '{"spec":{"size":"medium"}}'
# Now try to update with the OLD RV
kubectl --context kind-aggexp-0039 get widget test-create --raw | \
  jq ".metadata.resourceVersion=\"$RV\" | .spec.color=\"green\"" | \
  kubectl --context kind-aggexp-0039 replace -f -
# Expect: 409 Conflict
```

### Concurrent patches: one wins, one loses
```bash
# Create a widget
kubectl --context kind-aggexp-0039 apply -f - <<'EOF'
apiVersion: widgets.aggexp.io/v1
kind: Widget
metadata:
  name: race-widget
spec:
  color: white
  size: small
EOF
# Fire two patches in parallel
kubectl --context kind-aggexp-0039 patch widget race-widget --type=merge -p '{"spec":{"color":"red"}}' &
kubectl --context kind-aggexp-0039 patch widget race-widget --type=merge -p '{"spec":{"color":"blue"}}' &
wait
# One succeeds, one may get 409 (depends on timing)
```

## Status

complete

## Decisions made

- Update with empty resourceVersion: reject with "resourceVersion must be specified for an update" — matches standard kube behavior.
- OCC wrapper lives in pkg/occ/ as a local fork of the storage adapter's Update path; substrate code untouched.
- Single-replica deployment (OCC is a single-process concern; 0032/0033 handle cross-replica).
- RV stamping reuses the substrate's PublishAdded/PublishModified which auto-increment the atomic counter.
- The OCC check runs AFTER Get returns the current object but BEFORE calling Backend.Update.
- SSA (`kubectl apply --server-side`) fails due to incomplete OpenAPI schema (missing apiVersion/kind field declarations); this is a schema-completeness issue orthogonal to OCC.
- The OCC Store maintains a per-object RV map because the substrate's REST adapter doesn't persist assigned RVs back into the backend. The wrapper stamps tracked RVs on Get/List responses.

## Prerequisites

- cluster: kind cluster `aggexp-0039`
- No secrets or external systems required.

## What we're looking to learn

**Watch and consistency semantics**: Is the library layer the right
place for optimistic concurrency control? Does the simple
"compare incoming RV vs stored RV" check compose cleanly with the
substrate's synthetic-RV scheme? How does it interact with kubectl
apply (client-side and server-side)?
