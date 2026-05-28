# Experiment 0032: lease-based-object-locking

Multi-replica library-mode aggregated apiserver where writes acquire
per-object or per-resource ownership via Kubernetes Lease objects.
Explores whether the Lease API is suitable for fine-grained
(per-object) write locking to enable horizontal write scaling
without global leader election.

## Hypothesis

Kubernetes `coordination.k8s.io/v1 Lease` objects can provide
per-object write ownership for a multi-replica AA, enabling
horizontal write scaling without global leader election.

Fundamentals touched:
- **Storage independence** (primary). Lock state is an additional
  storage axis; the Lease objects are ancillary to business data.
- **Watch and consistency semantics** (secondary). Locking is the
  write-side dual of watch consistency; without it, concurrent
  writers produce undefined ordering.

## Tracks

- **Track A (per-object)**: One Lease per (group, resource, ns, name).
  Replica acquires before writing; on contention returns 409.
- **Track B (per-resource)**: One Lease per (group, resource).
  Coarser — one replica owns all writes for a resource type.

## How to run

```bash
# 1. Create kind cluster
hack/make-kind.sh aggexp-0032

# 2. Build and load image
cd experiments/0032-lease-based-object-locking
docker build -t aggexp-0032:latest .
kind load docker-image aggexp-0032:latest --name aggexp-0032

# 3. Deploy
export KUBECONFIG=$(kind get kubeconfig-path --name aggexp-0032 2>/dev/null || echo ~/.kube/config)
kubectl config use-context kind-aggexp-0032
kubectl apply -f manifests/

# 4. Wait for pods
kubectl -n aggexp-system wait --for=condition=Ready pod -l app=aggexp --timeout=60s

# 5. Generate certs (reuse hack/gen-certs.sh adapted for this cluster)
../../hack/gen-certs.sh

# 6. Demo scenarios (see below)
```

## Demo scenarios

### Per-object: distinct objects on different replicas
```bash
# Port-forward to each pod
kubectl -n aggexp-system port-forward pod/aggexp-0 8443:8443 &
kubectl -n aggexp-system port-forward pod/aggexp-1 8444:8443 &
# Create widget-a on pod-0, widget-b on pod-1
kubectl --server=https://localhost:8443 ... create widget-a
kubectl --server=https://localhost:8444 ... create widget-b
# Both succeed (different locks)
```

### Per-object: contention on same object
```bash
# Update widget-a via pod-0 (holds lock)
# Simultaneously update widget-a via pod-1 → 409 Conflict
```

### Per-resource: any write blocks others
```bash
# With --lock-mode=per-resource:
# Create widget-a on pod-0 (acquires resource-level lock)
# Create widget-b on pod-1 → 409 Conflict (lock held by pod-0)
```

### Holder crash recovery
```bash
# Pod-0 holds lock on widget-a
kubectl -n aggexp-system delete pod aggexp-0
# After leaseDuration (15s), pod-1 can acquire
```

## Decisions made

- Lease duration 15s; chosen arbitrarily, long enough to not expire
  during normal writes, short enough for crash recovery demo.
- Lock namespace: `aggexp-locks` (dedicated namespace for lock Leases).
- Lock name format (per-object): `<group>.<resource>.<ns>.<name>`
  truncated to 63 chars via SHA256 suffix if needed.
- Lock name format (per-resource): `<group>.<resource>`.
- Conflict semantics: immediate 409 on contention (no wait/retry).
- Replica identity: `os.Getenv("POD_NAME")` falling back to hostname.
- Replica pinning: port-forward directly to pods; StatefulSet gives
  stable pod names.
- Did NOT use client-go's `resourcelock.LeaseLock` package: it's
  designed for leader election (single holder, periodic renew,
  callbacks) not for short-lived per-request lock acquisition. Direct
  `coordinationv1.Lease` Create/Get/Update is simpler.
- In-memory storage diverges between replicas — scope cut; this
  experiment is purely about locking mechanics.
- Release-on-write-complete: after a successful write, the lock is
  explicitly released (Lease deleted or holderIdentity cleared) so
  other replicas don't wait for leaseDuration.

## Status

complete
