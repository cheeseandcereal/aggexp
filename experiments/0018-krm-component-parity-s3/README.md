# Experiment 0018: krm-component-parity-s3

Parity re-implementation of experiment 0009 (ACK-inverted S3 Bucket
as aggregated API) on top of experiment 0013's KRM component-server
shape. The user-facing behavior — `kubectl get buckets`, `kubectl
apply -f bucket.yaml`, live S3 as the source of truth, poll-diff
watch — is intended to be identical to 0009. The implementation
pattern is inverted: instead of a substrate-linked Go apiserver
binary that `import`s a `s3backend` package, we run the generic
0013 component server (unchanged) and a separate `backend-s3` gRPC
service that uses `aws-sdk-go-v2` behind the `backend.proto`
contract.

## Hypothesis

- **Wire protocol fidelity** (primary). The 0013 component server
  can honor the Kubernetes wire contract for a real cloud-backed
  resource (0009's S3 Bucket), not just the in-memory Note of
  0013. The UX should match 0009.
- **Resource modeling freedom** (secondary). The Bucket schema
  stays the same as 0009, but is now advertised over gRPC rather
  than defined in typed Go. Whatever works / breaks should be a
  function of 0013's unstructured-registration path, not of the
  resource.
- **Storage independence** (tertiary). This is the **fourth**
  storage axis (per SYNTHESIS): component-server-plus-thin-backend,
  with the backend using an external cloud API as its source of
  truth. 0013 proved the shape; 0018 stress-tests it on 0009's
  backend.

## What this is

- `backend-s3/` — a Go gRPC service implementing `Backend` from
  `experiments/0013-krm-component-skeleton/gen`. Internally it's a
  port of 0009's `pkg/s3backend/backend.go` but without any
  `k8s.io/apiserver` imports: objects travel as JSON, errors as
  `grpc/codes`.
- `component/` — a copy of 0013's component server with its module
  path rewritten to live under 0018. Imports 0013's `gen/` module
  **directly** (via a `replace` pointing at `../../0013-.../gen`);
  no fork of the proto.
- `s3-mock/` — a verbatim copy of 0009's s3-mock (same source,
  same Dockerfile). Only the module path in `go.mod` is different.
- `manifests/` — permissive RBAC, s3-mock deployment, backend-s3
  deployment, aws credentials secret, and the aggexp deployment
  override.

## What this is not

- Not a new protocol. The `backend.proto` in 0013 is used as-is.
- Not a promotion. Everything stays under `experiments/`.
- Not modifying 0013: all edits are copies inside 0018.

## How to run

From the repo root (worktree or otherwise):

```
./hack/gen-certs.sh
kind create cluster --name aggexp-krms3
kubectl config use-context kind-aggexp-krms3
kubectl create namespace aggexp-system

./hack/deploy.sh deploy/manifests

docker build -t aggexp-krm-component:dev \
  -f experiments/0018-krm-component-parity-s3/component/Dockerfile .
docker build -t aggexp-backend-s3:dev \
  -f experiments/0018-krm-component-parity-s3/backend-s3/Dockerfile .
docker build -t aggexp-s3-mock:dev \
  experiments/0018-krm-component-parity-s3/s3-mock/

kind load docker-image aggexp-krm-component:dev --name aggexp-krms3
kind load docker-image aggexp-backend-s3:dev    --name aggexp-krms3
kind load docker-image aggexp-s3-mock:dev       --name aggexp-krms3

AGGEXP_IMAGE=aggexp-krm-component:dev \
  BACKEND_S3_IMAGE=aggexp-backend-s3:dev \
  S3_MOCK_IMAGE=aggexp-s3-mock:dev \
  ./hack/deploy.sh experiments/0018-krm-component-parity-s3/manifests

kubectl -n aggexp-system rollout status deploy/s3-mock
kubectl -n aggexp-system rollout status deploy/backend-s3
kubectl -n aggexp-system rollout status deploy/aggexp

# kubectl discovery-cache refresh if jumping from another experiment:
rm -rf ~/.kube/cache/discovery/

kubectl get buckets
cat <<'YAML' | kubectl apply -f -
apiVersion: aggexp.io/v1
kind: Bucket
metadata:
  name: my-first-bucket
spec:
  region: us-east-1
  tags:
    env: dev
    owner: lab
YAML
kubectl get bucket my-first-bucket -o yaml
kubectl get buckets -w &
WATCH_PID=$!
cat <<'YAML' | kubectl apply -f -
apiVersion: aggexp.io/v1
kind: Bucket
metadata:
  name: my-second-bucket
spec: { region: us-east-1, tags: { env: staging } }
YAML
sleep 3
kill $WATCH_PID
kubectl delete bucket my-first-bucket
kubectl explain bucket.spec
kubectl apply --server-side -f - <<'YAML'
apiVersion: aggexp.io/v1
kind: Bucket
metadata:
  name: ssa-demo
spec:
  region: us-east-1
  tags:
    env: demo
YAML
```

Against real AWS: drop the `--aws-endpoint-url` arg from
`manifests/25-backend-s3.yaml`, replace `aws-creds` with real
credentials, remove `--aws-s3-path-style=true`, and make sure
bucket names are globally unique.

## Status

complete

<!-- See FINDINGS/0018-krm-component-parity-s3.md for results. -->

## Decisions made

- **Component server copied, not symlinked.** Task explicitly
  said copy. The copy's module path was rewritten; no other code
  changes were needed (the server is already generic).
- **0013's `gen/` module is imported, not copied.** The `replace`
  directive points at `../../0013-krm-component-skeleton/gen`.
  The whole point of 0013 was that the protocol is a reusable
  contract; reuse it.
- **s3-mock copied verbatim, not symlinked.** Symlinks confuse
  Docker build contexts and `find`-based tooling; copying is
  cheaper than explaining.
- **Bucket is cluster-scoped** (namespaced=false), matching
  0009. 0013's Note was namespace-scoped to stress that path;
  0018 chooses the simpler axis because the task is parity.
- **Poll interval 15s.** Same as 0009's manifest override; short
  enough to feel responsive in a lab.
- **AWS SDK pins match 0009:** `aws-sdk-go-v2 v1.41.6`,
  `config v1.32.16`, `service/s3 v1.100.0`, `smithy-go v1.25.1`.
- **gnostic-models pinned to v0.6.8** transitively via the
  component's go.mod (inherited from 0013); no re-pinning needed
  because we imported 0013's go.mod as-is.
- **go 1.24 pin** on all three modules (backend-s3, component,
  s3-mock). No toolchain directive.
- **Kind cluster name: `aggexp-krms3`**, per task instruction.
  Distinct from 0009's `aggexp`, 0013's `aggexp-krm`.
- **On Delete, the tombstone UID is not preserved** across a
  restart of the backend-s3 pod — matching 0009's
  `uids` map. Deterministic UID (hash of bucket name +
  creationDate) would solve this; explicitly out of scope.
- **Tag clearing is not implemented** (dropping all tags in the
  spec does not call `DeleteBucketTagging`) — matching 0009.
- **The backend does NOT carry managedFields / labels /
  annotations in its JSON.** The component server's unstructured
  path may round-trip them once through the Update RPC; the
  backend drops them on the way out because S3 has no home for
  them. Informed by 0013's SSA failure + 0009's
  "managedFields don't persist" finding: they're both lost, just
  for different reasons now.

## Prerequisites

- `kind`, `kubectl`, `docker`, `go`, `envsubst`, `openssl` in PATH.
- `./hack/gen-certs.sh` has been run (produces `deploy/certs/*`).
- A fresh kind cluster named `aggexp-krms3` (see `How to run`).

## What we're looking to learn

Compared to 0009:

- Is `kubectl` UX (get / apply / explain / watch / delete) exactly
  equivalent? Differences are worth noting.
- SSA was "appears to work but managedFields vanish" in 0009. In
  0018 we inherit 0013's SSA-at-typed-converter failure. Confirm
  that failure shape; compare it to 0009's silent loss.
- Line counts: 0009's `pkg/s3backend` is ~660 lines; how much is
  the same work in 0018's `backend-s3` once Kubernetes types are
  replaced with JSON structs?
- Watch cost: 0009's poll loop runs in the AA pod; 0018's runs in
  a separate pod (`backend-s3`). Same total cost, different pod
  boundary.
