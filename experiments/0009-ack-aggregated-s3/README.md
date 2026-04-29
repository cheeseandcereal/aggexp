# Experiment 0009: ack-aggregated-s3

An AWS Controllers for Kubernetes (ACK)-style integration inverted:
instead of a **CRD + controller** pair (objects persisted in etcd;
separate process reconciles them against AWS), this is an
**aggregated API server** for which **AWS is the sole source of
truth**. The kubernetes resource representation has no backing
stateful store. `kubectl get buckets` is a live `ListBuckets` call
to AWS. `kubectl apply` is a live `CreateBucket` / `PutBucketTagging`.
Watch is re-implemented as a poll loop that diffs S3's state and
emits events.

The specific resource: AWS S3 buckets, modeled as
`buckets.aggexp.io/v1`.

## Hypothesis

1. An aggregated apiserver, with no persistence of its own, can
   be a drop-in replacement for a CRD+controller for a real cloud
   resource. `kubectl apply / get / delete` work end-to-end; watch
   emits real events.
2. Inverting the source-of-truth eliminates whole categories of
   problem a controller has to handle: drift between desired and
   actual state, reconciler backoff after transient AWS errors,
   "who's authoritative when etcd and AWS disagree", garbage from
   stale finalizers.
3. It creates a different category of problem, centered on
   per-request latency (every read is a round trip to AWS), partial
   failures with no retry loop, and the absence of declarative
   desired-state that humans and other tools can introspect.
4. Standard kubernetes tooling (`kubectl apply`, SSA, explain,
   watch-based informers) works without any special accommodation
   for the unusual storage model.

## What this is

- `pkg/s3backend/` — a `runtime/storage.Backend` + `WritableBackend`
  implementation whose Get/List are live S3 calls, Create/Update/
  Delete issue the corresponding S3 API call, and which runs a
  poll loop only to emit watch events.
- `pkg/apis/aggexp/v1/types.go` — `Bucket` with `Spec` (region,
  tags) and `Status` (region, creationDate, observedAt, phase).
- `s3-mock/` — a tiny stdlib HTTP server speaking the subset of
  the S3 XML wire protocol aws-sdk-go-v2 uses for ListBuckets,
  HeadBucket, CreateBucket, DeleteBucket, Get/PutBucketTagging.
  Keeps the experiment hermetic. Swap `--aws-endpoint-url` to real
  AWS and the same code works there.
- `pkg/server/`, `cmd/aggexp-s3/` — thin wiring over the
  `runtime/` substrate, matching the 0007 pattern.

## What this is not

- Not a production replacement for ACK. There's no retry, no
  eventual consistency handling beyond "next poll sees the truth",
  no cross-region story, no IAM integration beyond the credential
  chain.
- Not a writable-everything model. Only `spec.tags` is reconciled
  on update. Region is immutable on S3 after create; changing it
  would require delete-then-create and is out of scope.
- Not efficient at scale: a `kubectl get buckets` call incurs an
  AWS ListBuckets. An informer watching thousands of buckets would
  produce proportional AWS traffic.

## How to run

Requires a kind cluster, kubectl, docker, and the repo's base
manifests applied.

```
./hack/gen-certs.sh
./hack/make-kind.sh
./hack/deploy.sh deploy/manifests

docker build -t aggexp-s3:dev       experiments/0009-ack-aggregated-s3/
docker build -t aggexp-s3-mock:dev  experiments/0009-ack-aggregated-s3/s3-mock/
kind load docker-image aggexp-s3:dev       --name aggexp
kind load docker-image aggexp-s3-mock:dev  --name aggexp

AGGEXP_IMAGE=aggexp-s3:dev \
S3_MOCK_IMAGE=aggexp-s3-mock:dev \
  ./hack/deploy.sh experiments/0009-ack-aggregated-s3/manifests

kubectl -n aggexp-system rollout status deploy/s3-mock
kubectl -n aggexp-system rollout status deploy/aggexp

# Refresh kubectl's discovery cache if you were just on a different
# experiment:
rm -rf ~/.kube/cache/discovery/

kubectl get buckets                         # live ListBuckets on mock S3
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
kubectl get buckets
kubectl get bucket my-first-bucket -o yaml
kubectl get buckets -w &                    # polling-driven watch
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

# Swap the mock for real AWS:
#   - drop the --aws-endpoint-url flag in 30-aggexp-deployment-override
#   - replace AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY with real creds
```

Against real AWS, remember bucket names are globally unique.
Consider a per-user prefix.

## Status

complete

<!-- See FINDINGS/0009-ack-aggregated-s3.md for results. -->

## Decisions made

- **Resource name == S3 bucket name.** No escaping. kubernetes
  name regex is stricter than S3's in some respects but the
  intersection (DNS-safe lowercase) covers almost all realistic
  bucket names.
- **Read paths are always live.** `Get` issues `HeadBucket +
  GetBucketTagging`; `List` issues `ListBuckets`. The cache in
  the backend is *only* for diff-based watch emission, never
  served on reads. This is the point of the experiment.
- **No retry loop for partial failures on Create.** If
  `CreateBucket` succeeds but `PutBucketTagging` fails, the AA
  returns an error; the bucket exists on AWS untagged; the user
  retries. A CRD+controller would retry forever; we put the
  onus on the caller. Documented as a deliberate design choice.
- **Tag-clear is not implemented.** If the user's spec drops all
  tags, we do NOT call `DeleteBucketTagging`. Partial scope cut;
  adding it is trivial.
- **Poll interval: 15s in manifests, 30s default in code.**
  Shorter than 0004's 60s because tag changes on S3 aren't
  visible from ListBuckets — watch events driven by drift are
  coarse. A longer interval makes the experiment feel sluggish
  in a lab.
- **Mock is a hand-rolled HTTP server.** ~250 lines. Honors the
  0003/0006 precedent. Swapping to real AWS is one flag change.
- **No SigV4 verification in the mock.** aws-sdk-go-v2 signs
  every request; the mock accepts anything. A production-style
  mock would verify. Irrelevant for probing aggregated-API
  behavior.
- **`status.phase` is a two-value string** ("Ready" on a
  successful observation; unset otherwise). Not the full
  `Conditions` convention — this experiment is probing storage,
  not status conventions.

## Prerequisites

- kind cluster `aggexp` with the base manifests applied.
- Serving cert via `hack/gen-certs.sh`.

For real AWS: credentials in the environment or a mounted
shared-credentials file; `AWS_REGION` set; bucket names chosen
to avoid global collisions.

## What we're looking to learn

- **Storage independence** (primary). Is "AWS is the sole source
  of truth" a viable model for an aggregated API, or do corner
  cases force a local cache of intent?
- **Wire protocol fidelity.** Does the standard tooling
  (`kubectl apply`, SSA, explain, watch) work unchanged when
  the backend is a remote cloud API?
- **Per-request authorization** (secondary). The authorizer
  interface is inherited from the substrate but not specifically
  exercised here; future experiments can add policy.
- **Resource modeling freedom.** Does the Backend interface
  accommodate "cloud resource with globally-unique names and
  async semantics" without accommodations?
