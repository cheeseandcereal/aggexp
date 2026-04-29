# Findings — 0009 ack-aggregated-s3

## What we were trying to learn

ACK (AWS Controllers for Kubernetes) is the dominant AWS
integration pattern in Kubernetes-land: a CRD per AWS resource
type + a separate controller process that reconciles etcd-stored
desired-state against AWS. This experiment inverts that model.

The thesis under test: **an aggregated API server can replace the
CRD+controller pair entirely**, holding no state of its own, using
AWS as the sole source of truth for both desired and observed
state. `kubectl get` is a live `ListBuckets`. `kubectl apply` is a
live `CreateBucket` + `PutBucketTagging`. Watch is a poll loop that
diffs AWS state and fans events out through the substrate's
broadcaster.

Four hypotheses:

1. A stateless AA can be a drop-in replacement for a CRD+controller
   for a real cloud resource. kubectl CRUD + watch + explain all
   work.
2. Inverting the source-of-truth eliminates whole categories of
   problem the controller model has to handle (drift, reconciler
   backoff, stale finalizers).
3. It creates a different category of problem, centered on
   per-request latency, partial failures without a retry loop, and
   the absence of declarative desired-state as a discoverable
   artifact.
4. Standard kubernetes tooling works without accommodation.

## What we did

- Forked 0007 as substrate-consumer scaffolding. Swapped the
  File type for Bucket (`spec.region`, `spec.tags`;
  `status.region`, `creationDate`, `observedAt`, `phase`).
- Wrote `pkg/s3backend/` as a `runtime/storage.WritableBackend`
  whose Get/List are live S3 calls and Create/Update/Delete
  issue the corresponding S3 API calls. The polling loop exists
  only to generate watch events, not to populate a read cache.
- Wrote `s3-mock/`: ~250 lines of stdlib HTTP implementing
  ListBuckets, HeadBucket, CreateBucket, DeleteBucket, and
  Get/PutBucketTagging in the real S3 XML wire format.
  aws-sdk-go-v2 signs requests via its normal SigV4 path; the
  mock ignores signatures but parses the SDK's request bodies.
- Deployed to an isolated `aggexp-s3` kind cluster. Ran end-to-end
  against the mock: apply, list, get, watch, delete, update,
  server-side apply, impersonation-adjacent operations.

The experiment deliberately does not use real AWS in its
automated path. The mock endpoint is a one-flag swap
(`--aws-endpoint-url=`) away from real `api.s3.us-east-1.amazonaws.com`.

## What we observed

### The baseline works end-to-end

`kubectl apply -f bucket.yaml` creates a real bucket on mock S3.
`kubectl get buckets` returns it. `kubectl delete` removes it.
`kubectl get -w` streams events from the poll loop. `kubectl
explain bucket.spec` prints field docstrings from the generated
OpenAPI. No etcd on the AA side; no controller loop; no CRD.

Timing from the mock's logs on a typical apply:
```
HEAD /my-first-bucket          (exists check)
PUT  /my-first-bucket          (create)
PUT  /my-first-bucket?tagging  (tags)
HEAD /my-first-bucket          (response live-read)
GET  /my-first-bucket?tagging
GET  /                         (next scheduled poll)
HEAD /my-first-bucket          (subsequent kubectl get)
GET  /my-first-bucket?tagging
```

Seven round-trips to S3 to service one apply-and-get. That count
is a real cost; see consequents.

### SSA field management does NOT persist

`kubectl apply --server-side` **appears to succeed**
(`bucket.aggexp.io/ssa-demo serverside-applied`) but
`managedFields` is absent from subsequent GETs:

```
$ kubectl get --raw /apis/aggexp.io/v1/buckets/ssa-demo | jq .metadata
{
  "name": "ssa-demo",
  "uid": "...",
  "creationTimestamp": "..."
}
```

Tracing through the library with debug logs: the substrate
adapter's Update does receive an object with
`len(managedFields)=1` from the library's field manager. Our
backend's Update preserves it and returns it on the immediate
response path. But because the AA holds **no state**, the next
GET re-reads from S3, and S3 doesn't know about managedFields.

This is a **fundamental** finding about storage-independence +
SSA: **managedFields are library-layer state that the AA is
responsible for persisting**. Without a backing store, SSA
degrades to "appears to work but loses field ownership tracking
between requests." An informed caller would observe this (try to
apply from a different field manager and watch for conflict →
there's nothing to conflict against).

There are three ways to reconcile this:

1. **Don't support SSA.** Return 415 on apply-patch. The library
   makes this awkward — SSA is enabled by default.
2. **Persist managedFields in the backend.** Encode them as a
   magic tag prefix on the S3 bucket, or in a sidecar store.
   Working around the source-of-truth thesis to re-introduce
   state.
3. **Accept the limitation.** SSA works as a one-shot apply but
   not as the full ownership-tracking mechanism. Document it.
   This experiment picks option 3.

The implication for the ACK-as-AA thesis is sharp: **SSA is not
free under the inverted storage model**. A real production
replacement for ACK would have to choose between (a) abandoning
SSA, (b) writing ownership tracking back to the backend somehow,
or (c) shadow-storing managedFields in etcd — which resurrects
the controller-model problems this experiment set out to avoid.

### Other ObjectMeta fields face the same fate

The warning on the second apply is telling:

```
Warning: resource buckets/update-test is missing the
kubectl.kubernetes.io/last-applied-configuration annotation
which is required by kubectl apply. kubectl apply should only
be used on resources created declaratively by either kubectl
create --save-config or kubectl apply.
```

kubectl writes `last-applied-configuration` as an annotation on
the object. On a stateless AA, annotations don't survive.
kubectl works around this internally (it patches the annotation
back on each apply) so functional behavior is fine, but the
warning is an honest signal of the inverted model's cost: **any
Kubernetes-native ObjectMeta that isn't modeled in the backend
is lost**.

The same applies to:
- `labels` (not stored on S3)
- `resourceVersion` (synthesized per-AA-lifetime, not
  cluster-globally-unique)
- `uid` (synthesized on first observation; preserved for the
  AA's lifetime, regenerated on restart per the 0004 pod-restart
  amnesia pattern)
- `finalizers` (nowhere to store them; cascading delete
  semantics would need re-invention)
- `ownerReferences` (same)

A real ACK-replacement AA would either model these into the
backend (S3 tags are a natural home for some, but the mapping
is lossy) or drop these Kubernetes features. ACK's CRD+controller
model pays for these features by having etcd.

### What the inverted model gets right

The costs above sound bad. In exchange:

- **No drift.** A Kubernetes `Bucket` object with `spec.tags={env: prod}`
  cannot disagree with the S3 bucket it represents; they are
  literally the same bits, queried live. There is no "the
  controller reconciled but AWS rejected; now desired != actual"
  state to worry about.
- **No reconciler backoff.** A failed CreateBucket fails the
  HTTP request with a real error. The caller (human or other
  tool) decides whether to retry. No separate controller process
  churns forever.
- **No stuck finalizers.** There are no finalizers (see above).
  Deletion is a single DeleteBucket call.
- **No two-process coordination.** The AA is the whole story.
  No controller Deployment to monitor, no `status.conditions`
  to interpret, no "controller not running" failure mode
  distinct from "apiserver down."

These are real simplifications. For certain resource types —
simple enough that full CRUD maps cleanly, no async AWS
operations, no cross-resource references — the inverted model is
a *better* fit than CRD+controller. S3 Bucket is such a case.

### Where the inverted model breaks down (or would)

**Async AWS operations.** S3 CreateBucket is sync. IAM role
creation propagates eventually. EKS cluster provisioning takes
minutes. For async operations, the AA's CreateBucket-equivalent
would have to block the HTTP request until the operation
completed — which is not a workable pattern for minute-scale
operations. ACK's controller handles this with `status.conditions`
reflecting in-flight operations and a reconcile queue. An AA
replacement would need an equivalent state holder, which is the
controller pattern again.

**Cross-resource references.** An ACK `RoleBinding` depending on
a `Role` that doesn't exist yet in AWS would, in the CRD+controller
model, stay in Pending until the Role appears. In the AA model,
CreateRoleBinding fails at apply time; the caller re-applies
later. For humans editing with `kubectl apply -f dir/`, the order
of resources now matters. GitOps tools (ArgoCD, Flux) normally
handle this via retries; 0005 exposed the ArgoCD-side hazard of
default-deny, but cross-resource dependency is the more
fundamental issue.

**Compositions.** Crossplane-style "I want this whole stack"
YAMLs that assume a controller will figure out the ordering can't
run against an AA that's strictly synchronous.

**GitOps reconciliation.** ArgoCD's sync drift detection relies
on comparing desired (from git) to observed (from the cluster).
In this model, observed is always live from AWS. Drift detection
works — but "healing" a drifted resource means replaying the
git-specified apply, which our AA accepts. So it actually works;
it just has the feel of continuously pushing rather than
continuously reconciling.

### Table rendering exposes a consistency seam

```
$ kubectl get buckets                    # LIST
NAME               REGION      TAGS   CREATED   PHASE
my-first-bucket    us-east-1   0      17s       Ready

$ kubectl get bucket my-first-bucket     # single GET
NAME              REGION      TAGS   CREATED     PHASE
my-first-bucket   us-east-1   2      <unknown>   Ready
```

Same bucket; two reads; different tag count and different creation
timestamp. The cause: `ListBuckets` returns `CreationDate` but not
tags; `HeadBucket` returns neither; `GetBucketTagging` returns
tags only. Listing 200 buckets with full tags would be 200+1
round trips — an untenable fan-out. The trade-off here is
inherent to AWS's API shape, and the AA reflects it truthfully.

A naive CRD+controller wouldn't have this problem because the
controller would have fetched tags during reconciliation and
written them to etcd; subsequent LISTs would see the same state
as GETs. That's state. That's what this experiment chose not to
have.

### ObjectMeta.uid preservation across polls

The backend maintains a `map[name]UID` that survives poll cycles,
so consumers watching events see stable UIDs for the same
underlying S3 bucket within the AA's lifetime. On AA restart,
UIDs regenerate (same pod-restart-amnesia pattern as 0004).
A deterministic UID scheme (hash of bucket name + creationDate,
say) could preserve identity across restarts; not implemented in
0009.

### `kubectl apply` invariants

A sequence of 10 applies of the same Bucket object produces 10
PATCH round-trips (library behavior) but net zero change in S3
(idempotent creates via BucketAlreadyOwnedByYou + tag-set replace
with the same values). The AA behaves correctly; the cost is 10
round-trips of traffic. A controller-model would likely deduplicate
via status-observed-generation checks, avoiding most of those
round-trips.

### One consequent I didn't expect: we need AWS creds even for a mock

aws-sdk-go-v2 requires credentials to sign requests, even if the
mock endpoint ignores the signature. We set `AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test` via a Secret to satisfy the SDK.
Emphasizes that the SDK is not the right layer at which to
"short-circuit" for offline testing. A hand-rolled HTTP client
talking directly to the mock would avoid the need for dummy
creds entirely, but loses the fidelity of "this code path will
work against real AWS."

## Fundamentals touched

**Storage independence.** Primary fundamental. Three concrete
findings:

1. **SSA is not free under the inverted model.** managedFields are
   library-layer state. Without backend persistence they appear
   to work once and vanish on next read. Any pattern relying on
   ownership tracking between requests fails.
2. **ObjectMeta bookkeeping (labels, annotations, finalizers,
   ownerReferences) has no natural home** in a stateless AA. kubectl's
   `last-applied-configuration` mechanism still works because
   kubectl re-patches it each time, but warns you about the
   missing annotation first.
3. **Sync vs. async AWS operations bound the inverted model.**
   The model works cleanly for resources with synchronous
   lifecycle (S3 buckets, SNS topics); it breaks for resources
   with long async provisioning (EKS clusters, RDS instances).
   An AA replacement for ACK across the full AWS surface would
   need to handle async — which puts state back into the picture.

**Resource modeling freedom.** Fourth real backend. S3 Bucket
maps cleanly to a Kubernetes resource; the tag model is lossy in
one direction (LIST doesn't include tags) but honest about it.
The `<name>` global uniqueness of S3 buckets is a constraint
worth noting — on real AWS, colliding with another account's
bucket name returns BucketAlreadyExists (vs.
BucketAlreadyOwnedByYou for yours). The mock only models the
single-owner case.

**Wire protocol fidelity.** Compat scoreboard: 5 PASS + 2 SKIP
(apply-probe uses a different kind now), 0 FAIL. kubectl get /
apply / explain / watch / delete all work. The AA is indistinguishable
from a "normal" aggregated apiserver from the wire side.

**Watch and consistency semantics.** The backend's poll loop +
broadcaster is the 0004 pattern applied to AWS. Works. The 15s
poll interval is short enough to feel responsive in a lab; real
AWS with rate limits would want longer.

## Consequents (implementation-dependent; do not generalize)

- aws-sdk-go-v2 requires non-empty credentials even for a
  path-style mock endpoint. `AWS_ACCESS_KEY_ID=test
  AWS_SECRET_ACCESS_KEY=test` satisfies it. Not an AA property;
  an SDK property.
- The go.mod `go` directive auto-bumps to the host Go version
  (observed 1.26.0) on `go mod tidy` under 1.26-era toolchains;
  must be re-pinned to `go 1.24` after tidy or Docker builds
  against `golang:1.24-alpine` break. Same consequent as
  previously noted in 0002.
- `gnostic-models v0.7.0` brings in `go.yaml.in/yaml/v3` which
  conflicts with kube-openapi's `gopkg.in/yaml.v3`. Must pin
  `gnostic-models@v0.6.8`. Substrate-wide consequent; 0007 had
  already pinned but 0009 didn't inherit automatically.
- kubectl's table rendering on watch emits a second "re-render"
  of each row roughly a second after the first; observed before
  in 0001 / 0008. Not new.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**:

- **Storage independence** section: add that a fully-stateless
  AA loses SSA's ownership tracking, ObjectMeta bookkeeping
  (labels/annotations/finalizers/ownerReferences), and
  finalizer-based cascading deletion. These are not wire-protocol
  failures; they are library-layer features that assume a
  persistence layer the AA doesn't have. The `0006` experiment's
  "stateless AA is ~200 lines shorter and eliminates pod-restart
  amnesia" finding should be nuanced: statelessness eliminates
  SOME categories of cost and introduces OTHERS, and the
  introduced ones (field management, last-applied, finalizers)
  are the ones Kubernetes culture most assumes work.
- **Resource modeling freedom**: add a new named boundary:
  **sync vs. async backend operations**. The inverted model
  works cleanly for synchronous backends (S3-style) and breaks
  for async ones (EKS-style). ACK's controller pattern earns
  its complexity specifically from handling async.
- **Wire protocol fidelity**: the compat scoreboard remains
  green against a fully-stateless AA. Tooling sees a working
  API; the limitations are invisible until you try to rely on
  SSA ownership tracking or finalizers.

For **EXPERIMENTS.md**:

- `0009-ack-aggregated-s3` marked complete under Storage
  independence.
- New candidate: **`ssa-managedfields-in-backend`** — encode
  managedFields into S3 tags (or a sidecar store) and see
  whether SSA ownership semantics can be recovered under the
  inverted model. Directly answers the "option 2" from this
  finding's SSA section.
- New candidate: **`async-backend-sim`** — simulate an AWS-like
  async resource (fake 30-second provisioning delay) and see
  how the AA must model it. Tests the sync/async boundary
  explicitly.
- New candidate: **`cross-resource-references`** — two
  resource types where one references the other (e.g. `Role`
  and `RoleBinding`). Probes how the inverted model handles
  dependency ordering under declarative apply.
- The `argocd-application-targets-aa` candidate derived from
  `0005` is now sharper: the target is a writable AA like 0009,
  not a read-only one like 0004.

## Open questions raised

- Does it make sense to build a lightweight per-AA managedFields
  cache (in-memory, lost on restart) so SSA "mostly works" for
  single-AA-instance lifetimes? That's a halfway point between
  "persist everything" and "don't support SSA".
- How would an ACK-as-AA model handle `kubectl describe`? It
  works here because the library synthesizes the describe output
  from GET. But describe often includes related objects (events,
  owner references) that don't exist in the stateless model.
- Could the poll interval be replaced by CloudTrail event
  subscriptions for real-AWS deployments? Watch events driven by
  actual AWS state changes rather than periodic diffs would be
  operationally superior and reduce rate-limit pressure.
- What happens to `kubectl apply -f dir-with-100-buckets.yaml`?
  Per-bucket HEAD+PUT+PUT-tagging = 300 AWS calls, serially from
  kubectl's perspective. A CRD+controller batches this through
  the reconcile queue.
- On real AWS: what's the `kubectl get buckets` latency against
  a 1000-bucket account? Untested. Expected: dominated by the
  single ListBuckets call, probably 500ms-1s.
