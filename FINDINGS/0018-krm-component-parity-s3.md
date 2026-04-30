# Findings — 0018 krm-component-parity-s3

## What we were trying to learn

0009 built an ACK-inverted aggregated API server for AWS S3: S3 is
the sole source of truth; no etcd on the AA side; `kubectl get
buckets` is a live `ListBuckets`; watch is a poll loop. It was
implemented as a substrate-linked Go binary — `pkg/s3backend`
implementing `runtime/storage.WritableBackend`, typed Go `Bucket`
with codegen'd deepcopy, generated OpenAPI, a `runtime.Scheme`,
and a `cmd/aggexp-s3` main.

0013 proposed a different shape: a deployable generic "component
server" that speaks the Kubernetes wire contract for any resource
whose schema it receives at startup over gRPC. The actual resource
logic lives in a separate "backend" process that doesn't know
about `k8s.io/apiserver`. 0013's reference backend (`note-backend`)
was an in-memory toy.

0018 is 0009 re-implemented on top of 0013: same resource type,
same S3 mock, same user-facing behavior — but the S3 logic is a
gRPC backend behind the 0013 protocol, and the apiserver side is
the 0013 component server unchanged (just copied and module-path-
rewritten). The task is **parity**: show that the user's kubectl
experience is the same, measure what's structurally different,
and record where behavior diverges.

Three hypotheses:

1. `kubectl`-level parity: every scenario 0009 supports
   (get/list/apply/delete/watch/explain) still works, with the
   same observable quirks.
2. SSA parity, or rather: 0009 lost SSA silently (managedFields
   vanish on next GET); 0018 inherits 0013's failure, which is
   *louder* (typed-converter refuses the object outright). Both
   are failures; record which one users would find worse.
3. Line count: the substrate-linked 0009 spent ~1200 lines of
   Go total on Bucket; a thin-backend 0018 should be shorter in
   raw count because it skips the typed-scheme / codegen /
   apiserver wiring. The S3 call code itself should be roughly
   unchanged.

## What we did

- Copied the entire `component/` from 0013 with its module path
  rewritten (`experiments/0018-krm-component-parity-s3/component`)
  and a `replace` pointing at 0013's `gen/` module via `../../
  0013-krm-component-skeleton/gen`. No source changes.
- Wrote `backend-s3/cmd/backend-s3/main.go`: a single-file gRPC
  `BackendServer` using `aws-sdk-go-v2` (pinned to the exact
  versions 0009 uses). Translates the backend.proto RPCs to
  HeadBucket / ListBuckets / CreateBucket / DeleteBucket /
  Get+PutBucketTagging. Same poll loop + broadcaster pattern
  as 0009, running in the backend pod instead of the AA pod.
- Copied `s3-mock/` verbatim from 0009 (identical `main.go`,
  only the module path in `go.mod` changed).
- Wrote manifests (permissive RBAC, s3-mock, aws-creds,
  backend-s3, aggexp-deployment-override). Added a
  `BACKEND_S3_IMAGE` default to `hack/deploy.sh` next to the
  existing `S3_MOCK_IMAGE` and `NOTE_BACKEND_IMAGE` defaults.
- Deployed to a dedicated kind cluster `aggexp-krms3`. Ran the
  same scenarios 0009 ran.

## What we observed

### Per-scenario outcomes (vs. 0009)

All seven observable scenarios from 0009's README run end-to-end:

- `kubectl get buckets` (empty) — PASS; same output as 0009.
- `kubectl apply -f bucket.yaml` — PASS; `bucket.aggexp.io/my-
  first-bucket created`.
- `kubectl get buckets` (populated) — PASS; table renders with
  Name / Region / Tags / Created / Phase columns. **One
  presentational difference from 0009**: the Tags column
  displays blank on LIST (our row-field is `.spec.tags` and
  the List response carries no tags because `ListBuckets` on
  S3 doesn't include them; the generic row-renderer prints nil
  as the empty string). 0009 rendered this as `0` because its
  typed `RowsFor` returned `int64(len(Tags))`. Same underlying
  data; different render — the consequence of generic-lookup
  vs. typed-formatter.
- `kubectl get bucket my-first-bucket -o yaml` — PASS; full
  object round-trips. `uid`, `creationTimestamp`, `spec.tags`
  all present.
- `kubectl get buckets -w` — PASS; initial ADDED replayed,
  live events stream for subsequent applies and deletes.
- `kubectl delete bucket my-first-bucket` — PASS; object
  disappears from subsequent LIST.
- `kubectl explain bucket` — works but **heavily degraded**.
  The component server registers unstructured types with
  `x-kubernetes-preserve-unknown-fields: true`, so kubectl's
  explain returns only the generic description string (verbatim
  "Dynamic resource served by the 0013 KRM component skeleton.").
  `kubectl explain bucket.spec` returns `error: field "spec"
  does not exist` — the schema has no per-field shape it can
  index. 0009's generated OpenAPI produced full per-field docs
  here. This is not a Bucket-specific regression; it's the
  0013-arc's known unstructured-schema limitation applied to S3.

Compat scoreboard: identical to 0013's scoreboard today — 4 PASS +
1 FAIL (only because the script's Hello-apply SKIPs leave no object
to stream in the 5s watch window; a manual run with a pre-existing
Bucket produces clean events). 0009's scoreboard was 5 PASS / 0
FAIL because 0009 defined a `Hello`-equivalent test path.

### SSA: the failure mode is different from 0009

0009: `kubectl apply --server-side -f bucket.yaml` appears to
succeed; the response includes `managedFields`. On the next GET,
`managedFields` is gone. The failure is silent.

0018:

```
$ kubectl apply --server-side -f ssa.yaml
Error from server: failed to create manager for existing fields:
failed to convert new object (/; /, Kind=) to proper version
(aggexp.io/v1): Object 'Kind' is missing in 'unstructured object
has no kind'
```

Loud 500-class failure at `managedfields.NewTypeConverter`, before
any gRPC call happens. Same root cause as 0013 (no typed Go model
for the unstructured Bucket → typed-converter can't build a
field-ownership model). **Users in 0009 get partial SSA that
appears to work; users in 0018 get no SSA at all, but the failure
surface tells them why.** Neither is good. 0009's silent version
is arguably worse for consumers who assume SSA is working.

### New kubectl warnings on client-side apply

First client-side apply: same single warning 0009 emitted about
`last-applied-configuration`.

Second client-side apply (re-apply):

```
Warning: resource buckets/ssa-demo is missing the
  kubectl.kubernetes.io/last-applied-configuration annotation
  which is required by kubectl apply. ...
warning: error calculating patch from openapi v3 spec: unable to
  find api field "metadata"
warning: error calculating patch from openapi spec: expected kind,
  but got map
```

The two extra warnings are new relative to 0009. They come from
kubectl trying to use our advertised OpenAPI v3 to compute a
client-side patch, finding an object-valued `metadata` field (the
one we ship as `{"type":"object"}` in `GetSchema`), and giving up
with a generic "expected kind, but got map" complaint. The apply
still succeeds; the AA accepts the PATCH. But the UX is noisier.
This is a consequent of the backend's OpenAPI being minimal; a
richer schema that keys `metadata` to `ObjectMeta` would remove
the warnings.

### ObjectMeta bookkeeping: worse than 0009, for one specific reason

0009's `preserveManagedMeta` walked `ObjectMeta`'s `ManagedFields`,
`Labels`, `Annotations`, `ResourceVersion`, `UID`,
`CreationTimestamp` and copied them from the incoming object onto
the live-read from S3, so at least within a single request-
response cycle the library's metadata survived. The caller saw
round-tripped labels and annotations on the immediate response
(even though a subsequent GET lost them).

0018's backend JSON struct **deliberately does not** carry
`labels`, `annotations`, `managedFields`, `resourceVersion` — the
`Meta` type holds only `name / uid / resourceVersion /
creationTimestamp`. So kubectl's `last-applied-configuration`
annotation, if sent, is dropped immediately at the backend instead
of persisting for one response cycle. The component server's
unstructured path may round-trip some of these fields on the Update
call into the backend, but the backend strips them on the JSON
boundary. Deliberate choice: match 0009's stateless-AA posture
honestly, rather than pretending to persist fields we can't.

Observationally: the user sees the same end state (metadata gone
on next read) but gets there faster. The `last-applied-configuration`
warning fires on the *first* re-apply instead of the second.

### Line counts

- **0009** — Bucket-specific Go code **excluding** s3-mock,
  codegen'd helpers, and the tools file (but including every
  file under `pkg/apis`, `pkg/s3backend`, `pkg/apiserver`,
  `pkg/server`, `cmd/aggexp-s3`): **1163 lines** (1174 with
  tools.go). Including `zz_generated.deepcopy.go`: **1281**.
- **0018** — backend-s3's total Go: **674 lines** (single file).
  Component server code is a verbatim copy of 0013's — not
  counted as 0018-specific.
- **0009's `pkg/s3backend/backend.go`** (the piece this
  experiment replaces): **664 lines**.

So 0018's `backend-s3/cmd/backend-s3/main.go` is ~10 lines longer
than 0009's `pkg/s3backend/backend.go`. Roughly the same
S3-translation work ends up at roughly the same line count. What
the inversion eliminates is the **~500 lines of Kubernetes-type
scaffolding around it**: typed `Bucket` / `BucketList`, codegen'd
deepcopy, internal-vs-external conversion, scheme registration,
rest.Storage adapter, cmd wiring, generated OpenAPI (~2.7KLOC in
a generated file we didn't count in 0009's totals but which is
real code the experiment must produce).

Putting that the other way: at the **experiment-specific** level,
0018 is roughly 0.6x the lines of 0009. The generic component
server is amortized across every backend that uses the pattern —
but 0018 is the first repeat consumer, so from where we sit now
the skeleton's ~1000 component+gen lines are unamortized.

Net: if you need one or two backends for a big org, the
substrate-linked 0009 shape is simpler. The 0013 shape earns
itself at the third backend, or at the first non-Go backend.

### Watch cost: same total, different pod

0009's poll loop runs in the AA pod. Every 15s a ListBuckets call
goes out from the AA. Memory and CPU of the poll loop live in the
AA process.

0018's poll loop runs in the `backend-s3` pod. Every 15s a
ListBuckets call goes out from the backend. The AA (component
server) sits idle waiting on the long-running gRPC Watch stream.
The component's poll cost is the broadcaster fan-out to kubectl
clients, which both experiments pay.

The observable difference: in 0018 the component can be restarted
independently of the polling state (the backend keeps polling and
buffers events), and the backend can be restarted independently of
in-flight kubectl watches (the component would lose state and
re-open the upstream watch on reconnect, synthesizing a LIST-replay
to existing watchers). In 0009 both responsibilities live in one
process; restart loses everything.

Same total cost, different failure domains. Minor operational win
for 0018.

### The LIST vs GET consistency seam is the same

Both experiments show the identical inconsistency:

```
$ kubectl get buckets         # LIST
NAME        REGION      TAGS   CREATED   PHASE
ssa-demo    us-east-1          12s       Ready

$ kubectl get bucket ssa-demo # GET
NAME       REGION      TAGS            CREATED   PHASE
ssa-demo   us-east-1   map[env:demo]   24s       Ready
```

Tag-aware LIST requires one `GetBucketTagging` per bucket; neither
experiment pays that cost. 0009 flagged it as a storage-model
consequence; 0018 inherits it unchanged because the backend
behaves identically. The LIST column renders slightly differently
(blank vs. `0`) because the generic row-renderer can't express
"length of absent map" the way 0009's typed `RowsFor` could.

### Resource-rendering quirks worth flagging

- `kubectl describe bucket ssa-demo` works; output is clean and
  matches the structure of an `unstructured.Unstructured` — no
  Events section (none exist), labels/annotations shown as
  `<none>`. Same shape as 0009's describe output.
- `kubectl get bucket ssa-demo -o yaml` does NOT include a
  `resourceVersion` on the object's metadata in 0018. 0009 set
  one from the substrate's synthetic-RV counter. The component
  server sets `resourceVersion` on watch events but not on
  single-object GET responses (the library stamps it on the
  list, not on the individual item). Probably a grpcbackend
  omission worth revisiting in 0017; observationally benign for
  anything short of a client doing optimistic concurrency on
  individual Bucket GETs.
- `kubectl get --raw /apis/aggexp.io/v1/buckets/` on the list
  returns `"resourceVersion":"13"` on the list, which proves
  the component's RV counter is ticking.

## What surprised me

- **SSA fails earlier, not the same way.** I expected 0018 to
  produce the same silent drop-on-reload as 0009. Instead the
  library's typed-converter constructor refuses the object
  outright. 0013 already showed this; I'd anticipated that
  0009's `preserveManagedMeta` helpers (which preserved the
  managedFields for one response cycle) would somehow paper over
  it. They didn't — they can't, because in 0018 there's no
  `rest.Patcher` code path running against a typed `Bucket`;
  the whole stack is unstructured.
- **Line counts converge to almost exactly the same number on
  the S3-translation layer.** 664 vs. 674. I expected 0018 to
  be noticeably shorter because of the Kubernetes types being
  gone. They're gone, but they moved into the backend's plain
  Go structs (Bucket / BucketSpec / BucketStatus) which mass
  about the same. What shrinks is the *scaffolding around* the
  translation layer, not the translation itself.
- **The two new kubectl warnings on re-apply.** I expected the
  0009 `last-applied-configuration` warning to be the only one.
  The additional "error calculating patch from openapi v3 spec"
  messages come from kubectl trying to use our minimal OpenAPI
  to compute a client-side patch; the schema has `metadata:
  {"type":"object"}` and kubectl doesn't like that. 0009's
  openapi-gen produced a faithful `ObjectMeta` ref; 0018 does
  not.
- **Backend-side restart independence.** A concrete operational
  win of the inversion I hadn't anticipated: restarting the AA
  pod does NOT lose the backend's polling state. In 0009 the
  two are coupled.

## Fundamentals touched

**Wire protocol fidelity** (primary). The main datapoint: the
generic, schema-dynamic component server from 0013 **can stand
in for the library-linked typed-scheme path of 0009 end-to-end
against a real cloud-resource backend**, with the same
user-observable behavior on CRUD + watch + table rendering.
Where it degrades is exactly where 0013 said it would: SSA
(library's typed-converter rejects unstructured) and rich
per-field `explain` (no typed schema to key on). Neither
degradation is backend-specific; both would apply to any
component-served resource.

**Resource modeling freedom** (secondary). The Bucket shape is
the same as 0009 on the wire. What changes is where the shape is
defined: in 0009 it's Go code plus codegen'd deepcopy; in 0018
it's a JSON-serialized OpenAPI blob shipped over gRPC at startup.
The schema is **less expressive** in 0018 because the component
server doesn't actually use the OpenAPI to construct typed
objects — it registers `*unstructured.Unstructured`. So ship a
schema with full per-field shape or ship a minimal one: the
component server treats them the same. This is a known 0013
seam, not a new 0018 finding, but 0018 confirms it applies to a
non-toy resource.

**Storage independence** (tertiary). 0009 sits in the
"external-API-as-truth" storage axis. 0018 sits in the
"component-server + thin-backend" axis (SYNTHESIS's fourth).
These are not the same axis — but 0018's backend is **itself**
in the external-API-as-truth shape, because its own persistence
is "whatever ListBuckets tells me". So the axes compose cleanly:
you get thin-backend's polyglot-friendliness *on top of*
external-API-as-truth's zero-local-state posture. The
combinatorial result is the same stateless-AA thesis 0009 made,
now portable to non-Go backends.

## Consequents (implementation-dependent; do not generalize)

- aws-sdk-go-v2 still requires non-empty credentials to sign
  requests against a mock that ignores signatures. Same as 0009.
  `AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test`.
- The LIST "Tags" column renders as the empty string in 0018
  where it rendered as `0` in 0009. This is a consequence of
  `grpcbackend.rowFor` being a generic jsonpath-ish lookup
  rather than a typed `RowsFor`. A fix would either change the
  backend's row-field convention (e.g. a special jsonpath like
  `count(.spec.tags)`) or register a richer row-rendering
  primitive in the proto.
- On client-side re-apply, kubectl emits two additional
  warnings beyond the 0009 `last-applied-configuration` warning.
  They come from kubectl's attempt to compute a patch from our
  deliberately-minimal OpenAPI v3 schema. Functionally benign;
  operationally noisy.
- `kubectl get bucket <name> -o yaml` returns no
  `resourceVersion` on metadata in 0018 (0009 set one via the
  substrate's counter). The component server sets RV on watch
  events but not on GET responses. Probably worth fixing in the
  grpcbackend adapter in a later experiment (0017); not a
  fundamental.
- `go.yaml.in/yaml/v2` transitively lurks under 0009's go.mod
  (via aws-sdk-go-v2 dependencies) and caused no problem here
  because 0018's backend-s3 module has no
  k8s.io/{apimachinery,apiserver} imports to conflict with. This
  is an accidental property of the split: isolating the AWS SDK
  into a module that doesn't speak Kubernetes also isolates its
  yaml-module quirks. Worth noting.
- Kind cluster-context leakage remains a real hazard. Halfway
  through the scenarios my `kubectl config current-context` had
  silently moved to an unrelated sibling worktree's cluster,
  causing a bogus compat run. Future agents sharing this machine
  must pass `--context kind-aggexp-<slug>` to every kubectl
  invocation or pin it programmatically; `use-context` is
  globally-persistent config.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**:

- **Wire protocol fidelity**: no new finding; the
  unstructured-path caveat from 0013 (SSA fails at typed-
  converter; per-kind explain degrades) holds unchanged against
  a real cloud backend.
- **Resource modeling freedom**: incremental. 0013 said
  unstructured-typed registration works for an in-memory toy;
  0018 extends that to "also works for an external-cloud
  backend". One more datapoint; the boundary hasn't moved.
- **Storage independence**: the fourth storage axis (component
  + thin-backend) **composes with** the second axis (external-
  API-as-truth) cleanly. Neither imposes on the other. Worth
  naming explicitly in a SYNTHESIS rewrite: storage axes are
  not mutually exclusive; they stack.
- **The inversion thought-experiment pattern** (SYNTHESIS
  process observation 5) survives its fifth probe. 0018
  deliberately did not discover a new failure category; it was
  a pure parity exercise. That the parity held — and the
  failure-mode differences (SSA loud vs silent; LIST "Tags"
  column formatting) are cosmetic — is itself the finding.

For **EXPERIMENTS.md**:

- Mark 0018 complete under "Wire protocol fidelity" with
  cross-references to 0009 under Storage independence and
  Resource modeling freedom.
- The candidate `0019-polyglot-backend` (from 0013's FINDINGS)
  is now sharper: since 0018 showed the Go-backend path holds
  at the parity level, rewriting 0018's `backend-s3` in
  python or rust (using the same S3 SDK in that language) is
  a cleanly bounded polyglot test.
- The candidate `0017-krm-protocol-refinement` gains concrete
  wishlist items from 0018:
  (a) resourceVersion on single-object GETs,
  (b) richer row-rendering than jsonpath lookups (to recover
      0009's "Tags: N" style),
  (c) OpenAPI schema shipped in a way that at least stops
      kubectl's "expected kind, but got map" warnings on
      client-side apply,
  (d) SSA — the still-open big-rock item.

## Open questions raised

- If backend-s3's OpenAPI shipped a proper `metadata: {$ref:
  ObjectMeta}` instead of `metadata: {"type":"object"}`, would
  kubectl's client-side-apply warnings disappear? Cheap to try;
  in scope for 0017.
- Does 0018's backend-s3 survive a real AWS round-trip the way
  0009 does? The SDK code is identical; the only path change is
  the gRPC wrapper. Should be trivial; not tested here because
  the task scope is mock.
- With the backend loop decoupled from the AA, could two
  component servers talk to the same backend-s3 and both
  serve `buckets.aggexp.io/v1` with consistent views? The
  backend broadcaster fans events to whoever's connected; two
  AA pods would see the same event stream. Worth probing in a
  future experiment about horizontal scale.
- The `backend.proto` `UserInfo` is forwarded on every RPC but
  `backend-s3` never inspects it. A version of this experiment
  where identity matters (e.g. per-caller AWS credentials from
  a broker, per-caller IAM policy) would stress whether the
  identity-handoff fundamental composes with the component-
  server shape. Related to 0006's broker; tangential to 0018.
