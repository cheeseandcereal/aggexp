# Findings — 0021 runtime-component-parity

## What we were trying to learn

The component-server pattern from experiments 0013, 0017, and 0018
had survived three consumers: 0013's in-memory note-backend, 0017's
refined note-backend with SSA-capable OpenAPI, and 0018's S3
backend. The last two share a concrete shape (typed `dyn.Object`
wrapper, OpenAPI composition into the defs map, Apply RPC) that
kept appearing verbatim.

The question for 0021 was whether extracting that shape into
`runtime/component/` as substrate actually pays off for the next
consumer. If the substrate is right, the next consumer should be
two short main.go files (the AA binary + the backend) plus a
Dockerfile each. If the substrate is wrong, the next consumer will
have to fork or patch it in non-trivial ways.

This is the first promotion in the repo since the original
`runtime/{server,storage,group,authz}` extraction (see
`FINDINGS/0007-runtime-fs-driver.md`).

## What we did

Extracted `runtime/component/` with four subpackages:

- `runtime/component/proto` — the canonical `Backend` gRPC service.
  Shape is a clean-room rewrite of 0017's `proto/backend.proto`
  with a new `go_package` pointing inside the substrate. The
  committed `.pb.go` / `_grpc.pb.go` are regenerated at the
  substrate path so consumers don't need a local protoc.
- `runtime/component/scheme` — holds the typed `dyn.Object` wrapper
  (renamed from 0017's `pkg/dyn` to live alongside the scheme
  builder since the two are inseparable; a caller needs both or
  neither), plus `Build` which registers external+internal GVKs
  and emits a `Bundle` with canonical type-name keys for the
  OpenAPI defs map.
- `runtime/component/openapi` — `ParseBackendSchema`, `Fallback`,
  `WrapAsList`, and `Compose` helpers, plus the committed
  `openapi-gen` output for meta/v1 + runtime + unstructured +
  version (moved verbatim from 0017's `pkg/generated/openapi/`).
- `runtime/component/grpcbackend` — the `rest.Storage` adapter.
  Structurally 0017's `pkg/grpcbackend/backend.go` with the proto
  import repointed and with one behavioral fix: `Get` now stamps
  a `resourceVersion` on the returned object when the backend
  didn't supply one (0018's FINDINGS flagged this as a gap).

Plus the top-level `runtime/component/api.go` with `Options`,
`NewOptions`, `AddFlags`, `Validate`, and `Run(ctx, opts)`. The
Run function dials the backend, asks it for its schema, builds the
Scheme + OpenAPI + storage, and hands everything to
`runtime/server.Options.Run`.

Wrote 21 unit tests across the four subpackages (scheme: 5,
openapi: 7, grpcbackend: 7, top-level: 2). Tests exercise JSON
round-trip of the typed wrapper, the external/internal GVK
registration, the SSA-unblocking ObjectKinds behavior on zero-value
dyn.Object, the GVK-extension stamping on backend OpenAPI, the
Compose merge logic, and a subset of the REST storage's CRUD paths
against a fake in-memory BackendClient.

Built experiment `0021-runtime-component-parity`:

- `cmd/note-aa/main.go` — 38 lines of Go. `component.NewOptions()`,
  `AddFlags`, `Validate`, `component.Run`. That's it.
- `backend-note/cmd/note-backend/main.go` — a near-verbatim copy of
  0017's note-backend with imports repointed at
  `runtime/component/proto`. In-memory. Persists managedFields as
  `json.RawMessage`. 383 lines.
- Single experiment `go.mod` (no separate `gen/` module like 0017,
  since the substrate owns the proto package).
- Manifests, Dockerfiles, sample-note.yaml.
- Kind cluster `aggexp-substrate-component`. Deployed and ran the
  same scenarios 0017 did.

## What we observed

### Parity across the scenarios

Every scenario from 0017 reproduced without surprises:

- `kubectl api-resources --api-group=aggexp.io` → notes appears.
- `kubectl apply -f sample-note.yaml` → created.
- `kubectl get notes` → table with Name / Title / Age, Age rendered
  by the substrate's `durationShort` helper.
- `kubectl get note hello -o yaml` → full object with `uid`,
  `creationTimestamp`, `resourceVersion` present. (One behavioral
  change vs 0018: the grpcbackend now stamps RV on single-object
  GETs, closing that noted gap.)
- `kubectl explain note` and `kubectl explain note.spec` → rich
  per-field docs from the backend's OpenAPI, composed into the
  defs map at `DynObjectCanonicalName`.
- `kubectl apply --server-side --field-manager=alice` → succeeds,
  subsequent GET shows a populated `managedFields` block with
  `manager: alice, operation: Apply, fieldsV1: {f:spec: {f:title: {},
  f:body: {}}}`.
- Second apply as `bob` targeting `spec.title` is rejected with
  the expected loud conflict:
  `conflict with "alice": .spec.title`. `--force-conflicts` would
  re-own; not exercised here because 0017 already covered it.
- `kubectl get notes -w` → initial ADDED fires, `kubectl delete`
  fires DELETED live.

No substrate code change was needed to make any of this work. The
first-consumer test held.

### Line counts

- 0021 handwritten Go: **421 lines** total (38 note-aa + 383
  backend-note). No generated files in the experiment directory.
- 0017 handwritten Go: **1531 lines** in the component server
  (dyn 218 + scheme 123 + server 417 + grpcbackend 732 + main 41)
  plus 437 lines for backend-note. Total 1968 lines handwritten
  code in 0017, plus 2693 lines of generated openapi and 2049
  lines of generated proto per experiment.
- 0018 handwritten Go: 0017's component copied verbatim (1531
  lines, uncounted) + 674 lines of backend-s3.

So a new KRM consumer that uses `runtime/component/` writes
approximately **0.27x** the handwritten Go of 0017 (421 / 1531),
and carries zero generated code in its own tree. The component
server's generated openapi (~2700 lines) and generated proto
(~2100 lines) now live once in `runtime/component/openapi/
generated.go` and `runtime/component/proto/backend*.pb.go`
respectively, amortized across all future consumers.

The Go that a new consumer does still write is almost entirely the
**backend-side resource translation** (the 383-line Note backend
here, or the 674-line S3 backend in 0018), which is the work the
consumer can't skip regardless of approach. 0017's `dyn.Object`,
scheme, OpenAPI composition, and grpcbackend adapter code is what
the substrate absorbs.

### Substrate test count

`go test ./runtime/...` passes with 21 new unit tests in
`runtime/component/...` on top of the existing substrate tests.
Coverage is intentionally narrow: the unit tests cover invariants
that aren't caught by the end-to-end kind deployment (JSON
round-trip, GVK stamping, internal-version registration, canonical
type names). The full-stack behaviors (SSA, explain, watch) are
covered by this experiment itself; the ratio of unit-to-end-to-end
testing in `runtime/component` mirrors the rest of the substrate.

### One small shape change I chose to make

I decided to flip the default for `--use-typed-wrapper` from false
(0017's opt-in default, appropriate while it was probing the SSA
boundary) to **true** in the substrate's `Options`. Rationale: 0017
established the typed wrapper is required for SSA end-to-end, and
the substrate's Run path composes a full OpenAPI into the defs map
unconditionally. Flagging into the typed path by default means a
new consumer's expected baseline (SSA works) matches the substrate
default. Unstructured-only is still reachable via
`--use-typed-wrapper=false` for experiments that want to probe it.

No other protocol or API changes. The gRPC `Backend` service shape
is byte-identical to 0017's; the scheme+openapi+grpcbackend Go
code is the same Go at structural level. Imports repointed, one
Get-path RV-stamping fix, and the default flip are the only
deltas.

### What was easy

- Module layout. A single experiment `go.mod` with `replace
  github.com/cheeseandcereal/aggexp => ../..` was enough. No
  per-experiment `gen/` module (0017 carried one because the
  experiment owned its proto; now the substrate does).
- Dockerfile. The build context is the repo root; we copy `go.mod`,
  `go.sum`, `runtime/`, and the experiment dir. That's it.
- Deployment. Same `hack/deploy.sh` flow as 0017; only the image
  names and container args change.

### What was hard

- Not much. One issue surfaced late: an openapi-compose test
  originally used `func(path string) common.OpenAPIDefinition` as
  the ReferenceCallback, but the actual type is
  `func(path string) spec.Ref`. Five-minute fix.
- One substrate bug surfaced via tests:
  `grpcbackend.LookupField` returned nil (not "") when the first
  path segment was missing from the decoded object, so table cells
  rendered as `<nil>` instead of blank. Caught by a unit test
  against `.missing.path`; fixed to return `""` on type-assertion
  failure. This was a latent bug in 0017's code too but never
  surfaced in practice because the backend ships known paths.

## Fundamentals touched

**Wire protocol fidelity** (by inheritance from 0017/0018). No new
finding. The extracted substrate reproduces 0017's behavior byte-
for-byte on CRUD, watch, explain, and SSA. The small improvement
(resourceVersion stamped on single-object GETs) closes a known
0018-era gap but doesn't move any fundamental boundary.

**Substrate promotion as a process invariant** (meta, not one of
the six). This is the second substrate promotion in the repo. The
first produced `runtime/{server,authz,storage,group}` from 0002 +
0004. This one produces `runtime/component/` from 0013 + 0017 +
0018. In both cases the two-to-three-consumer precondition named
in ETHOS held: the shape was not promoted until it had survived
multiple concrete consumers with minor or zero per-consumer
edits. The process held cleanly; no process observation needed.

## Consequents

- `runtime/component/openapi/generated.go` is the committed
  openapi-gen output for meta/v1 + runtime + unstructured +
  version. Regenerating it requires running `openapi-gen` against
  the input-package list. Bumping kube-openapi or apimachinery
  versions may require regenerating it; bumps haven't been
  required across 0013, 0017, 0018, 0021. The substrate's
  `openapi` package does not re-run openapi-gen automatically.
- The `Apply` RPC exists on the wire but is not called by the
  substrate today. The library's SSA path routes through our
  Update. Kept in the proto so future experiments (backend-
  persisted applied-intent, e.g.) don't need to evolve the wire
  again.
- Backend TLS: the substrate's `component.Run` dials the backend
  with `insecure.NewCredentials()`. A second consumer demanding
  in-cluster mTLS (SPIFFE or similar) would justify adding a
  `Options.BackendTLSConfig` field; until then, YAGNI.
- The `--use-typed-wrapper` flag remains reachable even though its
  default flipped. An experiment specifically probing the
  unstructured-only SSA failure path can still invoke it.
- Kind cluster: `aggexp-substrate-component`.
- Go 1.24 pin, gnostic-models@v0.6.8 pin — same as every other
  experiment; unchanged by this promotion.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**: minimal. The wire-protocol-fidelity section's
datapoint about the typed-wrapper-unblocks-SSA result does not
shift; it was already established by 0017. This experiment does
not produce a new fundamental-level finding — it validates that an
existing one survived extraction into substrate. The "substrate
extraction was deliberate and worked" process observation gets a
second confirming datapoint alongside the original (0002+0004 →
runtime/{server,...}).

For **ARCHITECTURE.md**: adds `runtime/component/` to the package
tree with its four subpackages. Per-request flow diagram unchanged
— the component server installs on top of `runtime/server` +
`runtime/group` the same way an in-process library consumer does;
the difference is that `rest.Storage` lives in
`runtime/component/grpcbackend` proxying over gRPC rather than in
`runtime/storage` adapting an in-process Backend interface.

For **EXPERIMENTS.md**: 0021 complete under Resource modeling
freedom (or Wire protocol fidelity — the parity probe is about
whether the extracted shape holds, which is primarily a
resource-modeling-freedom datapoint because it's about library
vs component approaches, but the reproduced behavior is
wire-protocol-fidelity). Either placement is defensible; I put it
under Resource modeling freedom because the interesting content
is "there are now two ways to build a Kubernetes API" as a
substrate fact, not a new wire-level observation.

## Open questions raised

- **When should a consumer choose library vs component?** The
  substrate now exposes both (`runtime/storage.Backend` vs
  `runtime/component/proto.Backend`). The package docs in
  `runtime/component/doc.go` state a rule of thumb: polyglot or
  separate trust domain → component; Go-native single-process →
  library. A future experiment could stress-test whether the line
  shifts under real-world scale (watch fan-out cost, SSA
  round-trip latency).
- **`Apply` RPC in practice.** The substrate exposes it on the
  wire and the reference backend implements it, but nothing
  uses it yet. A future experiment with a backend that does its
  own field-management (e.g. record applied-intent separately
  from observed state) would answer whether the RPC's shape is
  right. Queued under Resource modeling freedom.
- **Substrate mTLS for the component↔backend link.** Insecure is
  fine for in-cluster traffic inside one namespace; cross-
  namespace or cross-cluster demands a TLS story. Shape is
  obvious; waiting for a consumer that wants it.
- **Multi-resource component server.** The substrate still
  serves one GVR per component process. 0013's FINDINGS flagged
  this as a followup; 0021 does not move it. A
  `component.Run` that accepts `[]BackendClient` with one GVR
  each would be straightforward but hasn't been demanded yet.
