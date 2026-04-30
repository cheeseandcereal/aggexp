# Findings — 0019 krm-polyglot-backend

## What we were trying to learn

0013, 0017, and 0018 all used Go backends behind the KRM component
server. The arc's implicit claim — that a backend in any language
can serve a Kubernetes resource through that component — was never
actually tested. 0019 tests it. Same resource type as 0017
(`notes.aggexp.io/v1`), same proto, same component-server binary;
only the backend moves from Go to Python.

Thesis: **the backend is language-agnostic.** If it holds, the
component server can't tell the backend's implementation language
apart; CRUD + watch + explain + SSA should all work with Python
standing in for Go.

## What we did

Wrote `backend-note-py/main.py`: a single-file gRPC `BackendServicer`
using `grpcio` + `protobuf` + `grpcio-tools`. Copied 0017's
`proto/backend.proto` verbatim into `experiments/0019-krm-polyglot-backend/proto/`;
the python Dockerfile runs `grpc_tools.protoc` at build time to
regenerate bindings under `aggexp/krm/v1/`. In-memory storage (a
`dict[(ns, name), dict]`) exactly mirroring the Go reference.

Built the component server image from 0017's Dockerfile verbatim
(`-t aggexp-krm-component-v2:dev`). 0017 was not touched. Built the
python backend image. Loaded both into a fresh kind cluster
`aggexp-krm-poly`. Deployed using 0019's manifests (identical in
structure to 0017's; the only substantive difference is the backend
Deployment's image).

Ran the scenarios 0017 ran: CRUD, yaml round-trip, explain,
explain.spec, server-side apply with `--field-manager`, conflict
detection, `--force-conflicts`, and watch. Then swapped the backend
Deployment's image to 0017's Go reference (without touching 0017's
manifests) and ran 10× serial `kubectl get` latency measurements
against each backend for fair comparison.

## What we observed

### Per-scenario outcomes

All seven scenarios pass against the python backend:

- `kubectl api-resources --api-group=aggexp.io` → lists `notes`
  with the SHORTNAME `nt` registered from `GetSchema.short_names`.
- `kubectl apply -f sample-note.yaml` → creates, returns the
  object with a server-assigned UID + creationTimestamp.
- `kubectl get notes` → table format; Name, Title, Age columns
  render from the backend's `row_fields` jsonpaths. Identical
  output shape to 0017's Go backend.
- `kubectl get note hello -o yaml` → full round-trip including
  `kubectl.kubernetes.io/last-applied-configuration` annotation.
- `kubectl explain note` → full per-field documentation including
  descriptions. `kubectl explain note.spec` descends into the
  spec schema and shows `title` and `body` with their descriptions.
  **Parity with 0017; the component server is ingesting the
  OpenAPI from the python backend identically to how it ingests it
  from the Go backend.**
- `kubectl apply --server-side --field-manager=alice` → creates
  with `managedFields` populated, `manager: alice, operation:
  Apply, fieldsV1: {f:spec: {f:body: {}, f:title: {}}}`.
- Second SSA by `bob` targeting `spec.title` → proper conflict
  rejection with the library's standard message
  (`conflict with "alice": .spec.title`).
- `--force-conflicts` → ownership split: alice owns `spec.body`,
  bob owns `spec.title`. Exactly the semantics 0017 recorded
  against the Go backend.
- `kubectl get notes -w` → streams `ADDED` on existing object,
  plus live `ADDED` and `DELETED` events for subsequent mutations.
  Initial-state replay + live events both work through python's
  server-streaming gRPC.

**The component server cannot distinguish the backend's language.**
Every library feature 0017 closed (rich explain, SSA conflict
detection, force-conflicts, managedFields persistence) works
unchanged. The proto transports cleanly; there are no hidden Go-
isms in the wire contract.

### Line-count comparison

Honest counts, with comments and blank lines stripped symmetrically
(python: docstrings and `#` comments; go: `//` and `/* */`):

|                           | total lines | semantic lines |
| ------------------------- | ----------- | -------------- |
| `backend-note/cmd/note-backend/main.go` (0017) | 437 | 374 |
| `backend-note-py/main.py`  (0019)              | 317 | 254 |
| Python as fraction of Go                       | 72% | 68% |

Python is ~30% shorter on the semantic line count. Where the
savings come from:

- **No explicit type declarations.** Go's `Note`, `Meta`,
  `NoteSpec`, `NoteStatus` structs with their JSON tags occupy
  ~30 lines that python collapses into "it's a dict". The
  backend never actually uses typed spec access — it treats the
  object as bytes-in-and-out — so the extra type declarations
  aren't buying anything at this layer.
- **Python `json.loads`/`json.dumps` vs Go's
  `json.Marshal`/`json.Unmarshal` error-handling boilerplate.**
  Each Go RPC carries 3-4 lines of `if err != nil { return nil,
  status.Errorf(codes.Internal, ...) }` per marshal call; python
  exceptions let those paths collapse (we abort with
  `context.abort(...)` only on genuine input errors).
- **Go's explicit `context.Context` parameter threading** (every
  RPC takes one and names it explicitly even when ignoring it)
  vs python's unused `context` parameter that sits quietly.

Where the costs **don't** differ:

- **OpenAPI schema dictionary** — the schema is the same ~45
  nested-map lines in both languages because it IS the schema.
  Python's `Dict` literal syntax vs Go's `map[string]any{}` is
  keystroke-for-keystroke.
- **Watch broadcaster logic** — about 30 lines in both. Python
  uses `queue.Queue` per watcher, Go uses a buffered channel.
  Same pattern, same line count.

**Conclusion: at the backend layer, python wins on compactness by
roughly a third. Most of the saving is in eliminating typed struct
declarations that the backend's "JSON in, JSON out" shape doesn't
use.**

What the line count does NOT capture:

- **Image size**: 12.3 MB (Go, distroless) vs 159 MB (Python
  slim + grpcio). Compactness of source trades 13× against
  compactness of image. Consequent.
- **Cold-start**: python backend container takes ~0.5s to reach
  "listening on ..." (dominated by `import grpc`); Go takes
  ~50ms. Invisible at lab scale; could matter on
  scale-to-zero.

### Latency comparison

Ten serial `kubectl get` invocations, on the same kind cluster,
hot (not first-call), single Note object present. Times in ms:

```
kubectl get notes        | mean   min   max   median
 python backend          | 71.6   66    77    71
 go backend              | 70.4   67    75    70

kubectl get note hello   | mean   min   max   median
 python backend          | 69.0   66    74    69
 go backend              | 71.3   66    78    74
```

**Difference is noise.** The python backend and the Go backend
are indistinguishable at the `kubectl` round-trip boundary. The
dominant cost is the aggregation-layer hop (kubectl →
kube-apiserver → aggregator → component server); the
component-to-backend gRPC leg is ~1-5ms and its language is
invisible inside that budget.

This matches SYNTHESIS's long-standing observation (`0003`,
`0006`) that the aggregation-layer floor is ~65ms and sub-floor
costs don't surface in end-to-end latency at small scale.

### Implementation notes worth capturing

- **Python protobuf gencode version mismatch is a warning, not
  an error.** `grpcio-tools 1.66.2` ships gencode 5.27.2 that
  imports a 5.28.2 runtime; it prints a `UserWarning` at import
  time and proceeds normally. Unpinning either side would
  silence it; we pinned both at their release-current versions
  and accepted the warning.
- **The proto's `go_package` option** (`option go_package = "...
  /experiments/0017-krm-protocol-refinement/gen/...";`) is a
  no-op for the python generator. Python keys output path off
  the `package` declaration; the go_package line is inert from
  python's perspective, which is exactly the polyglot property
  being tested. We kept the proto file byte-identical to 0017's
  rather than editing out Go-specific options.
- **Generated `backend_pb2_grpc.py` uses a same-directory
  import** (`import backend_pb2 as ...`) that only works when
  run from the output dir. The `gen.sh` and Dockerfile rewrite
  it to the absolute `from aggexp.krm.v1 import backend_pb2 as
  ...`. This is a protoc-for-python ergonomic consequent; every
  python grpc codebase past a single directory hits it.
- **Generated python bindings are NOT committed.** `gen.sh`
  regenerates them; the Dockerfile regenerates them at build
  time. Committing the output would add ~800 lines of
  machine-generated code to the repo for re-derivable content.
  The Go experiments commit their generated code because Go
  modules complicate build-time generation; Python has no such
  complication.
- **No authz-aware bits, no backend-side persistence of
  managedFields decisions.** The Apply RPC in the python
  backend, like the Go reference, treats SSA identically to
  Update; the library does the field-manager math in the
  component server. The backend just persists the merged
  object. This matches 0017's design.

### What was hard

- **Nothing was hard.** The python gRPC server was 2-3 hours of
  writing based on 0017's main.go. The proto imported cleanly,
  the stream-return idiom for Watch is natural in python
  (generator function yielding events), and the test scenarios
  all passed on first deploy. This absence-of-surprises is the
  finding.
- **The one gotcha** was remembering that in-memory backends
  lose state on redeployment. When we swapped the image to
  measure Go latency, our existing `hello` note disappeared and
  we had to recreate it. Neither backend persists; both are
  process-local. Not a polyglot issue; shared property.

### What surprised us

- **How exact the parity was.** I'd expected some wire-level
  quirk to leak — a field-name casing difference, a proto
  enum default handling mismatch, a gRPC framing nuance. None
  surfaced. The proto-over-JSON-bytes shape is clean enough
  that language boundaries don't show.
- **Python's 30% line-count win is smaller than expected.** I
  thought removing typed struct declarations would cut 40-50%
  of the code. It only cuts 30% because the bulk of the
  backend is the gRPC method skeletons (fixed shape) and the
  OpenAPI schema (content-equal in both languages). The typed
  declarations were ~30 lines of ~380; worth it but not
  dominant.
- **Latency is indistinguishable.** I'd budgeted for python's
  GIL + per-call overhead to add measurable cost. At one
  concurrent caller with a single-object list, it doesn't.
  A concurrent-callers benchmark would almost certainly
  diverge; out of scope here.
- **Image size is the real cost.** 159 MB vs 12.3 MB. If this
  pattern were deployed across many backends in a real
  cluster, the image-registry and pull-time costs would
  outweigh the code-compactness win. A python image built on
  alpine + compiled grpcio wheels could probably hit ~50-70
  MB; worth noting but not chased.

## Fundamentals touched

**Resource modeling freedom** (primary). The component server
needs no compile-time knowledge of any resource, AND no
compile-time knowledge of its backend's implementation language.
The Kubernetes API shape (GVR, OpenAPI, table columns, short
names) is fully parameterized over the gRPC contract. This
sharpens 0013's and 0017's findings: the "no per-resource Go
code" claim was really the weaker half of the real property,
which is "no per-resource code at all and no per-backend
language constraint on the component-server side." The
component server is a truly generic Kubernetes-wire-protocol
translator.

**Wire protocol fidelity** (secondary). The proto carries enough
structure — GVR + OpenAPI + columns + row-fields + managedFields
as opaque JSON — that library features layered on top (rich
explain, SSA, conflict detection, force-conflicts, watch
semantics) all work across the language boundary. Nothing in the
proto accidentally depends on Go's runtime or Go's JSON
serialization quirks. The JSON-bytes-for-payload decision in
0013 turns out to be load-bearing for language portability;
anything richer than JSON (e.g. protobuf-typed messages per
user resource) would have imported Go-specific codegen assumptions
into non-Go backends.

## Consequents (implementation-dependent; do not generalize)

- Python gRPC image is 159 MB against Go's 12.3 MB. A tighter
  python base (alpine + pre-built wheels, or even `python:3.12-
  slim-bullseye` with unused deps stripped) could cut this in
  half; still won't match distroless-static Go.
- `grpcio-tools 1.66.2` protoc-gen-python output uses
  same-directory imports that need rewriting for package-based
  imports. Handled by a `sed` in `gen.sh` / Dockerfile. Every
  python gRPC project past a toy handles this the same way.
- Protobuf gencode vs runtime version drift logs a warning at
  import time. Pin-or-ignore; non-functional.
- `python:3.12-slim` base uses `glibc 2.36`; the grpcio wheel
  is manylinux_2_17, which is compatible. A different base image
  (alpine) would force building grpcio from source; out of
  scope.
- `signal.signal(SIGINT, ...)` can only be registered on the
  main thread in CPython. Our setup respects this.
- `queue.Queue(maxsize=64)` matches the Go reference's channel
  buffer of 64. Arbitrary; either side of the RPC drops on
  full. Note backends are skeleton-grade; production would
  close the watcher and let the client relist.
- When we swapped the backend Deployment's image mid-experiment
  to measure Go latency, the existing Note disappeared (both
  backends are in-memory). We recreated for the comparison;
  the finding stands.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**:

- Resource-modeling-freedom section: the typed-vs-unstructured
  analysis from 0013/0017 can add a third datapoint: the typed
  Go wrapper needed in the component server is orthogonal to
  backend language. Component-server and backend share a proto,
  not a language.
- Wire-protocol-fidelity section: the "JSON-on-the-wire"
  decision from 0013's proto design is load-bearing for
  language portability in a way 0013 didn't explicitly name.
  Worth adding.
- Storage-independence section: the fourth storage axis
  (component + thin-backend) now has confirmed polyglot
  semantics. The backend can be in any language — the
  aggregation layer can sit in front of a Python service, a
  Rust service, a Node service, etc., and kubectl won't know.

For **EXPERIMENTS.md**:

- 0019 complete under Resource modeling freedom (primary) with
  cross-reference under Wire protocol fidelity.
- A "polyglot-backend-rust" follow-on candidate is natural
  (rust's `tonic` is the obvious mirror). Not required; the
  language-agnostic claim is validated.
- A concurrent-callers latency probe is a natural follow-on:
  does Python's per-RPC GIL cost surface under real load? Out
  of 0019's scope.

## Open questions raised

- **Does SSA's structured-merge-diff behave identically when
  the object's managedFields are produced by a python backend
  vs a Go backend?** 0019 confirmed the component server does
  the SMD work itself (backend just round-trips the opaque
  bytes), so the answer is "yes, structurally". A probe with a
  keyed-list field in spec would confirm this empirically.
- **Can the component server fan out to multiple backends in
  different languages concurrently (one backend per resource
  type)?** Out of scope; still one-backend-per-component-
  server.
- **Does the concurrent-kubectl-callers load profile favor one
  language?** Single-caller parity was established here; this
  is a natural follow-on.
- **Would a protobuf-typed object payload (instead of JSON
  bytes) be polyglot-friendly?** Superficially yes — protobuf
  generates bindings in every language — but each backend
  would need to regenerate bindings on schema change, which
  the JSON-bytes approach avoids. The current shape scales
  better for dynamic resources. Worth noting.
- **Is the 159 MB python image a real deployment blocker at
  scale?** For a lab no. For a real cluster with N backend
  services, yes it could be. A language-portability claim with
  a 13× image-size footprint isn't quite neutral.
