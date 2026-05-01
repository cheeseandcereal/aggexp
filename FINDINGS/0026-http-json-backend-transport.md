# Findings — 0026 http-json-backend-transport

## What we were trying to learn

Every KRM experiment so far (0013, 0017, 0018, 0019, 0020, 0021,
0023) uses gRPC for the component↔backend hop. The polyglot claim
from 0019 was established inside one transport; the transport itself
was never a variable. 0026 swaps it. Same protocol semantics
(GetSchema / Get / List / Create / Update / Delete / Watch), same
proto-level Kubernetes-wire position, different wire encoding:
HTTP/JSON for CRUD, real Server-Sent Events for watch.

The concrete questions:

1. Does the end-to-end wire contract hold across transport change?
   Same six scenarios as 0019/0021 expected to pass.
2. What's the debuggability delta? Can a human `curl` the backend
   and read the response, or are JSON-over-HTTP's wire forms as
   opaque as a grpc dump?
3. What does the backend-author LOC budget look like when the
   transport is stdlib HTTP + SSE instead of stdlib + grpc + protoc +
   generated code?
4. Is there a perf difference vs the gRPC baseline at lab scale?
5. What does the **real** SSE stream on the wire look like — is it
   `data: {...}\n\n` per spec, or does anything quirky surface?

0022's thesis commits to a swappable transport; 0026 is the
empirical probe that tells 0030's substrate promotion whether gRPC
or HTTP (or both) belongs as the default in `runtime/component/v2`.

## What we did

`experiments/0026-http-json-backend-transport/` has three pieces:

- **`backend-note/`** — a stdlib-only Go HTTP backend. Imports:
  `crypto/rand`, `encoding/hex`, `encoding/json`, `flag`, `fmt`,
  `log`, `net/http`, `sort`, `strings`, `sync`, `time`. Zero
  external modules (verified by its own `go.mod` which has no
  `require` block). Endpoints:
  ```
  GET    /schema
  GET    /objects/{namespace}
  GET    /objects/{namespace}/{name}
  POST   /objects/{namespace}
  PUT    /objects/{namespace}/{name}    (+ ?forceAllowCreate=true)
  DELETE /objects/{namespace}/{name}
  GET    /watch/{namespace}             (text/event-stream)
  ```
  Identity rides in `X-Aggexp-User-{Name,Uid,Groups,Extra-*}`
  headers. The `/watch` handler writes real SSE (`data: {json}\n\n`)
  with 20s comment-line keepalives.

- **`component/`** — a copy-fork of
  `experiments/0023-schema-source-exploration/track-b-middleware-synthesis/component/`
  with one architectural addition: a `--backend-transport=grpc|http`
  flag. For `grpc` it dials exactly as 0017/0021. For `http` it
  constructs an `httpClient` that implements
  `runtime/component/proto.BackendClient` by mapping each RPC to an
  HTTP request and parsing SSE for `Watch`. Everything above the
  wire (scheme, grpcbackend.REST, openapi.Compose, synthesis lift)
  is reused verbatim.

- **`manifests/`** — same shape as 0021/0023, with the aggexp
  Deployment's args carrying `--backend-transport=http` and
  `--backend-addr=http://note-backend.aggexp-system.svc:8080`.

Kind cluster `aggexp-http-tx`. Both transports exercised against the
same component binary on the same cluster (swapped between
`aggexp-note-backend-http:dev` and `aggexp-note-backend-grpc-0023b:dev`
by patching the Deployment image + Service port + aggexp's args).

## What we observed

### Per-scenario outcomes against `--backend-transport=http`

All six pass, byte-identical to 0019/0021 user-facing behavior:

| Scenario                                   | Result |
|--------------------------------------------|--------|
| `kubectl api-resources --api-group=aggexp.io` → `notes` | PASS |
| `kubectl apply -f sample-note.yaml`         | PASS |
| `kubectl get notes` (table render)          | PASS |
| `kubectl get note hello -o yaml`            | PASS |
| `kubectl explain note.spec` (rich)          | PASS |
| `kubectl apply --server-side --field-manager=alice` | PASS |
| SSA second applier (bob) → conflict         | PASS (`conflict with "alice": .spec.title`) |
| SSA with `--force-conflicts`                | PASS (split ownership: alice→body, bob→title) |
| `kubectl get notes -w` (streams)            | PASS (ADDED + MODIFIED observed) |

The component logs `component(0026): lifted 594 bytes JSON Schema ->
1057 bytes K8s OpenAPI` at startup, confirming 0023 Track B synthesis
runs once regardless of transport.

### Perf: HTTP vs gRPC, same machine, same cluster, back-to-back

Ten serial `kubectl` invocations, on `kind-aggexp-http-tx`, same
single Note object present. Transport swapped by patching the
component's `--backend-transport` flag and the backend Deployment's
image/port; everything else is constant. Times in ms
(`(time kubectl ... ) 2>&1 | grep real`):

```
kubectl get notes        mean   min   max   median
 HTTP backend            67.9    65    72    68
 gRPC backend            67.9    64    71    69

kubectl get note hello   mean   min   max   median
 HTTP backend            68.2    64    78    67
 gRPC backend            68.0    65    73    67
```

**Transport choice is invisible in end-to-end latency at lab scale.**
Same result 0019 found comparing gRPC-Python to gRPC-Go: the
aggregation-layer hop dominates (~65 ms floor; see 0003/0006/0019),
and the component↔backend leg is far under that floor regardless of
whether it's HTTP/1.1 or HTTP/2+grpc. This matters for the
substrate-promotion decision: the usual reason to prefer gRPC (wire
efficiency) is not observable.

### Backend-author LOC comparison

All three share the same 0013-descended proto-level contract (JSON
objects on the wire, `UserInfo` identity, server-streaming watch,
explicit `GetSchema` discovery). They differ in what the backend
author physically writes:

| Backend                                   | Total | Semantic | External deps       | Generated code |
|-------------------------------------------|-------|----------|---------------------|----------------|
| 0026 HTTP Go stdlib                       | 559   | 432      | 0                   | 0              |
| 0023 Track B gRPC Go (plain JSON Schema)  | 357   | 303      | grpc + uuid + pb    | ~2,000 LoC     |
| 0017 reference gRPC Go (full K8s OpenAPI) | 437   | 374      | grpc + uuid + pb    | ~2,000 LoC     |
| 0019 gRPC Python (for reference)          | 317   | 254      | grpcio + grpcio-tools + protobuf | regenerated at build |

Semantic-line counts strip blanks and `//` / `/* */` comments.
Generated-code LoC is the `.pb.go` + `_grpc.pb.go` output of
protoc-gen-go + protoc-gen-go-grpc for the 0017 proto
(`experiments/0017-krm-protocol-refinement/gen/aggexp/krm/v1/`: 1,581
+ 468 = 2,049).

**The stdlib-only HTTP backend is ~42% larger semantically than the
0023 Track B gRPC backend, and ~16% larger than the 0017 reference.**
That's the opposite of the naive expectation ("HTTP should be
simpler so it should be shorter"). Where the lines go:

1. **Hand-rolled HTTP routing** (`mux()` + a per-handler switch on
   method and path-arity) is ~40 lines that grpc generates for free
   in its `_grpc.pb.go` ServiceDesc registration.
2. **Per-handler error/JSON plumbing** (`writeJSON`, `writeError`,
   `errorBody`) is ~25 lines; grpc's equivalent is `status.Errorf`
   and return-a-nil-pointer, which compresses.
3. **SSE formatting** (`sseSend`, the event-loop in `watchHandler`
   with its keepalive ticker) is ~40 lines that grpc-go's
   server-streaming code path handles automatically.
4. **UID generation from `crypto/rand`** is 7 lines that
   `github.com/google/uuid` replaces with one import and one call.
5. **Protocol-envelope type declarations** (`schemaResponse`,
   `listResponse`, `watchEvent`, `errorBody`, `tableColumn`) are
   another ~30 lines — in the gRPC path these are the `.pb.go`
   generated structs and cost the backend author nothing in their
   source tree.

Offsetting those:

- **Go imports drop from 15 to 11** (all stdlib).
- **go.mod is empty** — the backend's entire supply chain is the
  Go distribution. No `go.sum`, no vendored protos, no
  protoc-gen-go toolchain to install.
- **Debuggability is real** (below). That cost doesn't show in
  LOC.

**Conclusion: HTTP/JSON + SSE does NOT reduce backend-author LOC in
Go.** The LOC cost of grpc generation is invisible to the backend
author's source tree (the `.pb.go` lives in `gen/`), and
`github.com/google/uuid` + the grpc-go runtime carry their weight.
What *is* reduced is the toolchain footprint — zero protoc, zero
codegen, zero non-stdlib deps — and the debuggability surface (see
below).

For languages where gRPC tooling is heavyweight (rust `tonic`,
TypeScript `grpc-js`, less-mainstream runtimes), the HTTP shape's
relative cost flips. This is the real polyglot argument for HTTP:
not "fewer lines" but "the lines you write use the ecosystem's
stock HTTP library instead of a grpc-specific one."

### Debuggability — the raw wire

`kubectl -n aggexp-system port-forward svc/note-backend 18080:8080`
and then `curl` the backend directly. Payloads are
indented-for-readability below but arrive on the wire as single-line
JSON:

```
$ curl -sS http://localhost:18080/schema
{"group":"aggexp.io","version":"v1","resource":"notes","kind":"Note",
 "singular":"note","namespaced":true,"writable":true,
 "supportsServerSideApply":true,
 "openapiV3":{"description":"A Note is a piece of titled text
  (HTTP/JSON + SSE backend).","properties":{"spec":{"description":
  "Caller-supplied fields.","properties":{"body":{"description":
  "Free-form body text. Optional.","type":"string"},"title":{
  "description":"Short display title. Required, 3-64 characters.",
  "maxLength":64,"minLength":3,"type":"string"}},"required":["title"],
  "type":"object"},"status":{"description":"Server-assigned fields.",
  "properties":{"updatedAt":{"description":"Server-assigned
  last-update time (RFC 3339).","format":"date-time","type":"string"}
  },"type":"object"}},"type":"object"},
 "columns":[{"name":"Name","type":"string","description":"Name of
  the note."},{"name":"Title","type":"string","description":"Note
  title."},{"name":"Age","type":"string","description":"Time since
  creation."}],
 "rowFields":[".metadata.name",".spec.title",
  ".metadata.creationTimestamp"],
 "shortNames":["nt"]}
```

```
$ curl -sS http://localhost:18080/objects/default/hello
{"apiVersion":"aggexp.io/v1","kind":"Note",
 "metadata":{"name":"hello","namespace":"default",
  "uid":"32138320d1d5ba83b5b134f0151d076d",
  "creationTimestamp":"2026-05-01T03:16:27Z",
  "managedFields":[{"apiVersion":"aggexp.io/v1","fieldsType":
   "FieldsV1","fieldsV1":{"f:metadata":{"f:annotations":{".":{},
   "f:kubectl.kubernetes.io/last-applied-configuration":{}}},
   "f:spec":{".":{},"f:body":{},"f:title":{}}},
   "manager":"kubectl-client-side-apply","operation":"Update",
   "time":"2026-05-01T03:16:27Z"}],
  "annotations":{"kubectl.kubernetes.io/last-applied-configuration":
   "{...}"}},
 "spec":{"title":"Hello HTTP",
  "body":"This note is served via the 0026 HTTP/JSON + SSE
   backend transport."},
 "status":{"updatedAt":"2026-05-01T03:16:27Z"}}
```

No tooling. No protoc. `jq` is optional. This is the headline
operational difference from the gRPC shape, and it is genuine: a
SRE diagnosing "is the backend returning what the middleware
expects" can `curl` and read.

### Raw SSE stream — the actual wire bytes

`curl -N http://localhost:18080/watch/default` with an `annotate`
triggered at t+2s and t+3s:

```
data: {"type":"ADDED","object":{"apiVersion":"aggexp.io/v1","kind":"Note","metadata":{"name":"hello","namespace":"default","uid":"32138320d1d5ba83b5b134f0151d076d","creationTimestamp":"2026-05-01T03:16:27Z","managedFields":[{...}],"annotations":{"kubectl.kubernetes.io/last-applied-configuration":"{...}"}},"spec":{"title":"Hello HTTP","body":"This note is served via the 0026 HTTP/JSON + SSE backend transport."},"status":{"updatedAt":"2026-05-01T03:16:27Z"}}}

data: {"type":"MODIFIED","object":{... "annotations":{"aggexp.io/curl-probe":"t1",...} ...}}

data: {"type":"MODIFIED","object":{... "annotations":{"aggexp.io/curl-probe":"t2",...} ...}}
```

(Each `data:` line in the capture above is a single line on the
wire; a blank line separates events; the whole event is
`data: <json>\n\n`. Middle events abbreviated for the FINDINGS;
they are byte-identical in shape to the first and last.)

This is canonical SSE per the HTML spec. The parser in the
component (`component/http_client.go` → `sseStream.Recv`) reads
line-by-line, drops comment lines (`:`), accumulates `data:`
lines until a blank line, JSON-decodes, and emits a
`componentpb.WatchEvent`. The 20-second keepalive comments
(`: keepalive\n\n`) from the backend are visible to curl users
but silently discarded by the parser, which is exactly what SSE
specifies.

### Dual-transport sanity check

After running the HTTP scenarios, the Deployment was patched to
swap in the gRPC backend + `--backend-transport=grpc` on the same
`aggexp-http-tx` cluster, and all six scenarios were re-run against
the same component binary. All pass. **The same
`aggexp-krm-component-http:dev` image serves a gRPC backend and an
HTTP backend by flag alone; no rebuild.** This is the hard-rule
test from the task: middleware supports both transports via a
flag, not a rebuild.

### What was hard

- **HTTP header name escaping.** The aggregation layer forwards
  `user.Info.Extra["authentication.kubernetes.io/credential-id"]`;
  the `/` character is illegal in HTTP field names. First try
  produced `net/http: invalid header field name
  "X-Aggexp-User-Extra-authentication.kubernetes.io/credential-id"`.
  Mirrored the aggregation layer's own `%2F` escaping — see
  `headerEscapeExtraKey` in `component/http_client.go`. Called out
  as a consequent below.

- **Grpc-codes-vs-HTTP-status translation.** `grpcbackend.REST`
  keys on gRPC status codes (`codes.NotFound`, `codes.AlreadyExists`,
  etc.) for its `translateErr` switch. The HTTP adapter re-maps HTTP
  status codes back to gRPC codes (`translateHTTPStatus`), which is
  ugly but meant the REST storage layer stayed unchanged. A cleaner
  shape in `runtime/component/v2` would abstract the error type into
  something transport-neutral.

- **`grpc.ServerStreamingClient[WatchEvent]` has 7 methods.**
  `sseStream` implements `Recv`, `Context`, `RecvMsg`, and stubs
  the rest (`Header`, `Trailer`, `CloseSend`, `SendMsg`). Only
  `Recv` and `Context` are actually used by
  `grpcbackend.REST.runUpstreamWatch`; the stubs are
  interface-compliance ballast. Not subtle, but worth flagging for
  0030: if `BackendClient` were a smaller hand-written interface
  instead of a grpc-generated one, half of this adapter's surface
  area would vanish.

### What surprised us

- **The backend-author LOC went UP, not down.** The naive
  expectation going in was "HTTP is simpler than gRPC so the
  backend should be smaller." In Go specifically, the simpler wire
  means more explicit routing and JSON-plumbing code, which
  exceeds the grpc boilerplate a `.pb.go` hides. This flips the
  polyglot-cost argument: HTTP's benefit isn't line count, it's
  zero-toolchain debuggability and ecosystem ubiquity for
  non-Go languages. See the "LOC comparison" table.

- **How thoroughly the transport is invisible at the wire boundary
  kubectl sees.** The component's JSON round-trip ends in the same
  REST storage layer, the same Scheme, the same field manager, the
  same SSA code path. SSA conflict strings are byte-identical.
  The `managedFields` rendered by `kubectl get -o yaml
  --show-managed-fields` looks the same on HTTP and gRPC. This is
  the substrate earning its keep: the transport is a seam; above
  the seam, nothing knows.

- **SSE just works.** Go's `net/http` Flusher path is exactly what
  SSE asks for; no framework, no dependency, no special
  configuration. The keepalive is a `fmt.Fprintf` + `Flush()`. The
  parser on the component side is 40 lines of bufio line-reading.
  One less reason to reach for gRPC for a server-streaming use
  case in a stdlib-friendly codebase.

## Fundamentals touched

**Wire protocol fidelity** (primary). Three observations add to
SYNTHESIS:

- **The component↔backend transport is freely swappable.** Given
  the same component binary, a flag selects between gRPC and
  HTTP/JSON+SSE; every user-facing Kubernetes behavior (CRUD +
  table rendering + rich explain + SSA + conflict detection +
  force-conflicts + watch) is identical. The proto-level shape
  (UserInfo forwarding, JSON-bytes payload, GetSchema at startup,
  server-streaming Watch) is the real contract; the serializer
  framing is an implementation detail. This sharpens the 0019
  finding: the "language-agnostic" claim was really "wire-shape-
  agnostic" all along; language is one dimension, transport is
  another, both orthogonal to Kubernetes-facing behavior.

- **End-to-end latency is transport-invariant at lab scale.**
  HTTP 67.9 ms mean vs gRPC 67.9 ms mean on `kubectl get notes`,
  10 iterations each, same cluster, same machine, same moment.
  The aggregation-layer floor (~65 ms) subsumes the
  sub-millisecond framing delta between HTTP/1.1 chunked and
  HTTP/2+grpc. Under concurrency or at higher-throughput hot
  paths this would likely diverge; at typical extension-apiserver
  scale it doesn't.

- **SSE is a first-class replacement for grpc server-streaming
  Watch.** The spec-shape (`data: {...}\n\n`) traverses the
  aggregation layer without special handling; kubectl's
  reflectors see the same `watch.Event` stream the component
  produces regardless of the upstream framing. SSE's long-lived
  connection handling, keepalive semantics, and reconnect
  behavior are well-understood and supported by every HTTP
  intermediary in existence.

**Resource modeling freedom** (secondary). The generic-component
shape now supports two transports with one binary; a third
(e.g. a durable message queue, a WebSocket flavor) is additive.
Confirms 0022's `BackendRef.Transport` commitment empirically:
the thesis's enum is meaningful, not aspirational.

## Consequents (implementation-dependent; do not generalize)

- **HTTP header name escaping.** The aggregation layer injects
  extras with `/` in the key (`authentication.kubernetes.io/
  credential-id`). `/` is illegal in HTTP field names per RFC 7230.
  The HTTP transport here percent-encodes disallowed chars
  (`headerEscapeExtraKey`), mirroring the aggregation layer's
  `%2F` convention. Any HTTP-backed backend author who wants the
  raw key must URL-decode after stripping the `X-Aggexp-User-Extra-`
  prefix.

- **SSE reconnect behavior** is the component's HTTP client
  responsibility, not the backend's. The substrate's
  `grpcbackend.REST.StartUpstreamWatch` retries with a 2s backoff
  on stream-disconnect; the same behavior covers the SSE path
  because `sseStream.Recv` returns `io.EOF` (or the
  context-cancelled error) on stream-end, which `runUpstreamWatch`
  treats uniformly.

- **In-cluster DNS + the 30s aggregator idle timeout.** The SSE
  handler fires a `: keepalive\n\n` every 20s. This is an
  arbitrary number; anything under the aggregation layer's own
  idle window would work. Not tested at 30s+, not tested through
  a long-running ingress.

- **grpc.ServerStreamingClient[WatchEvent] interface surface.**
  `sseStream` stubs four methods (`Header`, `Trailer`,
  `CloseSend`, `SendMsg`) that nothing in `grpcbackend.REST`
  calls. An evolving grpc-go interface would be a consequent risk
  for the HTTP adapter; the 0030 substrate should consider a
  smaller transport-neutral interface.

- **HTTP error-code → gRPC error-code retranslation.** The
  existing `translateErr` in `grpcbackend.REST` keys on gRPC
  codes; the HTTP adapter re-maps HTTP statuses back to gRPC
  codes at the boundary. Double translation works and is small
  but is an obvious 0030 refactor target.

- **Go 1.24 + gnostic-models v0.6.8** — recurring arc
  consequents. Honored via the root go.mod and the experiment's
  own go.mod.

- **The experiment's backend has its own `go.mod` with zero
  `require` entries**, to let `go mod tidy` empirically verify
  the "stdlib-only" claim. This structurally-isolates the backend
  from the rest of the repo's module graph.

- **Patching a live Deployment to swap transports** (the
  HTTP→gRPC→HTTP swap used to measure perf) worked without
  touching the component image; the component reconnects on its
  own once the Service's selector resolves to fresh endpoints.
  Consequent of how kind + kubectl + the component's startup
  dialing compose; not a property claim about the substrate.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**:

- **Wire protocol fidelity** section gains: the component↔backend
  transport is orthogonal to Kubernetes wire behavior. 0019 showed
  language-agnosticism inside one transport; 0026 shows transport-
  agnosticism inside the protocol shape. The two dimensions
  compose freely; a python backend over HTTP is as valid as a Go
  backend over gRPC, and neither changes what kubectl sees.

- **Resource modeling freedom** section gains a note that
  "transport cost is subordinate to toolchain cost." The naive
  measurement (LOC of the backend source) favors gRPC in Go;
  the operationally-useful measurement (toolchain / dep /
  debuggability) favors HTTP. 0030 should surface both.

For **EXPERIMENTS**: 0026 is complete. No new experiment lines
open. The `runtime/component/v2` substrate promotion in 0030
should carry an explicit `thesis.Transport` enum with both
values; the default is a design-time decision, not blocked on
more data.

## Recommendation for 0030's substrate default

**Dual-support, HTTP-preferred for new backends.**

- **Dual-support**: both `http` and `grpc` are first-class
  `Transport` values in `runtime/component/v2`. No deprecation.
  0017/0018/0019/0020/0021/0023 backends continue to work.

- **HTTP-preferred for new backends** because:
  1. Zero toolchain footprint (no protoc, no language-specific
     codegen, no `.pb.go` checked in).
  2. Real debuggability (`curl`, `httpie`, browser devtools).
  3. Ecosystem ubiquity — every language ships an HTTP server in
     its standard library or a zero-effort first-party module;
     grpc support varies widely.
  4. Transport-cost parity at lab scale (67.9 ms both ways);
     any future perf finding is small-constant-factor, not
     shape-changing.

- **gRPC retained** for two use cases: existing backends, and
  ecosystems with mature grpc tooling where the JSON-envelope's
  LOC overhead is felt. The Go-reference case in this FINDINGS
  (where grpc is slightly smaller) is one such.

The Go LOC-flip against HTTP's naive expectation is the single
non-obvious takeaway; it suggests 0030's promoted substrate should
not position HTTP as "the simpler transport" but as "the more
debuggable and more polyglot-friendly transport." The simplicity
argument is only true outside Go.

## Open questions raised

- **HTTP+SSE under concurrency.** Single-caller perf is a tie;
  many callers through HTTP/1.1 non-multiplexed connections may
  exhaust connection pools in a way HTTP/2+grpc would not. Not
  probed. Natural follow-on if 0030's substrate defaults to HTTP.

- **HTTP/2 for the HTTP backend.** The stdlib `net/http`
  server will serve HTTP/2 with TLS and ALPN; without TLS, it
  falls back to HTTP/1.1. kind's in-cluster Service routes at
  layer 4, so the backend's TLS posture is a 0030
  decision-point. Current experiment is plaintext HTTP/1.1 by
  design, same as the 0013/0017 gRPC is plaintext H2C.

- **Long-lived SSE through an ingress.** The aggregation-layer
  hop is in-cluster; an ingress-fronted backend (kube-apiserver
  → AA → ingress → backend) might terminate idle SSE differently
  than a direct ClusterIP hop. Not probed.

- **Backend-push over HTTP without SSE (e.g. long-poll).**
  The SSE choice is deliberate; 0025's push-capable backend
  discussion should decide whether long-polling is an acceptable
  fallback for backends that can't hold an SSE connection open.
  Out of 0026's scope.

- **The Go-specific LOC result.** Rerunning the measurement with
  a Rust backend (tonic vs stdlib axum/hyper) or a TypeScript
  backend (grpc-js vs native `http`) would tell us how much of
  the "HTTP is longer in Go" finding is Go-specific. 0019
  suggests the python grpc case would favor HTTP more
  decisively.

- **The `runtime/component/v2` interface shape.** 0030 should
  decide whether `Backend` is a grpc-generated interface (as
  today) or a hand-written one. If the latter, both transports
  become implementations of the same smaller contract and the
  HTTP adapter's 7-method interface-compliance ballast
  disappears.
