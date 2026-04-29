# Findings — 0007 runtime-fs-driver

## What we were trying to learn

Two things at once:

1. **Does the substrate extraction work?** Concretely: can a new
   aggregated-apiserver experiment be written against `runtime/`
   with meaningfully less per-experiment boilerplate than
   0002-hello-aggregated or 0004-github-driver-static-pat, and
   without compromising wire-protocol fidelity against kubectl?
2. **Does the `Backend` abstraction hold up against a shape that
   wasn't driving its design?** 0002 (in-memory) and 0004 (GitHub
   polling) shaped the extraction. A third backend — filesystem
   entries under a directory — is the first real test of whether
   the interface captures the shared shape or over-constrains it.

## What we did

- Factored `runtime/server` (generic Options, flags, Config, Run),
  `runtime/storage` (Backend interface + rest.Storage adapter owning
  RV + broadcaster), `runtime/authz` (external-policy authorizer,
  wire-compatible with 0003's JSON protocol), and `runtime/group`
  (group-install helper).
- Added unit tests to each runtime package (ErrNotFound, label
  selector, RV monotonicity, watch fan-out, ResourceExpired on
  stale RV, write-path fan-out, MethodNotSupported on read-only
  create, external-policy allow/deny/transport-error, scope gating,
  Input shape, Group.Install preconditions).
- Added `doc.go` to every `runtime/*` package.
- Built experiment 0007: `files.aggexp.io/v1`, a ticker-polled
  projection of top-level files under a server-side root directory
  as cluster-scoped read-only resources. The entire backend is
  ~280 lines (type shape + scan loop + sanitization). The server
  wiring is ~110 lines (compose substrate Options, add two flags,
  run).
- Stood it up in a dedicated kind cluster `aggexp-runtime`,
  mounted a ConfigMap with three sample files, confirmed
  `kubectl get files`, `kubectl get files -o yaml`, `kubectl
  explain files`, and `kubectl get files -w` all behave as
  expected.

## What we observed

### Line-count reduction is in the target range

Measured on the code the experiments own *per-experiment*
(excluding type-scheme code, which stays with the experiment by
design):

| | 0002 | 0004 | 0007 |
| --- | --- | --- | --- |
| server/wiring | 181 | 225 | 113 |
| storage / backend | 413 | 398 | 283 |
| authz (per-experiment) | — | 187 | 0 (in substrate) |
| **subtotal** | **594** | **810** | **396** |

0007 vs 0004 is a **~36% reduction** in experiment-side boilerplate
for the equivalent set of responsibilities (aggregated apiserver,
external polling driver, identity-gated storage via the substrate's
reusable external-policy authorizer). 0007 chose not to deploy a
policy service, so the authz column is zero; if it had, `runtime/
authz` would be re-used, not re-implemented. The target in the
task brief was 30–50%; we landed inside that band.

The substrate itself is 1,032 lines (server 243 + storage
adapter 341 + storage helpers 119 + storage backend 103 + authz
173 + group 53). That code is written once and consumed by every
future experiment.

### The `Backend` interface shape held

Final shape, unchanged between "sketched from 0002+0004" and
"survived 0007's consumption":

```go
type Backend interface {
    New() runtime.Object
    NewList() runtime.Object
    Kind() string
    SingularName() string
    NamespaceScoped() bool

    Get(ctx, user.Info, name) (runtime.Object, error)
    List(ctx, user.Info, ListOptions) (runtime.Object, error)

    TableColumns() []metav1.TableColumnDefinition
    RowsFor(obj runtime.Object) ([]metav1.TableRow, error)
}

type WritableBackend interface {
    Backend
    Create(ctx, user.Info, runtime.Object) (runtime.Object, error)
    Update(ctx, user.Info, name, runtime.Object, forceAllowCreate bool) (runtime.Object, bool, error)
    Delete(ctx, user.Info, name) (runtime.Object, bool, error)
}

type Publisher interface { // implemented by the adapter, consumed by backends
    PublishAdded(runtime.Object)
    PublishModified(runtime.Object)
    PublishDeleted(runtime.Object)
    CurrentResourceVersion() string
}
```

Key design calls:

- **Non-generic.** Go generics over `runtime.Object` get awkward
  fast (type constraints, deep-copy method sets, list vs. singleton
  splits). The pragmatic shape is a plain interface with
  `runtime.Object` return types and TypeScript-style "implement
  WritableBackend to get writes" capability detection. The
  adapter uses a type assertion at construction time.
- **The adapter owns the resourceVersion + broadcaster.** The
  Backend never touches the RV counter and doesn't hold a
  broadcaster. Polling backends call `Publisher.PublishAdded/Modified/
  Deleted` on the adapter after they detect a diff, and the adapter
  stamps RV + fans out. This is how we avoid backends re-implementing
  the exact piece of code that was copy-pasted between 0002 and 0004.
- **ListOptions is minimal.** Just `LabelSelector`. We deliberately
  did not expose field selectors, pagination, or continue tokens in
  the Backend interface; the adapter applies label filtering
  defensively if the backend returns unfiltered results, which lets
  backends prioritize what's easy (returning everything) over what's
  performant (pre-filtering). Future experiments under real load
  may push back on this.

### The adapter's scope matches what every backend needed

When implementing the fsbackend I reached for, in order:

- Resource-version assignment on watch events — in the adapter.
- Monotonic RV counter — in the adapter.
- Label-filtered list — in the adapter (defensive).
- Stale-RV `ResourceExpired` — in the adapter.
- Watch broadcaster + initial-ADDED seed — in the adapter.
- TableConvertor — in the adapter (it delegates to
  Backend.TableColumns + Backend.RowsFor).
- Nothing I wanted was missing; nothing I didn't need was in the
  way. First-iteration interface ergonomics held up.

### OpenAPI still has to come from somewhere

The substrate is deliberately silent about how the OpenAPI
definitions are produced; it accepts a `common.GetOpenAPIDefinitions`
function and passes it to `genericapiserver.DefaultOpenAPIConfig`.
For 0007 we did not re-run `openapi-gen` — instead we copied
0004's generated file, s/Repo/File/ mechanical rename, then
rewrote the four File/FileList/FileSpec/FileStatus schema
function bodies by hand while leaving the ~2600 lines of meta/v1 +
runtime + version schema functions verbatim. This is a consequent-
leaning shortcut, noted in the experiment README's "Decisions made"
section. A fuller substrate could ship a pre-generated "aggexp
common OpenAPI definitions" file so experiments don't re-generate
the meta/v1 schemas; we punt that to the next pressure point.

### The `runtime/server` post-start hook contract

The hook signature the substrate accepts is
`func(ctx context.Context) error` (mapped internally onto
genericapiserver's `PostStartHookContext` shape). The fs scanner
uses it both to launch its poll loop and to register a
`<-ctx.Done(); files.Shutdown()` goroutine so the watch
broadcaster closes cleanly on shutdown. In 0004 this pattern was
ad-hoc per experiment; in the substrate it's a map of named
functions passed to Run. Minor but deliberate.

### An asymmetry that may bite later

`WritableBackend.Update` accepts a `forceAllowCreate bool` which is
the upsert-semantics flag server-side apply relies on. The adapter
handles the `Get` → `UpdatedObject` → `createValidation` /
`updateValidation` dance generically before calling into the
backend. This works for simple writable backends but collapses two
concerns — "does the object exist" and "should we create if not" —
that the backend might want to resolve against the upstream system
atomically (e.g. GitHub's `create or update if absent` semantics).
Not exercised in 0007 (read-only); flag as a potential seam when a
writable external backend comes through. It may force the adapter
to hand the raw Update primitive through without pre-fetching.

### Kind cluster isolation pays off

Spinning up `aggexp-runtime` distinct from the default `aggexp` let
0007 run while 0002/0003/0004's prior deployments could, in
principle, still be running on the default cluster. Worth
formalizing if we start running experiments in parallel; for now
the manual `--name` flag in `kind create` suffices. This is a
small operational finding, not a fundamental one.

## Fundamentals touched

**Resource modeling freedom.** Third real backend, different
shape (files — process-local, trivially refreshable, no external
API, permissions-bearing). The mapping was clean:
`<basename>` → resource name, `spec.path|size|mode` → FileSpec,
`status.observedAt` → FileStatus. No accommodations to the
Kubernetes model were necessary beyond name sanitization
(lowercase + alphanumeric + `-`/`.` only) which is a constraint
of Kubernetes names, not the substrate.

**Storage independence.** The etcd-less pattern is now parameterized:
the substrate's `server.Options` has the same "no EtcdOptions"
shape as 0002/0004 by deliberate construction, and experiments can
inherit it without copy-paste. Confirmed that the extraction
neither regressed existing behavior nor added friction.

**Wire protocol fidelity.** Unchanged: a substrate-built AA passes
the same kubectl interactions as a hand-rolled one. `kubectl
explain` in particular works without per-experiment effort once
the generated (or hand-rolled-from-generated) OpenAPI is supplied.
This reinforces the 0002 finding — generated OpenAPI with GVK
extensions is necessary and sufficient for explain.

## Consequents (implementation-dependent; do not generalize)

- 0007 copied 0004's `zz_generated.openapi.go` as a base rather
  than re-running `openapi-gen`, then rewrote the 4 type-specific
  schema bodies by hand. A future substrate probably wants a
  pre-generated "aggexp-common-openapi" file so experiments carry
  only their own type schemas. Consequent of wanting to avoid
  the code-generator tooling setup for every new experiment.
- The `Publisher` interface is stateful (holds RV counter), so
  every test of the fsbackend's publish path needs an RV-holder
  double. Minor; the `Publisher` shape chosen is small enough that
  tests implement it ad-hoc.
- `runtime/storage`'s default broadcaster size is 100, matching
  0002/0004. If sustained publishers fan out faster than watchers
  drain, events are dropped (`DropIfChannelFull`). The same
  behavior as 0002/0004; not a regression, not yet measured under
  load.
- `kind-aggexp-runtime` as a distinct cluster is a lab convenience;
  production AAs would share the cluster with other workloads.
- Substitution in `hack/deploy.sh` via `envsubst` survives only as
  long as the shell env holds `AGGEXP_IMAGE`. A subtle trap: if
  you run `./hack/deploy.sh` once without the env var, it applies
  the default `aggexp:dev`; a subsequent run with `AGGEXP_IMAGE`
  set does apply the correct image — but stuck replicasets from
  the prior run can linger and mask the change until you force-
  delete pods. Hit this twice during 0007 bring-up.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**:

- `Resource modeling freedom`: third real datapoint, reinforcing
  the "most shapes map cleanly to Kubernetes resources if the name
  is sanitized" observation from 0004. No new caveats.
- `Storage independence`: the "etcd-less server shape" is now a
  substrate-level guarantee, not a per-experiment pattern.
  SYNTHESIS should note that promotion happened and that line
  counts for new experiments dropped by ~36%.
- No new fundamental emerged. The six existing ones remain the
  right frame.

For **EXPERIMENTS.md** and `ARCHITECTURE.md`:

- `extract-runtime` — complete. Substrate is no longer
  hypothetical; `runtime/server`, `runtime/storage`,
  `runtime/authz`, `runtime/group` exist with tests.
- ARCHITECTURE's "Anticipated substrate shape" can be rewritten as
  the "Current state" section.
- 0007 added as a complete experiment.

## Open questions raised

- The `WritableBackend.Update` pre-fetch-then-mutate seam may
  collapse atomicity concerns for external backends where "update
  or create atomically" is the natural primitive. Worth revisiting
  when a writable external driver (github-app writes, filesystem
  writes, http-driver) comes through.
- OpenAPI generation is still per-experiment. A pre-generated
  `runtime/openapi/common.go` covering meta/v1 + runtime + version
  schemas would let experiments carry only their own type schemas.
  Not done; needs its own promotion task.
- Does the substrate-adapter's broadcaster handle a long-running
  informer past a pod restart any better than 0002/0004 did?
  Expected no — the adapter has the same in-process RV counter —
  but not measured.
- The fsbackend's hidden-file filter happens inside `scanOnce`, so
  a user with a `.` prefix in their filename gets silently omitted.
  That is "correct" for a lab experiment but a production
  fs-driver would want the policy to be configurable; the
  substrate doesn't have an opinion and shouldn't.
- `fsnotify` would eliminate the polling floor and give us real
  "file changed" → "watch event" latency below 5s. A dedicated
  experiment (`fs-driver-fsnotify`) can probe what that looks like
  and whether fsnotify's semantics (directory-wide events, no
  per-file event) compose cleanly with diff-based synthetic events.
