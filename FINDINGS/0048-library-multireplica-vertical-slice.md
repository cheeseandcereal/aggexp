# FINDINGS: 0048 — library-multireplica-vertical-slice

## What I was trying to learn

This is the capstone of the multi-replica library composition arc. The
arc validated five mechanisms in isolation, each on its own fork of the
0042 skeleton: 0042's host-CR resourceVersion authority, 0043's embedded
lock with emission filtering and pre-acquire OCC, 0044's per-watcher
identity-carrying watch, 0045's backend-as-source-of-truth read-path
reconcile, and 0046's OpenAPI-first generated types. The capstone
introduces no new mechanism. The single question: **do they compose into
one multi-replica aggregated API indistinguishable from a built-in to
kubectl, client-go, and controller-runtime — without mutual
interference?** And, specifically: does the emission filter survive real
lock contention; does per-watcher watch hold the RV contract; does
read-path reconcile coexist with the lock; and does the 0046-generated
type layer drop cleanly into the multi-replica machinery?

The interesting part of a composition experiment is never the parts that
work — those were already shown to work in isolation. It is the
interference that only the composition surfaces. I went looking for it.

## What I composed

One AA binary (`cmd/aggexp-widgets`) wiring all five mechanisms on
`runtime/server` + `runtime/group`, over the shared body-CRD backend, in
a 3-replica StatefulSet (kind cluster `aggexp-0048`). The served type is
the **0046-generated** `widgets.aggexp.io/v1.Widget` package, copied in
verbatim (only the openapi-definition map keys were rewritten to this
module's import path so `NewDefinitionNamer` matches). No kin-openapi
dependency entered this experiment — only the generated output, which
has none.

The hardest integration point, exactly as the brief predicted, was
folding 0043's embedded lock into 0044's per-watcher path. 0043's
emission filter lived inside the *single broadcaster* (`metastore.handle`
computing a `VisibleSignature` and suppressing unchanged MODIFIEDs). But
0044 replaced the single broadcaster with a per-watcher `Hub` consuming
a `RawSink` — the broadcaster, and with it the emission filter, was
bypassed. The composition required **re-homing the emission filter into
the per-watcher path**: the `RawSink` now forwards the decoded `Record`
(carrying the lock + observed body hash), and the `Hub.OnMetadataEvent`
applies the `VisibleSignature` filter keyed per record *before* the
per-watcher fan-out. Without that move, every lock-acquire and renewal
write would fan out to every watcher. This is the re-homing the (unbuilt)
0047 brief anticipated; 0047 was never implemented, so the capstone did
the fold itself.

Owner identity (0044) is a backend-only authz tag: the 0046-generated
Widget has no owner field, so the per-user owner is server-stamped onto
the body CR and never surfaced on the served Widget. The capstone keeps
the generated type pristine — proving the generator's output composes
without being modified to carry composition concerns.

Two ecosystem probes were built (controller-runtime is in scope here, an
allowed exception to the no-heavy-frameworks anti-pattern):
`client-go-probe` (a dynamic reflector/informer) and
`controller-runtime-probe` (a manager + reconcile loop + finalizer over
the served Widget as unstructured).

~4,540 hand-written Go lines plus the 776-line generated package
(~5,300 total).

## What I observed

**All six scenarios ran; the composition holds end to end, with two
genuine interference findings the isolated experiments could not have
surfaced.**

**Scenario 1 (kubectl round-trip) — passes.** `api-resources`, `get`,
`get -o yaml` (real UID, host etcd RV), `create`/`apply`, `explain`
(per-field, from the generated GVK-stamped OpenAPI), `get -w`,
`wait --for=jsonpath`, and `delete` all behave as for a built-in.
`kubectl apply --server-side` persists managedFields (stored on the
metadata CR, stitched back), detects conflicts against a prior field
manager (4 conflicts reported), and `--force-conflicts` resolves them.

**Scenario 2 (multi-replica writes) — passes, with a sharp edge.** Of 12
concurrent patches to one object, the embedded CAS lock serialized
acquisition and the losers got `409 Conflict`. The emission filter held
under real contention: each replica logged 16-18 `emission-filter-suppress`
events (the acquire/renewal churn) while the watch stream carried only
the committed body changes. Cross-replica reads were byte-identical
(all three replicas: same size/color/uid/rv) — the 0042 RV authority
survives the full stack. **The sharp edge** (below).

**Scenario 3 (pod restart under watch) — passes.** With `get widgets -w`
open, deleting replica `aggexp-1` did not interrupt the stream (the
load-balanced Service routed to survivors); a write made during the
restart streamed through; the object's UID was identical before and
after (`56c7fca9…`), RVs stayed monotonic, and there was no spurious
delete/add. Identity lives on the metadata CR (0042/0035), not
process-local state, so a replica restart is invisible to watchers.

**Scenario 4 (client-go reflector/informer) — passes.** Initial LIST,
live ADD/MODIFIED/DELETE, and the periodic resync (re-LIST at the
configured interval, deduped by the SharedInformer because the RV is
unchanged) all behave. Resume-by-RV works across the load-balanced pool.

**Scenario 5 (controller-runtime) — passes.** The manager cached,
reconciled, added a finalizer through the OCC-checked Update path, and on
delete removed the finalizer so the object was collected
(`reconcile: gone`). One transient OCC 409 fired (the controller's Update
raced its own informer-cache RV); controller-runtime retried cleanly, as
it does for any built-in.

**Scenario 6 (compat scoreboard, phase boundary) — passes.** All `expect`
probes PASS (`api-resources`, `get`, `explain`, `get -w`, APIService
Available); the Hello write probes SKIP because this experiment serves
Widget, not Hello. Recorded in `FINDINGS/compat/2026-05-29.md`.

**Read-path reconcile coexists with the lock.** The 0045 `getAmplification`
held at exactly **1.0** under composition (every served Get is one
backend authoritative host read) and the periodic sweep ran in ~1.5 ms.
Read-path reconcile, the lock, and per-watcher watch run together without
breaking each other — the explicit coexistence question the README posed
is answered yes.

## What surprised me — the two composition-only interferences

These are the capstone's payload. Neither was visible to the isolated
experiments, and both are exactly the kind of thing a composition exists
to find.

**1. The generated bare-`$ref` nested object breaks the create strict
decoder and the SSA typed-converter.** A Widget carrying
`spec.coordinates` (a `$ref` to the generated `Coordinates` struct) is
rejected on create with `strict decoding error: unknown field
"spec.coordinates.true"`, and on `apply --server-side` with
`.spec.coordinates.true: field not declared in schema`. The phantom
`.true` is the structured-merge typed-converter mis-walking the generated
schema's bare `$ref` field (a property with a `Ref` and no `Type`).
Scalar, enum, map, and array fields compose cleanly; only the `$ref`
nested object trips. **0046 claimed SSA passed — and it did, but only for
scalar fields** (`spec.color`, `spec.size`); 0046's SSA scenario never
applied a `$ref`-typed field. The capstone is the first thing to drive a
generated `$ref` object through the create+SSA path, and it fails. This
is interference between two arc members (0046's generated schema shape
and the SSA/strict-decode path the rest of the stack relies on) that
neither could have surfaced alone. I did **not** modify the frozen 0046
output; the sample Widget omits `coordinates` and the finding stands.

**2. The lock serializes acquisition, but the post-acquire writes are not
themselves lock-protected across replicas — and they surface as 500, not
409.** Under genuine concurrency (writes actually spreading across
replicas, which 0043 explicitly noted it could *not* reliably produce
because aggregation connection-reuse pinned traffic to one replica), the
losers split three ways: a clean `409 Conflict` from exhausting the
lock-acquire CAS retry budget (the intended outcome), but **also** a
`500 InternalError` on `backend.Put` (body-CR CAS conflict) and a
`500 InternalError` on `metastore.Commit` (commit-write CAS conflict).
The reason: only the *acquire* path retries on CAS conflict; the body
write and the commit-release write are single-shot. When two writers
genuinely race the same object on different replicas, the lock makes one
of them win acquisition cleanly, but the body/commit writes of a writer
whose lease was taken over (or whose body Put raced a concurrent Put) can
lose their own CAS and 500 rather than 409. 0043 saw none of this because
its harness never produced real cross-replica races; the capstone's
genuine 12-way concurrency did. The lock's *acquisition* contract is
intact; the gap is that the lock does not extend its serialization
guarantee to the two writes that follow acquisition, so a sufficiently
adversarial cross-replica race can still collide on them and report the
collision with the wrong status code.

**A third, milder artifact:** the per-watcher live backend source can
fire an event for a just-committed write *before* that replica's metadata
informer has the record cached, so the first ADDED for a fresh object can
carry an empty UID (and a DELETE can carry a synthetic UID when the
metadata CR is already gone). Both the reflector and controller-runtime
absorbed this cleanly (the real UID arrives on the next event), but it
means the very first watch event for a newly created object is not always
fully stitched. This is the two-live-sources race 0044 named (per-watcher
source vs. shared metadata informer), now visible because the read-path
reconcile and lock add latency between the body write and the metadata
commit.

## Fundamentals

**Wire protocol fidelity (primary).** The headline holds: five mechanisms
that were validated separately compose into one multi-replica AA that
kubectl, client-go, and controller-runtime treat as a built-in — with the
re-homing caveat (the emission filter must move into whichever component
owns the fan-out; it is not portable between the single-broadcaster and
per-watcher shapes for free) and the two interferences above. The RV
authority (0042), the embedded lock + emission filter (0043), the
per-watcher identity-aware watch (0044), and read-path reconcile (0045)
are mutually orthogonal *in their happy paths*: each leaves the others'
invariant intact (single host-RV authority, lock churn invisible to
watchers, per-user authz on every read/watch path, backend-authoritative
existence). The interferences are at the *boundaries*: the generated
schema shape vs. the SSA/strict-decode machinery, and the lock's
acquisition-only serialization vs. genuinely concurrent post-acquire
writes. Both are real, neither is fatal, and neither was reachable from
an isolated experiment — which is the entire justification for running a
capstone.

**Watch and consistency semantics (secondary).** The single host-CRD RV
authority survives the full composition: cross-replica reads are
byte-identical, watch resumes across the LB pool, pod restart is invisible
to watchers, and the emission filter keeps lock/renewal churn out of every
per-watcher stream under real contention. The re-homing of the emission
filter into the per-watcher path (keyed on `VisibleSignature`) is the
load-bearing move that makes 0043 and 0044 coexist; it confirms the 0047
brief's prediction that the filter must live in the fan-out component.

**Per-request authorization (secondary).** Per-user owner gating (0044)
composes with read-path reconcile (0045): `GetAuthoritative` /
`ListAuthoritative` decide existence, and the owner gate decides
visibility, independently. The two never conflict because existence is
the backend's call and visibility is the identity's call.

## Consequents (explicitly; tied to this lab's versions)

- **The `$ref` strict-decode / SSA failure is a consequent of the 0046
  generator's bare-`$ref` schema emission and kube 1.32's
  structured-merge typed-converter.** A generator that emitted an inlined
  object schema (or a `$ref` with an explicit `type: object`) would
  likely not trip it. The *shape* of the finding (composition surfaces a
  generated-schema edge the isolated generator test missed) generalizes;
  the specific `.spec.coordinates.true` error is version-specific.
- **The 500-vs-409 on post-acquire CAS races is tied to this lab's
  single-node kind etcd and the aggregation layer's connection reuse**
  (which had to be defeated by hand to even produce the race). The
  *structural* point — the lock guards acquisition, not the body/commit
  writes — generalizes; the exact status codes and frequencies do not.
- **Compat scoreboard results** (kubectl v1.36.1 client, kube 1.32 server,
  client-go 0.32.3, controller-runtime 0.20.4): all `expect` probes PASS.
  Recorded in `FINDINGS/compat/2026-05-29.md`.
- **getAmplification = 1.0** is the 0045 prediction confirmed under
  composition; tied to a host-CRD-read-per-existence-query backend.
- **Dynamic-client QPS had to be raised to 200/400** from the default
  5/10, or the 2-CR-writes-per-served-write (0043) plus the
  host-read-per-Get (0045) saturate the client-side rate limiter. A
  composition consequent the isolated experiments did not hit because each
  did fewer writes per request.
- **`GOTOOLCHAIN=local` + `go 1.24.0`** were required (FINDINGS/0046
  toolchain trap); the host toolchain is go 1.26 and would otherwise bump
  the directive and break the `golang:1.24-alpine` build.
- **The empty-UID-first-event** is a consequent of the per-watcher source
  racing the metadata informer, made visible by the added lock+reconcile
  latency; ecosystem clients tolerate it.

## What this changes for SYNTHESIS and EXPERIMENTS

For SYNTHESIS (Wire protocol fidelity): the arc's thesis — that the
multi-replica mechanisms compose into one built-in-indistinguishable AA —
holds, but with a sharpened claim. Composition is clean on the happy
path and at the RV/watch/authz boundaries, and the emission filter is
*portable only by re-homing* (it belongs to whichever component owns the
watch fan-out). The two interferences belong in SYNTHESIS as named
composition boundaries: (a) an OpenAPI-first generator's schema must be
exercised through the SSA/strict-decode path for *every* shape it can
emit, not just scalars, before it is trusted in a composition —
explain-passing is not SSA-passing for `$ref` objects; (b) a per-object
embedded lock serializes *acquisition* but not the writes that follow it,
so a multi-replica design that needs the body and commit writes to also be
serialized must either hold the lock across them with a retrying CAS on
each, or accept that genuine cross-replica races can collide post-acquire
and must be reported as 409 (not 500). The split-store read-consistency
requirement from 0042 (every stitched component on shared storage) and the
1:1 read amplification from 0045 both survive composition unchanged.

For EXPERIMENTS: the multi-replica library arc is complete through its
capstone. Two clean follow-ons fell out: (1) make the post-acquire body +
commit writes retry-on-CAS (or hold the lock across them) so cross-replica
write races surface as 409, not 500 — a small, well-scoped fix to the
0043/0048 lock path; (2) extend the 0046 generator (or its consumer
contract) to emit `$ref` nested objects in a shape the SSA typed-converter
accepts, and add a generator test that drives every emitted shape through
`apply --server-side`, not just explain.

## Open questions

- Would inlining the `Coordinates` schema (no `$ref`) or stamping
  `type: object` on the `$ref` field fix the SSA/strict-decode failure
  without a generator rewrite? Untested; it is the obvious first probe for
  follow-on (2).
- The post-acquire 500s appeared only when I defeated aggregation
  connection-reuse by hand. How often do they occur under a realistic
  client mix that does *not* deliberately spread writes? Uncharacterized.
- The empty-UID-first-event: would a short delay between the body Put and
  the metadata commit (or committing metadata first) eliminate it, and at
  what cost to the OCC ordering? Not probed.
- Read-path reconcile's 1:1 amplification was measured at lab scale with a
  single object under light load. Its interaction with the lock's
  2-CR-writes-per-write under *sustained* multi-replica write load (the
  0047 host-etcd-write-ceiling question, never built) remains open.
