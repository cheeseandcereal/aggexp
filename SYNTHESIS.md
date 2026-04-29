# Synthesis

This file reflects the current best understanding of the **problem
space** — aggregated Kubernetes APIs and their boundaries. It is
silent about code structure (that's `ARCHITECTURE.md`'s job) and
silent about individual experiments' specifics (those live in
`FINDINGS/`).

This file is **rewritten**, not appended to. When the author's mental
model of the problem space shifts meaningfully, the relevant sections
are rewritten. History lives in git.

Organized around the six named fundamentals. An entry under each
fundamental says what is currently believed and which FINDINGS
provide the evidence.

---

## Current state

This file has been seeded but not yet informed by any experimental
findings beyond what was gathered during the initial planning
research. The entries below are **pre-experimental hypotheses** —
beliefs the lab starts with. As experiments run, this file will be
rewritten to reflect what has been observed.

Claims here without a `FINDINGS/*` reference are unvalidated.

## Wire protocol fidelity

Hypothesis: a Kubernetes-compatible API requires, at minimum, a
discovery document (`/apis/<group>`, `/apis/<group>/<version>`), valid
list/get responses shaped with `TypeMeta` + `ObjectMeta`, meaningful
`resourceVersion` semantics, health endpoints (`/livez`, `/readyz`),
and — to be a first-class citizen rather than a toy — watch with
bookmarks. OpenAPI (v2 and v3) is required for `kubectl explain` and
for server-side-apply tooling, but the lab's compat scoreboard treats
SSA as observe-only, not a pass/fail gate.

Open questions:

- What is the *actual* minimum the aggregation layer will tolerate
  before refusing to route? (Probe 0001 aims at this.)
- What does kubectl do when discovery is partial, when openapi is
  partial, when watch misbehaves? (Probe 0001 will log.)
- How tolerant is the ecosystem (ArgoCD, Flux, controller-runtime) of
  a server that honors the protocol approximately vs. exactly?

## Identity handoff

Hypothesis: the aggregation layer provides a cryptographically-trusted
identity (`user.Info`: name, groups, UID, extras) over `X-Remote-*`
headers authenticated by mTLS against the requestheader CA. It
**strips bearer tokens** — the extension apiserver cannot see the
original credential. This is architectural, not a bug: it forces
identity-to-credential exchange to happen deliberately at the
extension apiserver via a broker (workload identity, token exchange,
or similar).

Consequent to flag: `X-Remote-Extra-*` headers *can* carry arbitrary
per-user values including, in principle, a pre-fetched credential.
This is an implementation escape hatch with real security
implications; worth a deliberate probe with a threat model.

Open questions:

- How cleanly does the `user.Info` → external-credential exchange
  pattern work in practice against GitHub? Against other backends?
- What is the UX cost (if any) of forcing callers to have two
  credentials — one for kubectl, one for the backend?

## Storage independence

Hypothesis: an aggregated apiserver does not require etcd. metrics-
server is the canonical proof: no etcd, in-memory cache, ~2000 lines
of Go. The cost is that you implement `rest.Storage` directly rather
than using `genericregistry.Store`, which means you own
resourceVersion generation, watch emission, and (if you want it)
field management.

For stateless backends (project external state as a resource), the
pattern is: poll the backend on an interval, maintain an in-memory
cache, emit watch events from a broadcaster, synthesize monotonic
resourceVersions.

Open questions:

- Where does the polling-driven synthetic-watch pattern break at
  scale? (Rate limits, broadcaster fan-out limits, resourceVersion
  wraparound?)
- What's the smallest viable resourceVersion scheme that satisfies
  client-go's relist-on-expired behavior?

## Per-request authorization

Hypothesis: this is where aggregated APIs most differ from CRDs. RBAC
gates what objects you can touch by shape; a custom authorizer in an
aggregated apiserver can gate *actions against a backend* based on
runtime facts about the caller and the request body. The distinction
is meaningful: "can alice create a GitHubIssue on repo X?" is a
declarative RBAC question ("can she create GitHubIssue in this
namespace?"), but "is she a collaborator on repo X right now?" is a
runtime question only an aggregated apiserver can answer naturally.

Open questions:

- How much per-request authz latency does tooling tolerate before
  workflows degrade? (kubectl, controller-runtime informers,
  ArgoCD cache-based sync each have different tolerances.)
- What's the right caching story? Per-identity? Per-(identity,
  resource)? How stale is too stale?
- Does delegating vs. not-delegating to kube-apiserver's RBAC change
  the behavior of standard tools?

## Resource modeling freedom

Hypothesis: anything with an addressable identity and a schema can be
projected as a Kubernetes resource. The interesting boundaries are
(a) things without a natural list operation, (b) things with
inconsistent identity (e.g. GitHub repos: the name can change), and
(c) things whose schema is not stable or not declarative.

Open questions:

- Can a protocol-level "driver" interface (the substrate's job) cover
  the range from filesystem files to GitHub repos to arbitrary HTTP
  endpoints without forcing ugly accommodations in any of them?
- What does modeling look like when the backend has no stable
  resourceVersion analog?

## Watch and consistency semantics

Hypothesis: watch is the thing that separates toys from first-class
Kubernetes APIs. Informers, controller-runtime, ArgoCD's sync cache
all require it. Without it, polling-only clients work but are
second-class. Synthetic watch (poll + diff + broadcast) is a known
pattern but introduces its own consistency pitfalls (resourceVersion
monotonicity, bookmark cadence, staleness during backend disruption).

Open questions:

- How does a conformant synthetic-watch actually perform against
  long-lived controller-runtime informers past the 410-Gone
  relist boundary?
- What happens to ArgoCD when an aggregated apiserver has a
  polling-induced latency floor (say 15s) and it's syncing a
  resource that changes fast externally?

---

## Process observations

Nothing yet.

Once experiments have run, this section will record whether the
ethos, AGENTS.md rules, and FINDINGS/SYNTHESIS flow are producing
useful signal or getting in the way. If patterns emerge that suggest
the process should change, that observation goes here before ETHOS or
AGENTS is rewritten.
