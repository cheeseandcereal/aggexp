# Experiments

This is the **menu** of candidate experiments, organized by which
fundamental each primarily probes. The list is not an ordering.
Experiments are picked from the menu based on what is most interesting
to learn next. Numbering is sequential by start time; gaps are
fine.

Items without an `NNNN` prefix are candidates not yet started.

## Conventions

- Experiment slugs: `NNNN-<kebab-case-slug>`, zero-padded 4-digit
  number.
- `Status` in each experiment's README: `in-progress`, `complete`, or
  `abandoned`.
- After completion, the experiment is frozen. Later experiments may
  reference it but do not modify it.

---

## Wire protocol fidelity

**`0001-raw-http-aggregation`** — hand-rolled Go `net/http` probe. No
`k8s.io/apiserver`. Tests the minimum wire contract the aggregation
layer and kubectl actually demand. Status: complete. See
`FINDINGS/0001-raw-http-aggregation.md`.

**`0002-hello-aggregated`** — smallest real aggregated apiserver using
`k8s.io/apiserver`. Read/write Hello resource, watch via
`watch.NewBroadcaster`, synthetic resourceVersion, generated OpenAPI,
SSA working out-of-the-box. Status: complete. See
`FINDINGS/0002-hello-aggregated.md`.

- `argocd-compat` — install ArgoCD into the kind cluster, point at
  an Application referencing our API, observe what works/breaks.
- `flux-compat` — same with Flux.
- `protobuf-probe` — can we serve `application/vnd.kubernetes.protobuf`
  for basic kinds? Does it matter?
- `watch-table-rendering` — (consequent-leaning) why does kubectl's
  `-w` mode render differently depending on emitted events? Derived
  from `0001`.
- `apf-rbac-investigation` — what minimum RBAC lets an AA run with
  APF enabled cleanly, vs. the pragmatic `--enable-priority-and-fairness=false`
  we used in `0002`? Consequent-leaning but operationally useful.

**Retired candidates** (question already answered):
- ~~`openapi-explain-minimum`~~ — answered by `0002`: generated
  OpenAPI with GVK extensions from `openapi-gen` is sufficient; the
  hand-rolled minimal schema in `0001` was not, because the
  `x-kubernetes-group-version-kind` extension is the discriminator.
- ~~`ssa-probe`~~ — answered by `0002`: SSA works unchanged; no
  field-management code required on top of `rest.Patcher` +
  generated OpenAPI + internal version registration.

## Identity handoff

- `custom-authorizer-external-policy` — custom `authorizer.Authorizer`
  that consults an external HTTP policy service on every request.
  Proves per-request identity-based authz end-to-end.
- `github-driver-static-pat` — aggregated API exposing GitHub repos
  using a single static PAT. Identity is *observed* in logs, not yet
  forwarded.
- `identity-broker-github-app` — broker holding a GitHub App key;
  exchanges caller identity for a scoped installation token per
  request. The real identity-forwarding pattern.
- `oidc-federation` — kube-apiserver configured with structured
  authentication config to federate GitHub OIDC tokens; our AA
  observes GitHub claims arriving in `user.Info.Extra`.
- `extra-header-smuggling` — (consequent-leaning) what can round-trip
  through `X-Remote-Extra-*`? Includes a threat model.

## Storage independence

- `fs-driver` — filesystem files exposed as a Kubernetes resource;
  fsnotify-driven watch; real writes to disk.
- `in-memory-hello` — hello-level AA with ephemeral in-memory state
  and broadcaster-based watch. Likely subsumed by `hello-aggregated`.
- `external-db-driver` — postgres-backed driver; real resourceVersion
  derived from a sequence.

## Per-request authorization

- `custom-authorizer-external-policy` — (listed under identity
  handoff; probes both fundamentals).
- `authorizer-cel` — CEL expressions evaluated per-request against
  identity + request attributes. Compare to RBAC's declarative shape.
- `sar-delegation-compare` — compare AA with delegated
  `SubjectAccessReview` authz vs. AA with custom authorizer. Observe
  what each enables and constrains.
- `rbac-permissive-aa` — AA deployed with permissive upstream
  ClusterRole so the AA's authorizer becomes the real decision point.
  Probes "can-i" / SelfSubjectRulesReview UX when authz happens in
  the AA, not RBAC. Derived from `0001`'s observation that RBAC gates
  requests before they reach the AA.

## Resource modeling freedom

- `extract-runtime` — factor a `Driver` interface out of two
  experiments that demanded the same shape. Precondition: at least
  two drivers exist (e.g. `fs-driver` and `github-driver-static-pat`).
  Not yet — `0002` is the only library-backed experiment.
- `http-driver` — generic HTTP endpoint as a Kubernetes resource.
  The "anything as a resource" stress test.
- `grpc-as-resource` — expose a gRPC service through aggregation.
- `virtual-composition` — an AA that projects a join of two underlying
  resources (kcp-style virtual workspace).

## Watch and consistency semantics

- `watch-broadcaster-substrate` — real synthetic-watch implementation
  with monotonic RV and bookmarks. Likely arrives as part of
  `hello-aggregated`.
- `long-lived-informer` — controller-runtime informer sustained past
  the relist boundary. Observe 410-Gone handling and recovery.
- `cert-rotation-under-watch` — serving cert rotates mid-watch;
  observe client behavior.

---

## Consequent probes (worth doing; don't generalize)

- `extra-header-smuggling` — (listed under identity handoff).
- `openapi-aggregation-cost` — measure aggregator overhead as schema
  size grows.
- `availability-impact` — AA goes down; observe effect on kubectl,
  discovery cache refresh, and cluster-wide API latency.

---

## MVP-example track

**`example-e1-github-repos`** — the MVP-example scenario. `kubectl
get repos` returns the caller's GitHub repos; identity-aware authz
gates access; `kubectl get repos -w` streams updates. FINDINGS file
documents what the exercise revealed.

This is a composition of several lab experiments; it lands only
after its prerequisites are available (a real AA, a custom
authorizer, the github driver, and watch).
