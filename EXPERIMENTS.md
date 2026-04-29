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

- **`0005-argocd-compat`** — install ArgoCD into a dedicated kind
  cluster, point at an Application referencing plain Kubernetes
  manifests, observe what ArgoCD's cluster cache does with our
  read-only aggregated API. Status: complete. See
  `FINDINGS/0005-argocd-compat.md`.
- `flux-compat` — same with Flux. Now more interesting after 0005
  exposed the gitops-engine "one LIST failure bricks cluster cache"
  behavior; does Flux's source-controller / kustomize-controller
  react the same way?
- `argocd-application-targets-aa` — ArgoCD Application directly
  targets a `Repo` (requires writable AA; depends on MVP-example E3
  prerequisites). Probes ArgoCD's behavior when the AA refuses
  writes vs. when it refuses reads. Derived from `0005`.
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

- **`0003-custom-authorizer-external-policy`** — (primary fundamental:
  per-request authz; also touches identity handoff). Status:
  complete. See `FINDINGS/0003-custom-authorizer-external-policy.md`.
- **`0004-github-driver-static-pat`** — aggregated API exposing
  GitHub repos using a static PAT. Identity is observed in logs and
  gated by the AA's authorizer; not yet forwarded to GitHub. Status:
  complete. See `FINDINGS/0004-github-driver-static-pat.md`.
- **`0006-identity-broker-github-app`** — broker-mediated
  identity-to-backend token exchange. Mock broker + mock GitHub,
  per-request caller-scoped token issuance and introspection.
  Status: complete. See
  `FINDINGS/0006-identity-broker-github-app.md`.
- `oidc-federation` — kube-apiserver configured with structured
  authentication config to federate GitHub OIDC tokens; our AA
  observes GitHub claims arriving in `user.Info.Extra`.
- `extra-header-smuggling` — (consequent-leaning) what can round-trip
  through `X-Remote-Extra-*`? Includes a threat model.
- `extra-field-impersonation` — `kubectl --as --as-user-extra` (1.35+)
  populates `user.Info.Extra`; does it survive the aggregation
  handoff and arrive at a custom authorizer? Derived from `0003`.
  Sharper with `0006` as baseline: under default impersonation,
  Extras are empty.
- `broker-token-cache` — add a short-TTL cache keyed on (user,
  owner, action) to the broker client; measure latency under serial
  and concurrent bursts. Derived from `0006`.
- `broker-with-authorizer` — run `0003`'s custom authorizer and
  `0006`'s broker together; observe combined UX (loud denial at
  authz, quiet denial at broker). Derived from `0006`.

## Storage independence

- **`0004-github-driver-static-pat`** — (primary fundamental:
  storage independence; also touches identity handoff and resource
  modeling). Status: complete.
- `fs-driver` — filesystem files exposed as a Kubernetes resource;
  fsnotify-driven watch; real writes to disk.
- `in-memory-hello` — subsumed by `0002-hello-aggregated`. Retired.
- `external-db-driver` — postgres-backed driver; real resourceVersion
  derived from a sequence.
- `repo-uid-stability` — use a deterministic UID scheme derived
  from the backend's stable ID and observe whether consumer
  behavior after a pod restart improves. Derived from `0004`.
- `github-rate-limit` — probe what happens when the poll loop
  actually hits GitHub's rate limit. What does the AA log? What
  do clients see? Does the cache go stale silently or visibly?
  Derived from `0004`.
- `github-webhook-watch` — feed GitHub push/PR events into the
  watch broadcaster directly and skip (or reduce) polling.
  Derived from `0004`.

## Per-request authorization

- **`0003-custom-authorizer-external-policy`** — listed under identity
  handoff; probes both fundamentals. Status: complete.
- `authorizer-cel` — CEL expressions evaluated per-request against
  identity + request attributes. Compare to RBAC's declarative shape
  and to `0003`'s HTTP-round-trip approach.
- `sar-delegation-compare` — compare AA with delegated
  `SubjectAccessReview` authz vs. AA with custom authorizer. Observe
  what each enables and constrains.
- `rbac-permissive-aa` — AA deployed with permissive upstream
  ClusterRole so the AA's authorizer becomes the real decision point.
  Effectively answered by `0003`; retire unless a specific new angle
  emerges.
- `name-aware-admission` — a validating admission hook in the AA
  that enforces name-based creation policy (the `bob-*` rule we
  could not enforce in the authorizer because CREATE carries no
  `Attributes.GetName()`). Probes the authz-vs-admission boundary
  directly. Derived from `0003`.
- `authz-cache-latency` — add a TTL cache to the custom authorizer,
  measure round-trip latency under load, compare to library-
  provided SAR caching. Derived from `0003`.
- `aa-authz-aware-controllers` — an AA whose policy-service
  default-denies will brick any ecosystem controller that
  auto-discovers-and-watches every API group it has RBAC for (0005
  observed this with ArgoCD's gitops-engine cluster cache). What
  pattern best accommodates them? Allow-list by SA; blanket
  `get/list/watch` for any `system:serviceaccount:*`; upstream-RBAC
  strict + AA-refines. Derived from `0005`.

## Resource modeling freedom

- `extract-runtime` — factor a `Driver` interface out of two
  experiments that demanded the same shape. Precondition now
  satisfied: `0002-hello-aggregated` (in-memory) and
  `0004-github-driver-static-pat` (external polling) share most
  of the rest.Storage boilerplate. Ready when a third driver
  adds pressure.
- `http-driver` — generic HTTP endpoint as a Kubernetes resource.
  The "anything as a resource" stress test.
- `grpc-as-resource` — expose a gRPC service through aggregation.
- `virtual-composition` — an AA that projects a join of two underlying
  resources (kcp-style virtual workspace).
- `name-aware-admission` — validating admission hook in the AA
  enforcing name-based policy. Addresses the authz-vs-admission
  boundary flagged by `0003`.

## Watch and consistency semantics

- `watch-broadcaster-substrate` — real synthetic-watch implementation
  with monotonic RV and bookmarks. Likely arrives as part of
  `hello-aggregated`.
- **`0008-long-lived-informer`** — client-go `SharedInformer`
  sustained against a 0002-style synthetic-RV AA; drove 410,
  AA pod restart, cert rotation, slow-handler scenarios. Status:
  complete. See `FINDINGS/0008-long-lived-informer.md`.
- `cert-rotation-under-watch` — partially answered by `0008` for
  the "same CA, rotated serving cert" case (invisible to
  informers). Still open: CA-rotation with simultaneous
  APIService.caBundle rotation and any client-cache invalidation
  behavior that may depend on.
- `controller-runtime-manager-compat` — controller-runtime on top
  of a synthetic-RV AA. `0008` only probed the raw reflector;
  controller-runtime's cache + reconcile loop add their own
  assumptions. Derived from `0008`.
- `watch-list-feature-gate` — the `WatchListClient` feature gate
  (default-on in 1.32 client-go but default-off on 1.32 servers)
  is a different wire path. Not exercised by `0008`. Derived
  from `0008`.

---

## Consequent probes (worth doing; don't generalize)

- `extra-header-smuggling` — (listed under identity handoff).
- `openapi-aggregation-cost` — measure aggregator overhead as schema
  size grows.
- `availability-impact` — AA goes down; observe effect on kubectl,
  discovery cache refresh, and cluster-wide API latency.

---

## MVP-example track

**`example-e1-github-repos`** — **complete**. See
`FINDINGS/example-e1-github-repos.md`. Scenario:
`kubectl get repos` returns a GitHub owner's repositories, gated
by the AA's identity-aware authorizer, with live watch. Composed
from experiments 0001–0004.

Possible follow-on examples (no commitment):

- **E2**: `kubectl get repos` with **identity forwarding** — each
  caller's action against GitHub is performed as that caller's
  identity via the identity broker. Prerequisite
  `0006-identity-broker-github-app` is complete (mock broker +
  mock backend); E2 would replace the mocks with a real GitHub App
  and real `api.github.com`.
- **E3**: `kubectl apply` on a `Repo` creates a real GitHub
  repository. Depends on E2 and on a resolution of the
  authz-vs-admission boundary.
- **E4**: ArgoCD syncs a `Repo` manifest from a Git repository.
  Prerequisite `0005-argocd-compat` is complete; the remaining
  dependency is a writable AA (MVP-example E3).
