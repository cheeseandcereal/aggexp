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

## Stateful-middleware-refinement arc (0022-0031)

A targeted arc refining the KRM middle-layer around the axiom that
**state is required**. Separates three axes (wire protocol, KRM
metadata state, business data) that the existing `runtime/component/`
substrate conflates. Ends with `runtime/component/v2/` substrate
promotion.

- **`0022-stateful-middleware-thesis`** — arc kickoff. Go interface
  sketch + design commitments. Status: complete. See
  `FINDINGS/0022-stateful-middleware-thesis.md`.
- `0023-schema-source-exploration` — probes three OpenAPI-source
  paths (backend-ships-OpenAPI, config-ships-OpenAPI,
  config-ships-JSONSchema-middleware-synthesizes) with tooling
  ergonomics per language. Recommends one for the rest of the arc.
- `0024-metadata-crd-store` — separates KRM metadata from business
  data via a shared `ResourceMetadata` CRD; rebuilds 0018's S3
  Bucket AA with middleware-managed metadata overlay.
- `0025-push-backed-watch` — push-capable backend streams events
  instead of middleware polling.
- `0026-http-json-backend-transport` — HTTP/JSON + SSE transport
  alongside gRPC.
- `0027-multiplex-middleware-server` — one middleware, many AAs.
  Reconciler watches `APIDefinition` CRDs, registers/deregisters
  APIServices dynamically. Status written back to CRD.
- `0028-metadata-store-gc` — garbage collects stale metadata CRD
  entries when backend objects disappear out of band.
- `0029-declarative-admission-in-config` — admission rules (CEL
  validations, JSONPath mutations) live in `APIDefinition` config;
  middleware evaluates without backend round-trip. Additive to
  0020's backend-RPC admission.
- `0030-runtime-component-v2-promotion` — substrate promotion. New
  `runtime/component/v2/` package embodying the arc's commitments.
- `0031-runtime-component-v2-parity` — first post-promotion consumer.

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

**`0013-krm-component-skeleton`** — first experiment in the KRM
middle-layer arc. A deployable generic component server registers a
resource type dynamically at startup by asking a thin gRPC backend
for its schema. Proves wire-protocol fidelity holds when CRUD is
delegated to an unstructured-JSON backend, while SSA and rich
`kubectl explain` degrade because they assume typed Go models.
Status: complete. See `FINDINGS/0013-krm-component-skeleton.md`.

**`0017-krm-protocol-refinement`** — refines 0013 to close the two
library-feature gaps it surfaced. `kubectl explain` rendered only a
catch-all description because the backend's OpenAPI wasn't composed
into the defs map; SSA broke at `managedfields.NewTypeConverter`.
Both close: threading the backend's OpenAPI through the defs map
(keyed at the Scheme's sample-object canonical name) unblocks
explain, and registering a typed Go wrapper (`dyn.Object`) under
the GVK unblocks the library's empty-object-GVK path so SSA works
end-to-end including conflict detection and force-conflicts.
Sharpens the typed-vs-unstructured boundary from 0013: the wrapper
must be typed (for Scheme.ObjectKinds), but its content can remain
an untyped bag (for resource-agnostic CRUD). Status: complete. See
`FINDINGS/0017-krm-protocol-refinement.md`.

**`0018-krm-component-parity-s3`** — 0009's ACK-inverted S3 Bucket
re-implemented as a gRPC backend behind the 0013 KRM component
server. User-facing parity with 0009; the inversion of the
apiserver vs. backend boundary does not change the wire behavior.
SSA fails loudly (0013-style, at typed-converter construction)
rather than silently (0009-style, managedFields vanish on next
GET). Backend-side S3 translation is ~674 lines vs 0009's ~664;
the ~500 lines of scheme/codegen/apiserver wiring from 0009 are
replaced by the (amortized) component server. Status: complete.
See `FINDINGS/0018-krm-component-parity-s3.md`.

- **`0005-argocd-compat`** — install ArgoCD into a dedicated kind
  cluster, point at an Application referencing plain Kubernetes
  manifests, observe what ArgoCD's cluster cache does with our
  read-only aggregated API. Status: complete. See
  `FINDINGS/0005-argocd-compat.md`.
- **`0014-flux-compat`** — sibling to 0005 with Flux v2.8.6. Whole
  question answered negatively: Flux's default controller set does
  **not** do discovery-driven LIST, so the "one LIST failure bricks
  cluster cache" pattern from 0005 does not apply. Flux never
  touched our AA across a 10-minute observation through an AA
  outage. Status: complete. See `FINDINGS/0014-flux-compat.md`.
- **`0015-argocd-application-targets-aa`** — ArgoCD Application
  targets a writable aggregated resource (0010's `widgets.aggexp.io/v1`
  Widget, CRD-backed facade). Initial sync, drift+re-apply, prune,
  self-heal, and cascade-delete all pass end-to-end. Surfaces a new
  facade-level obligation: ecosystem controllers that stamp tracking
  annotations (argocd's `tracking-id`) cause double-tracking when
  the facade passes annotations through to the backing CRD
  verbatim. Status: complete. See
  `FINDINGS/0015-argocd-application-targets-aa.md`.
- `flux-applies-a-repo` — derived from `0014` + sharpened by
  `0015`. The probed 0014 configuration had our AA off to the
  side. If a Flux Kustomization rendered a `Repo` (or `Widget`)
  object as part of its inventory, kustomize-controller would
  register a PartialObjectMetadata informer on our group and Flux
  would start exercising our AA's wire path. Does Flux's
  kustomize inventory (ConfigMap-tracked) avoid the "double-
  tracked via annotation echo" problem 0015 hit with ArgoCD?
  Depends on a writable AA (0010 works).
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
- **`0007-runtime-fs-driver`** — third backend using the extracted
  `runtime/` substrate: files on disk as `files.aggexp.io/v1`.
  Status: complete. See `FINDINGS/0007-runtime-fs-driver.md`.
- **`0009-ack-aggregated-s3`** — ACK-inversion: AWS S3 buckets as
  an aggregated API with no local state; live reads, live writes;
  watch via poll-diff. Surfaced the SSA managedFields persistence
  problem and the sync-vs-async backend boundary. Status:
  complete. See `FINDINGS/0009-ack-aggregated-s3.md`.
- **`0010-etcd-crd-facade-with-ssa`** — AA as a facade over a CRD
  served by the host kube-apiserver. Storage is `dynamic.Interface`
  calls, not a local `genericregistry.Store`. Demonstrates that
  library features (SSA managedFields, finalizers, ownerReferences)
  that `0009` lost work again when the backing store is a CRD — at
  the cost of one extra kube-apiserver hop per request. Status:
  complete. See `FINDINGS/0010-etcd-crd-facade-with-ssa.md`.
- **`0011-async-backend-sim`** — async-provisioning mock (30s
  provision / 10s deprovision) fronted by a stateless AA; probes
  the sync/async boundary 0009 flagged. Softens the "async breaks
  the inversion" claim — the model works if Create returns
  immediately with phase=Provisioning. Surfaced the
  `initial-events-end` bookmark gap in the substrate
  (`kubectl wait --for=jsonpath` fails). Status: complete. See
  `FINDINGS/0011-async-backend-sim.md`.
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
- `etag-aware-polling` — add ETag / If-None-Match to the GitHub
  client; measure how much rate-limit headroom it buys.
  Derived from `0004`.
- ~~`ssa-managedfields-in-backend`~~ — absorbed for the CRD-as-
  backend case by `0010`, which shows SSA semantics recover with
  a small apiVersion / field-path rewrite. Still open for
  non-CRD backends where the encoding has to live in backend-
  native metadata (S3 tags, GitHub description fields).
- `facade-annotation-allowlist` — extend 0010's facade to
  allow-list which annotations cross the exposed→storage
  boundary. 0015 found that passing `argocd.argoproj.io/tracking-id`
  through to the backing CRD causes ArgoCD's cluster cache to
  see each widget as two managed resources (one per GVK) and
  breaks auto-prune. Derived from `0015`.
- ~~`async-backend-sim`~~ — answered by `0011`.
- `cross-resource-references` — two resource types where one
  references the other; probes declarative-apply ordering under
  the inverted model. Derived from `0009`. Sharper after 0011:
  the interesting case is async resources where "the thing I
  depend on is provisioning" is observable as a phase, not just a
  404.
- `aws-cloudtrail-watch` — replace the S3 poll loop with
  CloudTrail/EventBridge subscriptions for a real-AWS
  deployment. Derived from `0009`.

**Retired candidates**:
- ~~`fs-driver`~~ — answered by `0007`.
- ~~`in-memory-hello`~~ — subsumed by `0002`.
- ~~`async-backend-sim`~~ — answered by `0011`.

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
  directly. Derived from `0003`. **Resolved by
  `0020-krm-admission-hook`** in the component-server architecture.
- **`0020-krm-admission-hook`** — adds validating + mutating
  admission RPCs to the 0017 component-server protocol. Closes
  the 0003 authz-vs-admission boundary for the component-server
  architecture: name-based CREATE policy and spec-field-shape
  policy are enforceable via gRPC `Validate`/`Mutate` RPCs, with
  the reason string reaching kubectl verbatim as HTTP 422 Invalid.
  Status: complete. See `FINDINGS/0020-krm-admission-hook.md`.
- `authz-cache-latency` — add a TTL cache to the custom authorizer,
  measure round-trip latency under load, compare to library-
  provided SAR caching. Derived from `0003`.
- **`0016-aa-authz-aware-controllers`** — probed three concrete
  patterns (A allow-list by SA, B blanket-SA, C upstream-RBAC strict
  + AA refines) against ArgoCD's gitops-engine cluster cache in a
  dedicated kind cluster. All three unblock ArgoCD; they differ in
  blast radius, per-controller maintenance, `kubectl auth can-i`
  accuracy, and where the 403 originates (AA vs. kube-apiserver).
  Recommended: Pattern C. Status: complete. See
  `FINDINGS/0016-aa-authz-aware-controllers.md`.

## Resource modeling freedom

- **`0007-runtime-fs-driver`** — (primary: demonstrated substrate
  consumption; secondary: third shape in the resource-modeling
  dimension). Status: complete.
- **`0009-ack-aggregated-s3`** — (secondary here; primary is
  storage independence). Fourth real backend. Status: complete.
- **`0019-krm-polyglot-backend`** — 0017's backend-note
  re-implemented in Python, fronted by 0017's unchanged
  component-server image. CRUD + watch + rich explain + SSA
  (incl. conflict detection and force-conflicts) all pass
  end-to-end; the component server cannot distinguish the
  backend's language. Python backend is ~30% shorter than the
  Go reference on the semantic line count; `kubectl get`
  latency is indistinguishable (71.6 vs 70.4 ms mean over 10
  serial calls). The JSON-bytes payload decision from 0013's
  proto turns out to be load-bearing for language portability.
  Image-size is the real cost: 159 MB python vs 12.3 MB Go.
  Status: complete. See `FINDINGS/0019-krm-polyglot-backend.md`.
- **`0021-runtime-component-parity`** — first consumer of the
  extracted `runtime/component/` substrate. A ~40-line `note-aa`
  + a verbatim 0017-style note-backend, built on top of the
  promoted `runtime/component/{proto,scheme,openapi,grpcbackend}`.
  Demonstrates that after the promotion, a new KRM-style consumer
  writes ~0.27x the handwritten Go of 0017 and carries zero
  generated code in its own tree. Confirms the substrate extraction
  held under a fresh consumer with no per-consumer patches.
  Status: complete. See `FINDINGS/0021-runtime-component-parity.md`.
- `http-driver` — generic HTTP endpoint as a Kubernetes resource.
  The "anything as a resource" stress test.
- `grpc-as-resource` — expose a gRPC service through aggregation.
- `virtual-composition` — an AA that projects a join of two underlying
  resources (kcp-style virtual workspace).
- `name-aware-admission` — validating admission hook in the AA
  enforcing name-based policy. Addresses the authz-vs-admission
  boundary flagged by `0003`. **Resolved by
  `0020-krm-admission-hook`** for the component-server architecture.
- `unstable-schema-backend` — a backend whose objects of the
  same "kind" have inconsistent fields; probe how the AA's
  schema + OpenAPI behave.
- `status-conditions-in-aa` — model status using the Kubernetes
  Conditions convention (type/status/reason/message) and see
  whether `kubectl wait --for=condition=Ready` behaves better
  than the `--for=jsonpath` path 0011 found broken. Probes the
  intersection of resource modeling and tooling idioms. Derived
  from `0011`.

**Retired candidates**:
- ~~`extract-runtime`~~ — done; see `runtime/` and `0007`.

## Watch and consistency semantics

- **`0008-long-lived-informer`** — client-go `SharedInformer`
  sustained against a 0002-style synthetic-RV AA; drove 410,
  AA pod restart, cert rotation, slow-handler scenarios. Status:
  complete. See `FINDINGS/0008-long-lived-informer.md`.
- **`0012-controller-runtime-manager-compat`** — controller-runtime
  Manager (caches, reconcile loop, leader election, finalizer
  lifecycle, ownerReference handling) against 0007's read-only
  AA. Status: complete. See
  `FINDINGS/0012-controller-runtime-manager-compat.md`.
- `controller-runtime-on-writable-aa` — `0012` was limited by
  0007's read-only backend. The interesting half of the SSA /
  finalizer story (managedFields persistence under a writable
  backend, backend-modeled finalizers) needs a writable AA.
  Natural target: `0009-ack-aggregated-s3` or a bespoke writable
  fs-driver. Derived from `0012`.
- `controller-runtime-dynamic-client-phantom-reconciles` — `0012`
  observed that pod-restart amnesia produces one
  (delete-reconcile + add-reconcile) pair per object on a
  stateless AA. At 3 objects it is invisible; measure the cost
  at 10k+ and see whether it shifts the case for deterministic
  UIDs. Derived from `0012`.
- `watch-list-feature-gate` — the `WatchListClient` feature gate
  (default-on in 1.32 client-go but default-off on 1.32 servers)
  is a different wire path. Not exercised by `0008`. Derived
  from `0008`.
- `ca-rotation-under-watch` — `0008` answered the "same-CA,
  rotated serving cert" case (invisible to informers). Open:
  CA rotation with simultaneous `APIService.caBundle` rotation
  and any client-cache invalidation that may depend on it.
- `hours-long-informer` — `0008` was 15-minutes-ish. What happens
  over many hours, through multiple backend poll cycles, several
  AA restarts, and genuine resource churn? Derived from `0008`.
- `watch-initial-events-end-bookmark` — emit the
  `k8s.io/initial-events-end` BOOKMARK annotation at the end of
  the initial-events stream from `runtime/storage`, and confirm
  `kubectl wait --for=jsonpath` (and WatchList-aware informers)
  stop timing out. Substrate-level work; derived from `0011`.

**Retired candidates**:
- ~~`watch-broadcaster-substrate`~~ — done; lives in
  `runtime/storage`.
- ~~`long-lived-informer`~~ — answered by `0008`.
- ~~`cert-rotation-under-watch`~~ — the same-CA case answered
  by `0008`; residual CA-rotation case tracked as
  `ca-rotation-under-watch` above.
- ~~`controller-runtime-manager-compat`~~ — answered by `0012`.

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
  mock backend). E2 replaces the mocks with a real GitHub App
  and real `api.github.com`. Concrete residual work: mint real
  installation tokens from a GitHub App private key in the broker;
  map Kubernetes identities to GitHub logins; swap
  `mock-github` out for `api.github.com` with a caching client.
- **E3**: `kubectl apply` on a `Repo` creates a real GitHub
  repository. Depends on E2 and on a resolution of the
  authz-vs-admission boundary (name-based creation policy).
- **E4**: ArgoCD syncs a `Repo` manifest from a Git repository.
  Prerequisite `0005-argocd-compat` is complete; the wire
  side is confirmed. `0015-argocd-application-targets-aa` confirmed
  ArgoCD's write-side behavior against a 0010-style writable
  aggregated API (sync, drift, prune, self-heal, cascade all
  work). The remaining dependencies are (a) MVP-example E3
  (writable Repo AA with identity-aware creation semantics) and
  (b) handling the `aa-authz-aware-controllers` gap `0005`
  uncovered.
