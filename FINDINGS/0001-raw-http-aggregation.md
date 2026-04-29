# Findings — 0001 raw-http-aggregation

## What we were trying to learn

Can a hand-rolled Go `net/http` server — zero `k8s.io/*` dependencies,
single file, ~650 lines of stdlib — register as a Kubernetes
APIService, get routed to by the aggregation layer, and present itself
to `kubectl` convincingly enough that `kubectl get` / `kubectl get -w`
/ `kubectl api-resources` all work? And in the process, what does the
aggregation layer actually send us, and what does kubectl demand of
what comes back?

## What we did

Stood up a kind cluster, generated a CA + serving cert with
`hack/gen-certs.sh`, applied the base manifests (namespace, SA, RBAC,
Service, APIService with CA bundle inlined), built the probe image,
loaded it into kind, and applied the experiment's deployment overlay.
The probe served HTTPS on :8443 with the mounted serving cert; it did
**not** verify the aggregator's client cert (deliberate — the goal was
observation).

The probe supports the discovery path, a fixed two-item `HelloList`, a
table renderer for `Accept: application/json;as=Table;...`, a
chunked-NDJSON watch that emits initial `ADDED` events and periodic
`BOOKMARK`s, minimal OpenAPI v2 and v3 documents, and `/livez` /
`/readyz`.

Ran `kubectl api-resources`, `kubectl get hellos`, `kubectl get hello
<name> -o yaml`, `kubectl get hellos -w`, `kubectl explain hello`,
`kubectl get hellos -v=6` (to see the wire-level calls), and
`kubectl --as alice get hellos` (to probe the impersonation path).
Then `hack/test-compat.sh` to record the scoreboard.

## What we observed

### The bar is lower than feared

The APIService went `Available: True` within seconds of the probe pod
becoming ready. `kubectl api-resources | grep hellos` worked
immediately, with the configured short name (`hi`). `kubectl get
hellos` returned a correct, well-formatted table. `kubectl get hello
world -o yaml` produced the exact JSON the probe served, deserialized
through kubectl's codec path. All of this, from a stdlib-only HTTP
handler.

A meaningful amount of the "Kubernetes-ness" you experience as a
client does not require `k8s.io/apiserver`. It requires getting the
wire formats right.

### The aggregation layer really does strip bearer tokens

Every request arriving at the probe carried the kube-apiserver's own
mTLS client cert. The `Authorization` header from kubectl never
reached us. Identity came through as `X-Remote-User`, `X-Remote-Group`
(one header per group, repeated), and — surprise — an
`X-Remote-Extra-Authentication.kubernetes.io%2Fcredential-Id` header
containing the X.509 SHA256 fingerprint of the caller's client cert.
The URL-escaping of `/` as `%2F` in the header name is how the
aggregation layer encodes arbitrary keys into HTTP header syntax.

This is worth noting specifically: **the aggregation layer is already
forwarding non-trivial `extra[...]` metadata.** Whatever was observed
about the caller by kube-apiserver's authn (credential ID, in this
case) flowed through untouched.

Example (log-line excerpt):
```
x-remote=[
  X-Remote-User=[kubernetes-admin]
  X-Remote-Group=[kubeadm:cluster-admins system:authenticated]
  X-Remote-Extra-Authentication.kubernetes.io%2Fcredential-Id=[X509SHA256=3488...bfb1]
]
```

Kube-apiserver's own internal machinery also calls the probe with
`X-Remote-User=[system:kube-aggregator]` and
`X-Remote-Group=[system:masters]` during discovery and openapi
refresh. Those are clearly marked as control-plane traffic, not
end-user traffic.

### Discovery gets hammered

During the first ~10 seconds after the APIService came up, the probe
received **dozens** of `GET /apis/aggexp.io/v1` requests, plus several
`GET /apis` and OpenAPI fetches. Some came with the v2 aggregated
discovery Accept header
(`application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList`),
others with plain `application/json`. The probe returned an
`APIResourceList` for everything, which the aggregator was happy to
accept even for the newer `APIGroupDiscoveryList` media type — kube-
apiserver fell back gracefully to the legacy discovery it understood.

Once stable, discovery calls quiesced. This aligns with a reasonable
mental model: aggregation-layer and openapi refresh traffic is bursty
at APIService availability transitions, low-rate afterward.

### kubectl explain is strict about OpenAPI

`kubectl explain hello` failed with:

> `error: GVR (aggexp.io/v1, Resource=hellos) not found in OpenAPI schema`

The probe returns a minimal OpenAPI v3 document (`openapi: 3.0.0`, one
path, one schema) and a minimal Swagger 2.0 document. Both are
technically well-formed JSON with the right top-level shape. kubectl
still refuses to find the GVR in them. This means `explain` needs the
schema to register the resource in a way the probe is not doing —
probably `x-kubernetes-group-version-kind` extensions on the schema
components, or a specific path structure the aggregator expects in
`/openapi/v3/apis/aggexp.io/v1`.

This is a concrete example of **wire protocol fidelity having sharp
edges**: a minimal structurally-valid OpenAPI is not the same as
kubectl-compatible OpenAPI.

### Watch works, but kubectl's behavior is interesting

`kubectl get hellos -w` produced output, then the connection closed
after a few seconds (kubectl seems to have hit a client-side deadline
or table-render-mode limitation — the output format shifted mid-watch
from one table schema to another). The probe logged:

```
watch: sent initial ADDED events (2 items)
watch: client disconnected: context canceled
```

So the probe served watch correctly (chunked NDJSON, initial ADDED
events, bookmarks every 10s); kubectl accepted it, rendered something,
then moved on. Our compat-scoreboard `kubectl get hellos -w` check
`PASS`ed because data streamed within the 5-second window.

Two unresolved questions:
- kubectl's table-mode watch rendered the data as a **different** table
  format on the second render (losing the `GREETING` column, showing
  `AGE` instead). Either kubectl is picking a default table for watch
  events, or it's failing to match our table metadata's row format.
  The data keeps flowing regardless.
- Because the probe's `resourceVersion`s are static (1, 2) and the
  watch's BOOKMARK RVs come from a disjoint counter (starting at 10),
  the client never actually saw a change — we didn't exercise
  "reconnect after 410" or RV-monotonicity behavior at all in this
  probe.

### Impersonation is stopped cold by kube-apiserver RBAC

`kubectl --as alice get hellos` returned:

> `Error from server (Forbidden): hellos.aggexp.io is forbidden: User
> "alice" cannot list resource "hellos" in API group "aggexp.io" at
> the cluster scope`

Kube-apiserver checked RBAC *before* proxying to us. The request never
reached the probe. This means:
- If we want custom identity-based authz to ever get consulted for
  user Alice, Alice must have RBAC that permits the verb/resource in
  the first place.
- Or we need to make the APIService's group authorization permissive
  (a permissive ClusterRole granting `get/list/watch` on
  `aggexp.io/*/*` to `system:authenticated`) and rely on our own
  authorizer for the fine-grained decision.
- This is a concrete, early confirmation that **per-request
  authorization in the aggregated apiserver is additive to
  kube-apiserver's RBAC, not a replacement for it.** You can only make
  things *more* restricted, not less, from your extension apiserver
  (without explicit RBAC-level permissiveness).

### What the probe cannot do

- `kubectl apply` fails (the probe is read-only — no PUT, no PATCH).
  Compat check recorded as FAIL on `expect` (`kubectl apply Hello`)
  and on `observe` (`kubectl apply --server-side Hello`). Expected.
- `kubectl explain` fails as noted.
- No real change events (everything is static).
- No label or field selectors.
- No pagination (`limit`, `continue`).
- No namespace enforcement (our resource is cluster-scoped, so N/A).

## What surprised us

- The bar for passing `kubectl get` is really low. The probe looks
  convincing despite being stateless, read-only, and schema-incomplete.
- The `X-Remote-Extra-*` forwarding was richer than expected — the
  probe got `credential-Id` for free, with no extra kube-apiserver
  configuration. This is a larger surface area for the identity-
  handoff fundamental than we'd been thinking about.
- kubectl's v2 aggregated discovery Accept header (`APIGroupDiscoveryList`)
  gracefully fell back to our v1 `APIResourceList`. Implementations
  that want to support both don't have to — the fallback is kind.
- The probe gets a lot of internal-aggregator traffic
  (`system:kube-aggregator`, `system:aggregator`) in addition to end-
  user traffic. Worth filtering when analyzing logs.

## Fundamentals touched

**Wire protocol fidelity.** Confirmed much of the pre-experimental
hypothesis in `SYNTHESIS.md`: discovery + list/get + table renderer +
health checks + basic watch are enough to pass `kubectl api-resources`
and `kubectl get` / `kubectl get -w`. `kubectl explain` is strict and
needs real OpenAPI with GVK markers. Server-side apply needs real
PATCH handling plus real OpenAPI; both absent here.

**Identity handoff.** The aggregation layer forwards `X-Remote-User`,
`X-Remote-Group`, and `X-Remote-Extra-*` as expected. A credential
identifier (X.509 fingerprint) came through in an Extra header without
any configuration on our side. Bearer tokens do not pass through —
confirmed. The extra-header mechanism looks plenty rich for carrying
downstream-useful identity metadata, within the caveats that those
fields are derived from whatever kube-apiserver's authenticators
chose to populate.

**Per-request authorization.** Observed but not exercised: kube-apiserver
applies RBAC *before* proxying. Our probe has no authorizer of its own;
authz happens upstream. If custom authz is to ever matter for
non-privileged users, RBAC has to let the request through first.

**Watch and consistency semantics.** Watch works mechanically at the
wire level (chunked NDJSON, ADDED + BOOKMARK). The behavior of kubectl
specifically (table rendering on watch) is surprising and possibly
worth a dedicated follow-up. The "does kubectl reconnect on a stale
RV / 410 Gone?" question remains unanswered because our RVs are
static.

## Consequents

- The exact shape of `X-Remote-Extra-Authentication.kubernetes.io%2Fcredential-Id`
  is a consequent of kube-apiserver 1.32+ (and of the client-cert
  authenticator specifically). Older versions wouldn't produce this
  header; other authenticators (OIDC, SA tokens) would produce
  different extras.
- `kubectl 1.35` is what was tested. Earlier kubectls might render the
  table differently (or not at all) for the `aggexp.io/v1` group. The
  v2-discovery-falls-back-to-v1 observation is specifically about
  that version pair.
- kube-apiserver 1.32's aggregation-layer discovery hammering burst
  (~dozens of requests at startup) is a current-version observation,
  not a generalizable claim. Some of this may relax in later versions.
- The fact that the base Deployment was initially using a placeholder
  image (`aggexp:dev`) that didn't exist caused `MissingEndpoints`
  briefly during first-deploy. This is a consequent of our two-step
  deploy (base then overlay), not a fundamental issue. Worth noting
  so future experiments don't rediscover this as a bug.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**:

- The `Identity handoff` section can be tightened: `X-Remote-Extra-*`
  is confirmed to carry non-trivial authenticator-derived data even
  without explicit configuration. The "consequent to flag" about
  Extra-smuggling should be upgraded from "theoretical escape hatch"
  to "observed baseline behavior" — people are already getting
  credential identifiers passed through.
- The `Wire protocol fidelity` section can gain a concrete finding:
  stdlib-level protocol fidelity is achievable and the first break is
  `kubectl explain` (because of OpenAPI strictness), not `kubectl
  get`.
- The `Per-request authorization` section should be nuanced: kube-
  apiserver RBAC is a gate the request must pass first. Per-request
  identity-based authorization in the AA can only restrict, not
  expand, relative to what RBAC already permits.

For **EXPERIMENTS.md**:

- The `ssa-probe` candidate just became more interesting because we
  now know `kubectl explain` is a distinct failure mode from "apply"
  failing. The question "does a conformant OpenAPI fix both in one
  step?" is natural.
- A new candidate suggests itself: `watch-table-rendering` — why does
  kubectl's watch mode render a different table schema on repeated
  events? Consequent-leaning but concrete.
- A new candidate: `rbac-permissive-aa` — AA with a permissive
  ClusterRole upstream so our authorizer becomes the real gate. Would
  exercise per-request authz end-to-end.

## Open questions raised

- What exact OpenAPI v3 shape does `kubectl explain` demand? Minimum
  required fields, required extensions, required media-type
  negotiation?
- Does kubectl's watch-mode table rendering behavior change if we
  emit `MODIFIED` events (not just `ADDED` + `BOOKMARK`)? Do real
  change events smooth out the rendering?
- What happens to a client that was watching when the serving cert
  rotates? (Own experiment: `cert-rotation-under-watch`.)
- If we set `APIService.spec.groupPriorityMinimum` low, does our
  group get deprioritized in ambiguous resource lookups? Worth
  knowing before we ship multiple groups.
- `X-Remote-Extra-*` header-name escaping: the probe observed `%2F`
  for `/`. What other characters get escaped? Is this documented
  anywhere authoritatively?
