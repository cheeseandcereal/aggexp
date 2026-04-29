# Findings — 0006 identity-broker-github-app

## What we were trying to learn

Whether the "on behalf of the caller" pattern for an aggregated
apiserver actually works end-to-end: the AA never holds a static
credential, and every downstream backend call is authorized by
exchanging the caller's Kubernetes-native `user.Info` for a
short-lived, caller-scoped token at an identity **broker**.

`SYNTHESIS.md` had flagged this as untested under the *Identity
handoff* fundamental. 0004 left the broker path as
"`identity-broker-github-app` — the real identity-forwarding
pattern" in the menu. This experiment answers, with a fully mocked
chain, the following:

1. Can the AA cleanly pull `user.Info` from a request context and
   hand it to a broker?
2. Can the broker issue a caller-scoped credential that the AA
   stashes per-request and forwards downstream?
3. Does the mock downstream see exactly the caller the AA was
   serving, and refuse tokens it wasn't scoped to?
4. What does end-to-end latency look like for this chain?
5. Which fields of `user.Info` actually arrive at the broker?
6. What are the failure shapes when the broker or the backend is
   unreachable?

## What we did

Forked 0004. Kept the `repos.aggexp.io/v1` projection and the
generic-apiserver plumbing; ripped out the static-PAT GitHub
client, the poll loop, the process-wide cache, and 0003's custom
authorizer. In their place:

- `pkg/github/client.go` — `Client` now takes a `TokenProvider`
  interface (`FetchToken(ctx, user.Info, owner, repo, action)
  (string, error)`). Every HTTP call pays a broker round-trip and
  uses the returned bearer token.
- `pkg/broker/client.go` — HTTP client for the broker's
  `/exchange` endpoint. Exposes an `ErrDenied` so callers can
  distinguish "broker said no" from "broker is broken".
- `pkg/registry/repo/storage.go` — Get / List / Watch pull
  `genericapirequest.UserFrom(ctx)` and do per-caller fetches. No
  cross-caller cache. Watch does one fetch then parks (no diff
  loop; 0006 is not about watch semantics).
- `broker-service/` — stdlib Go. `/exchange` issues
  `fake-token-<user>-<rand>` on allow, 403 on deny. `/introspect`
  validates tokens on callback.
- `mock-github/` — stdlib Go. Serves canned `/users/{owner}/repos`
  and `/repos/{owner}/{name}` with 3–5 synthesized repos per
  owner. Validates incoming Bearer tokens by calling the broker's
  `/introspect`, scopes to (owner, action).
- manifests: broker Deployment+Service+ConfigMap, mock-github
  Deployment+Service, AA overlay with `--broker-url` and
  `--github-base-url=http://mock-github.aggexp-system.svc`.

Deployed to a fresh kind cluster `aggexp-identity` (distinct from
0004's `aggexp`). Exercised admin, `--as alice`, `--as bob`,
`--as mallory`, all against owner `kubernetes-sigs`. Captured
broker + mock-github + AA logs per run.

## End-to-end trace (single `kubectl --as alice get repos`)

Cleanly restarted pods. One impersonated call. All three services'
logs, interleaved by wall-clock time:

```
# broker-service
05:21:38  exchange user=alice groups=[system:authenticated] uid=
          extras=map[] owner=kubernetes-sigs repo= action=list
05:21:38  rule[2] ALLOW user=alice owner=kubernetes-sigs
          actions=[list get] reason="alice: read-only"
05:21:38  introspect token=fake-token-alice-713... -> valid
          user=alice owner=kubernetes-sigs actions=[list get]

# aggexp (AA)
05:21:38.206  repo-list user="alice" groups=["system:authenticated"]
              owner="kubernetes-sigs"
05:21:38.207  broker-exchange user="alice" status=200
              reason="alice: read-only" took="1.506764ms"
05:21:38.209  repo-list-ok user="alice" count=3 took="3.599343ms"

# mock-github
05:21:38  GET /users/kubernetes-sigs/repos user=alice
          owner=kubernetes-sigs action=list -> 200
```

The bearer token the AA sent to mock-github was embedded with
`alice` in its prefix — every hop in the chain could log the
caller without any cross-service context propagation. The
introspection log is correlated by the token string itself.

## What we observed

### Identity plumbing is clean

`genericapirequest.UserFrom(ctx)` inside a `rest.Storage` method
gives exactly the `user.Info` the library's authenticator
populated. Handed to a broker unchanged (name, groups, UID,
extras). Four distinct identities (kubernetes-admin, alice, bob,
mallory) arrived at the broker with their group memberships
intact; the broker matched rules against them and returned
decisions without any further identity manipulation.

No additional wiring was required. No context keys, no middleware,
no custom type. The "on behalf of the caller" pattern fits into
the existing `rest.Storage` interface.

### Impersonated identities arrive with empty `Extra`; real
### certificate identities carry `credential-id`

This was the sharpest-edged observation of the experiment. When
kubernetes-admin (real X.509 client cert) called, the broker saw:

```
user=kubernetes-admin
groups=[kubeadm:cluster-admins system:authenticated]
uid=
extras=map[
  authentication.kubernetes.io/credential-id:
    [X509SHA256=9fe9d75512417a9dc25141b2818764fe0a33148bd32af613332875528127ce19]
]
```

Same as 0001. But when the admin impersonated alice via
`kubectl --as alice`, the broker saw:

```
user=alice
groups=[system:authenticated]
uid=
extras=map[]
```

The `credential-id` is **not** forwarded through impersonation:
when `Impersonate-User` overrides the identity, `user.Info.Extra`
is whatever `Impersonate-Extra-*` headers specify (none, by
default). This is a real constraint: a broker that wants to
condition its decision on "what kind of credential the real caller
originally presented" cannot rely on the Extra for that under
impersonation. The original authenticator's observations are
erased by impersonation unless explicitly re-added. This matches
the Kubernetes impersonation wire contract; it is fundamental, not
a consequent.

For production brokers this implies: any decision about the root
caller's credential strength belongs upstream of impersonation.
The AA sees the effective identity; anything the authenticator
attached as Extra is only there for the *top-level* caller.

Equally worth naming: `UID` is empty for both the real admin and
impersonated identities in a kubeadm cluster. `user.Info.UID` is
only populated by authenticators that explicitly emit one (some
OIDC/SA token modes). It's available for a broker to use when
present but cannot be assumed.

### Broker denial → quiet empty list; broker outage → 500

Two failure modes with very different UX:

- **Policy denial** (broker returned 403): the AA returns an empty
  List and `NotFound` on Get. The caller sees `No resources
  found.` — visually identical to "you have no repos". Observable
  denial signal lives in the broker's logs, not the caller's.
- **Broker unreachable** (pod scaled to 0, dial connect-refused):
  the AA returns HTTP 500 with the dial error verbatim in the
  `message` field of the `metav1.Status`. `kubectl get repos`
  prints:
  ```
  Error from server (InternalError): Internal error occurred:
  broker: call: Post "http://broker.aggexp-system.svc/exchange":
  dial tcp 10.96.255.196:80: connect: connection refused
  ```

Mock-github unreachable gives an equivalent 500 with the `github
request: ... connect: connection refused` message.

The design choice this reflects: broker **policy** is
caller-visible only in outcome; broker **transport** failures are
caller-visible in detail. That's arguably wrong (the dial-error
string leaks the in-cluster DNS name of the broker) but it is the
library's default error surfacing and not something this
experiment decided to change. Consequent.

Interesting contrast with 0003: an HTTP authorizer's denial
becomes a verbose 403 with the policy's reason string in
`metav1.Status.Message`. A broker's denial becomes a silent empty
list. Same underlying decision; opposite UX. Neither is obviously
wrong; they suit different threat models. Operators picking one
need to understand which they picked.

### End-to-end latency

10 serial `kubectl get repos` under three identities, all end-to-
end including the full aggregation-layer proxy hop:

```
admin   (policy allow, mock-github hit):   real 0m0.700s  -> ~70ms/call
alice   (policy allow, mock-github hit):   real 0m0.693s  -> ~69ms/call
mallory (policy deny, no mock-github hit): real 0m0.681s  -> ~68ms/call
```

The broker exchange itself measured 200–400 µs inside the AA pod
(per the `took=` field on `broker-exchange` log lines). The
mock-github call is ~1ms. The remaining ~65ms is the aggregation-
layer proxy itself, consistent with 0003's 63–67ms.

The identity-handoff round trip is not the dominant cost at lab
scale. A broker in front of a real GitHub App would pay cold-JWT-
signing and installation-token-minting latency (GitHub quotes
~hundreds of ms); that *would* dominate and demands a token cache
keyed on (user, owner, action, ttl). This experiment does not
implement such a cache.

### Token lifetime and introspection

Broker issues `fake-token-<sanitized-user>-<6 hex bytes>` with
300s expiry. Mock-github validates on every request via
`/introspect`, which the broker's in-memory map answers. No
caching on either side. This adds one broker RTT per mock-github
call; in a real deployment the mock-github-equivalent (the actual
GitHub API) would validate its own installation token without a
callback, so this extra hop is a consequent of the mock chain.

### The resource-name projection survives identity-scoped data

Mock-github returns 3–5 canned repos per owner, deterministically.
`<owner>.<repo>` name shape from 0004 worked unchanged; UIDs are
now sha1(name) instead of per-pod random UUIDs (an incidental
improvement over 0004 — same name gives the same UID across
calls). Since there is no cross-request cache at all, "pod-restart
amnesia" from 0004 simply does not apply: there is no state to
lose.

### The authorizer-vs-broker distinction becomes concrete

0003 established a custom authorizer pattern. 0006 uses no custom
authorizer — the broker is the gate. The difference is where the
decision lives in the request lifecycle and what the caller sees:

| Aspect                  | Custom authorizer (0003/0004) | Broker (0006)                |
|-------------------------|-------------------------------|------------------------------|
| Decides                 | May I serve this request?     | May I fetch this backend data? |
| Runs before             | The handler dispatches        | Each backend call            |
| Denial surface to caller| HTTP 403 with reason string   | Empty list / NotFound (quiet)|
| AA code                 | Chains into `union.New`       | Becomes part of the storage path |
| Granularity             | per verb/resource/name        | per (owner, action)          |

These are complementary, not substitutes. A real production AA
would probably want both: an authorizer for "should this request
reach the AA at all" and a broker for "which backend calls does
this caller's action translate into". 0006 intentionally runs only
the broker to see what its UX feels like in isolation.

## What surprised us

- **Impersonated extras are empty.** The experiment was built
  assuming alice would carry some Extra. She didn't. Real X.509
  admin did. This is a real constraint on what a broker can key
  off for non-primary identities.
- **Wire-level nothing is new.** The aggregation layer, authn
  handoff, and `user.Info` plumbing are identical to 0003 and
  0004. The "broker in the hot path" pattern required zero new
  wiring from the library; it slotted into `rest.Storage`
  methods as plain Go calls. The pattern does not need new
  library support to exist.
- **Quiet denial is surprisingly non-disorienting.** An operator
  running `kubectl get repos` and seeing "No resources found"
  under mallory felt natural, not broken. The broker logs carry
  the signal. For an operator who *is* the broker-rules author,
  this is fine; for an end user trying to debug why they see
  nothing, it's frustrating. This is a UX choice worth naming
  explicitly in any substrate abstraction.
- **Stateless AA is actually simpler than the 0004 shape.** No
  poll loop, no broadcaster, no cache invalidation. Just: read
  user from ctx, call broker, call backend, return. The 0004
  findings file noted pod-restart amnesia; 0006 is "amnesia by
  construction" and is about 200 lines shorter.

## Fundamentals touched

**Identity handoff.** Primary. Headline findings:

- The `user.Info` → broker → scoped-token → backend pattern works
  end-to-end with zero new library support. The AA code is a
  few tens of lines around `FetchToken`.
- `kubectl --as` impersonation *erases* the original caller's
  `user.Info.Extra`. A broker cannot key on the credential-Id of
  the impersonating caller; it only sees the impersonated
  identity's (usually empty) Extra. This is fundamental: it
  follows from the wire shape of impersonation, not from a quirk
  of this library or version.
- `user.Info.UID` is not reliably populated. The broker must
  treat UID as optional, and base identity decisions primarily on
  (user, groups, extras) — which means its decisions about a
  caller's effective privileges are only as specific as the set
  of headers kube-apiserver's authenticators put on the wire.
- Broker latency at lab scale (local kind cluster, mock broker)
  is ~300µs — invisible next to the ~65ms aggregation-proxy
  floor. A real GitHub App broker would be the dominant cost and
  would need caching; the structure is ready for a cache layer
  but 0006 does not have one.

**Per-request authorization.** Secondary. 0006 is a concrete shape
comparison with 0003:

- Authorizer denial = 403 with reason (loud). Broker denial =
  empty result (quiet). Both are per-request identity-based; they
  sit in different parts of the request lifecycle.
- Authorizer needs no backend coupling. Broker is wedged into
  every backend call; its availability is on the critical path
  for *results*, not just for *access decisions*.
- The two patterns compose cleanly: nothing in the library
  prevents running both authorizer and broker in the same AA.

**Storage independence.** Tertiary. 0006 demonstrates a fully
stateless AA: no cache, no poll loop, no shared state. Every
request is an on-demand fetch. Compared to 0004:

- Pod-restart amnesia: not applicable — no state to lose.
- Rate-limit coupling: per-caller, not aggregated; each caller's
  actions are bounded by broker policy, not by a global quota the
  AA enforces.
- Watch semantics are degraded (per-caller initial fetch then
  park; no live updates). This trade-off would need revisiting if
  a real consumer wanted informer-grade semantics.

## Consequents (implementation-dependent; do not generalize)

- The dial-error leak in the `message` field of 500 responses
  from the AA is the library's default error surfacing. A
  production AA would normalize these messages to avoid leaking
  in-cluster service DNS names to external callers.
- The mock-github's `/introspect` callback costs one broker RTT
  per GitHub-shaped call; real GitHub wouldn't do this. It's an
  artifact of this lab's "single source of truth for token
  validity" decision.
- Token shape `fake-token-<user>-<6 hex bytes>` embedding the
  user in the token is a lab-local observability choice, not a
  production-safe pattern. Real installation tokens are opaque.
- Latency numbers (200–400µs broker exchange, ~1ms mock-github)
  are specific to: stdlib servers in the same kind node,
  no network, no encryption. Real numbers will differ.
- `credential-id` is populated by kube 1.32's client-cert
  authenticator automatically. Other authenticators (OIDC, SA
  tokens) populate Extras differently; some not at all.
- The `Impersonate-Extra-*` header mechanism exists
  (`kubectl --as-user-extra`); this experiment did not exercise
  it. Whether extras *set* via that mechanism survive
  aggregation-layer proxying is a separate probe. 0006 observed
  only the *default* impersonation behavior (no extras).

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**:

- The *Identity handoff* section's standing "open question" —
  "cleanest pattern for 'do X on behalf of the caller' to systems
  that don't speak Kubernetes identity" — is now answered. The
  broker-mediated pattern works end-to-end, slots cleanly into
  `rest.Storage` methods, and needs no new library support. Add
  the impersonation-erases-extras finding explicitly; it's a real
  constraint the previous prose did not name.
- The *Per-request authorization* section can add a contrast
  finding: authorizer-as-gate and broker-as-gate are different
  places in the request lifecycle with different UX surfaces.
  Both are valid; the choice depends on whether denial needs to
  be loud or quiet.
- The *Storage independence* section can observe that a fully
  stateless AA (no cache at all) is viable, at the cost of watch
  semantics and bounded per-call latency.

For **EXPERIMENTS.md**:

- `identity-broker-github-app` — marked complete.
- `extra-field-impersonation` becomes sharper: we now have a
  concrete baseline (default impersonation = empty Extra) to
  compare against once `--as-user-extra` is exercised.
- New candidate: **`broker-token-cache`** — add a short-TTL cache
  keyed on (user, owner, action) and observe latency under a
  burst of serial calls. The absence of a cache is the obvious
  follow-up knob. Derived from 0006.
- New candidate: **`broker-with-authorizer`** — run 0003's custom
  authorizer and 0006's broker together; observe the combined UX
  (loud denial at authz, quiet denial at broker). Probes the
  "these compose" claim explicitly.
- The MVP-example track's **E2** is now one step closer: its
  precondition experiment is complete; E2 itself remains (a real
  end-user-forwarded credential path against real GitHub, not
  the mock).

## Open questions raised

- If a production broker needs "what type of credential did the
  original caller present?", where does that data live? Not in
  `user.Info.Extra` post-impersonation. Would a validating
  admission webhook earlier in the chain capture it and pass
  through via a custom header? That's a concrete pattern worth
  probing.
- Does `kubectl --as-user-extra` populate extras that *do*
  survive to the AA under impersonation? 0006 observed the
  default (empty). Orthogonal but cheap probe.
- What is the right token cache shape? TTL = min(broker-returned
  `expiresIn`, wall-clock safety margin), keyed on (user,
  groups-hash, owner, action)? This is a substrate-level concern
  once two experiments have demanded it.
- Error-surface normalization: should the AA library translate
  "broker: call: dial tcp ...:80: connect: connection refused"
  into "backend identity service unavailable" before sending
  the 500? Leaks vs. debuggability.
- Under watch: a per-caller initial fetch was enough for kubectl
  here, but a controller-runtime informer would hold the watch
  open for hours and expect change events. The current stub
  parks silently. When does that break for real consumers? Worth
  a dedicated probe if watch semantics over broker-mediated
  backends matter for a future example.
