# Experiment 0006: identity-broker-github-app

A GitHub-shaped `repos.aggexp.io/v1` resource, with every
downstream call minted on behalf of the caller by a mock
**identity broker**. The AA no longer holds a static PAT; instead,
on each `kubectl get repos` it pulls `user.Info` off the request
context, asks the broker `"can I have a token for alice to
list owner=kubernetes-sigs?"`, and uses the returned opaque token
to call a **mock GitHub** service that enforces the token's scope.

This is the probe experiment for the **Identity handoff**
fundamental's headline pattern (broker exchange of
Kubernetes-native identity for a caller-scoped backend
credential). Forked from 0004; the static-PAT path is removed and
the custom authz-over-HTTP from 0003 is also dropped — the broker
is the single identity-to-backend gate.

Nothing in this experiment touches api.github.com. The mock chain
is entirely in-cluster:

```
kubectl (--as alice)
  -> kube-apiserver (aggregation)
    -> AA (aggexp-repos)
      -> broker.aggexp-system/exchange  (issues scoped token)
      -> mock-github.aggexp-system/users/<owner>/repos
           -> broker.aggexp-system/introspect (validates token)
```

## Hypothesis

1. The AA can cleanly extract `user.Info` from a request context
   (`genericapirequest.UserFrom(ctx)`) and pass identity + intended
   action to a broker.
2. The broker can issue a caller-scoped credential that the AA uses
   per-request; the mock GitHub service sees the expected identity
   shape.
3. Denial at the broker (no rule matches, or an explicit deny rule)
   surfaces to `kubectl` as an empty list, not a 500.
4. End-to-end latency (broker exchange + mock-github call) is on
   the same order as 0003/0004.
5. All `user.Info` fields arrive at the broker: name, groups, UID,
   extras including the X509 credential-Id observed in 0001.

## What's different from 0004

- `pkg/github/client.go` — `New(base, TokenProvider)` instead of
  `New(base, token string)`. Every call pays one broker round-trip
  in exchange for a caller-scoped bearer token.
- `pkg/broker/client.go` — new. HTTP client for the broker's
  `/exchange` endpoint. Surfaces broker denial via an `ErrDenied`
  type the caller can branch on.
- `pkg/registry/repo/storage.go` — **no poll loop, no shared
  cache**. Every Get / List / Watch pulls `user.Info` from
  `genericapirequest.UserFrom(ctx)` and does an on-demand
  per-caller fetch. Watch emits initial ADDED events from that
  per-caller fetch then parks; live-change events are out of scope.
- `pkg/authz/` — removed. The broker is the gate; the 0003 custom
  authorizer is not in play here.
- `broker-service/` — new stdlib-only service. Loads JSON rules
  from a ConfigMap; issues fake tokens via `/exchange`; validates
  tokens on `/introspect` for mock-github.
- `mock-github/` — new stdlib-only service. Serves
  `GET /users/{owner}/repos` and `GET /repos/{owner}/{name}` with
  3–5 canned repos per owner. Validates incoming Bearer tokens by
  calling the broker's `/introspect`.
- `manifests/` — broker Deployment+Service+ConfigMap,
  mock-github Deployment+Service, AA overlay with `--broker-url`
  and `--github-base-url=http://mock-github.aggexp-system.svc`.

## How to run

This experiment runs on a distinct kind cluster (`aggexp-identity`)
so it can coexist with the 0004 cluster on a dev machine.

From repo root:

```bash
# 1. certs + cluster
./hack/gen-certs.sh
kind create cluster --name aggexp-identity
kubectl --context kind-aggexp-identity create namespace aggexp-system

# 2. base manifests (namespace, SA, RBAC, Service, APIService).
#    hack/deploy.sh uses whatever is the current kubectl context;
#    pin it explicitly for safety.
kubectl config use-context kind-aggexp-identity
./hack/deploy.sh deploy/manifests

# 3. build images and load into the cluster
HERE=experiments/0006-identity-broker-github-app
docker build -t aggexp-repos-0006:dev       "${HERE}/"
docker build -t aggexp-broker-0006:dev      "${HERE}/broker-service/"
docker build -t aggexp-mock-github-0006:dev "${HERE}/mock-github/"
kind load docker-image aggexp-repos-0006:dev       --name aggexp-identity
kind load docker-image aggexp-broker-0006:dev      --name aggexp-identity
kind load docker-image aggexp-mock-github-0006:dev --name aggexp-identity

# 4. experiment overlay
GITHUB_OWNER=kubernetes-sigs \
AGGEXP_IMAGE=aggexp-repos-0006:dev \
BROKER_IMAGE=aggexp-broker-0006:dev \
MOCK_GITHUB_IMAGE=aggexp-mock-github-0006:dev \
  ./hack/deploy.sh "${HERE}/manifests"

kubectl -n aggexp-system rollout status deploy/broker
kubectl -n aggexp-system rollout status deploy/mock-github
kubectl -n aggexp-system rollout status deploy/aggexp

# 5. exercise
kubectl get repos                                # admin: sees all
kubectl --as alice get repos                     # read-only
kubectl --as mallory get repos                   # empty (broker denies)
kubectl --as bob   get repos                     # empty (bob scoped to bob-* owners)

kubectl -n aggexp-system logs deploy/broker       --tail=50
kubectl -n aggexp-system logs deploy/mock-github  --tail=50
kubectl -n aggexp-system logs deploy/aggexp       --tail=50 | grep -E 'broker-exchange|repo-(list|get|watch)'
```

Teardown: `kind delete cluster --name aggexp-identity`.

## Status

complete

<!-- See FINDINGS/0006-identity-broker-github-app.md for results. -->

## Decisions made

- **Broker and mock-github are stdlib-only.** Per ETHOS they
  duplicate the shape of 0003's policy-service rather than share
  code with it. That is on purpose.
- **No custom authorizer.** 0003's HTTP-authz path is not in the
  AA for 0006 — the broker is the identity-to-backend gate, not a
  separate authz layer in front of it. Upstream RBAC stays
  permissive so impersonated requests reach the AA.
- **Token shape `fake-token-<user>-<6-hex-bytes>`.** Not a secret
  in this lab; the user embedding makes logs legible. Production
  tokens would be opaque.
- **Token lifetime 300s**, arbitrary. The mock-github never
  revalidates against wall-clock expiry outside the broker's
  introspection response.
- **Broker denial → AA returns empty list / `NotFound` on Get.**
  Fail closed, but quietly: the operator UX is "you see no repos"
  rather than "HTTP 500 from backend". This is opposite to 0003's
  authorizer-denial (HTTP 403); the contrast is itself part of the
  finding.
- **No cache, no poll loop.** Every AA request re-exchanges.
  That's the whole point: the broker is in the hot path. Latency
  measurement is thus end-to-end per call.
- **Mock-github canned data: 3–5 repos per owner**, deterministic
  by owner name so two callers see different owner shapes.
- **Per-caller Watch emits initial ADDED events then parks.** No
  broadcaster, no diff loop. This experiment is about identity
  flow; the watch shape is 0004's problem.
- **Cluster name `aggexp-identity`.** Distinct from 0004's
  `aggexp` so both can coexist.
- **UID is a sha1 of the resource name** (cheap stability across
  a single request). Since there's no cross-call cache, a second
  kubectl call gets the same UID for the same `<owner>.<repo>`,
  which is actually a slight improvement over 0004's per-pod
  random UID.

## Prerequisites

- `kind`, `kubectl`, `docker`, `go` 1.24, `openssl`, `envsubst`.
- No external secrets; no `.env` needed. There is no real GitHub
  call in this experiment.

## What we're looking to learn

- **Identity handoff** (primary). Can the AA cleanly extract
  `user.Info` from the request context and feed it to a broker?
  What fields arrive? Can the broker make decisions off all of
  them (name, groups, UID, extras)?
- **Per-request authorization** (secondary, shape comparison). How
  does the broker-as-gate UX compare to 0003's authorizer-as-gate?
  In particular, where does denial surface?
- **Storage independence** (secondary). The AA is fully
  stateless — no process-wide cache. Every request does a round
  trip through the broker and the backend. What does that cost?
