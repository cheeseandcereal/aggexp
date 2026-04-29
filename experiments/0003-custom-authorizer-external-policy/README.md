# Experiment 0003: custom-authorizer-external-policy

A fork of 0002 that wires a custom `authorizer.Authorizer` into the
aggregated apiserver. The authorizer POSTs a JSON payload describing
the caller and request to an external HTTP policy service; the
service returns `{allow, reason}`. The AA's custom authorizer is
chained after the library's default union (privileged-groups,
always-allow-paths, delegated SAR).

This experiment exercises the **per-request authorization**
fundamental: can the AA, by itself, deny requests that upstream RBAC
allowed, based on identity + request contents?

## Hypothesis

1. A custom `authorizer.Authorizer` can be chained into the
   `serverConfig.Config.Authorization.Authorizer` via `union.New`
   and the HTTP filter will route requests through it.
2. With upstream RBAC made permissive (`system:authenticated` gets
   `get/list/watch/create/update/patch/delete` on `hellos.aggexp.io`),
   the AA's authorizer becomes the effective gate.
3. Denials from the AA arrive at kubectl as HTTP 403 Forbidden with
   a message that includes our `reason` string verbatim.
4. `kubectl auth can-i` against kube-apiserver will report incorrect
   allows, because `SubjectAccessReview` never reaches the AA — it
   is answered by kube-apiserver's RBAC. This is a wire-level
   property, not a bug we can fix from the extension.

## What's different from 0002

- `pkg/authz/authorizer.go` — the new Authorizer, ~150 lines.
- `pkg/server/server.go` — adds `--policy-service-url` /
  `--policy-service-timeout` flags and chains the authorizer after
  the delegating union.
- `cmd/aggexp-authz/main.go` — renamed binary.
- `policy-service/` — the toy Go policy server (stdlib, ~180 lines)
  with a JSON rules file + SIGHUP reload.
- `manifests/` — adds a permissive ClusterRole, the rules ConfigMap,
  the policy-service Deployment+Service, plus the AA Deployment
  overlay wired to call it.

The Hello resource itself and the full `k8s.io/apiserver` plumbing
are identical to 0002. Per AGENTS.md: we duplicate rather than
extract until pressure from a second driver demands an abstraction.

## What we're looking to learn

- **Per-request authorization.** Is the AA's custom authorizer
  actually consulted on every request? How does a Deny present to
  kubectl, controllers, and `kubectl auth can-i`?
- **Latency cost.** A typical request now does one HTTP round trip
  to the policy service per authorization check. Is this noticeable
  in practice? Where?
- **Identity handoff** (secondary). The authorizer logs the full
  `user.Info` it receives, so we can observe how identities arrive
  at this step of the pipeline compared to where the REST storage
  sees them.

## How to run

From repo root. The 0002 cluster must be wound down first (same
kind cluster, same APIService group).

```
# If 0002 is still running, stop it (optional; redeploy will replace):
#   kubectl -n aggexp-system delete deploy aggexp policy-service
#   kubectl delete clusterrolebinding aggexp-hellos-permissive
#   kubectl delete clusterrole aggexp-hellos-permissive

./hack/gen-certs.sh
./hack/make-kind.sh
./hack/deploy.sh deploy/manifests    # base (namespace, SA, auth-delegator RBAC, Service, APIService)

# Build both images and load into kind
docker build -t aggexp-authz:dev experiments/0003-custom-authorizer-external-policy/
docker build -t aggexp-policy:dev experiments/0003-custom-authorizer-external-policy/policy-service/
kind load docker-image aggexp-authz:dev  --name aggexp
kind load docker-image aggexp-policy:dev --name aggexp

# Apply the experiment's manifests (permissive RBAC, ConfigMap, policy-service, AA overlay)
AGGEXP_IMAGE=aggexp-authz:dev \
POLICY_IMAGE=aggexp-policy:dev \
  ./hack/deploy.sh experiments/0003-custom-authorizer-external-policy/manifests

kubectl -n aggexp-system rollout status deploy/policy-service
kubectl -n aggexp-system rollout status deploy/aggexp

# Try it:
kubectl get hellos                            # admin: allowed
kubectl --as alice get hellos                 # alice: rule-allowed readonly
kubectl --as alice apply -f - <<'YAML'        # alice: rule-denied writes
apiVersion: aggexp.io/v1
kind: Hello
metadata:
  name: alice-test
spec:
  greeting: "denied"
YAML
kubectl --as bob create -f - <<'YAML'         # bob: allowed for bob-*
apiVersion: aggexp.io/v1
kind: Hello
metadata:
  name: bob-hello
spec:
  greeting: "hi from bob"
YAML
kubectl --as bob create -f - <<'YAML'         # bob: denied for non-bob-*
apiVersion: aggexp.io/v1
kind: Hello
metadata:
  name: other
spec:
  greeting: "denied"
YAML

# Observe policy decisions in the policy-service logs:
kubectl -n aggexp-system logs deploy/policy-service -f
```

## Status

complete

<!-- See FINDINGS/0003-custom-authorizer-external-policy.md for results. -->


## Decisions made

- **Policy rules live in a ConfigMap, not a CRD or file in the
  image.** Lab-friendly: edit the ConfigMap, `kubectl rollout`
  restart policy-service (or SIGHUP it) to reload.
- **Policy service is HTTP, not gRPC, no TLS.** It runs in the same
  namespace. Network policy is not in play for this experiment.
- **`--as` impersonation via `kubectl`** is how we simulate
  different identities. Alternative (webhook-token auth, OIDC) is
  out of scope for 0003.
- **Permissive ClusterRole binds `system:authenticated`** to full
  CRUD on `hellos.aggexp.io`. This is intentional; the AA's
  authorizer is the gate. The RBAC-subsumes-everything alternative
  was rejected for this experiment precisely because we want to
  see the AA decide.
- **Authorizer fails-open-to-NoOpinion on transport errors**, not
  closed-to-Deny. Rationale: `NoOpinion` lets the rest of the
  chain still Allow; absent any Allow, the HTTP filter's default
  is 403 anyway, so we don't silently permit. A stricter posture
  would return an `err`, which yields 500 — noisier for a lab
  backend blip.
- **No caching in the AA's authorizer.** One HTTP call per authz
  check. If latency bites, a cache goes in.
- **Policy service's match semantics**: `*` any, `a|b|c`
  alternation, otherwise literal. Tiny DSL; good enough for
  probing behavior.

## Prerequisites

- kind cluster `aggexp` created via `hack/make-kind.sh`.
- Serving cert generated by `hack/gen-certs.sh`.
- Base manifests applied via `hack/deploy.sh deploy/manifests`.
- `0002` deployment is not running (or will be replaced by this
  experiment's overlay).
