# Example E1: GitHub repos end-to-end (MVP-example)

This document records the MVP-example deliverable, per `ETHOS.md`:
a concrete end-to-end scenario where `kubectl get repos` returns a
GitHub owner's repositories, gated by the AA's identity-aware
authorizer, with live watch.

MVP-example is not an experiment; it is a **composition** of four
completed experiments:

- `0001-raw-http-aggregation` — what the aggregation wire contract
  actually requires.
- `0002-hello-aggregated` — library-backed etcd-less AA, real
  OpenAPI for `kubectl explain`, SSA plumbing.
- `0003-custom-authorizer-external-policy` — per-request
  identity-based authz via an external policy service.
- `0004-github-driver-static-pat` — GitHub repos projected through
  a polling client as `repos.aggexp.io/v1`.

The MVP-example is done when the scenario runs from a clean
checkout with the artifacts that already exist in the lab. That is
true as of this writing.

## The scenario

From a clean checkout:

```bash
export GITHUB_OWNER=kubernetes-sigs    # a real GitHub owner

./hack/gen-certs.sh
./hack/make-kind.sh
./hack/deploy.sh deploy/manifests

docker build -t aggexp-repos:dev  experiments/0004-github-driver-static-pat/
docker build -t aggexp-policy:dev experiments/0004-github-driver-static-pat/policy-service/
kind load docker-image aggexp-repos:dev  --name aggexp
kind load docker-image aggexp-policy:dev --name aggexp

AGGEXP_IMAGE=aggexp-repos:dev \
POLICY_IMAGE=aggexp-policy:dev \
GITHUB_OWNER="${GITHUB_OWNER}" \
  ./hack/deploy.sh experiments/0004-github-driver-static-pat/manifests

kubectl -n aggexp-system rollout status deploy/policy-service
kubectl -n aggexp-system rollout status deploy/aggexp

sleep 5

kubectl get repos                          # admin: 206 rows from kubernetes-sigs
kubectl --as alice get repos               # readonly rule: allowed
kubectl --as mallory get repos             # no matching rule: 403
kubectl get repos -w                       # streams ADDED events
kubectl explain repos                      # generated OpenAPI works
```

Observed under these exact steps on 2026-04-29 against a kind
v1.32 cluster, kubectl 1.35, Go 1.24-alpine builds. See
`FINDINGS/0004-github-driver-static-pat.md` for detail and
`FINDINGS/compat/2026-04-29-03.md` for the compat scoreboard.

## What the exercise revealed

None of this is surprising *if you have read the per-experiment
FINDINGS*. It is the act of composing them that validates the
lab's thesis. Six specific things land cleanly together:

1. **Wire protocol fidelity is approachable.** ~400 lines of
   hand-written Go + 130KB generated OpenAPI, no etcd, nets full
   `kubectl get / explain / get -w` compatibility. This was
   established by 0002; 0004 confirms it survives re-tasking from
   a mock resource to a real backend without any changes to the
   apiserver plumbing.
2. **Identity-aware authz over external-state resources works.**
   A GitHub-backed resource behaves identically to an in-memory
   one from the authorizer's perspective. `kubectl --as alice get
   repos` succeeds; `kubectl --as mallory get repos` produces a
   403 with our reason string. Per-request decisions at the AA
   are not slowed or constrained by the backend being remote.
3. **Polling + synthetic watch is a viable external-state
   pattern.** 60-second poll; 206 repos cached; diff against
   previous state emits ADDED / MODIFIED / DELETED; watchers
   receive full initial state as prefix ADDED events. Rate-limit
   coupling to the backend is the only new concern vs. an
   in-memory source.
4. **kubectl's discovery caching is a real operational hazard.**
   After swapping 0003 for 0004, `kubectl api-resources` kept
   returning the stale `hellos` for up to ten minutes. The API
   server's discovery was correct; the client's cache was not.
   `rm -rf ~/.kube/cache/` is the workaround; future-facing
   design should minimize resource-type churn at the AA.
5. **`kubectl auth can-i` remains misleading** when the AA is the
   real authz gate. `kubectl --as mallory auth can-i get repos
   --as mallory` would say yes (because of the permissive
   upstream RBAC) while the actual request produces 403. This is
   a wire-level property, not fixable at the AA; any product
   built on this architecture must account for it.
6. **MVP-example is read-only.** We did not exercise writes
   (`kubectl apply` to create a repo, `delete` to archive/delete a
   GitHub repo, etc.). That path is a deliberate follow-up: it
   raises identity-forwarding questions that don't yet have good
   answers with a static PAT.

## What MVP-example does not demonstrate

Honesty is part of the ethos. The scenario works; several
adjacent claims it does *not* support:

- **ArgoCD compatibility.** The `argocd-compat` experiment has
  not run. ArgoCD would watch every discovered resource; against
  our 60s-stale polling cache, apply flows with a hypothetical
  writable Repo would exhibit behaviors we have not measured.
- **Flux compatibility.** Same, for Flux.
- **Controller-runtime informers across pod restarts.** The UID
  amnesia observation in 0004 suggests consumers would see
  apparent full churn after an AA restart; we have not
  directly probed this with a real controller.
- **Sustained rate-limited operation.** Unauthenticated GitHub
  capped at 60/hr; our poll burns 4 calls per 60s = 240/hr. We
  ran in a test window and did not hit the ceiling. A production
  equivalent would need a PAT.
- **Identity forwarding to GitHub.** The PAT is the AA's, shared
  across all callers. From GitHub's perspective all requests are
  the one PAT owner. This is by design for 0004 and is the
  headline question for the queued `identity-broker-github-app`
  experiment.
- **Admission-time field validation.** The 0003 authz-vs-
  admission observation means name-based CREATE policy is
  unenforceable from the authorizer. Repos is read-only, so the
  limit doesn't bite here; a writable version would need an
  admission mechanism.

## Status

complete
