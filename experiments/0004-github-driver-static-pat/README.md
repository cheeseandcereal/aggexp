# Experiment 0004: github-driver-static-pat

A GitHub user or org's repositories projected as a cluster-scoped
Kubernetes resource type (`repos.aggexp.io/v1`). Read-only. Backed by
a polling GitHub REST client.

This experiment is the heart of **MVP-example**: `kubectl get repos`
returns the caller's GitHub repos, gated by the AA's custom
identity-based authorizer.

Forked from 0003 (`custom-authorizer-external-policy`). The
authorizer wiring and policy-service are carried over unchanged.
The in-memory `Hello` type is replaced by a `Repo` type sourced from
GitHub.

## Hypothesis

1. A stateless AA can project an external system's state as a
   first-class Kubernetes resource type, driven by periodic
   polling. `kubectl get repos` and `kubectl get repos -w` behave
   as expected.
2. Synthetic watch events emitted on diff (Added / Modified /
   Deleted) during the poll loop are accepted by kubectl without
   complaint.
3. The custom authorizer continues to gate requests exactly as in
   0003, now over a resource type whose contents change on a
   schedule.
4. A static GitHub PAT mounted from a Secret suffices for the
   backend auth at this stage. The per-caller identity forwarding
   pattern is a later experiment (`identity-broker-github-app`).

## What's different from 0003

- `pkg/apis/aggexp/{types.go, v1/types.go, v1/conversion.go,
  v1/register.go}` — swapped `Hello` for `Repo`. Field set is
  GitHub-shaped.
- `pkg/github/client.go` — tiny stdlib HTTP client for GitHub REST.
- `pkg/registry/repo/storage.go` — polling-driven read-only
  `rest.Storage`. Get / List / Watch / TableConvertor. No writes.
- `pkg/server/server.go` — adds `--github-owner`,
  `--github-token-file`, `--github-base-url`, `--github-poll-interval`
  flags.
- `cmd/aggexp-repos/main.go` — renamed binary.
- `manifests/` — permissive RBAC now covers `repos`, policy rules
  unchanged in shape but apply to repos, new `github-token` Secret
  with `${GITHUB_TOKEN}` substitution, deployment overlay mounts
  the token file.

The REST storage intentionally does not implement Patcher / Creater
/ Updater / Deleter. GitHub is the source of truth; the AA reads
from it and serves a Kubernetes-shaped projection.

## What we're looking to learn

- **Storage independence.** Can the polling-cache + broadcaster
  pattern handle a real external system's state with acceptable
  freshness and cost? What breaks when the backend rate-limits us?
- **Resource modeling freedom.** Does a GitHub repo map cleanly to
  a Kubernetes resource? What fields require shaping? What about
  repos whose names contain characters Kubernetes names don't
  accept?
- **Watch and consistency semantics.** With only poll-driven
  synthetic events, does `kubectl get -w` see updates in a
  reasonable time after a real GitHub change? Do informers behave?
- **Identity handoff / per-request authz** (secondary). Policy
  rules written for `hellos` mostly carry over; confirm they do.

## How to run

Requires a GitHub owner (user or org) to project. Public repos
work with an unauthenticated client but GitHub caps unauthenticated
calls at 60/hour. If you have a Personal Access Token (classic or
fine-grained, scopes `public_repo` or `repo`), set `GITHUB_TOKEN`
in your environment before deploying.

From repo root:

```
export GITHUB_OWNER=cheeseandcereal    # or any github username / org
export GITHUB_TOKEN=ghp_your_token_here  # optional but recommended

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
GITHUB_TOKEN="${GITHUB_TOKEN}" \
  ./hack/deploy.sh experiments/0004-github-driver-static-pat/manifests

kubectl -n aggexp-system rollout status deploy/policy-service
kubectl -n aggexp-system rollout status deploy/aggexp

# Wait ~10s for the first poll, then:
kubectl get repos
kubectl get repos -w &
WATCH_PID=$!
sleep 3
kill $WATCH_PID

# Identity-gated access:
kubectl --as alice get repos            # allowed (readonly)
kubectl --as mallory get repos          # denied (default deny)

kubectl -n aggexp-system logs deploy/policy-service --tail=20
kubectl -n aggexp-system logs deploy/aggexp --tail=20 | grep github-refresh
```

## Status

complete

<!-- See FINDINGS/0004-github-driver-static-pat.md for results. -->

## Decisions made

- **Resource name format: `<owner>.<repo-name>`.** Dots are valid
  in K8s names; slashes are not. A future experiment can probe
  edge cases (repo names with dots, non-DNS characters, etc.).
- **Poll interval: 60s.** Arbitrary; short enough to demo watch
  semantics, long enough to stay well below GitHub rate limits.
- **Max pages: 4 at 100 per_page = 400 repos cap.** Bounded for
  experimentation; a user with more repos will see only the first
  400. Good enough for the MVP; easy to tune.
- **Read-only.** Writes to the GitHub API (e.g. `kubectl apply`
  creating a new repo) are out of scope for 0004. A later
  experiment can probe what that means: identity to use (the
  PAT belongs to *someone*), rate limits, idempotency.
- **Token is optional.** Empty `GITHUB_TOKEN` -> unauthenticated
  GitHub calls. Useful for probing behavior under rate limits.
- **Request logging in the REST storage was removed.** The
  authorizer already logs per-request identity; the repo storage
  adds its own "github-refresh" log with count + duration. Keeps
  signal focused.
- **Polling uses `ListOptions` semantics with `sort=full_name`.**
  Stable order across polls means the diff loop behaves
  deterministically.
- **Fresh poll is eager on startup.** The poll loop issues its
  first request immediately; we do not block `/readyz` on that
  completing, so a very early `kubectl get repos` can see an
  empty list. Clients are expected to retry or watch.

## Prerequisites

- kind cluster `aggexp` created via `hack/make-kind.sh`.
- Serving cert generated by `hack/gen-certs.sh`.
- Base manifests applied via `hack/deploy.sh deploy/manifests`.
- A GitHub owner (user or org) name in `GITHUB_OWNER`. Recommended:
  a GitHub PAT in `GITHUB_TOKEN`.

See `.env.example` for the env vars.
