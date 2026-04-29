# Findings — 0004 github-driver-static-pat

## What we were trying to learn

Can an aggregated apiserver project a real external system's state
(GitHub) as a first-class Kubernetes resource type, gated by the AA's
per-request identity-based authorizer, using only a polling client
and a synthetic watch broadcaster?

This is the experiment that carries the most weight toward the
MVP-example deliverable.

Four hypotheses going in:

1. A polling-cache + broadcaster pattern satisfies `kubectl get`,
   `kubectl get -w`, and `kubectl explain` over an external
   system's state.
2. Diff-based synthetic watch events (Added / Modified / Deleted
   on each poll) are accepted by kubectl without complaint.
3. The custom authorizer from 0003 continues to work unchanged over
   a resource type whose contents change on a schedule.
4. A static GitHub PAT mounted from a Secret suffices for backend
   auth at this stage; per-caller identity forwarding is a later
   experiment.

## What we did

Forked 0003 into 0004. Replaced the in-memory `Hello` storage with
a polling GitHub-backed `Repo` storage:

- `pkg/github/client.go` — stdlib HTTP client for GitHub REST
  (list user repos, get repo by name).
- `pkg/registry/repo/storage.go` — read-only `rest.Storage` with a
  `sync.Map` cache, `watch.NewBroadcaster`, and a 60-second poll
  loop that diffs against the previous state to emit Added /
  Modified / Deleted events.
- `pkg/server/server.go` — `--github-owner`, `--github-token-file`,
  `--github-base-url`, `--github-poll-interval` flags.
- `cmd/aggexp-repos/main.go` — renamed binary.
- `manifests/` — permissive RBAC now covers `repos`; new
  `github-token` Secret populated from `${GITHUB_TOKEN}`; AA
  deployment overlay mounts the token and points at the policy
  service.

Deployed to the same kind cluster that had been running 0003.
Owner: `kubernetes-sigs`. No PAT supplied (unauthenticated).
Policy rules carried over verbatim from 0003: admin allows
everything, `alice` readonly, `bob` scoped to `bob-*` names
(denies everything here because no repo name matches), `mallory`
falls through to default deny.

## What we observed

### `kubectl get repos` returns real GitHub data

```
$ kubectl get repos
NAME                                 OWNER             REPO               STARS   LANGUAGE     AGE
kubernetes-sigs.karpenter-provider-ibm-cloud  kubernetes-sigs  karpenter-provider-ibm-cloud  11  Go  1m
kubernetes-sigs.metrics-server        kubernetes-sigs   metrics-server     6610    Go           1m
kubernetes-sigs.kind                  kubernetes-sigs   kind               15199   Go           1m
...
(206 rows)
```

Initial poll completed in 1.6s (4 paginated calls to GitHub REST,
100 per page, reaching 206 repos). `kubectl get repo
kubernetes-sigs.kustomize -o yaml` returned a well-formed Repo
object with the expected spec (description, defaultBranch,
language, stars, htmlURL) and the status block populated with the
observation time.

### `kubectl get repos -w` streams 206 ADDED events on connect

Each new watcher receives the full cache as initial ADDED events
via `watch.WatchWithPrefix` before entering the live stream. On a
5-second window we captured 413 lines of output; the doubling is
kubectl's watch-mode re-rendering behavior previously observed in
0001, not duplicate events. The server-side emission is correct;
kubectl's table rendering in watch mode is idiosyncratic.

### `kubectl explain repos` works

Describes the `Repo` kind, its `spec`, and per-field documentation
from the Go type's doc comments. Same generated-OpenAPI-with-GVK-
extensions path proved by 0002; nothing new to learn here.

### Identity-based authz works unchanged

With policy rules from 0003 left as-is, only the resource name
changed from `hellos` to `repos` in kubectl calls:

- `admin`: all 206 repos, unchanged.
- `alice`: all 206 repos (rule: readonly allow). List operation
  was denied once early when alice tried a write verb; for repos
  (read-only) that path doesn't apply.
- `bob`: list denied ("bob: default deny"). No repo name starts
  with `bob-` so the `name: bob-*` rule can never match on a list
  request anyway.
- `mallory`: denied with "no matching rule" default.

The authorizer's chain-order and denial-message findings from 0003
carry over identically. Latency remains imperceptible.

### Polling runs silently on schedule

The `github-refresh` log line fires every 60s:

```
I0429 05:00:25.921275 storage.go:317] "github-refresh"
  owner="kubernetes-sigs" count=206 took="1.637089583s"
I0429 05:01:26.312911 storage.go:317] "github-refresh"
  owner="kubernetes-sigs" count=206 took="1.042713428s"
```

Unauthenticated GitHub REST is rate-limited to 60/hour. Each poll
uses 4 calls (100 per page × 4 pages = up to 400 repos). A 60s poll
interval means 240 calls/hour, well over the limit. **We survived
this by accident** — unauthenticated requests from a single kind
cluster IP during a test window didn't hit the limit hard, but
sustained operation would. The library does not help here.

Noted as a consequent: **poll interval, page size, and
authentication mode are joint decisions that must match GitHub's
rate limits, not chosen independently.** For sustained operation,
the PAT path is not optional.

### `kubectl` discovery cache masked our changes

After redeploying 0004 over 0003, `kubectl api-resources
--api-group=aggexp.io` kept returning `hellos` for several
minutes. The server was serving `repos` correctly (confirmed via
`kubectl get --raw /apis/aggexp.io/v1`), but kubectl's
`~/.kube/cache/discovery/` cache held the prior result with a
10-minute TTL.

`rm -rf ~/.kube/cache/` fixed it. This is the discovery-caching
consequent the research brief flagged; first real encounter.

### Controller-manager watches are denied (noisy but fine)

The `system:kube-controller-manager` SA watches every registered
resource it has RBAC for. Our permissive ClusterRole lets it list
and watch repos; the policy service sees these requests and denies
them (no matching rule; default deny). The policy-service logs
fill with `user=system:kube-controller-manager verb=watch
resource=repos name=` denies.

Two ways to handle this if it becomes annoying:
- Add a rule allowing system-component SAs.
- Remove them from the permissive ClusterRole subject list.

Neither changes the experiment's outcome. Documented for future
experiments that notice the same.

### Repo state resets on pod restart

Restarting the AA pod means the next polling cycle emits 206
ADDED events (everything is "new" to the fresh broadcaster). A
running informer would see the whole list re-added. Not a bug
per se — the cache and the broadcaster are intrinsically tied to
process lifetime — but noteworthy for consumers that expect
object UIDs to survive server restarts. They don't: we generate
new UIDs on each rediscovery.

If a consumer needs stable UIDs across restarts, UID would need
to be deterministic (e.g. derived from `gh.ID` which is stable
across GitHub's API). Worth recording as a candidate for a
later experiment or for substrate design.

## Fundamentals touched

**Storage independence.** Confirmed end-to-end with a real
external system. ~150 lines of GitHub client + ~250 lines of
polling-cache rest.Storage is sufficient to project GitHub repos
as a fully kubectl-compatible Kubernetes resource, including
watch. The library does not resist this pattern.

Concrete costs newly observed:
- Pod-restart amnesia: cache and UID identity are process-local.
  A new deployment round produces a second-generation 206 ADDED
  events from consumers' perspective.
- Rate-limit coupling: poll interval + page size + auth mode are
  a joint decision against the backend's rate limits, not
  independent knobs.

**Resource modeling freedom.** First confirmation against a
nontrivial real backend. Mapping GitHub repos to Kubernetes
resources was clean:
- `<owner>.<name>` as a Kubernetes name worked for all 206 real
  repo names without collision or rejection.
- Spec fields mapped 1:1 from GitHub JSON fields.
- Status carried only server-observation metadata, as expected.

Boundary cases not tested: repo names containing dots, forks whose
names collide with parents, transfers changing owner, deletions
causing the same GitHub "name" to refer to something else later.

**Per-request authorization.** Carried over from 0003 unchanged.
Identity-based policy over an ephemerally-cached external-state
resource is structurally identical to identity-based policy over
an in-memory resource; the authorizer doesn't care what the
backend is. Reinforces the 0003 finding that this is clean
architecture.

**Watch and consistency semantics.** Broadcast-based synthetic
watch from a polling source is usable for the minimum kubectl
interactions probed. We have **not** exercised:
- A long-lived controller-runtime informer across multiple poll
  cycles.
- Watch resumption with a non-current resourceVersion (we return
  `410 Gone` for anything not-current; reflectors relist, but
  we didn't measure the cost).
- Pod-restart recovery as observed by a running informer (the
  identity amnesia above will look like a full churn to the
  consumer).

**Wire protocol fidelity.** Unchanged. Compat scoreboard 5 PASS
+ 2 SKIP (write probes benignly skipped because Repo is
read-only; scoreboard updated to stop treating these as
`expect` failures).

## Consequents (implementation-dependent; do not generalize)

- kubectl `~/.kube/cache/discovery/` 10-minute TTL caused the
  stale-`hellos` observation. A consequence of kubectl's cache
  defaults, not of aggregated APIs.
- Unauthenticated GitHub REST is 60/hr; 4-calls-per-poll at 60s
  interval is 240/hr and will quickly hit that ceiling under
  sustained operation. Consequent of GitHub's rate-limit policy.
- Our paginated ListRepos caps at 400 items. Consequent of the
  hand-rolled client; tune for specific owners with more repos.
- `system:kube-controller-manager` watches everything it can,
  including our repos. This is consistent cluster-controller
  behavior against any registered aggregated API; noise to filter
  from logs, not a bug.
- Pod restart generates new UIDs. Consequent of our deliberately
  non-deterministic UID generation (uuid.NewV4 per rediscovery);
  a different UID scheme (hash of `gh.ID`) would preserve
  identity.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**:

- `Storage independence` gets a concrete second datapoint: the
  pattern works against a real external system, not just an
  in-memory map. Add: **pod-restart amnesia and backend
  rate-limit coupling are joint concerns**; they don't appear
  until the backend is real.
- `Resource modeling freedom` upgraded from hypothesis to first
  real confirmation. The clean mapping is specific to GitHub
  repos as "things with stable IDs and schema"; extrapolate
  carefully.
- `Watch and consistency semantics` gets the "pod-restart looks
  like a full churn to a consumer" observation. Worth exploring
  in a dedicated experiment.

For **EXPERIMENTS.md**:

- `0004-github-driver-static-pat` — marked complete.
- New candidate: **`repo-uid-stability`** — use a deterministic
  UID scheme derived from `gh.ID` and observe whether consumers'
  behavior after a pod restart improves.
- New candidate: **`github-rate-limit`** — probe what happens
  when the poll loop actually hits GitHub's rate limit. What does
  the AA log? What do clients see? Is the cache stale silently or
  visibly?
- `extract-runtime` now has its prerequisite: a second backend
  (GitHub) that needed almost the same rest.Storage boilerplate
  as the first (in-memory Hello). The next `fs-driver` experiment
  would establish pressure for extracting a `runtime/` Driver
  interface.
- Replace `rbac-permissive-aa` (retired; answered by 0003) with
  the new candidates above where appropriate.

## Open questions raised

- How does a controller-runtime manager talking to `repos.aggexp.io/v1`
  behave across a pod restart that regenerates UIDs? Expected:
  apparent full-churn; unmeasured.
- What's the right cache layer between the polling loop and the
  per-user authz check? We call the policy service on every
  request; the library caches SAR answers for 10s but we don't.
- GitHub API ETags would let us avoid re-downloading unchanged
  pages. The client has no ETag support today; that's a
  consequent-leaning win that could matter under rate-limit
  pressure.
- The poll interval is uniform. A real backend with webhook
  support (GitHub does) could skip polling entirely and react to
  pushed events. Worth a later experiment.
- For a private repo, unauthenticated access returns 404 — we
  observed this as "not found" which is not quite accurate. A
  later experiment should try authenticated access against a
  private repo and confirm it surfaces correctly.
