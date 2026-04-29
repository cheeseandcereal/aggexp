# aggexp

An experimentation lab for Kubernetes aggregated APIs.

This repo is not a product. It is a place to probe the power and
boundaries of aggregated API servers — particularly around identity,
stateless hosting, per-request authorization, and compatibility with
the Kubernetes ecosystem (`kubectl`, `client-go`, ArgoCD, Flux).

If you are new here, read `VISION.md` and `ETHOS.md` first. They are
short and the rest of the repo will not make sense without them.

## Repo tour

| Path | What lives here |
|---|---|
| `VISION.md`, `ETHOS.md` | Why this repo exists and how work happens here. |
| `AGENTS.md` | Guidance for AI agents working in this repo. Read if you are one. |
| `ARCHITECTURE.md` | Current architectural state of the substrate. |
| `SYNTHESIS.md` | Current best understanding of the problem space. |
| `EXPERIMENTS.md` | Menu of candidate experiments, grouped by fundamental. |
| `FINDINGS/` | Immutable per-experiment records. `FINDINGS/compat/` holds dated compat scoreboard runs. |
| `experiments/` | Disposable experiments. Any language, any style. |
| `runtime/`, `drivers/` | Disciplined substrate code. Tests + docs required. Empty until experiments demand promotion. |
| `deploy/manifests/` | Base Kubernetes manifests (namespace, RBAC, Service, Deployment, APIService). |
| `deploy/certs/` | Generated TLS certs (gitignored). |
| `hack/` | Scripts for standing up the lab. |

## Prerequisites

- `kind` (v0.20+)
- `kubectl`
- `docker`
- `go` (1.22+)
- `openssl`
- `envsubst` (from `gettext`)

Tested on Linux. macOS should work (the deploy script has a
platform-specific base64 fallback); other platforms are unverified.

## Quickstart

Stand up the lab and run the Phase 0 probe experiment
(`0001-raw-http-aggregation`):

```bash
./hack/gen-certs.sh
./hack/make-kind.sh
./hack/deploy.sh deploy/manifests                     # base manifests

# Build and load the probe image into the kind node
docker build -t aggexp-probe-0001:dev \
  experiments/0001-raw-http-aggregation/
kind load docker-image aggexp-probe-0001:dev --name aggexp

# Apply the experiment's Deployment overlay
AGGEXP_IMAGE=aggexp-probe-0001:dev \
  ./hack/deploy.sh experiments/0001-raw-http-aggregation/manifests

kubectl -n aggexp-system rollout status deploy/aggexp
kubectl api-resources | grep hellos
kubectl get hellos
```

Tear down:

```bash
kind delete cluster --name aggexp
```

## Running the compatibility scoreboard

`hack/test-compat.sh` runs a fixed set of probes against the
currently-deployed AA and writes a dated markdown record to
`FINDINGS/compat/YYYY-MM-DD.md`. Some checks are pass/fail gates;
others are observe-only.

```bash
./hack/test-compat.sh
```

## Verifying the spine

`hack/verify-spine.sh` checks the repo's structural invariants: spine
files present, experiments have READMEs, completed experiments have
FINDINGS files, etc. It does not require a cluster.

```bash
./hack/verify-spine.sh
```

## License

Public domain. See `UNLICENSE`.
