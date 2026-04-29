# deploy/manifests

Base Kubernetes manifest templates for the `aggexp` aggregated
apiserver. Collectively they:

- Create namespace `aggexp-system` and a `ServiceAccount` for the AA.
- Wire the standard extension-apiserver RBAC: `system:auth-delegator`
  (TokenReview / SubjectAccessReview) and `extension-apiserver-authentication-reader`
  in `kube-system` (read the request-header CA ConfigMap). This is the
  same pattern used by `sample-apiserver` and friends.
- Expose the AA via a ClusterIP `Service` on 443 → 8443.
- Run a single-replica `Deployment` mounting the serving cert from
  the `aggexp-serving-cert` Secret at `/etc/aggexp/certs`.
- Register the API group/version with an `APIService` (`v1.aggexp.io`).

## Substitution

These files are templates consumed by `hack/deploy.sh`, which runs
`envsubst` over each one. The variables are:

- `${CA_BUNDLE}` — base64-encoded CA cert, injected into `50-apiservice.yaml`.
- `${AGGEXP_IMAGE}` — container image. GNU `envsubst` does not support
  `${VAR:-default}` syntax, so `hack/deploy.sh` is responsible for
  exporting `AGGEXP_IMAGE` (falling back to `aggexp:dev`) before
  rendering.

## Overriding for an experiment

Experiments that need different flags, extra volumes, or a different
image usually copy this directory (e.g. to
`experiments/<name>/manifests/`) and modify the relevant file (most
often `40-deployment.yaml`). Point `hack/deploy.sh` at the override
directory; it does not merge, it renders whatever directory you hand
it.
