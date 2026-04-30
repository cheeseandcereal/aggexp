# Experiment 0020: krm-admission-hook

Adds validating and mutating admission to the KRM middle-layer arc.
Forked from `0017-krm-protocol-refinement`.

## The boundary this closes

`FINDINGS/0003-custom-authorizer-external-policy` named two policy
shapes the authorizer interface cannot enforce:

- **Name-based CREATE policy.** `Attributes.GetName()` is empty on
  CREATE because the name lives in the request body, not the URL.
- **Spec-field-shape policy.** The authorizer has no body at all.

Kubernetes' answer for both is admission: mutating and validating
webhooks run *after* the object has been decoded. 0003 described
this as a boundary "for a future experiment"; 0020 is that
experiment, for the component-server architecture established by
0013/0017/0018.

## The shape

- **gRPC extensions** (two new RPCs on the `Backend` service):
  - `Validate(ValidateRequest) -> ValidateResponse { allowed, reason, warnings }`
  - `Mutate(MutateRequest) -> MutateResponse { patched_object_json, warnings }`
  - Two opt-in schema flags: `supports_validation`, `supports_mutation`.
- **Component server** (`pkg/grpcbackend`) now wraps Create/Update:
  1. Encode the incoming object to JSON.
  2. If `supports_mutation`, call `Mutate` and replace the object
     with the patched JSON.
  3. If `supports_validation`, call `Validate`; on deny return HTTP
     422 with the backend's reason string verbatim.
  4. Proceed to `Create` / `Update`.
- **Note backend** advertises both flags and implements:
  - **Validate rules**: DNS-1123 name; `spec.title` between 3 and
    64 characters; on CREATE, `metadata.name` must start with
    `test-` or `prod-`.
  - **Mutate rule**: stamps `aggexp.io/accepted-at=<RFC3339>` on
    every write.

Deletion does NOT invoke admission in this experiment — DELETE has
no body, same reason the authorizer couldn't enforce it either.
The component could be extended to validate deletes against the
stored pre-image, but that's out of scope here.

## Hypothesis

- **Per-request authorization** (primary; admission is the
  authz-companion). Admission closes the concrete policy shapes the
  authorizer interface can't reach. The 0003 CREATE-name case
  becomes enforceable.
- **Wire protocol fidelity** (secondary). HTTP 422 with
  `metav1.Status.Message` carries the reason to kubectl the same
  way a webhook denial would; warnings flow as `Warning: 299`
  response headers.

## How to run

```
./hack/gen-certs.sh
kind create cluster --name aggexp-krm-adm
kubectl config use-context kind-aggexp-krm-adm
kubectl create namespace aggexp-system

./hack/deploy.sh deploy/manifests

docker build -t aggexp-krm-component:dev \
  -f experiments/0020-krm-admission-hook/component/Dockerfile .
docker build -t aggexp-note-backend:dev \
  -f experiments/0020-krm-admission-hook/backend-note/Dockerfile .
kind load docker-image aggexp-krm-component:dev --name aggexp-krm-adm
kind load docker-image aggexp-note-backend:dev --name aggexp-krm-adm

AGGEXP_IMAGE=aggexp-krm-component:dev \
  NOTE_BACKEND_IMAGE=aggexp-note-backend:dev \
  ./hack/deploy.sh experiments/0020-krm-admission-hook/manifests

kubectl -n aggexp-system rollout status deploy/aggexp
kubectl -n aggexp-system rollout status deploy/note-backend

# Pass:
kubectl apply -f experiments/0020-krm-admission-hook/sample-note-valid.yaml
kubectl get note test-hello -o yaml | grep aggexp.io/accepted-at

# Rejected: bad name prefix (0003's CREATE case)
kubectl apply -f experiments/0020-krm-admission-hook/sample-note-bad-name.yaml

# Rejected: title too short
kubectl apply -f experiments/0020-krm-admission-hook/sample-note-bad-title.yaml
```

## Status

complete

<!-- See FINDINGS/0020-krm-admission-hook.md for results. -->

## Decisions made

- **Forked 0017 via `cp -r`** rather than abstracting shared code,
  per repo ethos.
- **Kept 0017's typed-wrapper default** (`--use-typed-wrapper=true`
  in the manifest). Admission is orthogonal to SSA; it runs in the
  same Create/Update path regardless.
- **Reason string returned as `apierrors.NewInvalid` → HTTP 422**
  with the reason in `metav1.Status.Message`. This matches how
  standard validating-webhook denials surface to kubectl.
- **Mutate runs before Validate**, matching the standard Kubernetes
  webhook ordering (mutating webhooks always precede validating
  webhooks in the kube-apiserver admission chain).
- **DELETE does not invoke admission** in this experiment. Same
  reason DELETE is not authorized by body content: there is no
  body. This is a deliberate simplification worth naming.
- **Upsert via Update (create-if-missing)** is treated as CREATE
  for admission, so the 0003 name-based policy still applies on
  that code path.
- **Name-prefix rule `test-|prod-`** chosen arbitrarily to mirror
  the `bob-*` rule from 0003. The point is the boundary, not the
  specific prefix.
- **Warnings wire**: uses the library's
  `k8s.io/apiserver/pkg/warning.AddWarning`. Kubectl renders them
  as yellow `Warning:` lines. The backend emits a warning when
  `spec.body` is empty (no impact on allow/deny).
- **Admission transport failures fail closed** (500 InternalError).
  A production deployment would likely surface 503 ServiceUnavailable
  and allow fail-open for specific low-criticality resources.
- **Kind cluster: `aggexp-krm-adm`.** Distinct from 0013's
  `aggexp-krms` and 0017's `aggexp-krmp`.

## Prerequisites

- A kind cluster named `aggexp-krm-adm`.
- `hack/gen-certs.sh` to produce serving cert.
- `hack/deploy.sh deploy/manifests` for base resources.
- `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` only if
  regenerating `gen/`.

## What we're looking to learn

- Does a reason string emitted by the backend reach the kubectl
  client verbatim? (Expected: yes, via `metav1.Status.Message`.)
- Does the mutation annotation appear on the stored object as the
  caller sees it? (Expected: yes — `Mutate` runs before persist,
  so the backend stores the mutated object; `kubectl get -o yaml`
  shows the annotation.)
- Does UPDATE validate correctly with `OldObjectJson` carrying the
  pre-image? (Expected: yes — we fetch current from the backend
  before invoking admission, so diff-based rules work.)
- How does this architecture compare, conceptually, to the standard
  Kubernetes `ValidatingAdmissionWebhook` / `MutatingAdmissionWebhook`?
  (Answer in FINDINGS: identical shape; different wire protocol
  (gRPC vs. HTTPS); runs inside the component server's request path
  instead of being called out from kube-apiserver.)
