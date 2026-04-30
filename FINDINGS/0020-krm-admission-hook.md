# Findings — 0020 krm-admission-hook

## What we were trying to learn

`FINDINGS/0003-custom-authorizer-external-policy` named an
architectural boundary that custom Kubernetes authorization cannot
cross: the `authorizer.Authorizer` interface sees the request URL
attributes but not the body, so neither a CREATE-time name rule nor
a spec-field-shape rule can be enforced there. Kubernetes' standard
answer is admission (mutating + validating webhooks, or CEL admission
policies in 1.30+). The 0013/0017 component-server architecture had
no admission seam at all through 0018; this experiment adds one.

Four concrete hypotheses:

1. Extending the gRPC Backend service with `Validate` + `Mutate`
   RPCs and calling them from the component server's REST adapter
   is a clean seam — no library forking needed.
2. A deny reason string emitted by a remote backend reaches kubectl
   verbatim via `metav1.Status.Message`, identically to a
   ValidatingAdmissionWebhook denial.
3. Standard mutating-then-validating ordering composes: the Mutate
   RPC can stamp an annotation that the Validate RPC sees, and the
   stored object reflects the mutation.
4. The resulting shape is conceptually the same as a Kubernetes
   AdmissionWebhook — just with a different wire protocol and a
   different chain-of-command.

## What we did

Forked `experiments/0017-krm-protocol-refinement` into
`experiments/0020-krm-admission-hook` (copy + `sed` on the module
paths). Changes:

- `proto/backend.proto`: added `Validate` and `Mutate` RPCs on the
  `Backend` service. Both carry user identity, namespace+name,
  operation ("CREATE" | "UPDATE"), object JSON, old-object JSON
  (empty on CREATE), and a dry-run bit. `ValidateResponse` returns
  `{allowed, reason, warnings[]}`; `MutateResponse` returns
  `{patched_object_json, warnings[]}`. `GetSchemaResponse` gained
  two opt-in flags (`supports_mutation`, `supports_validation`).
- `component/pkg/grpcbackend/backend.go`: new `admit(...)` helper.
  The `Create` and `Update` adapters encode the object to JSON and
  call `admit` before forwarding to the backend's Create/Update.
  `admit` calls `Mutate` (if opted in), replaces the object with
  the returned patched JSON, then calls `Validate` (if opted in).
  A `ValidateResponse{allowed=false}` is translated into
  `apierrors.NewInvalid(...)` — HTTP 422 with the reason threaded
  into the error's `message`. Warnings from either hook are fed
  through `k8s.io/apiserver/pkg/warning.AddWarning(ctx, ...)` so
  kubectl prints them.
- `backend-note`: implemented `Validate` (DNS-1123 name; title 3-64
  chars; on CREATE `metadata.name` must start with `test-` or
  `prod-`) and `Mutate` (stamps annotation
  `aggexp.io/accepted-at=<RFC3339>` on every write). Advertised
  both flags in `GetSchema`.
- Deployed against kind cluster `aggexp-krm-adm` with
  `--use-typed-wrapper=true` (from 0017), so SSA still works.

Six scenarios exercised via kubectl.

## What we observed

### The reason string reaches the client verbatim

Scenario: `kubectl apply -f sample-note-bad-name.yaml` (name
`foo-hello`, which violates the CREATE prefix rule).

Client output:

```
The Note "foo-hello" is invalid: []: Invalid value: "foo-hello":
  notes created in this cluster must have a name prefixed with
  "test-" or "prod-"; got "foo-hello" (the 0003 name-based CREATE
  policy case, enforced in admission because the authorizer cannot
  see the request body)
```

Raw HTTP response (captured via `kubectl ... -v=9`): HTTP 422 with

```json
{"kind":"Status","apiVersion":"v1","status":"Failure",
 "message":"Note.aggexp.io \"foo-hello\" is invalid: []: Invalid value:
  \"foo-hello\": notes created in this cluster must have a name prefixed
  with \"test-\" or \"prod-\"; got \"foo-hello\" (...)",
 "reason":"Invalid","code":422,
 "details":{"name":"foo-hello","group":"aggexp.io","kind":"Note",
   "causes":[{"reason":"FieldValueInvalid","message":"...","field":"[]"}]}}
```

The format is identical to what a standard validating admission
webhook produces when it returns `{allowed:false, status:{message}}`.
Every character of the backend's reason string survived through the
wire without encoding or truncation. `kubectl` formatted it with
its own prefix `the Note "foo-hello" is invalid:`; the prefix is
library-supplied from `apierrors.NewInvalid`.

### Short title (`"x"`) rejected identically

```
The Note "test-hello-shorttitle" is invalid: []: Invalid value:
  "test-hello-shorttitle": spec.title must be 3-64 characters; got
  1 character(s): "x"
```

### Valid CREATE accepted; mutation annotation present on GET

```
note.aggexp.io/test-hello created
```

`kubectl get note test-hello -o yaml` then shows:

```yaml
metadata:
  annotations:
    aggexp.io/accepted-at: "2026-04-30T06:36:11Z"
  name: test-hello
```

The mutation is visible to the caller because the component server
replaces the request body with the patched object BEFORE the
backend's Create runs; the backend persists the mutated object; the
backend returns the mutated object to the component; the component
returns it to the client.

### UPDATE with empty title rejected; valid UPDATE accepted

```
$ kubectl patch note test-hello --type merge -p '{"spec":{"title":""}}'
The Note "test-hello" is invalid: []: Invalid value: "test-hello":
  spec.title must be 3-64 characters; got 0 character(s): ""

$ kubectl patch note test-hello --type merge -p '{"spec":{"title":"updated title"}}'
note.aggexp.io/test-hello patched
```

Post-update annotation stamp:

```
aggexp.io/accepted-at: "2026-04-30T06:36:26Z"
```

Proves that Mutate runs on every UPDATE, not just CREATE, and that
`OldObjectJson` flows correctly (the component's Update path fetches
current from the backend and passes it through in `admit`).

### DELETE does not invoke admission

Running `kubectl delete note test-hello` produced zero
validate/mutate log lines on the backend. This is deliberate: the
experiment only wires admission into CREATE and UPDATE paths,
consistent with the 0003 boundary observation that DELETE carries
no body (same reason the authorizer also can't enforce body-shape
policy on DELETE). A future experiment could add a DELETE-validate
hook that operates on the stored pre-image; we chose not to, to
keep the seam small.

### Server-Side Apply also flows through admission

```
$ kubectl apply --server-side --field-manager=alice \
    -f sample-note-bad-name.yaml
The Note "foo-hello" is invalid: []: Invalid value: "foo-hello":
  notes created in this cluster must have a name prefixed with "test-"
  or "prod-"; ...

$ kubectl apply --server-side --field-manager=alice \
    -f sample-note-valid.yaml
note.aggexp.io/test-hello serverside-applied
```

Post-SSA GET shows both the mutation annotation and the populated
`managedFields`:

```yaml
metadata:
  annotations:
    aggexp.io/accepted-at: "2026-04-30T06:37:32Z"
  managedFields:
  - manager: alice
    operation: Apply
    fieldsV1: {f:spec: {f:title: {}, f:body: {}}}
```

Confirms that 0017's typed-wrapper SSA path composes with 0020's
admission path without modification. The library routes SSA through
the same `rest.Updater` we wrapped, so admission runs the same way.

### Warnings wire

Backend emits a warning when `spec.body` is empty or when the name
is longer than 40 characters. Both surfaced to kubectl as Warning:
299 headers which kubectl rendered as:

```
$ kubectl apply -f - <<EOF
apiVersion: aggexp.io/v1
kind: Note
metadata: {name: test-warn, namespace: default}
spec: {title: "Valid title but empty body"}
EOF
Warning: spec.body is empty; a note without a body is boring
note.aggexp.io/test-warn created
```

The mechanism is `k8s.io/apiserver/pkg/warning.AddWarning(ctx, "",
msg)`, which the library attaches to the response as the HTTP
`Warning: 299 - "msg"` header(s). Kubectl's built-in formatter turns
those into the yellow `Warning:` lines above.

### DNS-1123 rule fires on uppercase names

```
$ kubectl apply -f - <<EOF
apiVersion: aggexp.io/v1
kind: Note
metadata: {name: TEST-upper, namespace: default}
spec: {title: "Uppercase name invalid", body: "x"}
EOF
The Note "TEST-upper" is invalid: []: Invalid value: "TEST-upper":
  metadata.name "TEST-upper" is not a valid DNS-1123 label
  (lowercase alphanumeric and dashes only, must start and end
  alphanumeric)
```

Independent of the CREATE-prefix rule; fires on both CREATE and
UPDATE paths.

### Side-finding: `kubectl apply --dry-run=server` is honored at
### the wire level but not by this backend

A dry-run create of a valid note produced `(server dry run)` output
from kubectl but *also* persisted the note in the backend:

```
$ kubectl apply --dry-run=server -f sample-note-valid.yaml
note.aggexp.io/test-dryrun created (server dry run)
$ kubectl get note test-dryrun
NAME          TITLE            AGE
test-dryrun   A proper title   1s
```

Root cause: the library's `DryRunnableStorage` wrapper is part of
the `registry/generic/registry.Store` path, which our
`rest.Storage` bypasses. The component forwards `CreateOptions.DryRun`
to the backend via the admission RPCs' `dry_run` field, but the
backend ignores it and does its in-memory write regardless. For
admission specifically, the experiment's core scenarios still hold:
a dry-run CREATE that would be denied IS denied (the 422 surfaces),
so the admission seam is correct; the bug is in backend-note's
persistence path. Calling this out as a consequent, not a boundary.

## What this compares to: Kubernetes ValidatingAdmissionWebhook

Conceptually identical:

| aspect | k8s AdmissionWebhook | 0020 admission |
|---|---|---|
| who calls admission | kube-apiserver | component server |
| how | HTTPS POST with `AdmissionReview` body | gRPC `Validate`/`Mutate` |
| ordering | mutating before validating | same |
| deny shape | `AdmissionResponse{allowed:false, status:{message}}` | `ValidateResponse{allowed:false, reason}` |
| client outcome | HTTP 403 (failurePolicy=Fail) / 500 (Ignore) / 4xx (validating-reason surfaced) | HTTP 422 Invalid with reason in Status.Message |
| warnings | `AdmissionResponse.warnings` | `ValidateResponse.warnings` (same mechanism in kubectl) |

Differences that actually matter:

1. **Wire protocol: gRPC, not HTTPS webhook.** No
   `admissionregistration.k8s.io/v1` CRD on the host cluster; no
   `MutatingWebhookConfiguration` to maintain; no certificate
   management for the webhook endpoint (kube-apiserver → webhook).
   The component-to-backend channel is already the backend's trust
   boundary, so admission inherits whatever transport security the
   backend channel has. In this experiment that's insecure
   in-cluster gRPC; a production deployment would mTLS the gRPC.
2. **Runs inside the component server's request path, not
   kube-apiserver's.** kube-apiserver does its own admission chain
   (MutatingAdmissionWebhook runs, then built-in admission plugins,
   then ValidatingAdmissionWebhook) BEFORE it proxies to the
   aggregated apiserver. Our admission runs AFTER the proxy lands
   in the component server. This means:
   - External `ValidatingAdmissionWebhook` configurations targeting
     `aggexp.io/v1` would run first (upstream), then our admission
     would run second (downstream). A denial at either layer stops
     the write. A mutation at the upstream layer is visible to our
     Validate; a mutation at our layer is NOT visible to upstream
     (the proxy happens before our path runs).
   - kubectl sees one HTTP response; whether it came from upstream
     admission, proxy error, our admission, or our storage is
     opaque to the caller beyond the 4xx code + reason.
3. **No separate admission CRD; opt-in is part of the schema.**
   The backend declares `supports_validation:true` /
   `supports_mutation:true` in its GetSchema response; the
   component wires admission per-resource. There is no external
   selector ("operate on these resources, not those"), no failure
   policy ("Fail" vs "Ignore"), no timeout knob — all of which
   are configurable for webhooks.
4. **No dynamic update.** Changing the rules requires redeploying
   the backend. A real webhook can be registered, mutated, and
   deregistered at runtime via the host cluster's API. This trade
   is fine for the component-server architecture (the backend is
   already a per-resource component) but it IS narrower.
5. **Admission transport failure handling is explicit.** A
   webhook config has `failurePolicy: Fail | Ignore`. Our code
   currently fails closed (gRPC error → HTTP 500), with no knob.
   A production version would probably want the same two-mode
   choice per resource.

Put differently: we reinvented ValidatingAdmissionWebhook for the
component-server case, in maybe 150 lines of Go and 60 lines of
proto. The shape is identical because the underlying need is:
"intercept a write, consult a policy, accept or deny with a reason,
optionally modify." Once there is a proxy in the request path, you
will want this. The wire-protocol choice follows from what else the
component/backend already speak; in a CRD-land you'd necessarily
be on HTTPS-webhook; in a gRPC-backed component you are already on
gRPC, so admission rides along.

## Fundamentals touched

**Per-request authorization** (primary; admission is the
authz-companion). 0003 drew the boundary: the authorizer can't
enforce name-based CREATE policy or spec-field-shape policy.
0020 confirms the other side of that boundary empirically:

- Name-based CREATE rule. The `test-|prod-` prefix rule is the
  shape of the `bob-*` rule 0003 could not enforce. Moved into
  `Validate` on operation=CREATE, it fires on exactly the case
  0003 fell through. No RBAC/authz changes needed.
- Spec-field-shape rules (DNS-1123 on name, title-length). Trivial
  in admission; impossible in the authorizer because there is no
  body.
- Mutating admission. Annotations stamped on every write;
  invisible to authz (the authz decision has already happened by
  the time the body is visible); observable through GET.

The admission seam does NOT replace the authorizer. The authorizer
still runs first (on every request, with identity + URL attributes).
It is the coarse gate. Admission is the fine gate and requires the
body, which is why it runs second, in a different seam.

**Wire protocol fidelity** (secondary). Two observations:

1. `apierrors.NewInvalid(GroupKind, name, field.ErrorList{...})`
   produces a wire-shape that is byte-identical to what a
   validating webhook produces. The client has no way to tell
   apart "webhook denied" vs "component admission denied"; both
   look like HTTP 422 Invalid with the reason in
   `metav1.Status.Message` and a `causes[]` array with a
   FieldValueInvalid entry. This is fine — arguably ideal: from
   the caller's perspective the denials should look the same,
   so existing client tooling (argocd error display, terraform
   provider retry logic) will treat them the same.
2. `k8s.io/apiserver/pkg/warning.AddWarning(ctx, agent, msg)`
   composes cleanly with the existing REST adapter. No new
   library work; warnings appear in kubectl as
   `Warning: <msg>` lines above the normal output. The agent
   string we pass is empty (library default behaviour); a
   richer implementation might pass `"aggexp.io/notes.admission"`
   so operators can audit which subsystem emitted which warning.

## Consequents (implementation-dependent; do not generalize)

- `DryRun` is not honored by backend-note's in-memory store. A
  server-dry-run CREATE that admission allows is actually
  persisted. The admission RPC correctly receives `dry_run=true`;
  the backend's store ignores it. Fix is backend-local; no seam
  change needed.
- `apierrors.NewInvalid(GroupKind, ...)` requires a `GroupKind`,
  not a `GroupVersionResource`. Our `REST.desc.GroupResource`
  is `GroupResource`, so we synthesized the GroupKind from
  descriptor fields. A future refactor could cache the GroupKind
  alongside GroupResource.
- The `warning.AddWarning` context-based API requires the warning
  recorder to have been installed by the library; in this
  component server it is installed by default as part of the
  generic apiserver's request-filter chain. If we ever strip the
  filter chain for a leaner probe, warnings would silently stop.
- gRPC errors from admission RPC are translated to 500
  (InternalError). No failure-policy knob. Production would want
  per-resource Fail/Ignore and probably per-op timeouts.
- Admission runs with the same context as Create/Update —
  identity, request attributes, etc. available to the backend.
  The experiment passes `UserInfo` across the wire; the backend
  logs it. Didn't deliberately probe: whether a slow admission
  call extends kube-apiserver's proxy timeout before the caller
  sees a 504. At ms-scale in-cluster gRPC it's not relevant; at
  real-world webhook scale it would be.
- Library's `metav1.Status.Message` template for
  `apierrors.NewInvalid` prepends `Note.aggexp.io "foo-hello" is
  invalid: []:` before our reason. The `[]` is the JSONPath
  we passed via `field.Invalid(field.NewPath(""), ...)`;
  passing a more meaningful path like `field.NewPath("metadata",
  "name")` would produce a crisper error prefix. We kept the
  empty path to let the reason string stand alone, but a real
  backend probably wants the path.
- kubectl prints the reason verbatim in its "Error from server"
  output. Multi-line reasons are preserved. We did not test
  Unicode (the reason strings in this experiment are ASCII).

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**:

- `Per-request authorization` gets a companion entry:
  the authz-vs-admission boundary flagged by `0003` is concretely
  closable for the component-server architecture. Name-based
  CREATE policy and spec-field-shape policy live in admission;
  the seam is a pair of gRPC RPCs and ~150 lines in the adapter.
  The authz boundary observations from `0003` are unchanged — they
  still apply to the authorizer. 0020 adds the statement
  "admission is the home for body-shape and name-based-CREATE
  policy; it composes cleanly with the authorizer".
- `Resource modeling freedom`: extend the typed-vs-unstructured
  line with a note that admission, like SSA, runs inside the REST
  adapter — so admission composes with whichever scheme path (SSA
  or not, typed or unstructured) the component server happens to
  use. It is orthogonal.

For **EXPERIMENTS.md**:

- 0020 complete under Per-request authorization (primary) with
  cross-references under Wire protocol fidelity.
- The `name-aware-admission` candidate is resolved by 0020; mark
  it done with pointer to this experiment.
- A natural follow-up: **admission-with-denied-authz combo**.
  What happens when the authorizer allows but admission denies
  vs authorizer denies (which short-circuits admission)? The
  UX/audit implications are worth a short probe.
- Another follow-up: **admission-backed-dryrun**. Wire the
  backend to honor the `dry_run` bit in the admission messages
  (and, separately, for the persistence path). Would produce a
  clean story for `kubectl apply --dry-run=server` against
  component-server resources.

## Open questions raised

- **Admission + SSA conflicts**: what does an SSA conflict look
  like if the backend's Mutate stamps a field that becomes
  managed by the component server's field-manager implicitly?
  (Our annotation is under `metadata.annotations` which SSA
  already treats as a map; a field-level conflict would require
  the mutator to touch `spec.*`.)
- **External admission chain interleaving**. If a cluster
  operator configures a host-cluster
  `ValidatingAdmissionWebhook` matching `aggexp.io/v1`, it runs
  upstream in kube-apiserver's admission chain — before the
  proxy to the AA. Does a kube-apiserver admission denial
  surface differently to kubectl than our component denial?
  (Hypothesis: same HTTP 422; different audit trail.)
- **CEL admission (1.30+ `ValidatingAdmissionPolicy`)** targeting
  an aggregated resource: does it work at all? The policy's CEL
  binding expects a `ValidatingAdmissionPolicyBinding`; whether
  the host kube-apiserver runs the CEL against `aggexp.io/v1`
  objects in the proxied request path is untested. If it does,
  then the component's gRPC admission is a third layer, not a
  second.
- **Scale**. This experiment's backend is in-process with the
  gRPC call. A backend whose Validate logic takes 500ms per
  call would add 500ms to every write. The library offers no
  per-resource latency budget on admission RPCs; neither does
  our component. At some scale this will matter.
- **Audit.** kube-apiserver emits audit events for admission
  decisions by webhooks; our component server's admission
  decisions have no such audit trail beyond the backend's
  log.Printf. Production would want structured audit events.
