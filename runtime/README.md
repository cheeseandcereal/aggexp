# runtime/

The **substrate** of the aggexp lab. Disciplined code (tests + package
docs required), factored out of experiments only when two or more have
demanded the same abstraction. Experiments use these packages; the
packages themselves know nothing about any specific experiment.

The first extraction lands here: `runtime/` was carved out of
experiments 0002 (in-memory Hello) and 0004 (GitHub repos), which
independently re-implemented the same rest.Storage boilerplate,
etcd-less Options struct, and external-policy authorizer.

## Packages

### `runtime/server`

The etcd-less generic-apiserver plumbing. Defines `Options`
(SecureServing + DelegatingAuth* + Audit + Features + CoreAPI,
no Etcd), `AddFlags`, `Validate`, `Config(Input)`, and a
`Run(ctx, name, Input, []GroupInstaller, postStartHooks)` entry
point. The external-policy authorizer from `runtime/authz` is
wired in when `--policy-service-url` is non-empty.

Experiments bring their own `Scheme`, `Codecs`, and
`openapi.GetOpenAPIDefinitions` (from `openapi-gen`) via the
`Input` struct.

### `runtime/authz`

External-HTTP-service-backed `authorizer.Authorizer`. Returns
`NoOpinion` for requests outside the configured API group so the
library's union chain stays intact. Fails open to `NoOpinion` on
transport errors; wire your own wrapper to fail closed. The JSON
payload is wire-compatible with 0003's policy-service protocol, by
design, so existing policy services keep working.

### `runtime/storage`

The rest.Storage adapter. Given a `Backend` (read-only data plane:
New/NewList/Kind/SingularName/NamespaceScoped/Get/List + Table*),
`storage.New(Options{Backend: b})` returns a `*REST` that
satisfies every rest.* interface the library demands, including
watch fan-out, synthetic monotonic resourceVersion, label-selector
filtering, and `ResourceExpired` on stale watch resume.

Backends that also implement `WritableBackend` (Create/Update/Delete)
get rest.Creater + rest.Updater + rest.Patcher + rest.GracefulDeleter
automatically. The adapter stamps RVs and emits watch events on the
backend's behalf; backends never own a broadcaster directly.

Backends that emit events on a schedule (e.g. a polling loop diffing
upstream state) call `REST.PublishAdded/Modified/Deleted` to inject
events with a fresh RV.

### `runtime/group`

A small helper that installs an API group into the generic
apiserver. Bundles `GroupVersion + Scheme + Codecs + resources
map`, does the `NewDefaultAPIGroupInfo + InstallAPIGroup` dance. A
`Group` implements `server.GroupInstaller`, so it plugs into
`Options.Run` as one of the installers.

## What is *not* in runtime/

- Scheme/types/install packages for any specific API group. Those
  live with the experiment that owns the type. The substrate is
  generic across type schemes.
- OpenAPI generation. Still done per-experiment via
  `kube_codegen.sh` + `openapi-gen`.
- Drivers (`drivers/` would be the right home). The substrate does
  not ship concrete backends; those are per-experiment.

## Stability

None. This package tree is pre-1.0 lab substrate. Interfaces may
change under experimental pressure; the point of promotion-from-
experiments is to let real usage reshape the abstractions.
Consumers (experiments) pin exact module versions in their own
`go.mod` if they need stability.
