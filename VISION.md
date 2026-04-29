# Vision

Kubernetes' aggregation layer is an underexplored power.

CRDs absorbed roughly 90% of the extension-mindshare in the Kubernetes
ecosystem because they are easy. They pay for that ease with two costs:
state must live in etcd, and authorization must be expressed as RBAC
over declarative shapes. These costs are often invisible until the thing
you're trying to model doesn't fit them.

Aggregated APIs don't have those costs. When kube-apiserver proxies a
request to an extension apiserver, it hands the extension:

- A cryptographically-verified identity (`user.Info` — username, groups,
  UID, extras) arriving over mTLS as `X-Remote-*` headers.
- A full HTTP request body.
- The freedom to decide anything, talk to any backend, project any
  state, and respond however makes sense.

And — if and only if the extension honors the wire protocol — the
promise that standard Kubernetes tooling (kubectl, client-go informers,
controller-runtime, ArgoCD, Flux) will continue to work against the
response.

This means an aggregated apiserver can make **per-request decisions
about real identities talking to real backends** — not per-object
decisions against declarative shapes that have to pre-exist in etcd.
The space of what "a Kubernetes API" can mean gets much larger.

## What this repo is

An experimentation lab for exploring that space. Its purpose is to find
and document the power and the limits of aggregated APIs, particularly
along these axes:

- What is the minimum wire contract the ecosystem actually demands?
- What does identity-based authorization enable that declarative-object
  authorization cannot?
- What can be projected into a Kubernetes API? Files? Git repositories?
  HTTP endpoints? Anything HTTP-addressable?
- Where does compatibility with existing tooling (kubectl, ArgoCD,
  Flux, controller-runtime) break?

## What this repo is not

It is not a product. It is not a framework ready to ship. It is not a
replacement for CRDs. The "controller-runtime for aggregated APIs"
shape that may emerge in `runtime/` is a byproduct of experiments, not
a designed-up-front framework.

## The motivating example

"I want to `kubectl get repos` and have it return my GitHub
repositories, with access controlled by who I am as an authenticated
Kubernetes identity, and have that same resource be manageable by
ArgoCD, Flux, or a custom controller."

That scenario requires identity forwarding, a stateless apiserver that
mirrors external state, a real watch implementation, and compatibility
with the ecosystem's assumptions. Whether any of those things are
achievable — and at what cost — is what this repo is trying to find
out.
