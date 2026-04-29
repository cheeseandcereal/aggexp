# Goals: open research questions

This file lists the open questions that motivate experiments in this
repo. Questions are grouped by which fundamental they touch.

Distinct from SYNTHESIS: this file is questions, not conclusions.
SYNTHESIS evolves as questions are answered. Questions here get
answered (or refined, or retired) through experiments referenced by
their NNNN number.

## Wire protocol fidelity

- What is the minimum wire contract that the aggregation layer will
  route at all? (Probed by `0001`.)
- What is the minimum wire contract that `kubectl get`, `kubectl
  describe`, `kubectl explain`, and `kubectl get -w` require to
  appear functional? (Probed by `0001`.)
- How does kubectl behave with partial or malformed openapi? With a
  watch that never emits events? With `resourceVersion` that is not
  monotonic?
- What do controller-runtime informers tolerate from a synthetic
  apiserver?
- What does ArgoCD's cluster cache do when discovery for our group
  times out or returns partial data? How does it behave with a
  synthetic watch?

## Identity handoff

- What is the cleanest pattern for "do X on behalf of the caller"
  when the caller is a Kubernetes identity and the target is a
  system that speaks a different identity (GitHub, AWS, an internal
  service)?
- How much of the identity-exchange pattern can be factored into a
  reusable "broker" component, and how much has to be per-backend?
- When a caller's `user.Info.Extra` carries useful side-data (e.g.
  a GitHub login injected by an OIDC authenticator), does that
  survive the aggregation-layer handoff as `X-Remote-Extra-*`? What
  are the security implications of relying on this?
- What happens to UX when a caller needs two credentials — one for
  kubectl, one for the backend the AA fronts? Is there a pattern
  where the AA owns the backend credential entirely?

## Storage independence

- Where does the polling-driven synthetic-watch pattern break at
  scale? What is the latency floor it imposes on downstream tools?
- What is the smallest viable resourceVersion scheme that satisfies
  client-go's relist semantics without backend state to derive from?
- Can we back an AA with a non-etcd store (e.g. postgres) and
  preserve watch semantics cleanly?

## Per-request authorization

- Compared to RBAC-on-CRDs, what classes of policy does per-request
  identity-aware authz make cleanly expressible that were awkward or
  impossible before?
- What is the performance budget for a per-request authz call to an
  external policy service? How does cache staleness trade off
  against correctness?
- Does delegating authz to kube-apiserver's RBAC (via SAR) vs.
  making the decision locally change how standard tooling behaves?

## Resource modeling freedom

- Can a single "driver" interface cover filesystem files, GitHub
  repos, arbitrary HTTP endpoints, and other shapes — or does each
  demand accommodations that make the interface leaky?
- What happens when the backend has no stable name, no stable
  resourceVersion analog, no list operation, or no deletion primitive?
- How useful is a *virtual* / composed AA that joins data from two
  underlying resources (one real, one aggregated) at request time?

## Watch and consistency semantics

- What watch behavior does a long-lived controller-runtime informer
  actually require? Bookmarks? Strict RV monotonicity? Precisely
  correct event ordering?
- What happens when the AA's serving cert rotates mid-watch? Does
  the client reconnect cleanly?
- If the backend emits change events of its own (webhooks, pub-sub),
  can we skip polling? What does that look like?

## Consequent questions (worth documenting; do not generalize)

- How expensive is aggregated openapi at the kube-apiserver level as
  schemas grow? (This is about the 2024-2025 kube-apiserver
  implementation, not about aggregated APIs in general.)
- What is the blast radius of an AA being slow or unavailable?
  (Again: implementation-dependent, but operationally important.)
- How does cert-manager vs. a script-generated static cert behave
  under kind reset?
