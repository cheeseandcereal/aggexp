# Ethos

This is a laboratory, not a product. The ethos below is how work
happens here. Deviations from it are bugs.

## The spine is stable, everything else is disposable

The **spine** of this repo — the files committed to always being
coherent — is:

- `VISION.md`, `ETHOS.md`, `AGENTS.md`: why this repo exists and how
  work happens here.
- `ARCHITECTURE.md`: current architectural understanding of the
  substrate. Living; rewritten when architectural reality shifts.
- `SYNTHESIS.md`: current best understanding of the problem space.
  Living; rewritten when understanding shifts.
- `EXPERIMENTS.md`: the menu of candidate experiments.
- `README.md`: how to run what exists.
- `FINDINGS/`: immutable per-experiment records and timestamped compat
  scoreboard runs.
- `hack/`: the scripts that make the lab operable.
- `deploy/manifests/`: the base Kubernetes manifests.

Everything outside the spine is disposable. Code under `experiments/`
is allowed to be ugly, duplicative, polyglot, and short-lived.

## Two deliverables, two done-states

- **MVP-lab.** The repo functions as a lab: spine complete, one
  experiment run end-to-end, compat record seeded, SYNTHESIS seeded.
  Done when a new contributor (human or agent) could pick up a new
  experiment cleanly.
- **MVP-example.** The GitHub repos end-to-end scenario: `kubectl get
  repos` returns the caller's GitHub repos, identity-aware authz gates
  access, watch streams updates, a FINDINGS file documents the
  exercise. Done when it runs from a clean checkout.

The lab enables the example; the example demonstrates the lab.

## Six named fundamentals

The problem space is organized around these. SYNTHESIS curates around
them; experiments probe them; FINDINGS reference them by name.

1. **Wire protocol fidelity** — what kubectl, client-go, and
   controllers actually demand of a conformant apiserver.
2. **Identity handoff** — the aggregation layer gives identity, not a
   credential; what that architecturally enables and constrains.
3. **Storage independence** — etcd is optional; stateless and
   alternative-backed apiservers are viable; what that costs.
4. **Per-request authorization** — the extension apiserver decides in
   real time, based on identity and request contents, against whatever
   policy it wants.
5. **Resource modeling freedom** — the boundary of what "anything as a
   Kubernetes resource" really means.
6. **Watch and consistency semantics** — resourceVersion, bookmarks,
   list+watch ordering, informer contracts.

New fundamentals may emerge. When they do, SYNTHESIS is rewritten to
reflect them.

## Fundamental vs. consequent

Every finding is tagged (in prose) as either:

- **Fundamental**: something that flows from the architecture of
  aggregated APIs itself. Generalizes across implementations.
- **Consequent**: something tied to specific library versions,
  kube-apiserver quirks, tool behavior, or environmental specifics.
  Real, worth recording, but does not generalize.

This distinction prevents the repo from over-indexing on implementation
quirks.

## Experiments are unbounded; substrate is disciplined

Under `experiments/`:
- Any language, any style, any dependency, any quality bar.
- No required tests.
- Duplication between experiments is encouraged over premature
  abstraction.
- Ugly code is allowed.
- Experiments that produce a FINDINGS file are **frozen**: do not
  modify them unless the task explicitly names them.

Under `runtime/` and `drivers/` (the substrate):
- Tests required.
- Package documentation required.
- Changes are deliberate.
- Code only arrives here via explicit promotion tasks. Promotion
  requires that at least two experiments have demanded the same
  abstraction.

## SYNTHESIS is a standing obligation

`SYNTHESIS.md` reflects the current best understanding of the problem
space. It is:

- Rewritten, not appended to. History lives in git.
- Updated whenever the author's mental model shifts meaningfully, not
  on a schedule.
- Organized around the six fundamentals.
- Silent about code structure (that's ARCHITECTURE's job).

Stale SYNTHESIS is a bug.

## FINDINGS are immutable

`FINDINGS/NNNN-<slug>.md` records what happened in experiment NNNN and
what was learned. Once written, it is not edited — even if the author's
understanding later shifts. That shift belongs in SYNTHESIS.

Compat scoreboard runs go in `FINDINGS/compat/YYYY-MM-DD.md`. They are
also immutable once written.

## The compat scoreboard is the accountability mechanism

There is no CI. `hack/test-compat.sh` is run at phase boundaries (the
author's judgment) and its output is committed as a dated FINDINGS
record. Silent drift is caught by the longitudinal record, not by
automation.

## Process is mutable

The rules in this file and in `AGENTS.md` are themselves experimental.
If the process is visibly failing, record the observation in a "Process
observations" section at the bottom of SYNTHESIS.md. When a pattern
emerges, rewrite AGENTS.md or ETHOS.md. Process changes are spine-level
commits.

## What this repo deliberately is not

- Not a product.
- Not open-source-ready. No CONTRIBUTING, CODE_OF_CONDUCT, issue
  templates, PR templates, CI workflows.
- Not versioned. No release tags, no semver, no CHANGELOG. The first
  time any of those become interesting, they are a deliberate
  experiment.
- Not conformant by default. Experiments may violate Kubernetes API
  conventions when doing so is the point of the experiment.

## What success looks like

Not "the code works." Success is: the FINDINGS files accumulate
genuine, non-obvious knowledge about aggregated APIs; SYNTHESIS
distills that knowledge into something another engineer could read to
orient themselves in this space; the compat scoreboard provides
longitudinal evidence of what works and what doesn't.

The code is evidence. The learnings are the asset.
