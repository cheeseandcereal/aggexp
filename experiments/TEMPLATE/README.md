# Experiment NNNN: <slug>

<One-paragraph description of what this experiment is.>

## Hypothesis

<What are we trying to learn, or what are we trying to break? Name fundamentals by name if relevant (wire protocol fidelity, identity handoff, storage independence, per-request authorization, resource modeling freedom, watch and consistency semantics).>

## How to run

<Copy-paste commands to stand this up from a clean repo checkout. Assume the reader has `kind`, `kubectl`, `go`, `docker`. Reference `hack/` scripts by path.>

## Status

in-progress

<!-- valid values: in-progress, complete, abandoned -->

## Decisions made

<!--
Record arbitrary tactical choices as a simple list, one line each:
- chose X because Y
- default to N because we had to pick something

If this experiment later produces a finding affected by one of these,
the FINDINGS file will reference this list.
-->

## Prerequisites

<!--
Anything external required to run this experiment:
- cluster: kind cluster `aggexp`
- secrets: list required .env vars if any
- network: list external systems this talks to
Delete this section if there's nothing external.
-->

## What we're looking to learn

<!--
Reference the fundamentals list. A single experiment usually probes one
or two. Be concrete about the question the experiment is designed to
answer.
-->
