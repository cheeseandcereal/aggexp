# Experiment 0030: runtime-component-v2-promotion

This is a **substrate promotion**, not an experiment in the usual
sense. The output of this task is:

- `runtime/component/v2/` — a new substrate package family that
  consolidates the stateful-middleware-refinement arc (experiments
  0022-0029).
- `FINDINGS/0030-runtime-component-v2-promotion.md` — the findings
  doc explaining what was consolidated, what was deferred, and what
  surprised the author.
- Rewrite of `ARCHITECTURE.md` to reflect the v1/v2 split.

This directory exists only so `hack/verify-spine.sh`'s
FINDINGS↔experiment cross-check passes. There is no code to run
here; the substrate lives under `runtime/component/v2/` and the
tests live alongside each sub-package. Run `go test ./runtime/...`
from the repo root to exercise them.

## Hypothesis

The arc's commitments (0022 thesis.go + 0023-0029 evidence) can be
consolidated into a single substrate-quality package alongside the
existing `runtime/component` (v1). v1 stays frozen for its existing
consumers; v2 is what new consumers should reach for.

Fundamentals touched: all six. See the FINDINGS file.

## How to run

```
cd ${WORKTREE}
go test ./runtime/...
hack/verify-spine.sh
```

## Status

complete

## Decisions made

Recorded as scope cuts in `FINDINGS/0030-runtime-component-v2-promotion.md`.
The most load-bearing:

- Shipped single-version `ResourceMetadata` CRD; migration story
  deferred.
- Shipped dynamic-install cache-defeat fix (Compose returns a live
  closure); V3 endpoint refresh + SSA typed-converter rebuild for
  dynamically-installed groups remain known gaps.
- Shipped gRPC Dial + HTTP Client as first-class transports; push-vs-poll
  capability is a static descriptor field, runtime probe deferred.
- Pinned `cel-go@v0.22.0` (already on the module graph from 0029).
- Re-derived fresh; did not copy-and-patch v1's `grpcbackend.REST`.
  Final v2 hand-written LOC: ~4,565.

## What we're looking to learn

Whether the arc's design survives one coherent substrate, and what
gaps remain for a follow-on consumer experiment (0031). The
consumer experiment is the test of the promotion.
