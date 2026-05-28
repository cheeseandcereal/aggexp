# 0041-library-promotion

## Status

complete

## Hypothesis

Nine experiments (0032-0040) each validated a narrow, production-grade
capability for the v1 library-mode path. Consolidating them into a
single `runtime/library/` substrate package — with composable, opt-in
features — will provide a drop-in upgrade path for library-mode
consumers without changing the Backend/WritableBackend interface.

## How to run

This is a substrate promotion, not a runnable experiment. The
deliverable is `runtime/library/`:

```bash
go test ./runtime/library/ -v
```

## What we're looking to learn

Whether the nine independently-validated capabilities compose cleanly
into a single adapter layer without mutual interference.

## Decisions made

- Kept CRDStore and LockedBackend in the same package rather than a
  sub-package. The import graph doesn't meaningfully change since
  k8s.io/client-go is already transitively imported.
- Used an Options struct with boolean flags for each capability rather
  than builder-pattern or middleware-chain composition. Simpler API
  surface for the common case.
- Made PollWatcher independent of the REST adapter (takes its own
  PollLister + PollPublisher) so consumers can use it with any
  publisher, not just library.REST.
- Status subresource helpers are generic via callbacks rather than
  type-asserting. This avoids coupling the substrate to any
  experiment's types.
- CRD-CAS locking (0033) not included separately; the CRDStore's
  built-in RV-based CAS covers the same ground.
