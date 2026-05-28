# FINDINGS/0041 — Library Promotion

## What we were trying to do

Consolidate nine experiments (0032-0040) — the production-library-readiness arc — into a single substrate package (`runtime/library/`) that sits alongside `runtime/storage/` (v1, frozen) and `runtime/component/v2/` (frozen). The goal was a composable, opt-in enhancement layer for the v1 library-mode path.

## What we did

Created `runtime/library/` with 13 source files and 1 test file. The package provides an enhanced `REST` adapter that wraps `runtime/storage.Backend` (same interface, unchanged) with nine independently-enabled capabilities:

1. **Deterministic UIDs** — `DeterministicUID(gr, namespace, name)` produces `SHA256` → `8-4-4-4-12` hex. Stamped on Create and create-on-update. From 0035.
2. **Pagination** — `limit` + `continue` token with point-in-time snapshot semantics. Sorts by name, encodes `rv:offset` as base64 token, returns 410 on stale. From 0036.
3. **Field selectors** — `metadata.name`/`metadata.namespace` always supported; custom fields via `FieldAccessor` callback. Validates unknown fields with 400. Filters on List and Watch. From 0037.
4. **Optimistic concurrency** — Per-object RV map. Get/List stamp tracked RVs. Update checks incoming RV against stored; rejects with 409 Conflict on mismatch or empty RV. From 0039.
5. **WatchList BOOKMARK** — Emits `k8s.io/initial-events-end` annotation as a BOOKMARK event at tail of watch prefix when `allowWatchBookmarks=true`. Respects the flag (no emission when false). From 0025/0040.
6. **Poll-mode consumer watch** — `PollWatcher` goroutine: polls `PollLister.List`, diffs by JSON hash, emits ADDED/MODIFIED/DELETED via `PollPublisher`. `BackendPollLister` adapts any `Backend` to `PollLister`. From 0040.
7. **Status subresource** — `StatusREST` and `SpecREST` helpers. Generic via consumer-provided `StatusUpdater`/`SpecUpdater` callbacks. No type assertion to experiment-specific types. From 0038.
8. **CRD-backed shared storage** — `CRDStore` implements `WritableBackend` backed by a host-cluster CRD via dynamic informer. Provides `CRDStoreEventSink` for RV-authority and cross-replica watch consistency. From 0034.
9. **Lease-based locking** — `LockedBackend` wraps `WritableBackend` with per-object or per-resource Lease acquisition before writes. 3-retry acquire loop with expiry-based takeover. From 0032.

## What we observed

- The nine capabilities compose without interference. A consumer can enable any subset.
- The `Backend`/`WritableBackend` interface did not need changes. All enhancements layer above it.
- Total hand-written Go: ~1,100 lines. Tests: ~450 lines. Well within the 2-3k budget (tests excluded).
- The existing `runtime/storage` tests continue to pass (v1 frozen).
- The existing `runtime/component/v2` tests continue to pass (v2 frozen).

## What surprised us

Nothing fundamentally surprising. The consolidation was clean because each experiment validated a narrow, well-scoped capability. The most interesting design decision was making OCC tracking a transparent layer: PublishAdded/Modified record RVs, Get/List/Update stamp/check them, all without the consumer's backend knowing OCC exists.

## Fundamentals touched

**Watch and consistency semantics**: BOOKMARK emission, poll-mode watch, OCC (RV-based concurrency). These are the heart of the library's value over raw `runtime/storage`.

**Storage independence**: CRDStore provides a fifth storage axis for the library-mode path (previously only available in v2). PollWatcher makes any read-only backend watchable.

**Wire protocol fidelity**: Pagination, field selectors, and BOOKMARK are all wire-protocol features that ecosystem tooling expects. The library makes them zero-cost to adopt.

## Consequents

- The `PollWatcher` JSON-hash diff is a consequent of the experimental approach (fast, correct, not optimal at scale). A production deployment at 10k+ objects would want a content-hash or generation-counter.
- The `LockedBackend` uses `POD_NAME` env var or hostname for identity. This is a deployment convention, not architecturally necessary.
- The CRDStore's `CRDStoreConverter` interface requires consumer implementation for each type. This is a consequence of keeping the library generic over types.

## What this changes for SYNTHESIS and EXPERIMENTS

SYNTHESIS: No shift in understanding. The nine capabilities were already validated individually; this promotion codifies them as substrate.

EXPERIMENTS: The production-library-readiness arc (0032-0040) is now complete with substrate promotion. Future library-mode consumers can import `runtime/library` directly.

## Scope cuts

- **ConvertToTable multi-object edge cases from 0036**: Included the core fix (populating `Row.Object` and propagating pagination metadata) but deferred the full multi-page table rendering edge case.
- **CRD-CAS locking from 0033**: Not included as a separate mechanism. The CRDStore itself provides CAS via `resourceVersion` on Update (as 0034 demonstrated). Lease-based locking from 0032 covers the general case.
- **AddFieldLabelConversionFunc scheme registration**: Documented as consumer obligation. The library validates at the REST layer but doesn't touch the scheme.

## Open questions

- Should the library provide a pre-built "full featured" constructor that enables all non-multi-replica features by default? Currently requires explicit opt-in for each.
- At what object count does PollWatcher's JSON-hash approach become a bottleneck? Untested above ~100 objects.
- Should CRDStore and LockedBackend move to a sub-package (`runtime/library/multihost/`) to keep the core library's import graph lighter? Currently in the same package; the imports are already present in the module via `runtime/storage`'s transitive deps.
