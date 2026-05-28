# Findings — 0040 watchlist-and-consumer-watch

## What we were trying to learn

The v1 library-mode storage adapter (`runtime/storage`) does NOT emit the
`k8s.io/initial-events-end` BOOKMARK after the initial watch prefix.
FINDINGS/0011 identified this gap; FINDINGS/0025 closed it for the v2
component-server path. This experiment back-ports the fix to the v1
library path and additionally probes whether a poll-based consumer watch
wrapper (library calls List + diffs) can deliver identical watch
behavior to the push model (consumer calls Publisher directly).

Three hypotheses:

1. Emitting the BOOKMARK at the tail of the watch prefix closes
   `kubectl wait --for=jsonpath` for the v1 library mode.
2. Respecting `allowWatchBookmarks=false` is correct protocol behavior.
3. Push and poll consumer modes produce identical watch behavior from
   the client's perspective.

## What we did

Forked 0032's library-mode Widget AA. Added a second resource type
(Gadget) on a separate API group (`gadgets.aggexp.io/v1`). Widget uses
the existing push model (writable backend, adapter calls
`PublishAdded/Modified/Deleted` directly). Gadget uses a new poll wrapper
(`pkg/pollwatch/`): the library periodically calls a `Lister.List`
function, diffs against a cached snapshot keyed by object name, and emits
Added/Modified/Deleted events via the Publisher interface.

The watch fix is a `BookmarkWatchREST` wrapper (~80 LOC) around
`*runtimestorage.REST` that intercepts the `Watch` method:

1. If `opts.AllowWatchBookmarks` is false, delegate to the underlying
   REST without modification.
2. Otherwise, wrap the returned `watch.Interface` with a
   `bookmarkInjector` goroutine that:
   - Drains all immediately-buffered prefix events (ADDED) from the
     upstream channel (they are synchronously queued by
     `WatchWithPrefix` before the method returns).
   - Emits a BOOKMARK event using the resource's own type (required
     for the GVK-specific watch serializer) with annotation
     `k8s.io/initial-events-end=true` and resourceVersion set to
     current.
   - Forwards all subsequent live events unchanged.

The bookmark object uses the resource's `New()` function (not
`PartialObjectMetadata`) because the watch stream encoder serializes
each event using the resource's negotiated codec — a foreign GVK
silently fails to encode.

Deployed to kind cluster `aggexp-0040` with one pod serving both API
groups.

## What we observed

### BOOKMARK emission

The watch stream with `allowWatchBookmarks=true`:

```
ADDED   {"kind":"Widget", ...}
BOOKMARK {"kind":"Widget","metadata":{"resourceVersion":"2","annotations":{"k8s.io/initial-events-end":"true","kubernetes.io/initial-events-list-blueprint":"..."}}}
```

The library augments our BOOKMARK with a
`kubernetes.io/initial-events-list-blueprint` annotation (base64-encoded
empty WidgetList). This confirms the library's watch handler recognizes
the BOOKMARK type and processes it further — the bookmark is
composing cleanly with the library's own WatchList machinery.

### kubectl wait --for=jsonpath

```
$ kubectl wait --for=jsonpath='{.spec.color}'=red widget/foo --timeout=8s
widget.widgets.aggexp.io/foo condition met   (exit 0)

$ kubectl wait --for=jsonpath='{.spec.model}'=X100 gadget/gizmo-alpha --timeout=8s
gadget.gadgets.aggexp.io/gizmo-alpha condition met   (exit 0)
```

Both push-mode (Widget) and poll-mode (Gadget) resources pass. This
closes the gap FINDINGS/0011 identified for the v1 library path.

### allowWatchBookmarks=false

With `allowWatchBookmarks=false`, the stream contains only ADDED events —
no BOOKMARK. The wrapper correctly respects the client's opt-out.

### Poll mode observation

The poll wrapper (`pkg/pollwatch/`) is 95 LOC. It implements:

- First poll at t=0 (immediate snapshot seed)
- Periodic polls at configured interval (5s in this experiment)
- Diff by name: new names → ADDED, missing names → DELETED, changed
  JSON → MODIFIED

From `kubectl get gadgets -w`, the poll-driven watch is
indistinguishable from push-driven watch: clients see ADDED events for
initial state, then MODIFIED/DELETED as the background mutator changes
the source. The only observable difference is latency: poll-driven
changes are delayed by up to one poll interval (5s), while push-driven
changes are immediate.

### Table converter fix

During deployment, `kubectl get` failed with "object does not implement
the Object interfaces" until we populated `TableRow.Object` with
`runtime.RawExtension{Object: &item}`. The v1 `runtime/storage`
adapter's `ConvertToTable` relies on the backend's `RowsFor` to set
this field. This is a consumer obligation that previous experiments
(0032, 0035, etc.) did not surface because their list results were
empty during testing. Documented as a consequent.

## Which fundamentals this touches

**Watch and consistency semantics** (primary). Three findings:

1. The `initial-events-end` BOOKMARK is a **~80-line wrapper** on the
   v1 library path, not a substrate modification. The wrapper intercepts
   `Watch`, drains the prefix, injects the bookmark, and forwards live
   events. The fix is the same pattern 0025 documented for v2: emit the
   annotation after the prefix, using the resource's own GVK, at the
   current resourceVersion. What 0025 did as 10 lines inline in the
   component server takes ~80 lines as a wrapper because the v1 adapter
   returns a fully-formed `watch.Interface` and the bookmark must be
   injected between prefix and live events via a goroutine relay.

2. `allowWatchBookmarks=false` suppression is trivial: skip the wrapper.
   The flag arrives in `metainternalversion.ListOptions.AllowWatchBookmarks`
   — a single boolean check before wrapping. Correct protocol behavior
   confirmed: clients that don't opt into bookmarks don't receive them.

3. The **poll-mode consumer watch** delivers client-identical behavior
   to push mode from the wire protocol perspective. The only difference
   is latency (bounded by poll interval) and lossiness (mutations
   between polls are collapsed into the next snapshot diff — the same
   observation FINDINGS/0025 made). The poll wrapper is small enough
   (~95 LOC) to be a viable substrate-level primitive.

**Storage independence** (secondary). The poll wrapper decouples the
consumer from needing to implement any watch infrastructure. A backend
that can only List (read-only data sources, external APIs, databases)
gets full watch semantics for free — the library diffs snapshots and
emits events. This closes the ergonomic gap between "easy to implement"
(poll-only backends) and "full wire compatibility" (watch required).

## Consequents

- Poll interval 5s is arbitrary. Shorter intervals increase
  responsiveness at the cost of more List calls. The right default for
  a production library would be configurable (already is: `--poll-interval`
  flag).
- The 5ms sleep in the bookmarkInjector before draining prefix events
  is a defensive measure to ensure the broadcaster's `WatchWithPrefix`
  has fully populated the output channel. In practice the prefix is
  synchronously queued before `WatchWithPrefix` returns, so the sleep
  is belt-and-suspenders. A production implementation might use a
  deterministic signal instead.
- `TableRow.Object` must be set by `RowsFor` implementations —
  discovered when `kubectl get` returned 500. This is a consumer
  obligation that the `runtime/storage` adapter documents in the
  `TableConvertor` interface but doesn't enforce. Previous experiments
  didn't hit it because they tested with empty lists.
- The `kubernetes.io/initial-events-list-blueprint` annotation is
  added by the library's watch handler (not by our code). It encodes
  an empty `{Kind}List` and is used by WatchList-aware clients to
  assemble a cache. Our typed bookmark object (from `New()`) is what
  makes this work — the library can construct the blueprint because it
  knows the list type from the scheme.
- Docker image reference `docker.io/library/aggexp-0040:latest` needed
  the full prefix for kind to find it. Short form `aggexp-0040:latest`
  caused `ErrImageNeverPull`. Consequent of the kind + containerd
  image naming convention.

## What this changes for SYNTHESIS and EXPERIMENTS

For **SYNTHESIS**:

- The `initial-events-end` bookmark gap is now closed for BOTH the v1
  library path (this experiment) and the v2 component path (0025/0030).
  The gap entry in SYNTHESIS should be updated to reflect this.
- Poll-mode consumer watch is validated as a ~95-LOC library primitive.
  The storage-independence section can note that read-only backends get
  watch for free via periodic diff.

For **EXPERIMENTS.md**:

- `0040-watchlist-and-consumer-watch` — marked complete.

## Open questions raised

- Should the poll wrapper be promoted to `runtime/storage/` as a
  first-class primitive? Two experiments (0004's GitHub poll loop, this
  one) implement the same pattern. The two-experiment precondition for
  promotion is met.
- The bookmarkInjector's 5ms sleep is fragile. A production version
  should use a count-based approach (the adapter knows the prefix size
  because it built it from `List`). This would require the adapter to
  expose the prefix count or emit the bookmark itself.
- The bookmark fix is a wrapper, not integrated into `runtime/storage`.
  A substrate-level fix would modify `REST.Watch` directly to emit the
  bookmark — ~10 lines in the adapter, matching the v2 approach. Worth
  doing in a promotion task.
