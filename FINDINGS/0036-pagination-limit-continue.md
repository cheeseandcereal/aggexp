# FINDINGS: 0036-pagination-limit-continue

## What we were trying to learn

Can cursor-based pagination (`limit` + `continue` token) be implemented
in the library/adapter layer without any backend support? The substrate's
`runtime/storage` adapter returns all items from the backend on every
List call. We wanted to know if pagination can be cleanly layered on
top — truncating the result, encoding a continuation token, and
validating staleness — without touching the `Backend` interface.

## What we did

Built a `PaginatedREST` wrapper around `runtime/storage.REST` that
intercepts `List()`. When the request includes `limit` or `continue`,
the wrapper:

1. Calls the inner storage to get the full item set (backend unaware).
2. Sorts items by name for deterministic ordering.
3. If a `continue` token is present, decodes it (`base64(rv:offset)`)
   and validates the RV matches the current resourceVersion.
4. Slices the sorted items from offset to offset+limit.
5. Encodes a new continue token if more items remain.
6. Sets `Continue` and `RemainingItemCount` on the list metadata.

The backend never knows pagination exists. Pre-populated 20 widgets at
startup to provide enough items for demonstration.

## What we observed

**Pagination works end-to-end.** `kubectl get widgets --chunk-size=5`
makes exactly 4 requests (limit=5 each), chaining continue tokens, and
displays all 20 widgets correctly. Manual `curl`-equivalent with
`?limit=3` returns 3 items plus a continue token; following the token
returns the next 3.

**Stale token → 410 works.** Creating a widget after obtaining a
continue token bumps the RV; using the old token produces `410
ResourceExpired` with a clear message. kubectl and client-go handle
this by relisting from scratch (standard reflector behavior).

**Two substrate gaps surfaced:**

1. **Table conversion lacks `Object` on rows.** The substrate's
   `ConvertToTable` returns TableRows with only `Cells` populated.
   The apiserver's response handler (`asTable` in
   `endpoints/handlers/response.go`) expects `item.Object.Object` to
   be set so it can produce `PartialObjectMetadata` for each row (the
   default `includeObject=Metadata` mode). Without it, `meta.Accessor`
   fails with "object does not implement the Object interfaces" →
   HTTP 500. Fix: the wrapper populates `Object` from the list items.
   This bug exists in the substrate's adapter regardless of pagination;
   it was masked in earlier experiments because those may not have
   hit the table path under the same conditions (kubectl 1.32's default
   `limit=500` query triggers it).

2. **Table conversion doesn't propagate Continue/RemainingItemCount.**
   The substrate's `ConvertToTable` copies `ResourceVersion` from the
   list to the Table's ListMeta but not `Continue` or
   `RemainingItemCount`. kubectl relies on the Table response carrying
   these to know there's another page. Fix: the wrapper copies them
   explicitly.

Both are pre-existing substrate gaps, not pagination-specific bugs. They
should be fixed in `runtime/storage.REST.ConvertToTable` when the
substrate is next revised.

**Performance characteristics observed:**

- Each paginated page makes a full List call to the backend (all 20
  items loaded into memory, sorted, then sliced). At 20 items this is
  invisible (<1ms per page). At 10k+ items this would be O(n log n)
  sort + O(n) memory on every page request.
- The RV-equality staleness check is maximally conservative: any write
  (even to an unrelated item) invalidates all outstanding continue
  tokens. A production implementation might use a content-hash or
  generation counter scoped to the sort order rather than the global RV.

## Which fundamentals this touches

**Wire protocol fidelity** (primary). Pagination is part of the standard
Kubernetes List wire contract:
- `metainternalversion.ListOptions.Limit` and `.Continue` are populated
  from the `?limit=N&continue=<token>` query params by the apiserver's
  parameter codec.
- The list response must set `metadata.continue` and optionally
  `metadata.remainingItemCount` to signal more pages.
- `410 ResourceExpired` is the standard error for stale tokens.
- kubectl's `--chunk-size` flag (default 500 since kubectl 1.9) relies
  on all three working correctly.

The experiment confirms that the wire contract for pagination is fully
satisfiable from the adapter layer alone. No backend cooperation needed.

## Consequents

- The table-conversion `Object` gap is a consequent of the substrate's
  current implementation, not of the aggregated-API architecture. It's
  fixable in a single location (`runtime/storage.REST.ConvertToTable`).
- The `--chunk-size=500` default in kubectl means almost every `kubectl
  get` against a library-mode AA was hitting this table bug; earlier
  experiments may have been testing with `-o yaml` or `-o json`
  (bypassing table) without noticing.
- The full-list-in-memory-per-page cost is a consequent of the adapter's
  lack of backend pagination support. It's acceptable at lab scale and
  could be addressed by adding optional `Limit`/`Offset` fields to the
  `Backend.List` interface in a future substrate revision.

## What this changes for SYNTHESIS and EXPERIMENTS

**SYNTHESIS**: adds to wire protocol fidelity — pagination is now
confirmed satisfiable at the adapter layer with zero backend involvement.
The table-conversion gap is a new substrate-level finding worth noting.

**EXPERIMENTS**: no menu changes needed. The table-conversion fix could
be folded into the next substrate promotion (alongside 0035-0040 arc
findings).

## Open questions

- Should the staleness check be more granular? A hash of item names
  (or a generation counter only bumped on add/delete) would allow
  continue tokens to survive updates to existing items.
- Should pagination state be cached server-side (snapshot the sorted
  list on first page, serve subsequent pages from cache)? This trades
  memory for consistency and avoids re-sorting on each page.
- Can `Backend.List` optionally accept `Limit`/`Offset` to push
  pagination into the backend for large datasets?
- The table-conversion `Object` population and `Continue` propagation
  should be fixed in the substrate. How does this interact with the
  0032/0033/0034 experiments that also use `runtime/storage.REST`?
