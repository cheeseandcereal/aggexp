# FINDINGS 0039: optimistic-concurrency

Optimistic concurrency control (OCC) — rejecting writes with stale
resourceVersions — is straightforwardly implementable in the library
layer. The implementation is ~180 lines of Go wrapping the existing
`runtime/storage.REST` struct, maintaining a per-object RV map, and
intercepting the Update path to compare incoming RVs against stored
RVs before allowing the write through. Standard Kubernetes 409
Conflict semantics work correctly against kubectl replace, kubectl
patch (with its built-in retry), kubectl apply (client-side), and
the kubectl edit pattern.

## What I was trying to learn

Whether the library layer (specifically the `runtime/storage`
adapter) is the right place for RV-based conflict detection, and
whether the simple "compare incoming RV vs stored RV" approach
composes cleanly with the substrate's synthetic-RV scheme (a
monotonic `atomic.Uint64`).

## What I did

Forked 0032's library-mode Widget AA (stripped the Lease-based
locking), added a `pkg/occ` package that wraps `*runtimestorage.REST`
with:

1. A per-object RV map (`sync.RWMutex` + `map[string]string`)
   tracking the last assigned RV for each object.
2. Overridden `Get` and `List` that stamp tracked RVs onto objects
   before returning them to clients.
3. Overridden `Create` that delegates to the embedded REST's Create
   (which calls Backend.Create + PublishAdded to stamp RV) then
   records the assigned RV.
4. Overridden `Update` that:
   - Gets the current object from the backend.
   - Stamps the tracked RV onto it.
   - Calls `objInfo.UpdatedObject` to compute the caller's desired state.
   - Checks: if the object exists and the incoming RV is empty → reject.
   - Checks: if the incoming RV != stored RV → 409 Conflict.
   - Otherwise proceeds to `Backend.Update` + `PublishModified`.
5. Overridden `Delete` that cleans up the RV map entry.

Deployed as a single-replica Deployment in a kind cluster
(`aggexp-0039`). Ran five scenario classes.

## What I observed

**All five scenario classes pass:**

1. **Create (no RV)**: Always succeeds. The OCC check is skipped
   because there's no existing object to compare against. RV "2"
   is assigned on the first create (RV "1" is the initial counter
   value set at adapter construction).

2. **Correct-RV update**: Succeeds with 200. The object's RV bumps
   from N to N+1. Replace with the correct RV passes the check and
   the backend stores the update.

3. **Stale-RV update**: Returns exactly `409 Conflict` with the
   standard Kubernetes error message "Operation cannot be fulfilled
   on widgets.widgets.aggexp.io: the object has been modified;
   please apply your changes to the latest version and try again".

4. **Concurrent writes**: Two clients that read the same RV and
   then both try to replace — first one wins (200), second gets
   409. `kubectl patch` (which has built-in retry on 409) retries
   transparently: both patches succeed because the loser re-reads
   the new RV and reapplies. This matches real Kubernetes behavior.

5. **kubectl apply (client-side)**: Works correctly. `kubectl apply`
   internally reads the object (getting the current RV), computes a
   three-way-merge patch, and sends it as a strategic-merge-patch
   request carrying the RV. The library's PATCH machinery calls our
   `Update` method, which validates the RV. First apply creates;
   subsequent applies update with the correct RV.

   **kubectl apply --server-side**: Fails with "failed to convert
   new object to smd typed: .apiVersion: field not declared in
   schema". This is NOT an OCC issue — it's the same schema-
   completeness gap from 0013/0017. The minimal hand-written
   OpenAPI doesn't declare `apiVersion`/`kind` as explicit schema
   properties, so the SSA typed-converter can't parse the object.
   This is orthogonal to OCC; a fully-generated OpenAPI (or the
   0017 typed-wrapper approach) would fix it.

## What surprised me

**The substrate doesn't persist RVs back into the backend.** The
`runtime/storage.REST` adapter assigns RVs via `PublishAdded` and
`PublishModified` (which stamp the returned object via `stampRV`),
but the backend's internal store never sees the RV — because the
backend returns a copy, and the stamp modifies the copy. This means
`Backend.Get` returns objects without any RV. A library-layer OCC
implementation must maintain its own RV tracking or modify the
backend contract.

This is a design choice in the current substrate: the backend is
treated as a pure data store and RV is a middleware concern. The
choice is defensible (it keeps the Backend interface simpler and
decouples RV policy from storage), but it creates an impedance
mismatch when you want to compare RVs in the Update path. The
per-object RV map in the OCC wrapper is the minimal fix: ~20 lines
of map bookkeeping.

**`kubectl patch` has built-in conflict retry.** Sending two
concurrent `kubectl patch` commands doesn't surface the 409 to the
user because kubectl's client-side retry logic re-reads and
re-patches. Only `kubectl replace` (and raw API calls) surface the
conflict directly.

**The OCC check composes trivially with the PATCH machinery.**
The library's strategic-merge-patch and JSON-merge-patch
implementations call `objInfo.UpdatedObject(ctx, current)` which
computes the merged result from the original read. The merged
result carries the original's RV (because the PATCH request body
doesn't override `metadata.resourceVersion` unless the user
explicitly sets it). This means PATCH requests naturally participate
in OCC without any special handling — the RV from the initial read
flows through the merge and arrives at our check.

## Fundamentals touched

### Watch and consistency semantics (primary)

ResourceVersion is the foundation of both watch ordering and
optimistic concurrency. The two uses are architecturally the same
mechanism: a monotonic counter that serializes all mutations. OCC
is the write-side enforcement of the same invariant that watch
depends on for event ordering. Without OCC, a client could read
at RV=5, write at RV=5 (stale), and the resulting mutation would
be invisible to a watch stream that already passed RV=5 — but
since the substrate always assigns monotonically-increasing RVs
to mutations, the write IS visible on the watch stream (at
RV=6). The issue without OCC is not watch correctness but
*client-side data integrity*: a client's read-modify-write can
silently overwrite another client's concurrent modification.

OCC closes this: the stale-RV writer gets 409 and must re-read
before retrying, which means it observes the concurrent
modification.

### Storage independence (secondary)

The OCC wrapper operates entirely in the library layer. It doesn't
need backend cooperation beyond the basic `Get/Update` contract.
The per-object RV map is in-process memory — no additional
external state. This confirms that optimistic concurrency is a
middleware concern, not a storage concern. Any backend (in-memory,
external API, CRD facade, gRPC backend) can be wrapped with OCC
at the library level without modification.

## Consequents noted

- The substrate's `PublishAdded`/`PublishModified` stamp RVs on
  the returned copy but not on the backend's stored copy. This is
  a substrate-level design decision, not a fundamental limit. A
  future substrate version could optionally persist RVs into the
  backend if the Backend interface gained a
  `SetResourceVersion(name, rv)` method — but it's simpler to keep
  RV management in the middleware.

- The hand-written minimal OpenAPI schema (reusing 0007's generated
  meta-type defs) doesn't declare `apiVersion`/`kind` as explicit
  properties, so SSA's typed-converter rejects objects. This is the
  same consequent as 0013/0017. OCC and SSA are orthogonal; fixing
  SSA requires schema completeness, not OCC changes.

- `kubectl patch` retries on 409 automatically (client-go's
  `resource.Helper.Patch` has retry logic). This means concurrent
  `kubectl patch` commands don't surface conflicts to the user —
  only raw `kubectl replace` or direct API calls do.

## What this changes for SYNTHESIS and EXPERIMENTS

For SYNTHESIS: the Watch and consistency semantics section should
note that OCC (stale-RV rejection) is implementable purely in the
library layer with ~180 lines, requires no backend cooperation, and
composes naturally with the existing PATCH machinery. The substrate
gap (RVs not persisted to backend) is a design point, not a bug.

For EXPERIMENTS: 0039 can be marked complete. The interaction with
0032/0033 (locking) is compositional: locking prevents
cross-replica conflicts at the lock-acquisition layer; OCC prevents
stale-read-then-write within one client. The two are complementary,
not alternatives. A future substrate promotion could fold OCC into
the `runtime/storage.REST` struct directly (as an opt-in option),
eliminating the need for per-experiment wrapper code.

## Open questions

- Should OCC be opt-in or default-on in a future substrate? Real
  Kubernetes always enforces it. But for stateless-projection AAs
  (where the backend is external and RVs are synthetic), OCC on
  synthetic RVs prevents only same-process races — cross-replica
  conflicts are invisible. The answer likely depends on whether the
  AA is single-replica or multi-replica.

- The per-object RV map in the OCC wrapper is unbounded. At scale
  (10k+ objects), this is fine in memory, but if objects are deleted
  from the backend out-of-band (not through our Delete path), the
  map leaks entries. A periodic sweep (similar to 0028's GC) could
  address this at scale.

- How does OCC interact with watch-resume? A client that watches
  from RV=5 and then tries to update an object it received at
  RV=5 will succeed only if no one else has modified that specific
  object since. This is correct behavior but may surprise clients
  that assume "I just saw it on my watch at RV=5, so my update
  at RV=5 should succeed" — the watch RV is the global counter,
  not a per-object version.
