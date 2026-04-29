// Package storage provides a reusable rest.Storage adapter that turns
// a minimal Backend implementation into a full-shaped aggregated-API
// resource. The adapter owns the wire-compatibility concerns that
// every experiment in this repo has had to re-solve:
//
//   - synthetic monotonic resourceVersion (atomic.Uint64);
//   - watch fan-out via watch.Broadcaster;
//   - label-selector filtering on List and Watch;
//   - ErrResourceExpired on stale resume;
//   - TableConvertor bridging backend columns + rows.
//
// A Backend implements only its own data shape: type identity,
// read operations, and table rendering. If a backend is also a
// WritableBackend, the adapter exposes Create / Update / Delete /
// Patch via the same REST object. The adapter's invariants hold as
// long as all mutations reach it through the adapter's
// Create/Update/Delete methods or through Publish.
package storage
