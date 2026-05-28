// Package library provides production-grade enhancements for the v1
// runtime/storage path. It wraps runtime/storage.REST with composable
// capabilities that experiments 0032-0040 validated independently:
//
//   - Deterministic UIDs: SHA256(group/resource/namespace/name) formatted
//     as 8-4-4-4-12 hex. Eliminates pod-restart phantom-reconcile storms.
//
//   - Pagination: limit+continue token semantics in the adapter layer,
//     point-in-time snapshot consistency, 410 on stale tokens.
//
//   - Field selectors: metadata.name/metadata.namespace implicit support
//     plus consumer-registered custom fields via FieldAccessor.
//
//   - Optimistic concurrency: per-object RV tracking with 409 Conflict
//     on stale-RV Update. Standard Kubernetes OCC behavior.
//
//   - WatchList BOOKMARK: emits k8s.io/initial-events-end annotation at
//     tail of watch prefix. Closes kubectl wait --for=jsonpath gap.
//
//   - Poll-mode consumer watch: periodically polls Backend.List, diffs
//     against cached snapshot, emits ADDED/MODIFIED/DELETED via Publisher.
//     Gives full watch semantics to read-only backends.
//
//   - Status subresource: helpers for registering resource/status in the
//     API group with spec-preserving or status-preserving Update wrappers.
//
//   - CRD-backed shared storage: multi-replica support using a host-cluster
//     CRD as the single source of truth with informer-driven watch.
//
//   - Lease-based locking: per-object write locking via Kubernetes Lease
//     objects for multi-replica deployments.
//
// Each capability is independently opt-in via the Options struct. A consumer
// that only needs pagination + OCC can enable those without pulling in CRD
// store or locking dependencies.
//
// The Backend/WritableBackend/Publisher interfaces from runtime/storage are
// unchanged. This package provides enhanced REST adapters on top of the
// same interface.
//
// Usage:
//
//	import "github.com/cheeseandcereal/aggexp/runtime/library"
//
//	store := library.New(library.Options{
//	    Backend:          myBackend,
//	    GroupResource:    schema.GroupResource{Group: "example.io", Resource: "widgets"},
//	    DeterministicUIDs: true,
//	    Pagination:       true,
//	    OptimisticConcurrency: true,
//	    Bookmark:         true,
//	})
package library
