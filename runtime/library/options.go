package library

import (
	"k8s.io/apimachinery/pkg/runtime/schema"

	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// Options configures the enhanced REST adapter. Each capability is
// independently opt-in.
type Options struct {
	// Backend is the resource-specific data plane. Required.
	Backend runtimestorage.Backend

	// GroupResource is used in error messages and field-selector
	// validation. Required.
	GroupResource schema.GroupResource

	// BroadcasterSize is the watch.Broadcaster queue size.
	// Defaults to 100 if 0.
	BroadcasterSize int

	// DeterministicUIDs enables SHA256-based UID generation. When
	// true, the adapter stamps UIDs on objects returned by Create
	// using DeterministicUID(GroupResource, namespace, name).
	DeterministicUIDs bool

	// Pagination enables limit+continue pagination on List.
	Pagination bool

	// OptimisticConcurrency enables per-object RV tracking with
	// 409 Conflict on stale-RV Update.
	OptimisticConcurrency bool

	// Bookmark enables emission of the k8s.io/initial-events-end
	// BOOKMARK event at the tail of watch prefix when
	// allowWatchBookmarks is true in the watch options.
	Bookmark bool

	// FieldSelectors configures field-selector support.
	// If nil, field selectors are not supported (requests with
	// fieldSelector will be rejected).
	FieldSelectors *FieldSelectorOptions

	// --- Multi-replica features (opt-in) ---
	// These are typically NOT needed for single-replica deployments.

	// CRDStoreConfig, if non-nil, enables CRD-backed shared storage.
	// The Backend field in Options is still required but may be the
	// CRDStore itself.
	CRDStoreConfig *CRDStoreOptions
}

// FieldSelectorOptions configures field selector support.
type FieldSelectorOptions struct {
	// SelectableFields is the list of fields beyond metadata.name
	// and metadata.namespace that can be selected on.
	SelectableFields []string
	// Accessor extracts field values from objects. Required if
	// SelectableFields is non-empty.
	Accessor FieldAccessor
}
