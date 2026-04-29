package storage

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authentication/user"
)

// ListOptions is the narrow subset of list options the Backend cares
// about. The adapter translates metainternalversion.ListOptions into
// this shape so backends don't need to import the internalversion
// package.
type ListOptions struct {
	// LabelSelector, if non-nil, filters the returned objects.
	// Backends MAY pre-filter for efficiency; the adapter also
	// applies the selector defensively after the call.
	LabelSelector labels.Selector
}

// Backend is the minimum contract a resource type must implement to
// be served by the adapter. It is read-only; writable backends
// additionally implement WritableBackend.
//
// All methods must be safe for concurrent use.
type Backend interface {
	// New returns a new empty instance of the resource's top-level
	// type. It mirrors rest.Storage.New.
	New() runtime.Object
	// NewList returns a new empty list type.
	NewList() runtime.Object
	// Kind returns the resource's Kind (CamelCase singular).
	Kind() string
	// SingularName returns the resource's singular name
	// (lowercase, kubectl-friendly).
	SingularName() string
	// NamespaceScoped indicates whether the resource is
	// namespace-scoped. Most experiments in this repo have been
	// cluster-scoped; both are supported.
	NamespaceScoped() bool

	// Get returns a single object by name. On not-found it should
	// return a kerrors.NewNotFound error so the library produces a
	// proper 404.
	Get(ctx context.Context, u user.Info, name string) (runtime.Object, error)
	// List returns a list object. Resource-version accounting is the
	// adapter's job; the backend only needs to populate Items.
	List(ctx context.Context, u user.Info, opts ListOptions) (runtime.Object, error)

	// TableColumns returns the kubectl table column headers for
	// this resource.
	TableColumns() []metav1.TableColumnDefinition
	// RowsFor returns the rows for either a single object or a
	// list object. Implementations typically type-switch on obj.
	RowsFor(obj runtime.Object) ([]metav1.TableRow, error)
}

// WritableBackend extends Backend with mutation operations. The
// adapter exposes rest.Patcher (= Getter+Updater) when a backend
// implements this interface.
type WritableBackend interface {
	Backend

	// Create stores obj. The returned object is what the caller
	// sees; typical implementations assign UID, creation timestamp,
	// and the adapter-provided resourceVersion before returning.
	// The adapter does NOT set RV on the returned object; backends
	// are expected to call Broadcaster.Action themselves if they do
	// not use the Publisher API.
	//
	// A well-behaved WritableBackend should instead call the
	// Publisher interface to inject new objects, so the adapter can
	// stamp the resourceVersion and fan out the watch event.
	Create(ctx context.Context, u user.Info, obj runtime.Object) (runtime.Object, error)

	// Update stores obj under name. forceAllowCreate indicates the
	// caller requested upsert semantics (PATCH with apply).
	// Returned bool is "created".
	Update(ctx context.Context, u user.Info, name string, obj runtime.Object, forceAllowCreate bool) (runtime.Object, bool, error)

	// Delete removes obj by name. Returned bool indicates whether
	// the returned object reflects an actual deletion (true) vs. a
	// pending / already-gone response (false).
	Delete(ctx context.Context, u user.Info, name string) (runtime.Object, bool, error)
}

// Publisher is given to backends that want to emit watch events
// (e.g. a polling loop that diffs upstream state on a tick). The
// adapter stamps the resourceVersion and fans out to all watchers.
type Publisher interface {
	// PublishAdded is called when a new object has appeared in the
	// backend. The adapter assigns a fresh RV.
	PublishAdded(obj runtime.Object)
	// PublishModified is called when an existing object changed.
	PublishModified(obj runtime.Object)
	// PublishDeleted is called when an object disappeared.
	PublishDeleted(obj runtime.Object)
	// CurrentResourceVersion returns the current RV as a string,
	// for backends that need to stamp it on list results etc.
	CurrentResourceVersion() string
}
