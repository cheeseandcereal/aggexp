// Package thesis captures the interface commitments for the
// stateful-middleware-refinement arc. It does not provide runtime
// behavior; the experiments in this arc (0023-0029) and the 0030
// substrate promotion implement these shapes.
//
// The types here are deliberately minimal. They name the seams, not
// the implementation. Signatures can evolve during the arc; any
// evolution is recorded in the experiment's FINDINGS with a
// reference back to this package.
//
// Three axes separated:
//
//	(1) Wire protocol        — middleware's job. The apiserver-facing
//	                           side, scheme registration, openapi,
//	                           SSA field management, bookmarks, watch
//	                           HTTP semantics.
//	(2) KRM metadata state   — middleware's job, backed by a shared
//	                           ResourceMetadata CRD on the host.
//	(3) Business data        — backend's job. Plain CRUD+watch over
//	                           JSON bytes. No Kubernetes knowledge
//	                           required.
//
// The arc's question-1 (where does the OpenAPI live?) is
// deliberately left open; see SchemaSource below. 0023 will
// probe three concrete options and recommend one.
package thesis

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is the sentinel a Backend returns from Get when the
// named object does not exist. The middleware translates this to a
// Kubernetes 404 Status response for the client.
var ErrNotFound = errors.New("thesis: backend object not found")

// APIDefinition is the declarative configuration that creates an AA
// served by the multiplex middleware. Lives as a CRD on the host
// cluster at aggexpapidefinition.aggexp.io/v1. The middleware watches
// these and reconciles APIService registrations dynamically (0027).
type APIDefinition struct {
	// Group, Version, Resource, Kind identify the exposed API.
	Group    string `json:"group"`
	Version  string `json:"version"`
	Resource string `json:"resource"` // plural, lowercase
	Kind     string `json:"kind"`     // CamelCase singular
	Singular string `json:"singular"` // lowercase singular

	Namespaced bool `json:"namespaced"`

	// Backend is how the middleware reaches the business-logic
	// server.
	Backend BackendRef `json:"backend"`

	// SchemaSource is where the middleware obtains the resource's
	// OpenAPI schema. 0023 probes the three values below and
	// recommends one. This field exists to enumerate the arc's
	// design-space; the winning track becomes the default.
	SchemaSource SchemaSource `json:"schemaSource"`

	// SchemaInline, if set, carries the OpenAPI schema (or plain
	// JSON Schema, depending on SchemaSource) directly. Used by
	// SchemaSourceConfig and SchemaSourceConfigJSONSchema.
	SchemaInline []byte `json:"schemaInline,omitempty"`

	// Admission is declarative validation and mutation run by the
	// middleware (0029). Additive to any Validate/Mutate RPCs the
	// backend provides.
	Admission AdmissionConfig `json:"admission,omitempty"`

	// TableColumns is the kubectl-get column definition. Optional;
	// middleware falls back to name+age.
	TableColumns []TableColumn `json:"tableColumns,omitempty"`
}

// BackendRef tells the middleware how to reach the backend.
type BackendRef struct {
	// Transport is one of "grpc", "http". Default "grpc".
	Transport string `json:"transport"`
	// Address is the hostport (grpc) or URL (http) of the backend.
	Address string `json:"address"`
	// Watch declares the backend's watch capability so the middleware
	// can pick the cheapest mode. See Watch*Capability constants.
	Watch WatchCapability `json:"watch"`
}

// SchemaSource enumerates where the middleware obtains the OpenAPI
// schema. 0023 explores these.
type SchemaSource string

const (
	// SchemaSourceBackend asks the backend for its OpenAPI via
	// GetSchema. Matches 0013/0017 status quo.
	SchemaSourceBackend SchemaSource = "backend"
	// SchemaSourceConfig reads the full OpenAPI from
	// APIDefinition.SchemaInline. Backend is never asked.
	SchemaSourceConfig SchemaSource = "config"
	// SchemaSourceConfigJSONSchema reads a plain JSON Schema from
	// APIDefinition.SchemaInline and synthesizes full Kubernetes
	// OpenAPI around it (ObjectMeta, List wrapper, GVK extension).
	// Backend never touches Kubernetes-specific schema knowledge.
	SchemaSourceConfigJSONSchema SchemaSource = "config-jsonschema"
)

// WatchCapability enumerates what a backend can offer for watch.
// The middleware picks based on what's advertised; backends that
// only want to implement polling set WatchCapabilityPollOnly and
// never get Watch RPC calls.
type WatchCapability string

const (
	// WatchCapabilityPollOnly means the middleware runs a periodic
	// List poll; backend doesn't implement Watch.
	WatchCapabilityPollOnly WatchCapability = "poll"
	// WatchCapabilityPushed means the backend implements a streaming
	// Watch and the middleware forwards.
	WatchCapabilityPushed WatchCapability = "push"
	// WatchCapabilityBoth advertises push preferred with poll
	// fallback.
	WatchCapabilityBoth WatchCapability = "both"
)

// TableColumn mirrors metav1.TableColumnDefinition. RowField is a
// JSONPath-ish reference the middleware extracts per row.
type TableColumn struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Format      string `json:"format,omitempty"`
	Description string `json:"description,omitempty"`
	Priority    int32  `json:"priority,omitempty"`
	RowField    string `json:"rowField"`
}

// AdmissionConfig is declarative admission. CEL rules are evaluated
// by the middleware before the backend sees the request. 0029.
type AdmissionConfig struct {
	Validations []ValidationRule `json:"validations,omitempty"`
	Mutations   []MutationRule   `json:"mutations,omitempty"`
}

// ValidationRule is a CEL expression that evaluates over the request
// object. Expression returning false rejects with Message.
type ValidationRule struct {
	Name       string `json:"name"`
	Expression string `json:"expression"`
	Message    string `json:"message"`
}

// MutationRule is a JSONPath-addressed default-value rule the
// middleware applies when the field is missing. CEL can come later.
type MutationRule struct {
	Name  string      `json:"name"`
	Path  string      `json:"path"`
	Value interface{} `json:"value"`
}

// -----------------------------------------------------------------
// Backend contract (what a backend author implements)
// -----------------------------------------------------------------

// Backend is the business-data surface. A backend author in any
// language implements this. It does NOT handle:
//
//   - ObjectMeta bookkeeping (managedFields, finalizers,
//     ownerReferences, labels, annotations) — middleware handles.
//   - resourceVersion monotonicity — middleware handles.
//   - OpenAPI schema plumbing — middleware handles.
//   - SSA field management — middleware handles.
//   - Watch bookmarks — middleware handles.
//
// The backend sees and returns only the spec+status portion of the
// object (or the whole object if it wishes; the middleware will
// strip metadata it manages before forwarding).
//
// This interface is a Go sketch. The wire encoding (gRPC vs
// HTTP/JSON) is transport-adjacent; both forms carry the same
// logical shape. See runtime/component/proto/backend.proto for the
// concrete gRPC and runtime/component/v2 (0030) for the HTTP layer.
type Backend interface {
	// Get returns a single object's business data by namespace+name.
	// Return ErrNotFound on miss.
	Get(ctx context.Context, req GetRequest) (GetResponse, error)

	// List returns the business data for all objects matching the
	// selector.
	List(ctx context.Context, req ListRequest) (ListResponse, error)

	// Create stores a new object. The middleware will have already
	// assigned UID and persisted metadata; the backend just writes
	// the spec/status.
	Create(ctx context.Context, req CreateRequest) (CreateResponse, error)

	// Update replaces an object's spec.
	Update(ctx context.Context, req UpdateRequest) (UpdateResponse, error)

	// Delete removes an object.
	Delete(ctx context.Context, req DeleteRequest) (DeleteResponse, error)

	// Watch streams change events to the middleware. Only called if
	// the backend advertised WatchCapabilityPushed or
	// WatchCapabilityBoth.
	Watch(ctx context.Context, req WatchRequest, sink EventSink) error
}

// UserInfo is the forwarded aggregation-layer identity.
type UserInfo struct {
	Name   string
	UID    string
	Groups []string
	Extra  map[string][]string
}

// GetRequest ... through WatchRequest: simple envelopes.

type GetRequest struct {
	User      UserInfo
	Namespace string
	Name      string
}

type GetResponse struct {
	// ObjectJSON is the backend's view of the object (typically
	// {apiVersion, kind, metadata.name, spec, status}). The
	// middleware will overlay its own metadata before responding to
	// the client.
	ObjectJSON []byte
}

type ListRequest struct {
	User          UserInfo
	Namespace     string
	LabelSelector string
}

type ListResponse struct {
	ItemsJSON [][]byte
}

type CreateRequest struct {
	User         UserInfo
	Namespace    string
	ObjectJSON   []byte
	FieldManager string
}

type CreateResponse struct {
	ObjectJSON []byte
}

type UpdateRequest struct {
	User             UserInfo
	Namespace        string
	Name             string
	ObjectJSON       []byte
	ForceAllowCreate bool
	FieldManager     string
}

type UpdateResponse struct {
	ObjectJSON []byte
	Created    bool
}

type DeleteRequest struct {
	User      UserInfo
	Namespace string
	Name      string
}

type DeleteResponse struct {
	ObjectJSON []byte
	Deleted    bool
}

type WatchRequest struct {
	User            UserInfo
	Namespace       string
	LabelSelector   string
	ResourceVersion string
}

// EventType mirrors watch.EventType.
type EventType string

const (
	EventAdded    EventType = "ADDED"
	EventModified EventType = "MODIFIED"
	EventDeleted  EventType = "DELETED"
)

// WatchEvent is what the backend pushes.
type WatchEvent struct {
	Type       EventType
	ObjectJSON []byte
}

// EventSink receives WatchEvents from the backend. The middleware
// implements this; the backend calls Send.
type EventSink interface {
	Send(WatchEvent) error
	Done() <-chan struct{}
}

// -----------------------------------------------------------------
// Metadata store (middleware's job)
// -----------------------------------------------------------------

// MetadataStore is the middleware's persistence layer for KRM
// metadata. Every Get the middleware serves reads
// {backend spec+status} and overlays {MetadataStore Record}. Writes
// go to both; admission rules may adjust metadata before write.
//
// The canonical implementation (0024) is backed by a shared
// host-cluster CRD aggexpmetadata.aggexp.io/v1 ResourceMetadata.
// A memory-backed implementation exists for tests.
type MetadataStore interface {
	// Get returns the recorded metadata for a resource, or
	// (nil, nil) if none exists yet.
	Get(ctx context.Context, ref ResourceRef) (*Record, error)
	// Put stores or updates metadata. resourceVersion monotonicity
	// is the store's responsibility.
	Put(ctx context.Context, ref ResourceRef, record Record) (Record, error)
	// Delete removes a metadata entry. Idempotent.
	Delete(ctx context.Context, ref ResourceRef) error
	// List enumerates metadata for reconciliation / GC.
	List(ctx context.Context, filter ListFilter) ([]Record, error)
}

// ResourceRef identifies a resource instance the metadata applies to.
type ResourceRef struct {
	Group     string
	Resource  string
	Namespace string
	Name      string
}

// Record is the KRM-metadata payload the store persists. Matches
// what the middleware overlays onto backend responses.
type Record struct {
	Ref             ResourceRef
	UID             string
	ResourceVersion string
	CreatedAt       time.Time

	Labels      map[string]string
	Annotations map[string]string

	// ManagedFields is the library's SSA field-ownership record.
	// Encoded as raw JSON bytes to avoid depending on metav1 types
	// in this interface sketch.
	ManagedFields []byte

	// Finalizers are ordered strings; deletion is blocked while
	// non-empty.
	Finalizers []string
	// OwnerReferences is raw JSON of metav1.OwnerReference[].
	OwnerReferences []byte

	// DeletionTimestamp, if set, indicates the object is pending
	// finalization.
	DeletionTimestamp *time.Time
}

// ListFilter scopes MetadataStore.List calls for the reconciler /
// garbage collector.
type ListFilter struct {
	Group    string
	Resource string
}

// -----------------------------------------------------------------
// Multiplex middleware server (0027)
// -----------------------------------------------------------------

// Multiplex is the substrate-level type that hosts many AAs in one
// process. Runtime concerns (reconciliation, APIService lifecycle,
// per-AA status) live inside this type. 0027 implements it; 0030
// promotes.
type Multiplex interface {
	// Register adds or updates an AA by APIDefinition. Idempotent.
	Register(ctx context.Context, def APIDefinition) error
	// Unregister removes an AA. Drains in-flight requests.
	Unregister(ctx context.Context, group, resource string) error
	// Status reports the current per-AA status.
	Status(ctx context.Context, group, resource string) (AAStatus, error)
}

// AAStatus is what 0027 writes back into APIDefinition.status.
type AAStatus struct {
	Phase              string     // "Ready" | "Provisioning" | "Failed"
	ObservedGeneration int64
	Conditions         []Condition
}

// Condition is a simplified metav1.Condition.
type Condition struct {
	Type    string
	Status  string // True | False | Unknown
	Reason  string
	Message string
	LastTransitionTime time.Time
}
