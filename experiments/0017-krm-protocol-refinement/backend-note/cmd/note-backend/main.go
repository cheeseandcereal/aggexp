// Command note-backend is the reference backend for experiment 0013.
// It serves a single resource type — Note — via the aggexp KRM
// gRPC protocol. It deliberately does not import k8s.io/apiserver;
// the point is that a thin backend can expose a Kubernetes API
// through the component server without pulling in Kubernetes
// machinery.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	krmv1 "github.com/cheeseandcereal/aggexp/experiments/0017-krm-protocol-refinement/gen/aggexp/krm/v1"
)

// Note is the resource this backend serves. JSON tags line up with
// the OpenAPI schema we report in GetSchema.
type Note struct {
	APIVersion string     `json:"apiVersion"`
	Kind       string     `json:"kind"`
	Metadata   Meta       `json:"metadata"`
	Spec       NoteSpec   `json:"spec,omitempty"`
	Status     NoteStatus `json:"status,omitempty"`
}

type Meta struct {
	Name              string          `json:"name"`
	Namespace         string          `json:"namespace,omitempty"`
	UID               string          `json:"uid,omitempty"`
	ResourceVersion   string          `json:"resourceVersion,omitempty"`
	CreationTimestamp string          `json:"creationTimestamp,omitempty"`
	// ManagedFields is stored as raw JSON so the backend doesn't
	// need a dependency on k8s.io/apimachinery. Server-side apply
	// requires the backend to round-trip this field.
	ManagedFields json.RawMessage `json:"managedFields,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
	Annotations   map[string]string `json:"annotations,omitempty"`
}

type NoteSpec struct {
	Title string `json:"title,omitempty"`
	Body  string `json:"body,omitempty"`
}

type NoteStatus struct {
	UpdatedAt string `json:"updatedAt,omitempty"`
}

// key is (namespace, name).
type key struct {
	ns, name string
}

// backend is an in-memory store for Notes plus a set of watchers.
type backend struct {
	krmv1.UnimplementedBackendServer

	mu      sync.Mutex
	items   map[key]*Note
	watches map[int]chan *krmv1.WatchEvent
	nextWID int
}

func newBackend() *backend {
	return &backend{
		items:   map[key]*Note{},
		watches: map[int]chan *krmv1.WatchEvent{},
	}
}

func (b *backend) GetSchema(_ context.Context, _ *krmv1.GetSchemaRequest) (*krmv1.GetSchemaResponse, error) {
	// OpenAPI v3 schema for a Note. The component server keys this
	// at the canonical name for *unstructured.Unstructured and the
	// library's managedfields / explain paths index it by the
	// x-kubernetes-group-version-kind extension. 0017 relies on
	// this extension to exist; the component server also stamps
	// it defensively if the backend forgets.
	schema := map[string]any{
		"type":        "object",
		"description": "Note is a free-form piece of text served by the 0017 KRM refinement experiment.",
		"properties": map[string]any{
			"apiVersion": map[string]any{
				"type":        "string",
				"description": "APIVersion defines the versioned schema of this representation of an object.",
			},
			"kind": map[string]any{
				"type":        "string",
				"description": "Kind is a string value representing the REST resource this object represents.",
			},
			"metadata": map[string]any{
				"description": "Standard object metadata.",
				"$ref":        "#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta",
			},
			"spec": map[string]any{
				"type":        "object",
				"description": "NoteSpec carries the caller-supplied fields. Writable; participates in server-side apply.",
				"properties": map[string]any{
					"title": map[string]any{
						"type":        "string",
						"description": "Short display title. Rendered in the Title column of `kubectl get notes`.",
					},
					"body": map[string]any{
						"type":        "string",
						"description": "Free-form body text. Not rendered by kubectl get.",
					},
				},
			},
			"status": map[string]any{
				"type":        "object",
				"description": "NoteStatus is server-assigned. Read-only to clients other than the backend itself.",
				"properties": map[string]any{
					"updatedAt": map[string]any{
						"type":        "string",
						"description": "Server-assigned last-update time (RFC 3339).",
					},
				},
			},
		},
		"x-kubernetes-group-version-kind": []map[string]any{
			{"group": "aggexp.io", "version": "v1", "kind": "Note"},
		},
	}
	raw, err := json.Marshal(schema)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal schema: %v", err)
	}
	return &krmv1.GetSchemaResponse{
		Group:                    "aggexp.io",
		Version:                  "v1",
		Resource:                 "notes",
		Kind:                     "Note",
		Singular:                 "note",
		Namespaced:               true,
		Writable:                 true,
		SupportsServerSideApply:  true,
		OpenapiV3:                raw,
		Columns: []*krmv1.TableColumn{
			{Name: "Name", Type: "string", Description: "Name of the note."},
			{Name: "Title", Type: "string", Description: "Note title."},
			{Name: "Age", Type: "string", Description: "Time since creation."},
		},
		RowFields:  []string{".metadata.name", ".spec.title", ".metadata.creationTimestamp"},
		ShortNames: []string{"nt"},
	}, nil
}

func (b *backend) Get(_ context.Context, req *krmv1.GetRequest) (*krmv1.GetResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, ok := b.items[key{req.Namespace, req.Name}]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "notes.aggexp.io %q not found", req.Name)
	}
	raw, err := json.Marshal(n)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal note: %v", err)
	}
	return &krmv1.GetResponse{ObjectJson: raw}, nil
}

func (b *backend) List(_ context.Context, req *krmv1.ListRequest) (*krmv1.ListResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	keys := make([]key, 0, len(b.items))
	for k := range b.items {
		if req.Namespace != "" && k.ns != req.Namespace {
			continue
		}
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].ns != keys[j].ns {
			return keys[i].ns < keys[j].ns
		}
		return keys[i].name < keys[j].name
	})
	out := make([][]byte, 0, len(keys))
	for _, k := range keys {
		raw, err := json.Marshal(b.items[k])
		if err != nil {
			return nil, status.Errorf(codes.Internal, "marshal note: %v", err)
		}
		out = append(out, raw)
	}
	return &krmv1.ListResponse{ItemsJson: out}, nil
}

func (b *backend) Create(_ context.Context, req *krmv1.CreateRequest) (*krmv1.CreateResponse, error) {
	var n Note
	if err := json.Unmarshal(req.ObjectJson, &n); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal object: %v", err)
	}
	if n.Metadata.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "metadata.name is required")
	}
	if req.Namespace != "" {
		n.Metadata.Namespace = req.Namespace
	}
	n.APIVersion = "aggexp.io/v1"
	n.Kind = "Note"

	b.mu.Lock()
	defer b.mu.Unlock()
	k := key{n.Metadata.Namespace, n.Metadata.Name}
	if _, exists := b.items[k]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "notes.aggexp.io %q already exists", n.Metadata.Name)
	}
	n.Metadata.UID = uuid.NewString()
	n.Metadata.CreationTimestamp = time.Now().UTC().Format(time.RFC3339)
	n.Status.UpdatedAt = n.Metadata.CreationTimestamp
	b.items[k] = &n

	raw, err := json.Marshal(&n)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal note: %v", err)
	}
	b.broadcastLocked(krmv1.EventType_EVENT_ADDED, raw)
	return &krmv1.CreateResponse{ObjectJson: raw}, nil
}

func (b *backend) Update(_ context.Context, req *krmv1.UpdateRequest) (*krmv1.UpdateResponse, error) {
	var n Note
	if err := json.Unmarshal(req.ObjectJson, &n); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal object: %v", err)
	}
	if req.Namespace != "" {
		n.Metadata.Namespace = req.Namespace
	}
	if n.Metadata.Name == "" {
		n.Metadata.Name = req.Name
	}
	n.APIVersion = "aggexp.io/v1"
	n.Kind = "Note"

	b.mu.Lock()
	defer b.mu.Unlock()
	k := key{n.Metadata.Namespace, req.Name}
	existing, ok := b.items[k]
	if !ok {
		if !req.ForceAllowCreate {
			return nil, status.Errorf(codes.NotFound, "notes.aggexp.io %q not found", req.Name)
		}
		n.Metadata.UID = uuid.NewString()
		n.Metadata.CreationTimestamp = time.Now().UTC().Format(time.RFC3339)
		n.Status.UpdatedAt = n.Metadata.CreationTimestamp
		b.items[k] = &n
		raw, err := json.Marshal(&n)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "marshal note: %v", err)
		}
		b.broadcastLocked(krmv1.EventType_EVENT_ADDED, raw)
		return &krmv1.UpdateResponse{ObjectJson: raw, Created: true}, nil
	}
	n.Metadata.UID = existing.Metadata.UID
	n.Metadata.CreationTimestamp = existing.Metadata.CreationTimestamp
	n.Status.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	b.items[k] = &n
	raw, err := json.Marshal(&n)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal note: %v", err)
	}
	b.broadcastLocked(krmv1.EventType_EVENT_MODIFIED, raw)
	return &krmv1.UpdateResponse{ObjectJson: raw, Created: false}, nil
}

func (b *backend) Delete(_ context.Context, req *krmv1.DeleteRequest) (*krmv1.DeleteResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	k := key{req.Namespace, req.Name}
	existing, ok := b.items[k]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "notes.aggexp.io %q not found", req.Name)
	}
	delete(b.items, k)
	raw, err := json.Marshal(existing)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal note: %v", err)
	}
	b.broadcastLocked(krmv1.EventType_EVENT_DELETED, raw)
	return &krmv1.DeleteResponse{ObjectJson: raw, Deleted: true}, nil
}

// Apply is the server-side apply hook. The component server hands us
// the merged object it already computed; we persist it with the same
// semantics as Update. A more sophisticated backend might inspect
// req.FieldManager and record ownership; this reference backend
// treats Apply as Update with force_allow_create=true.
func (b *backend) Apply(_ context.Context, req *krmv1.ApplyRequest) (*krmv1.ApplyResponse, error) {
	var n Note
	if err := json.Unmarshal(req.ObjectJson, &n); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal object: %v", err)
	}
	if req.Namespace != "" {
		n.Metadata.Namespace = req.Namespace
	}
	if n.Metadata.Name == "" {
		n.Metadata.Name = req.Name
	}
	n.APIVersion = "aggexp.io/v1"
	n.Kind = "Note"

	b.mu.Lock()
	defer b.mu.Unlock()
	k := key{n.Metadata.Namespace, req.Name}
	existing, ok := b.items[k]
	created := false
	if !ok {
		n.Metadata.UID = uuid.NewString()
		n.Metadata.CreationTimestamp = time.Now().UTC().Format(time.RFC3339)
		created = true
	} else {
		n.Metadata.UID = existing.Metadata.UID
		n.Metadata.CreationTimestamp = existing.Metadata.CreationTimestamp
	}
	n.Status.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	b.items[k] = &n

	raw, err := json.Marshal(&n)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal note: %v", err)
	}
	evType := krmv1.EventType_EVENT_MODIFIED
	if created {
		evType = krmv1.EventType_EVENT_ADDED
	}
	b.broadcastLocked(evType, raw)
	log.Printf("apply by fm=%q user=%s name=%s created=%v",
		req.FieldManager, userLabel(req.User), n.Metadata.Name, created)
	return &krmv1.ApplyResponse{ObjectJson: raw, Created: created}, nil
}

// broadcastLocked pushes an event to all active watchers. Caller
// must hold b.mu.
func (b *backend) broadcastLocked(t krmv1.EventType, raw []byte) {
	for _, ch := range b.watches {
		select {
		case ch <- &krmv1.WatchEvent{Type: t, ObjectJson: raw}:
		default:
			// Drop on full channel — skeleton-grade. A production
			// backend would probably close the watcher instead.
		}
	}
}

// Watch: the component server opens this stream at startup. We push
// an initial ADDED for every existing object, then live events.
func (b *backend) Watch(req *krmv1.WatchRequest, stream krmv1.Backend_WatchServer) error {
	b.mu.Lock()
	wid := b.nextWID
	b.nextWID++
	ch := make(chan *krmv1.WatchEvent, 64)
	b.watches[wid] = ch
	// Seed with current state (namespace-scoped optionally).
	initial := make([]*krmv1.WatchEvent, 0, len(b.items))
	for k, n := range b.items {
		if req.Namespace != "" && k.ns != req.Namespace {
			continue
		}
		raw, err := json.Marshal(n)
		if err != nil {
			continue
		}
		initial = append(initial, &krmv1.WatchEvent{Type: krmv1.EventType_EVENT_ADDED, ObjectJson: raw})
	}
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		delete(b.watches, wid)
		close(ch)
		b.mu.Unlock()
	}()

	log.Printf("watch open wid=%d user=%s ns=%q", wid, userLabel(req.User), req.Namespace)

	for _, ev := range initial {
		if err := stream.Send(ev); err != nil {
			return err
		}
	}

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			log.Printf("watch close wid=%d: %v", wid, ctx.Err())
			return nil
		case ev, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(ev); err != nil {
				return err
			}
		}
	}
}

func userLabel(u *krmv1.UserInfo) string {
	if u == nil {
		return "<nil>"
	}
	if u.Name == "" {
		return "<anon>"
	}
	return u.Name + "[" + strings.Join(u.Groups, ",") + "]"
}

func main() {
	addr := flag.String("addr", ":9090", "gRPC listen address")
	flag.Parse()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	krmv1.RegisterBackendServer(s, newBackend())
	log.Printf("note-backend listening on %s", *addr)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
	_ = fmt.Sprintf
}
