// Command note-backend is the Track B reference backend for
// experiment 0023. It serves the notes.aggexp.io/v1 Note resource
// over the runtime/component/proto Backend gRPC service and ships a
// minimal JSON Schema — just spec + status — from GetSchema. It
// does NOT know about x-kubernetes-group-version-kind, ObjectMeta,
// apiVersion, kind, or the List wrapper. The middleware synthesizes
// those.
//
// The point of Track B: the backend author should be able to write
// a Kubernetes API without learning any Kubernetes-specific OpenAPI
// dialect.
package main

import (
	"context"
	"encoding/json"
	"flag"
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

	componentpb "github.com/cheeseandcereal/aggexp/runtime/component/proto"
)

type Note struct {
	APIVersion string     `json:"apiVersion"`
	Kind       string     `json:"kind"`
	Metadata   Meta       `json:"metadata"`
	Spec       NoteSpec   `json:"spec,omitempty"`
	Status     NoteStatus `json:"status,omitempty"`
}

type Meta struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace,omitempty"`
	UID               string            `json:"uid,omitempty"`
	ResourceVersion   string            `json:"resourceVersion,omitempty"`
	CreationTimestamp string            `json:"creationTimestamp,omitempty"`
	ManagedFields     json.RawMessage   `json:"managedFields,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
}

type NoteSpec struct {
	Title string `json:"title,omitempty"`
	Body  string `json:"body,omitempty"`
}

type NoteStatus struct {
	UpdatedAt string `json:"updatedAt,omitempty"`
}

type key struct{ ns, name string }

type backend struct {
	componentpb.UnimplementedBackendServer

	mu      sync.Mutex
	items   map[key]*Note
	watches map[int]chan *componentpb.WatchEvent
	nextWID int
}

func newBackend() *backend {
	return &backend{
		items:   map[key]*Note{},
		watches: map[int]chan *componentpb.WatchEvent{},
	}
}

// GetSchema — Track B. The backend ships ONLY a plain JSON Schema
// describing its business data (spec + status). It does not include:
//   - x-kubernetes-group-version-kind extension
//   - apiVersion / kind properties
//   - metadata ($ref to ObjectMeta)
//   - a List wrapper type
//
// The middleware's synthesis package adds all of that.
//
// Compare to Track A's GetSchema in the sister directory: the
// properties below match Track A's spec/status sub-schemas exactly,
// but Track A wraps them in the Kubernetes dialect.
func (b *backend) GetSchema(_ context.Context, _ *componentpb.GetSchemaRequest) (*componentpb.GetSchemaResponse, error) {
	plainJSONSchema := map[string]any{
		"type":        "object",
		"description": "A Note is a piece of titled text.",
		"properties": map[string]any{
			"spec": map[string]any{
				"type":        "object",
				"description": "Caller-supplied fields.",
				"required":    []any{"title"},
				"properties": map[string]any{
					"title": map[string]any{
						"type":        "string",
						"minLength":   3,
						"maxLength":   64,
						"description": "Short display title. Required, 3-64 characters."},
					"body": map[string]any{
						"type":        "string",
						"description": "Free-form body text. Optional."},
				},
			},
			"status": map[string]any{
				"type":        "object",
				"description": "Server-assigned fields.",
				"properties": map[string]any{
					"updatedAt": map[string]any{
						"type":        "string",
						"format":      "date-time",
						"description": "Server-assigned last-update time (RFC 3339)."},
				},
			},
		},
	}
	raw, err := json.Marshal(plainJSONSchema)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal schema: %v", err)
	}
	return &componentpb.GetSchemaResponse{
		Group:                   "aggexp.io",
		Version:                 "v1",
		Resource:                "notes",
		Kind:                    "Note",
		Singular:                "note",
		Namespaced:              true,
		Writable:                true,
		SupportsServerSideApply: true,
		OpenapiV3:               raw, // proto field is named OpenapiV3 but we ship JSON Schema
		Columns: []*componentpb.TableColumn{
			{Name: "Name", Type: "string", Description: "Name of the note."},
			{Name: "Title", Type: "string", Description: "Note title."},
			{Name: "Age", Type: "string", Description: "Time since creation."},
		},
		RowFields:  []string{".metadata.name", ".spec.title", ".metadata.creationTimestamp"},
		ShortNames: []string{"nt"},
	}, nil
}

// CRUD implementation below is a verbatim copy of Track A's: the
// only difference between tracks is in GetSchema.

func (b *backend) Get(_ context.Context, req *componentpb.GetRequest) (*componentpb.GetResponse, error) {
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
	return &componentpb.GetResponse{ObjectJson: raw}, nil
}

func (b *backend) List(_ context.Context, req *componentpb.ListRequest) (*componentpb.ListResponse, error) {
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
	return &componentpb.ListResponse{ItemsJson: out}, nil
}

func (b *backend) Create(_ context.Context, req *componentpb.CreateRequest) (*componentpb.CreateResponse, error) {
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

	raw, _ := json.Marshal(&n)
	b.broadcastLocked(componentpb.EventType_EVENT_ADDED, raw)
	log.Printf("create by user=%s fm=%q name=%s", userLabel(req.User), req.FieldManager, n.Metadata.Name)
	return &componentpb.CreateResponse{ObjectJson: raw}, nil
}

func (b *backend) Update(_ context.Context, req *componentpb.UpdateRequest) (*componentpb.UpdateResponse, error) {
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
		raw, _ := json.Marshal(&n)
		b.broadcastLocked(componentpb.EventType_EVENT_ADDED, raw)
		return &componentpb.UpdateResponse{ObjectJson: raw, Created: true}, nil
	}
	n.Metadata.UID = existing.Metadata.UID
	n.Metadata.CreationTimestamp = existing.Metadata.CreationTimestamp
	n.Status.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	b.items[k] = &n
	raw, _ := json.Marshal(&n)
	b.broadcastLocked(componentpb.EventType_EVENT_MODIFIED, raw)
	return &componentpb.UpdateResponse{ObjectJson: raw, Created: false}, nil
}

func (b *backend) Delete(_ context.Context, req *componentpb.DeleteRequest) (*componentpb.DeleteResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	k := key{req.Namespace, req.Name}
	existing, ok := b.items[k]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "notes.aggexp.io %q not found", req.Name)
	}
	delete(b.items, k)
	raw, _ := json.Marshal(existing)
	b.broadcastLocked(componentpb.EventType_EVENT_DELETED, raw)
	return &componentpb.DeleteResponse{ObjectJson: raw, Deleted: true}, nil
}

func (b *backend) broadcastLocked(t componentpb.EventType, raw []byte) {
	for _, ch := range b.watches {
		select {
		case ch <- &componentpb.WatchEvent{Type: t, ObjectJson: raw}:
		default:
		}
	}
}

func (b *backend) Watch(req *componentpb.WatchRequest, stream grpc.ServerStreamingServer[componentpb.WatchEvent]) error {
	b.mu.Lock()
	wid := b.nextWID
	b.nextWID++
	ch := make(chan *componentpb.WatchEvent, 64)
	b.watches[wid] = ch
	initial := make([]*componentpb.WatchEvent, 0, len(b.items))
	for k, n := range b.items {
		if req.Namespace != "" && k.ns != req.Namespace {
			continue
		}
		raw, err := json.Marshal(n)
		if err != nil {
			continue
		}
		initial = append(initial, &componentpb.WatchEvent{Type: componentpb.EventType_EVENT_ADDED, ObjectJson: raw})
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

func userLabel(u *componentpb.UserInfo) string {
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
	componentpb.RegisterBackendServer(s, newBackend())
	log.Printf("note-backend-0023b (track B) listening on %s", *addr)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
