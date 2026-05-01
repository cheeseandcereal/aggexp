// Command note-backend is the Track A reference backend for
// experiment 0023. It serves the notes.aggexp.io/v1 Note resource
// over the runtime/component/proto Backend gRPC service and ships a
// full Kubernetes-flavored OpenAPI v3 schema from GetSchema.
//
// This track is the baseline: the backend author must know
// Kubernetes OpenAPI conventions (x-kubernetes-group-version-kind,
// ObjectMeta wrapping, apiVersion/kind fields).
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

// GetSchema — Track A. Returns a full Kubernetes-flavored OpenAPI v3
// schema: apiVersion/kind properties, $ref to ObjectMeta,
// x-kubernetes-group-version-kind extension on the top-level object.
// This is what the 0017/0021 protocol expected.
func (b *backend) GetSchema(_ context.Context, _ *componentpb.GetSchemaRequest) (*componentpb.GetSchemaResponse, error) {
	schema := map[string]any{
		"type":        "object",
		"description": "Note is a free-form piece of text served by Track A of experiment 0023.",
		"properties": map[string]any{
			"apiVersion": map[string]any{"type": "string",
				"description": "APIVersion defines the versioned schema of this representation of an object."},
			"kind": map[string]any{"type": "string",
				"description": "Kind is a string value representing the REST resource this object represents."},
			"metadata": map[string]any{
				"description": "Standard object metadata.",
				"$ref":        "#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta",
			},
			"spec": map[string]any{
				"type":        "object",
				"description": "NoteSpec carries the caller-supplied fields.",
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
				"description": "NoteStatus is server-assigned.",
				"properties": map[string]any{
					"updatedAt": map[string]any{
						"type":        "string",
						"format":      "date-time",
						"description": "Server-assigned last-update time (RFC 3339)."},
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
	return &componentpb.GetSchemaResponse{
		Group:                   "aggexp.io",
		Version:                 "v1",
		Resource:                "notes",
		Kind:                    "Note",
		Singular:                "note",
		Namespaced:              true,
		Writable:                true,
		SupportsServerSideApply: true,
		OpenapiV3:               raw,
		Columns: []*componentpb.TableColumn{
			{Name: "Name", Type: "string", Description: "Name of the note."},
			{Name: "Title", Type: "string", Description: "Note title."},
			{Name: "Age", Type: "string", Description: "Time since creation."},
		},
		RowFields:  []string{".metadata.name", ".spec.title", ".metadata.creationTimestamp"},
		ShortNames: []string{"nt"},
	}, nil
}

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
	log.Printf("note-backend-0023a (track A) listening on %s", *addr)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
