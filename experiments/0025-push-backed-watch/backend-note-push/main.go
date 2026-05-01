// Command note-backend-push is the push-variant backend for
// experiment 0025. It implements the runtime/component/proto Backend
// gRPC service for notes.aggexp.io/v1 Notes with a real streaming
// Watch: a single broadcaster fans all change events (from kubectl
// writes and from the internal event generator) to all connected
// watchers.
//
// An internal event generator mutates state on a fixed schedule so
// the scenario scripts can correlate backend-side log timestamps
// with kubectl-side observed timestamps without random noise.
//
// The generator is enabled when --enable-event-generator=true (the
// default); the fixed schedule is:
//
//	t+3s  : CREATE  note "genrunner-N" (N counts up per generator cycle)
//	t+10s : UPDATE  genrunner-N     (body bump #1)
//	t+16s : UPDATE  genrunner-N     (body bump #2)
//	t+22s : DELETE  genrunner-N
//
// After each full cycle the generator sleeps for 30s and starts over
// with N+1. Each mutation logs a line of the form
// "gen t=... op=... name=... body=...".
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
	// rvCounter drives resourceVersion. The backend is the
	// authority — the middleware will pass these through.
	rvCounter uint64
}

func newBackend() *backend {
	return &backend{
		items:     map[key]*Note{},
		watches:   map[int]chan *componentpb.WatchEvent{},
		rvCounter: 1,
	}
}

func (b *backend) nextRV() string {
	b.rvCounter++
	return fmt.Sprintf("%d", b.rvCounter)
}

func (b *backend) GetSchema(_ context.Context, _ *componentpb.GetSchemaRequest) (*componentpb.GetSchemaResponse, error) {
	plain := map[string]any{
		"type":        "object",
		"description": "A Note served by the PUSH variant of 0025.",
		"properties": map[string]any{
			"spec": map[string]any{
				"type":     "object",
				"required": []any{"title"},
				"properties": map[string]any{
					"title": map[string]any{"type": "string", "minLength": 3, "maxLength": 64},
					"body":  map[string]any{"type": "string"},
				},
			},
			"status": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"updatedAt": map[string]any{"type": "string", "format": "date-time"},
				},
			},
		},
	}
	raw, err := json.Marshal(plain)
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
			{Name: "Name", Type: "string"},
			{Name: "Title", Type: "string"},
			{Name: "Body", Type: "string"},
			{Name: "Age", Type: "string"},
		},
		RowFields:  []string{".metadata.name", ".spec.title", ".spec.body", ".metadata.creationTimestamp"},
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
	raw, _ := json.Marshal(n)
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
		raw, _ := json.Marshal(b.items[k])
		out = append(out, raw)
	}
	return &componentpb.ListResponse{ItemsJson: out}, nil
}

// applyWriteLocked is the shared code path for Create/Update/Delete
// that broadcasts a WatchEvent after mutating state. mu must be held.
func (b *backend) broadcastLocked(t componentpb.EventType, raw []byte) {
	for _, ch := range b.watches {
		select {
		case ch <- &componentpb.WatchEvent{Type: t, ObjectJson: raw}:
		default:
			// Watcher is slow; drop. A production server would
			// bound the queue or close the slow watcher.
		}
	}
}

func (b *backend) Create(_ context.Context, req *componentpb.CreateRequest) (*componentpb.CreateResponse, error) {
	var n Note
	if err := json.Unmarshal(req.ObjectJson, &n); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal: %v", err)
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
	n.Metadata.ResourceVersion = b.nextRV()
	n.Status.UpdatedAt = n.Metadata.CreationTimestamp
	b.items[k] = &n
	raw, _ := json.Marshal(&n)
	log.Printf("create name=%s user=%s fm=%q rv=%s", n.Metadata.Name, userLabel(req.User), req.FieldManager, n.Metadata.ResourceVersion)
	b.broadcastLocked(componentpb.EventType_EVENT_ADDED, raw)
	return &componentpb.CreateResponse{ObjectJson: raw}, nil
}

func (b *backend) Update(_ context.Context, req *componentpb.UpdateRequest) (*componentpb.UpdateResponse, error) {
	var n Note
	if err := json.Unmarshal(req.ObjectJson, &n); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal: %v", err)
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
		n.Metadata.ResourceVersion = b.nextRV()
		n.Status.UpdatedAt = n.Metadata.CreationTimestamp
		b.items[k] = &n
		raw, _ := json.Marshal(&n)
		b.broadcastLocked(componentpb.EventType_EVENT_ADDED, raw)
		return &componentpb.UpdateResponse{ObjectJson: raw, Created: true}, nil
	}
	n.Metadata.UID = existing.Metadata.UID
	n.Metadata.CreationTimestamp = existing.Metadata.CreationTimestamp
	n.Metadata.ResourceVersion = b.nextRV()
	n.Status.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	b.items[k] = &n
	raw, _ := json.Marshal(&n)
	log.Printf("update name=%s rv=%s", req.Name, n.Metadata.ResourceVersion)
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
	existing.Metadata.ResourceVersion = b.nextRV()
	raw, _ := json.Marshal(existing)
	log.Printf("delete name=%s rv=%s", req.Name, existing.Metadata.ResourceVersion)
	b.broadcastLocked(componentpb.EventType_EVENT_DELETED, raw)
	return &componentpb.DeleteResponse{ObjectJson: raw, Deleted: true}, nil
}

// Watch opens a server-streaming RPC. The backend sends an ADDED
// event for each existing item in scope, then a BOOKMARK event with
// the `k8s.io/initial-events-end` annotation on its metadata, then
// relays all live changes from the broadcaster.
//
// The BOOKMARK is the backend's attempt to close the gap 0011
// surfaced: kubectl wait --for=jsonpath and WatchList-aware informers
// require this event to consider the watch synced.
func (b *backend) Watch(req *componentpb.WatchRequest, stream grpc.ServerStreamingServer[componentpb.WatchEvent]) error {
	b.mu.Lock()
	wid := b.nextWID
	b.nextWID++
	ch := make(chan *componentpb.WatchEvent, 256)
	b.watches[wid] = ch
	// Snapshot the initial state while we hold the lock.
	initial := make([]*componentpb.WatchEvent, 0, len(b.items))
	for k, n := range b.items {
		if req.Namespace != "" && k.ns != req.Namespace {
			continue
		}
		raw, _ := json.Marshal(n)
		initial = append(initial, &componentpb.WatchEvent{Type: componentpb.EventType_EVENT_ADDED, ObjectJson: raw})
	}
	// The bookmark object carries only metadata per Kubernetes
	// convention. The annotation is the signal the library expects.
	bookmarkRV := b.rvCounter
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		delete(b.watches, wid)
		close(ch)
		b.mu.Unlock()
	}()

	log.Printf("watch open wid=%d user=%s ns=%q initial=%d", wid, userLabel(req.User), req.Namespace, len(initial))

	for _, ev := range initial {
		if err := stream.Send(ev); err != nil {
			return err
		}
	}
	// Emit initial-events-end BOOKMARK. The object is a minimal
	// Note with just metadata populated — its resourceVersion is
	// the authoritative RV as of the snapshot above, and its
	// annotations carry k8s.io/initial-events-end=true.
	bookmark := Note{
		APIVersion: "aggexp.io/v1",
		Kind:       "Note",
		Metadata: Meta{
			ResourceVersion: fmt.Sprintf("%d", bookmarkRV),
			Annotations:     map[string]string{"k8s.io/initial-events-end": "true"},
		},
	}
	braw, _ := json.Marshal(&bookmark)
	if err := stream.Send(&componentpb.WatchEvent{Type: componentpb.EventType_EVENT_BOOKMARK, ObjectJson: braw}); err != nil {
		return err
	}
	log.Printf("watch wid=%d sent initial-events-end BOOKMARK rv=%d", wid, bookmarkRV)

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

// eventGenerator runs a deterministic mutation schedule. Logging is
// tagged so scenario scripts can correlate backend-side events with
// kubectl-side observations.
func (b *backend) eventGenerator(ctx context.Context, namespace string) {
	cycle := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		cycle++
		name := fmt.Sprintf("genrunner-%d", cycle)

		// t+3s : CREATE
		if !sleep(ctx, 3*time.Second) {
			return
		}
		n := &Note{
			APIVersion: "aggexp.io/v1",
			Kind:       "Note",
			Metadata: Meta{
				Name:              name,
				Namespace:         namespace,
				UID:               uuid.NewString(),
				CreationTimestamp: time.Now().UTC().Format(time.RFC3339),
				Labels:            map[string]string{"source": "generator"},
			},
			Spec:   NoteSpec{Title: "gen", Body: "v1"},
			Status: NoteStatus{UpdatedAt: time.Now().UTC().Format(time.RFC3339)},
		}
		b.mu.Lock()
		n.Metadata.ResourceVersion = b.nextRV()
		b.items[key{namespace, name}] = n
		raw, _ := json.Marshal(n)
		b.broadcastLocked(componentpb.EventType_EVENT_ADDED, raw)
		b.mu.Unlock()
		log.Printf("gen t=%s op=CREATE name=%s body=v1 cycle=%d rv=%s",
			time.Now().UTC().Format(time.RFC3339Nano), name, cycle, n.Metadata.ResourceVersion)

		// t+7s from create -> t+10s wall: UPDATE #1
		if !sleep(ctx, 7*time.Second) {
			return
		}
		b.mu.Lock()
		if cur, ok := b.items[key{namespace, name}]; ok {
			cur.Spec.Body = "v2"
			cur.Status.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
			cur.Metadata.ResourceVersion = b.nextRV()
			raw, _ := json.Marshal(cur)
			b.broadcastLocked(componentpb.EventType_EVENT_MODIFIED, raw)
			log.Printf("gen t=%s op=UPDATE name=%s body=v2 cycle=%d rv=%s",
				time.Now().UTC().Format(time.RFC3339Nano), name, cycle, cur.Metadata.ResourceVersion)
		}
		b.mu.Unlock()

		// t+6s -> t+16s wall: UPDATE #2
		if !sleep(ctx, 6*time.Second) {
			return
		}
		b.mu.Lock()
		if cur, ok := b.items[key{namespace, name}]; ok {
			cur.Spec.Body = "v3"
			cur.Status.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
			cur.Metadata.ResourceVersion = b.nextRV()
			raw, _ := json.Marshal(cur)
			b.broadcastLocked(componentpb.EventType_EVENT_MODIFIED, raw)
			log.Printf("gen t=%s op=UPDATE name=%s body=v3 cycle=%d rv=%s",
				time.Now().UTC().Format(time.RFC3339Nano), name, cycle, cur.Metadata.ResourceVersion)
		}
		b.mu.Unlock()

		// t+6s -> t+22s wall: DELETE
		if !sleep(ctx, 6*time.Second) {
			return
		}
		b.mu.Lock()
		if cur, ok := b.items[key{namespace, name}]; ok {
			delete(b.items, key{namespace, name})
			cur.Metadata.ResourceVersion = b.nextRV()
			raw, _ := json.Marshal(cur)
			b.broadcastLocked(componentpb.EventType_EVENT_DELETED, raw)
			log.Printf("gen t=%s op=DELETE name=%s cycle=%d rv=%s",
				time.Now().UTC().Format(time.RFC3339Nano), name, cycle, cur.Metadata.ResourceVersion)
		}
		b.mu.Unlock()

		// Quiet 30s before next cycle.
		if !sleep(ctx, 30*time.Second) {
			return
		}
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
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
	enableGen := flag.Bool("enable-event-generator", true, "Run the internal event generator on a fixed schedule.")
	genNS := flag.String("generator-namespace", "default", "Namespace for generator-created Notes.")
	flag.Parse()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	b := newBackend()
	s := grpc.NewServer()
	componentpb.RegisterBackendServer(s, b)
	log.Printf("note-backend-push (0025 variant B) listening on %s", *addr)

	if *enableGen {
		ctx, cancel := context.WithCancel(context.Background())
		go b.eventGenerator(ctx, *genNS)
		defer cancel()
	}

	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
