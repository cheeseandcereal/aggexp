// Command backend-gadget-grpc is the 0031 gRPC backend for the
// gadgets.aggexp.io/v1 API. Implements
// runtime/component/v2/proto.BackendServer with an in-memory store.
// The middleware dials this over plaintext gRPC.
//
// Watch capability is declared as "poll" so the middleware drives
// list-based diff rather than calling the server's Watch. This
// intentionally contrasts with the widget backend's "push" to
// exercise both grpcbackend.ModePoll and ModePush in the same
// multiplex process.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	componentv2pb "github.com/cheeseandcereal/aggexp/runtime/component/v2/proto"
)

const (
	group    = "gadgets.aggexp.io"
	version  = "v1"
	resource = "gadgets"
	kind     = "Gadget"
	singular = "gadget"
)

// JSON schema for gadgets. Distinct fields from widgets so kubectl
// treats them as genuinely different APIs.
var gadgetSchema = mustJSON(map[string]any{
	"type":        "object",
	"description": "A Gadget is a 0031 demo resource served over gRPC.",
	"properties": map[string]any{
		"spec": map[string]any{
			"type":        "object",
			"description": "Caller-supplied gadget fields.",
			"required":    []any{},
			"properties": map[string]any{
				"brand": map[string]any{
					"type":        "string",
					"description": "Gadget brand.",
					"minLength":   1,
				},
				"horsepower": map[string]any{
					"type":        "integer",
					"description": "Gadget horsepower.",
					"minimum":     1,
				},
			},
		},
		"status": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"updatedAt": map[string]any{"type": "string", "format": "date-time"},
			},
		},
	},
})

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

type key struct{ ns, name string }

type server struct {
	componentv2pb.UnimplementedBackendServer
	mu    sync.Mutex
	items map[key][]byte // raw JSON business object
}

func newUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func (s *server) GetSchema(_ context.Context, _ *componentv2pb.GetSchemaRequest) (*componentv2pb.GetSchemaResponse, error) {
	return &componentv2pb.GetSchemaResponse{
		Group: group, Version: version, Resource: resource, Kind: kind, Singular: singular,
		Namespaced: true, Writable: true, SupportsServerSideApply: true,
		Schema:          gadgetSchema,
		SchemaIsOpenapi: false,
		WatchCapability: "poll",
		Columns: []*componentv2pb.TableColumn{
			{Name: "Name", Type: "string", Description: "Gadget name"},
			{Name: "Brand", Type: "string", Description: "Brand"},
			{Name: "HP", Type: "integer", Description: "Horsepower"},
			{Name: "Age", Type: "string", Description: "Time since creation"},
		},
		RowFields: []string{
			".metadata.name",
			".spec.brand",
			".spec.horsepower",
			".metadata.creationTimestamp",
		},
	}, nil
}

// shape matches v2's narrow business-JSON projection: apiVersion,
// kind, metadata (name,namespace + uid/creationTimestamp stamped
// here), spec, status.
type obj struct {
	APIVersion string                 `json:"apiVersion"`
	Kind       string                 `json:"kind"`
	Metadata   map[string]interface{} `json:"metadata"`
	Spec       interface{}            `json:"spec,omitempty"`
	Status     interface{}            `json:"status,omitempty"`
}

func parse(raw []byte) (*obj, error) {
	var o obj
	if err := json.Unmarshal(raw, &o); err != nil {
		return nil, err
	}
	if o.Metadata == nil {
		o.Metadata = map[string]interface{}{}
	}
	return &o, nil
}

func (o *obj) stampCreateMeta() {
	o.APIVersion = group + "/" + version
	o.Kind = kind
	o.Metadata["uid"] = newUID()
	o.Metadata["creationTimestamp"] = time.Now().UTC().Format(time.RFC3339)
	o.Status = map[string]interface{}{
		"updatedAt": time.Now().UTC().Format(time.RFC3339),
	}
}

func (s *server) Get(_ context.Context, req *componentv2pb.GetRequest) (*componentv2pb.GetResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.items[key{req.GetNamespace(), req.GetName()}]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "gadget %q/%q not found", req.GetNamespace(), req.GetName())
	}
	return &componentv2pb.GetResponse{ObjectJson: raw}, nil
}

func (s *server) List(_ context.Context, req *componentv2pb.ListRequest) (*componentv2pb.ListResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := [][]byte{}
	for k, raw := range s.items {
		if req.GetNamespace() != "" && k.ns != req.GetNamespace() {
			continue
		}
		out = append(out, raw)
	}
	return &componentv2pb.ListResponse{ItemsJson: out}, nil
}

func (s *server) Create(_ context.Context, req *componentv2pb.CreateRequest) (*componentv2pb.CreateResponse, error) {
	o, err := parse(req.GetObjectJson())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse: %v", err)
	}
	name, _ := o.Metadata["name"].(string)
	ns, _ := o.Metadata["namespace"].(string)
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "metadata.name required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key{ns, name}
	if _, exists := s.items[k]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "gadget %q exists", name)
	}
	o.stampCreateMeta()
	raw, _ := json.Marshal(o)
	s.items[k] = raw
	log.Printf("gadget create ns=%s name=%s", ns, name)
	return &componentv2pb.CreateResponse{ObjectJson: raw}, nil
}

func (s *server) Update(_ context.Context, req *componentv2pb.UpdateRequest) (*componentv2pb.UpdateResponse, error) {
	o, err := parse(req.GetObjectJson())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse: %v", err)
	}
	name, _ := o.Metadata["name"].(string)
	ns, _ := o.Metadata["namespace"].(string)
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key{ns, name}
	existing, ok := s.items[k]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "gadget %q not found", name)
	}
	// preserve uid + creationTimestamp
	var existingObj obj
	_ = json.Unmarshal(existing, &existingObj)
	if u, ok := existingObj.Metadata["uid"]; ok {
		o.Metadata["uid"] = u
	}
	if c, ok := existingObj.Metadata["creationTimestamp"]; ok {
		o.Metadata["creationTimestamp"] = c
	}
	o.APIVersion = group + "/" + version
	o.Kind = kind
	o.Status = map[string]interface{}{"updatedAt": time.Now().UTC().Format(time.RFC3339)}
	raw, _ := json.Marshal(o)
	s.items[k] = raw
	log.Printf("gadget update ns=%s name=%s", ns, name)
	return &componentv2pb.UpdateResponse{ObjectJson: raw}, nil
}

func (s *server) Apply(ctx context.Context, req *componentv2pb.ApplyRequest) (*componentv2pb.ApplyResponse, error) {
	// Apply goes through Update for this demo backend; middleware
	// already computed managedFields.
	uresp, err := s.Update(ctx, &componentv2pb.UpdateRequest{
		User: req.GetUser(), Namespace: req.GetNamespace(), Name: req.GetName(),
		ObjectJson: req.GetObjectJson(),
	})
	if err != nil {
		// If not found, create instead.
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			cresp, cerr := s.Create(ctx, &componentv2pb.CreateRequest{
				User: req.GetUser(), Namespace: req.GetNamespace(), ObjectJson: req.GetObjectJson(),
			})
			if cerr != nil {
				return nil, cerr
			}
			return &componentv2pb.ApplyResponse{ObjectJson: cresp.GetObjectJson()}, nil
		}
		return nil, err
	}
	return &componentv2pb.ApplyResponse{ObjectJson: uresp.GetObjectJson()}, nil
}

func (s *server) Delete(_ context.Context, req *componentv2pb.DeleteRequest) (*componentv2pb.DeleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key{req.GetNamespace(), req.GetName()}
	existing, ok := s.items[k]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "gadget %q not found", req.GetName())
	}
	delete(s.items, k)
	log.Printf("gadget delete ns=%s name=%s", req.GetNamespace(), req.GetName())
	return &componentv2pb.DeleteResponse{ObjectJson: existing}, nil
}

// Watch declared as poll; the middleware won't call this but we
// implement codes.Unimplemented so it's explicit.
func (s *server) Watch(_ *componentv2pb.WatchRequest, _ grpc.ServerStreamingServer[componentv2pb.WatchEvent]) error {
	return status.Error(codes.Unimplemented, "this backend advertises poll-only watch capability")
}

func main() {
	lis, err := net.Listen("tcp", ":8081")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	componentv2pb.RegisterBackendServer(s, &server{items: map[key][]byte{}})
	log.Printf("gadget-grpc backend listening on :8081")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
	_ = fmt.Sprintf // keep import
}
