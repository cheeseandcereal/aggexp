// Command backend-s3 is the gRPC backend for experiment 0024. It
// serves the buckets.aggexp.io/v1 resource type's *business data only*
// over the runtime/component/proto Backend gRPC service. KRM metadata
// (uid, resourceVersion, labels, annotations, managedFields,
// finalizers, ownerReferences, deletionTimestamp) is the middleware's
// concern and is NOT stored on the backend.
//
// Compared to 0018: the Meta struct shrinks to only {name} on the
// wire; we do not fabricate a UID, creationTimestamp, or
// resourceVersion. The middleware overlays those from the
// ResourceMetadata CRD. The backend is thinner by construction.
//
// The schema shipped from GetSchema is a **plain JSON Schema** of
// just spec+status per 0023 Track B. The middleware synthesizes the
// Kubernetes-dialect OpenAPI around it.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	componentpb "github.com/cheeseandcereal/aggexp/runtime/component/proto"
)

// Bucket is the on-wire JSON shape for the backend. No KRM metadata;
// only name + spec + status. The middleware handles everything else.
type Bucket struct {
	APIVersion string       `json:"apiVersion,omitempty"`
	Kind       string       `json:"kind,omitempty"`
	Metadata   Meta         `json:"metadata"`
	Spec       BucketSpec   `json:"spec,omitempty"`
	Status     BucketStatus `json:"status,omitempty"`
}

type Meta struct {
	Name string `json:"name"`
}

type BucketSpec struct {
	Region string            `json:"region,omitempty"`
	Tags   map[string]string `json:"tags,omitempty"`
}

type BucketStatus struct {
	Region       string `json:"region,omitempty"`
	CreationDate string `json:"creationDate,omitempty"`
	ObservedAt   string `json:"observedAt,omitempty"`
	Phase        string `json:"phase,omitempty"`
}

type backend struct {
	componentpb.UnimplementedBackendServer

	client        *s3.Client
	defaultRegion string
	prefix        string
	pollInterval  time.Duration

	mu      sync.Mutex
	seen    map[string]*Bucket
	watches map[int]chan *componentpb.WatchEvent
	nextWID int
}

func newBackend(client *s3.Client, defaultRegion, prefix string, poll time.Duration) *backend {
	return &backend{
		client:        client,
		defaultRegion: defaultRegion,
		prefix:        prefix,
		pollInterval:  poll,
		seen:          map[string]*Bucket{},
		watches:       map[int]chan *componentpb.WatchEvent{},
	}
}

// plainJSONSchema describes only spec+status. The middleware adds
// apiVersion/kind/metadata/GVK extension via synthesis (Track B).
func plainJSONSchema() []byte {
	s := map[string]any{
		"type":        "object",
		"description": "Bucket is an AWS S3 bucket projected as an aggregated-API resource. Business data only; KRM metadata is overlaid by the middleware from ResourceMetadata.",
		"properties": map[string]any{
			"spec": map[string]any{
				"type":        "object",
				"description": "BucketSpec captures the desired S3 bucket state.",
				"properties": map[string]any{
					"region": map[string]any{"type": "string", "description": "AWS region; immutable after create."},
					"tags": map[string]any{
						"type":                                 "object",
						"description":                          "Bucket tag set (written via PutBucketTagging).",
						"additionalProperties":                 map[string]any{"type": "string"},
						"x-kubernetes-preserve-unknown-fields": true,
					},
				},
			},
			"status": map[string]any{
				"type":        "object",
				"description": "BucketStatus mirrors S3 observations.",
				"properties": map[string]any{
					"region":       map[string]any{"type": "string"},
					"creationDate": map[string]any{"type": "string", "description": "Creation timestamp reported by S3."},
					"observedAt":   map[string]any{"type": "string", "description": "When the backend last read S3."},
					"phase":        map[string]any{"type": "string", "description": "Coarse state; 'Ready' on a successful observation."},
				},
			},
		},
	}
	raw, _ := json.Marshal(s)
	return raw
}

func (b *backend) GetSchema(_ context.Context, _ *componentpb.GetSchemaRequest) (*componentpb.GetSchemaResponse, error) {
	return &componentpb.GetSchemaResponse{
		Group:                   "aggexp.io",
		Version:                 "v1",
		Resource:                "buckets",
		Kind:                    "Bucket",
		Singular:                "bucket",
		Namespaced:              false,
		Writable:                true,
		SupportsServerSideApply: true,
		OpenapiV3:               plainJSONSchema(),
		Columns: []*componentpb.TableColumn{
			{Name: "Name", Type: "string", Format: "name", Description: "S3 bucket name."},
			{Name: "Region", Type: "string", Description: "AWS region."},
			{Name: "Phase", Type: "string", Description: "Coarse state."},
			{Name: "Age", Type: "date", Description: "Time since creation."},
		},
		RowFields: []string{".metadata.name", ".spec.region", ".status.phase", ".metadata.creationTimestamp"},
	}, nil
}

// ---- Get: LIVE read from S3 ----

func (b *backend) Get(ctx context.Context, req *componentpb.GetRequest) (*componentpb.GetResponse, error) {
	name := req.GetName()
	if !b.matchesPrefix(name) {
		return nil, grpcstatus.Errorf(codes.NotFound, "buckets.aggexp.io %q not found", name)
	}
	head, err := b.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(name)})
	if err != nil {
		if isNotFoundErr(err) {
			return nil, grpcstatus.Errorf(codes.NotFound, "buckets.aggexp.io %q not found", name)
		}
		return nil, grpcstatus.Errorf(codes.Unavailable, "HeadBucket %s: %v", name, err)
	}
	tags, err := b.getTags(ctx, name)
	if err != nil {
		log.Printf("get-tags-failed name=%s err=%v", name, err)
		tags = map[string]string{}
	}
	obj := &Bucket{
		APIVersion: "aggexp.io/v1",
		Kind:       "Bucket",
		Metadata:   Meta{Name: name},
		Spec: BucketSpec{
			Region: aws.ToString(head.BucketRegion),
			Tags:   tags,
		},
		Status: BucketStatus{
			Region:     aws.ToString(head.BucketRegion),
			ObservedAt: time.Now().UTC().Format(time.RFC3339),
			Phase:      "Ready",
		},
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.Internal, "marshal bucket: %v", err)
	}
	return &componentpb.GetResponse{ObjectJson: raw}, nil
}

func (b *backend) List(ctx context.Context, _ *componentpb.ListRequest) (*componentpb.ListResponse, error) {
	items, err := b.listBuckets(ctx)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(items))
	for _, it := range items {
		raw, err := json.Marshal(it)
		if err != nil {
			return nil, grpcstatus.Errorf(codes.Internal, "marshal bucket: %v", err)
		}
		out = append(out, raw)
	}
	return &componentpb.ListResponse{ItemsJson: out}, nil
}

func (b *backend) listBuckets(ctx context.Context) ([]*Bucket, error) {
	out, err := b.client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, grpcstatus.Errorf(codes.Unavailable, "ListBuckets: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	items := make([]*Bucket, 0, len(out.Buckets))
	for _, gb := range out.Buckets {
		name := aws.ToString(gb.Name)
		if !b.matchesPrefix(name) {
			continue
		}
		item := &Bucket{
			APIVersion: "aggexp.io/v1",
			Kind:       "Bucket",
			Metadata:   Meta{Name: name},
			Spec: BucketSpec{
				Region: aws.ToString(gb.BucketRegion),
			},
			Status: BucketStatus{
				Region:     aws.ToString(gb.BucketRegion),
				ObservedAt: now,
				Phase:      "Ready",
			},
		}
		if gb.CreationDate != nil {
			item.Status.CreationDate = gb.CreationDate.UTC().Format(time.RFC3339)
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Metadata.Name < items[j].Metadata.Name
	})
	return items, nil
}

func (b *backend) Create(ctx context.Context, req *componentpb.CreateRequest) (*componentpb.CreateResponse, error) {
	var in Bucket
	if err := json.Unmarshal(req.ObjectJson, &in); err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "unmarshal bucket: %v", err)
	}
	name := in.Metadata.Name
	if name == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "metadata.name is required")
	}
	region := in.Spec.Region
	if region == "" {
		region = b.defaultRegion
	}
	input := &s3.CreateBucketInput{Bucket: aws.String(name)}
	if region != "" && region != "us-east-1" {
		input.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(region),
		}
	}
	_, err := b.client.CreateBucket(ctx, input)
	if err != nil {
		var bae *s3types.BucketAlreadyExists
		var baou *s3types.BucketAlreadyOwnedByYou
		switch {
		case errors.As(err, &bae):
			return nil, grpcstatus.Errorf(codes.AlreadyExists, "buckets.aggexp.io %q already exists", name)
		case errors.As(err, &baou):
			// idempotent
		default:
			return nil, grpcstatus.Errorf(codes.Unavailable, "CreateBucket %s: %v", name, err)
		}
	}
	if len(in.Spec.Tags) > 0 {
		if err := b.putTags(ctx, name, in.Spec.Tags); err != nil {
			return nil, grpcstatus.Errorf(codes.Unavailable,
				"bucket %s created but tagging failed: %v (retry apply to complete)", name, err)
		}
	}
	resp, err := b.Get(ctx, &componentpb.GetRequest{Name: name, User: req.User})
	if err != nil {
		return nil, err
	}
	b.broadcast(componentpb.EventType_EVENT_ADDED, resp.ObjectJson)
	log.Printf("backend:create user=%s name=%s", userLabel(req.User), name)
	return &componentpb.CreateResponse{ObjectJson: resp.ObjectJson}, nil
}

func (b *backend) Update(ctx context.Context, req *componentpb.UpdateRequest) (*componentpb.UpdateResponse, error) {
	var in Bucket
	if err := json.Unmarshal(req.ObjectJson, &in); err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "unmarshal bucket: %v", err)
	}
	if in.Metadata.Name == "" {
		in.Metadata.Name = req.Name
	}
	if in.Metadata.Name != req.Name {
		return nil, grpcstatus.Errorf(codes.InvalidArgument,
			"body name %q != path name %q", in.Metadata.Name, req.Name)
	}
	name := req.Name

	_, err := b.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(name)})
	exists := err == nil
	if err != nil && !isNotFoundErr(err) {
		return nil, grpcstatus.Errorf(codes.Unavailable, "HeadBucket %s: %v", name, err)
	}

	if !exists {
		if !req.ForceAllowCreate {
			return nil, grpcstatus.Errorf(codes.NotFound, "buckets.aggexp.io %q not found", name)
		}
		resp, err := b.Create(ctx, &componentpb.CreateRequest{ObjectJson: req.ObjectJson, User: req.User})
		if err != nil {
			return nil, err
		}
		return &componentpb.UpdateResponse{ObjectJson: resp.ObjectJson, Created: true}, nil
	}

	// Tag update: set-or-replace the tag set unconditionally so that
	// removed tags are reflected. A zero-tag update deletes the tag
	// set by convention.
	if err := b.putTags(ctx, name, in.Spec.Tags); err != nil {
		return nil, grpcstatus.Errorf(codes.Unavailable, "PutBucketTagging %s: %v", name, err)
	}

	resp, err := b.Get(ctx, &componentpb.GetRequest{Name: name, User: req.User})
	if err != nil {
		return nil, err
	}
	b.broadcast(componentpb.EventType_EVENT_MODIFIED, resp.ObjectJson)
	log.Printf("backend:update user=%s name=%s", userLabel(req.User), name)
	return &componentpb.UpdateResponse{ObjectJson: resp.ObjectJson, Created: false}, nil
}

func (b *backend) Delete(ctx context.Context, req *componentpb.DeleteRequest) (*componentpb.DeleteResponse, error) {
	name := req.Name
	prior, _ := b.Get(ctx, &componentpb.GetRequest{Name: name, User: req.User})

	_, err := b.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(name)})
	if err != nil {
		if isNotFoundErr(err) {
			return nil, grpcstatus.Errorf(codes.NotFound, "buckets.aggexp.io %q not found", name)
		}
		var ae smithy.APIError
		if errors.As(err, &ae) && ae.ErrorCode() == "BucketNotEmpty" {
			return nil, grpcstatus.Errorf(codes.FailedPrecondition, "bucket %s not empty", name)
		}
		return nil, grpcstatus.Errorf(codes.Unavailable, "DeleteBucket %s: %v", name, err)
	}

	b.mu.Lock()
	delete(b.seen, name)
	b.mu.Unlock()

	var raw []byte
	if prior != nil {
		raw = prior.ObjectJson
	} else {
		t, _ := json.Marshal(&Bucket{
			APIVersion: "aggexp.io/v1",
			Kind:       "Bucket",
			Metadata:   Meta{Name: name},
		})
		raw = t
	}
	b.broadcast(componentpb.EventType_EVENT_DELETED, raw)
	log.Printf("backend:delete user=%s name=%s", userLabel(req.User), name)
	return &componentpb.DeleteResponse{ObjectJson: raw, Deleted: true}, nil
}

func (b *backend) Watch(req *componentpb.WatchRequest, stream grpc.ServerStreamingServer[componentpb.WatchEvent]) error {
	b.mu.Lock()
	wid := b.nextWID
	b.nextWID++
	ch := make(chan *componentpb.WatchEvent, 64)
	b.watches[wid] = ch
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		delete(b.watches, wid)
		close(ch)
		b.mu.Unlock()
	}()

	log.Printf("backend:watch open wid=%d user=%s", wid, userLabel(req.User))

	items, err := b.listBuckets(stream.Context())
	if err == nil {
		for _, it := range items {
			raw, err := json.Marshal(it)
			if err != nil {
				continue
			}
			if err := stream.Send(&componentpb.WatchEvent{
				Type: componentpb.EventType_EVENT_ADDED, ObjectJson: raw,
			}); err != nil {
				return err
			}
		}
	}

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			log.Printf("backend:watch close wid=%d: %v", wid, ctx.Err())
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

func (b *backend) broadcast(t componentpb.EventType, raw []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.watches {
		select {
		case ch <- &componentpb.WatchEvent{Type: t, ObjectJson: raw}:
		default:
		}
	}
}

func (b *backend) startPollLoop(ctx context.Context) {
	go func() {
		b.pollOnce(ctx)
		t := time.NewTicker(b.pollInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				b.pollOnce(ctx)
			}
		}
	}()
}

func (b *backend) pollOnce(ctx context.Context) {
	items, err := b.listBuckets(ctx)
	if err != nil {
		log.Printf("s3-poll-failed: %v", err)
		return
	}
	next := make(map[string]*Bucket, len(items))
	for _, it := range items {
		next[it.Metadata.Name] = it
	}
	b.mu.Lock()
	prev := b.seen
	var added, modified, deleted []*Bucket
	for name, cur := range next {
		old, existed := prev[name]
		if !existed {
			added = append(added, cur)
		} else if !bucketEqual(old, cur) {
			modified = append(modified, cur)
		}
	}
	for name, old := range prev {
		if _, still := next[name]; !still {
			deleted = append(deleted, old)
		}
	}
	b.seen = next
	b.mu.Unlock()

	for _, it := range added {
		raw, _ := json.Marshal(it)
		b.broadcast(componentpb.EventType_EVENT_ADDED, raw)
	}
	for _, it := range modified {
		raw, _ := json.Marshal(it)
		b.broadcast(componentpb.EventType_EVENT_MODIFIED, raw)
	}
	for _, it := range deleted {
		raw, _ := json.Marshal(it)
		b.broadcast(componentpb.EventType_EVENT_DELETED, raw)
	}
}

// ---- helpers ----

func (b *backend) getTags(ctx context.Context, name string) (map[string]string, error) {
	out, err := b.client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(name)})
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) && ae.ErrorCode() == "NoSuchTagSet" {
			return map[string]string{}, nil
		}
		return nil, err
	}
	m := make(map[string]string, len(out.TagSet))
	for _, t := range out.TagSet {
		m[aws.ToString(t.Key)] = aws.ToString(t.Value)
	}
	return m, nil
}

func (b *backend) putTags(ctx context.Context, name string, tags map[string]string) error {
	if len(tags) == 0 {
		return nil
	}
	tagSet := make([]s3types.Tag, 0, len(tags))
	for k, v := range tags {
		tagSet = append(tagSet, s3types.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	_, err := b.client.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{
		Bucket:  aws.String(name),
		Tagging: &s3types.Tagging{TagSet: tagSet},
	})
	return err
}

func (b *backend) matchesPrefix(name string) bool {
	return b.prefix == "" || strings.HasPrefix(name, b.prefix)
}

func isNotFoundErr(err error) bool {
	var nsb *s3types.NoSuchBucket
	var nf *s3types.NotFound
	if errors.As(err, &nsb) || errors.As(err, &nf) {
		return true
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "NoSuchBucket", "NotFound":
			return true
		}
	}
	return false
}

func bucketEqual(a, b *Bucket) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Spec.Region != b.Spec.Region {
		return false
	}
	if len(a.Spec.Tags) != len(b.Spec.Tags) {
		return false
	}
	for k, v := range a.Spec.Tags {
		if b.Spec.Tags[k] != v {
			return false
		}
	}
	return true
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
	region := flag.String("aws-region", "us-east-1", "AWS region")
	endpoint := flag.String("aws-endpoint-url", "", "AWS S3 endpoint URL override (for mock).")
	pathStyle := flag.Bool("aws-s3-path-style", false, "Use path-style addressing for S3 (required for mock).")
	poll := flag.Duration("poll-interval", 30*time.Second, "How often to poll S3 for watch diffs.")
	prefix := flag.String("name-prefix", "", "If set, only buckets with this name prefix are projected.")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(*region))
	if err != nil {
		log.Fatalf("load aws config: %v", err)
	}
	s3Opts := []func(*s3.Options){
		func(o *s3.Options) { o.UsePathStyle = *pathStyle },
	}
	if *endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) { o.BaseEndpoint = aws.String(*endpoint) })
	}
	client := s3.NewFromConfig(cfg, s3Opts...)

	b := newBackend(client, *region, *prefix, *poll)
	b.startPollLoop(ctx)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	componentpb.RegisterBackendServer(srv, b)
	log.Printf("backend-s3 (0024) listening on %s endpoint=%q region=%s", *addr, *endpoint, *region)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
