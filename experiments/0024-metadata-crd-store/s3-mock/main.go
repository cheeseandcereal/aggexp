// Command s3-mock is a tiny in-memory S3 wire-protocol mock
// implementing the minimum subset the 0009 aggregated apiserver
// exercises: ListBuckets, HeadBucket, CreateBucket, DeleteBucket,
// GetBucketTagging, PutBucketTagging.
//
// This is deliberately not LocalStack: the wire surface for these
// endpoints is small and LocalStack's setup pain outweighs writing
// a ~200-line handler that matches exactly what the aws-sdk-go-v2
// client expects.
//
// Auth is not enforced. Any Authorization header is accepted. Region
// is whatever the client sends via LocationConstraint on create, or
// "us-east-1" by default.
package main

import (
	"encoding/xml"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

type bucket struct {
	Name         string
	CreationDate time.Time
	Region       string
	Tags         map[string]string
}

type store struct {
	mu      sync.RWMutex
	buckets map[string]*bucket
	ownerID string
}

func (s *store) list() []*bucket {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*bucket, 0, len(s.buckets))
	for _, b := range s.buckets {
		out = append(out, b)
	}
	return out
}

type errorResponse struct {
	XMLName    xml.Name `xml:"Error"`
	Code       string   `xml:"Code"`
	Message    string   `xml:"Message"`
	BucketName string   `xml:"BucketName,omitempty"`
	RequestID  string   `xml:"RequestId"`
	HostID     string   `xml:"HostId"`
}

func writeError(w http.ResponseWriter, status int, code, msg, bucket string) {
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("x-amz-request-id", "mock-req-0001")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	e := errorResponse{
		Code: code, Message: msg, BucketName: bucket,
		RequestID: "mock-req-0001", HostID: "mock-host-0001",
	}
	_ = xml.NewEncoder(w).Encode(e)
}

// ---- ListBuckets ----

type ownerXML struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type bucketXML struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
	BucketRegion string `xml:"BucketRegion,omitempty"`
}

type listAllMyBucketsResult struct {
	XMLName xml.Name    `xml:"ListAllMyBucketsResult"`
	XMLNS   string      `xml:"xmlns,attr"`
	Owner   ownerXML    `xml:"Owner"`
	Buckets bucketsNode `xml:"Buckets"`
}

type bucketsNode struct {
	Bucket []bucketXML `xml:"Bucket"`
}

func (s *store) handleListBuckets(w http.ResponseWriter, _ *http.Request) {
	bs := s.list()
	out := listAllMyBucketsResult{
		XMLNS: "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner: ownerXML{ID: s.ownerID, DisplayName: "mock"},
	}
	for _, b := range bs {
		out.Buckets.Bucket = append(out.Buckets.Bucket, bucketXML{
			Name:         b.Name,
			CreationDate: b.CreationDate.UTC().Format(time.RFC3339),
			BucketRegion: b.Region,
		})
	}
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("x-amz-request-id", "mock-req-list")
	w.WriteHeader(200)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(out)
}

// ---- HeadBucket ----

func (s *store) handleHeadBucket(w http.ResponseWriter, _ *http.Request, name string) {
	s.mu.RLock()
	b, ok := s.buckets[name]
	s.mu.RUnlock()
	if !ok {
		w.Header().Set("x-amz-request-id", "mock-req-head")
		w.WriteHeader(404)
		return
	}
	w.Header().Set("x-amz-bucket-region", b.Region)
	w.Header().Set("x-amz-request-id", "mock-req-head")
	w.WriteHeader(200)
}

// ---- CreateBucket ----

type createBucketConfiguration struct {
	XMLName            xml.Name `xml:"CreateBucketConfiguration"`
	LocationConstraint string   `xml:"LocationConstraint"`
}

func (s *store) handleCreateBucket(w http.ResponseWriter, r *http.Request, name string) {
	region := "us-east-1"
	defer r.Body.Close()
	var cfg createBucketConfiguration
	if err := xml.NewDecoder(r.Body).Decode(&cfg); err == nil && cfg.LocationConstraint != "" {
		region = cfg.LocationConstraint
	}

	s.mu.Lock()
	if _, exists := s.buckets[name]; exists {
		s.mu.Unlock()
		writeError(w, 409, "BucketAlreadyOwnedByYou",
			"Your previous request to create the named bucket succeeded and you already own it.", name)
		return
	}
	s.buckets[name] = &bucket{
		Name:         name,
		CreationDate: time.Now().UTC(),
		Region:       region,
		Tags:         map[string]string{},
	}
	s.mu.Unlock()

	w.Header().Set("Location", "/"+name)
	w.Header().Set("x-amz-request-id", "mock-req-create")
	w.WriteHeader(200)
}

// ---- DeleteBucket ----

func (s *store) handleDeleteBucket(w http.ResponseWriter, _ *http.Request, name string) {
	s.mu.Lock()
	_, ok := s.buckets[name]
	if !ok {
		s.mu.Unlock()
		writeError(w, 404, "NoSuchBucket", "The specified bucket does not exist.", name)
		return
	}
	delete(s.buckets, name)
	s.mu.Unlock()
	w.Header().Set("x-amz-request-id", "mock-req-delete")
	w.WriteHeader(204)
}

// ---- GetBucketTagging ----

type tagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type tagSetNode struct {
	Tag []tagXML `xml:"Tag"`
}

type taggingXML struct {
	XMLName xml.Name   `xml:"Tagging"`
	XMLNS   string     `xml:"xmlns,attr,omitempty"`
	TagSet  tagSetNode `xml:"TagSet"`
}

func (s *store) handleGetTagging(w http.ResponseWriter, _ *http.Request, name string) {
	s.mu.RLock()
	b, ok := s.buckets[name]
	if !ok {
		s.mu.RUnlock()
		writeError(w, 404, "NoSuchBucket", "The specified bucket does not exist.", name)
		return
	}
	tags := make(map[string]string, len(b.Tags))
	for k, v := range b.Tags {
		tags[k] = v
	}
	s.mu.RUnlock()

	if len(tags) == 0 {
		writeError(w, 404, "NoSuchTagSet", "The TagSet does not exist", name)
		return
	}

	out := taggingXML{XMLNS: "http://s3.amazonaws.com/doc/2006-03-01/"}
	for k, v := range tags {
		out.TagSet.Tag = append(out.TagSet.Tag, tagXML{Key: k, Value: v})
	}
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("x-amz-request-id", "mock-req-get-tagging")
	w.WriteHeader(200)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(out)
}

// ---- PutBucketTagging ----

func (s *store) handlePutTagging(w http.ResponseWriter, r *http.Request, name string) {
	s.mu.Lock()
	b, ok := s.buckets[name]
	if !ok {
		s.mu.Unlock()
		writeError(w, 404, "NoSuchBucket", "The specified bucket does not exist.", name)
		return
	}
	defer r.Body.Close()
	var t taggingXML
	if err := xml.NewDecoder(r.Body).Decode(&t); err != nil {
		s.mu.Unlock()
		writeError(w, 400, "MalformedXML", err.Error(), name)
		return
	}
	b.Tags = make(map[string]string, len(t.TagSet.Tag))
	for _, tt := range t.TagSet.Tag {
		b.Tags[tt.Key] = tt.Value
	}
	s.mu.Unlock()
	w.Header().Set("x-amz-request-id", "mock-req-put-tagging")
	w.WriteHeader(204)
}

// ---- router ----

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	owner := flag.String("owner-id", "mock-owner", "canonical owner ID reported in XML")
	flag.Parse()

	s := &store{buckets: map[string]*bucket{}, ownerID: *owner}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s", r.Method, r.URL.String())
		path := strings.TrimPrefix(r.URL.Path, "/")
		tagging := r.URL.Query().Has("tagging")
		path = strings.TrimSuffix(path, "/")

		switch {
		case path == "" && r.Method == http.MethodGet:
			s.handleListBuckets(w, r)
		case path == "":
			writeError(w, 405, "MethodNotAllowed", "Method not allowed on /", "")
		case tagging && r.Method == http.MethodGet:
			s.handleGetTagging(w, r, path)
		case tagging && r.Method == http.MethodPut:
			s.handlePutTagging(w, r, path)
		case r.Method == http.MethodHead:
			s.handleHeadBucket(w, r, path)
		case r.Method == http.MethodPut:
			s.handleCreateBucket(w, r, path)
		case r.Method == http.MethodDelete:
			s.handleDeleteBucket(w, r, path)
		default:
			writeError(w, 400, "MethodNotAllowed", fmt.Sprintf("%s not allowed on /%s", r.Method, path), path)
		}
	})

	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Printf("s3-mock listening on %s (owner=%s)", *addr, *owner)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
