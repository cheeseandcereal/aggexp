// Command note-backend-http is the reference HTTP/JSON + SSE backend
// for experiment 0026. It serves a single resource type — Note —
// over a plain net/http server and streams watch events as real
// Server-Sent Events.
//
// This backend is deliberately stdlib-only. No grpc, no generated
// code, no uuid package, no logging framework. The point of 0026 is
// to quantify how cheaply a backend author can expose a Kubernetes
// resource through the aggexp middleware when the transport is
// HTTP/JSON. Adding dependencies would muddy that measurement.
//
// Protocol shape (mirrors runtime/component/proto but over HTTP):
//
//	GET    /schema                       -> GetSchemaResponse
//	GET    /objects/{namespace}/{name}   -> GetResponse
//	GET    /objects/{namespace}          -> ListResponse
//	POST   /objects/{namespace}          -> CreateResponse
//	PUT    /objects/{namespace}/{name}   -> UpdateResponse
//	DELETE /objects/{namespace}/{name}   -> DeleteResponse
//	GET    /watch/{namespace}            -> SSE stream of WatchEvents
//
// Caller identity is forwarded in X-Aggexp-User-* headers; we log
// it but do no authz here — the middleware gates every request
// before dispatching.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// ----- domain types ---------------------------------------------------------

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

// ----- protocol envelopes ---------------------------------------------------
//
// These mirror runtime/component/proto's messages in structure, minus
// the proto-only fields that only gRPC cares about. Object bodies
// travel as JSON objects (not base64-wrapped byte strings); this is
// a real wire-level difference from the gRPC path and part of the
// experiment's debuggability thesis.

type tableColumn struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Format      string `json:"format,omitempty"`
	Description string `json:"description,omitempty"`
	Priority    int32  `json:"priority,omitempty"`
}

type schemaResponse struct {
	Group                   string          `json:"group"`
	Version                 string          `json:"version"`
	Resource                string          `json:"resource"`
	Kind                    string          `json:"kind"`
	Singular                string          `json:"singular"`
	Namespaced              bool            `json:"namespaced"`
	Writable                bool            `json:"writable"`
	SupportsServerSideApply bool            `json:"supportsServerSideApply"`
	OpenAPIV3               json.RawMessage `json:"openapiV3"`
	Columns                 []tableColumn   `json:"columns,omitempty"`
	RowFields               []string        `json:"rowFields,omitempty"`
	ShortNames              []string        `json:"shortNames,omitempty"`
	Categories              []string        `json:"categories,omitempty"`
}

type listResponse struct {
	Items           []json.RawMessage `json:"items"`
	ResourceVersion string            `json:"resourceVersion,omitempty"`
}

type watchEvent struct {
	Type   string          `json:"type"`   // ADDED | MODIFIED | DELETED | BOOKMARK
	Object json.RawMessage `json:"object"`
}

// errorBody is the uniform error shape; the component translates its
// .message into a Kubernetes metav1.Status reason.
type errorBody struct {
	Message string `json:"message"`
}

// ----- store ----------------------------------------------------------------

type key struct{ ns, name string }

type backend struct {
	mu      sync.Mutex
	items   map[key]*Note
	watches map[int]chan *watchEvent
	nextWID int
}

func newBackend() *backend {
	return &backend{
		items:   map[key]*Note{},
		watches: map[int]chan *watchEvent{},
	}
}

func newUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failure is effectively unrecoverable at this
		// skeleton's level; falling back to time-based IDs would
		// risk collisions. Panic is correct for a lab tool.
		panic("rand.Read: " + err.Error())
	}
	// Emit RFC-4122-ish canonical hex without the dashes; the
	// apiserver + kubectl accept any string here.
	return hex.EncodeToString(b[:])
}

// ----- handlers -------------------------------------------------------------

func (b *backend) schemaHandler(w http.ResponseWriter, r *http.Request) {
	// Plain JSON Schema describing just the business payload. The
	// middleware lifts this into full Kubernetes OpenAPI v3 (0023
	// Track B shape).
	jsonSchema := map[string]any{
		"type":        "object",
		"description": "A Note is a piece of titled text (HTTP/JSON + SSE backend).",
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
						"description": "Short display title. Required, 3-64 characters.",
					},
					"body": map[string]any{
						"type":        "string",
						"description": "Free-form body text. Optional.",
					},
				},
			},
			"status": map[string]any{
				"type":        "object",
				"description": "Server-assigned fields.",
				"properties": map[string]any{
					"updatedAt": map[string]any{
						"type":        "string",
						"format":      "date-time",
						"description": "Server-assigned last-update time (RFC 3339).",
					},
				},
			},
		},
	}
	raw, _ := json.Marshal(jsonSchema)
	writeJSON(w, http.StatusOK, &schemaResponse{
		Group:                   "aggexp.io",
		Version:                 "v1",
		Resource:                "notes",
		Kind:                    "Note",
		Singular:                "note",
		Namespaced:              true,
		Writable:                true,
		SupportsServerSideApply: true,
		OpenAPIV3:               raw,
		Columns: []tableColumn{
			{Name: "Name", Type: "string", Description: "Name of the note."},
			{Name: "Title", Type: "string", Description: "Note title."},
			{Name: "Age", Type: "string", Description: "Time since creation."},
		},
		RowFields:  []string{".metadata.name", ".spec.title", ".metadata.creationTimestamp"},
		ShortNames: []string{"nt"},
	})
}

func (b *backend) getHandler(w http.ResponseWriter, r *http.Request, ns, name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, ok := b.items[key{ns, name}]
	if !ok {
		writeError(w, http.StatusNotFound,
			fmt.Sprintf("notes.aggexp.io %q not found", name))
		return
	}
	writeJSON(w, http.StatusOK, n)
}

func (b *backend) listHandler(w http.ResponseWriter, r *http.Request, ns string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	keys := make([]key, 0, len(b.items))
	for k := range b.items {
		if ns != "" && k.ns != ns {
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
	out := make([]json.RawMessage, 0, len(keys))
	for _, k := range keys {
		raw, _ := json.Marshal(b.items[k])
		out = append(out, raw)
	}
	writeJSON(w, http.StatusOK, &listResponse{Items: out})
}

func (b *backend) createHandler(w http.ResponseWriter, r *http.Request, ns string) {
	var n Note
	if err := json.NewDecoder(r.Body).Decode(&n); err != nil {
		writeError(w, http.StatusBadRequest, "decode object: "+err.Error())
		return
	}
	if n.Metadata.Name == "" {
		writeError(w, http.StatusBadRequest, "metadata.name is required")
		return
	}
	if ns != "" {
		n.Metadata.Namespace = ns
	}
	n.APIVersion = "aggexp.io/v1"
	n.Kind = "Note"

	b.mu.Lock()
	defer b.mu.Unlock()
	k := key{n.Metadata.Namespace, n.Metadata.Name}
	if _, exists := b.items[k]; exists {
		writeError(w, http.StatusConflict,
			fmt.Sprintf("notes.aggexp.io %q already exists", n.Metadata.Name))
		return
	}
	n.Metadata.UID = newUID()
	n.Metadata.CreationTimestamp = time.Now().UTC().Format(time.RFC3339)
	n.Status.UpdatedAt = n.Metadata.CreationTimestamp
	b.items[k] = &n

	raw, _ := json.Marshal(&n)
	b.broadcastLocked("ADDED", raw)
	log.Printf("create user=%s name=%s ns=%s", userLabel(r), n.Metadata.Name, n.Metadata.Namespace)
	writeJSON(w, http.StatusCreated, &n)
}

func (b *backend) updateHandler(w http.ResponseWriter, r *http.Request, ns, name string) {
	var n Note
	if err := json.NewDecoder(r.Body).Decode(&n); err != nil {
		writeError(w, http.StatusBadRequest, "decode object: "+err.Error())
		return
	}
	if ns != "" {
		n.Metadata.Namespace = ns
	}
	if n.Metadata.Name == "" {
		n.Metadata.Name = name
	}
	n.APIVersion = "aggexp.io/v1"
	n.Kind = "Note"

	// Support upsert semantics via ?forceAllowCreate=true.
	forceAllowCreate := r.URL.Query().Get("forceAllowCreate") == "true"

	b.mu.Lock()
	defer b.mu.Unlock()
	k := key{n.Metadata.Namespace, name}
	existing, ok := b.items[k]
	if !ok {
		if !forceAllowCreate {
			writeError(w, http.StatusNotFound,
				fmt.Sprintf("notes.aggexp.io %q not found", name))
			return
		}
		n.Metadata.UID = newUID()
		n.Metadata.CreationTimestamp = time.Now().UTC().Format(time.RFC3339)
		n.Status.UpdatedAt = n.Metadata.CreationTimestamp
		b.items[k] = &n
		raw, _ := json.Marshal(&n)
		b.broadcastLocked("ADDED", raw)
		log.Printf("upsert-create user=%s name=%s", userLabel(r), n.Metadata.Name)
		writeJSON(w, http.StatusCreated, &n)
		return
	}
	n.Metadata.UID = existing.Metadata.UID
	n.Metadata.CreationTimestamp = existing.Metadata.CreationTimestamp
	n.Status.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	b.items[k] = &n
	raw, _ := json.Marshal(&n)
	b.broadcastLocked("MODIFIED", raw)
	log.Printf("update user=%s name=%s", userLabel(r), n.Metadata.Name)
	writeJSON(w, http.StatusOK, &n)
}

func (b *backend) deleteHandler(w http.ResponseWriter, r *http.Request, ns, name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	k := key{ns, name}
	existing, ok := b.items[k]
	if !ok {
		writeError(w, http.StatusNotFound,
			fmt.Sprintf("notes.aggexp.io %q not found", name))
		return
	}
	delete(b.items, k)
	raw, _ := json.Marshal(existing)
	b.broadcastLocked("DELETED", raw)
	log.Printf("delete user=%s name=%s", userLabel(r), name)
	writeJSON(w, http.StatusOK, existing)
}

// watchHandler writes an SSE stream. Each event is the canonical
// SSE shape: `data: {json}\n\n`. We seed with the current state
// (one ADDED per item) then stream live events.
//
// SSE requires the HTTP response to be flushable. net/http's
// response writer supports http.Flusher when the protocol is
// HTTP/1.1 and no compression middleware is intercepting; our
// server uses neither, so this works directly.
func (b *backend) watchHandler(w http.ResponseWriter, r *http.Request, ns string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Explicitly flush headers so the client sees the 200 immediately.
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	b.mu.Lock()
	wid := b.nextWID
	b.nextWID++
	ch := make(chan *watchEvent, 64)
	b.watches[wid] = ch
	initial := make([]*watchEvent, 0, len(b.items))
	for k, n := range b.items {
		if ns != "" && k.ns != ns {
			continue
		}
		raw, _ := json.Marshal(n)
		initial = append(initial, &watchEvent{Type: "ADDED", Object: raw})
	}
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		delete(b.watches, wid)
		close(ch)
		b.mu.Unlock()
	}()

	log.Printf("watch open wid=%d user=%s ns=%q", wid, userLabel(r), ns)

	for _, ev := range initial {
		if err := sseSend(w, flusher, ev); err != nil {
			return
		}
	}

	ctx := r.Context()
	// Periodic comment-line keepalive. kubectl's HTTP client handles
	// long-idle streams fine, but kube-apiserver's aggregation-layer
	// proxy has a 30s idle window; a 20s keepalive keeps the pipe
	// alive through it.
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("watch close wid=%d: %v", wid, ctx.Err())
			return
		case ev, open := <-ch:
			if !open {
				return
			}
			if err := sseSend(w, flusher, ev); err != nil {
				return
			}
		case <-ticker.C:
			// SSE comment lines begin with ":". Clients ignore them;
			// the bytes traverse the network and reset idle timers.
			if _, err := fmt.Fprintf(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func sseSend(w http.ResponseWriter, flusher http.Flusher, ev *watchEvent) error {
	raw, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	// Canonical SSE: `data: <bytes>\n\n`. The body is the whole
	// watchEvent JSON object; the middleware will json-decode the
	// `data:` payload.
	if _, err := fmt.Fprintf(w, "data: %s\n\n", raw); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

func (b *backend) broadcastLocked(kind string, raw []byte) {
	ev := &watchEvent{Type: kind, Object: raw}
	for _, ch := range b.watches {
		select {
		case ch <- ev:
		default:
			// Drop on full channel — skeleton-grade. Production
			// would close the watcher and let the client relist.
		}
	}
}

// ----- multiplexer ----------------------------------------------------------

func (b *backend) mux() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/schema", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "GET required")
			return
		}
		b.schemaHandler(w, r)
	})

	mux.HandleFunc("/objects/", func(w http.ResponseWriter, r *http.Request) {
		// /objects/{namespace}           (list, create)
		// /objects/{namespace}/{name}    (get, put, delete)
		rest := strings.TrimPrefix(r.URL.Path, "/objects/")
		parts := strings.SplitN(rest, "/", 2)
		ns := parts[0]
		var name string
		if len(parts) == 2 {
			name = parts[1]
		}
		switch {
		case name == "" && r.Method == http.MethodGet:
			b.listHandler(w, r, ns)
		case name == "" && r.Method == http.MethodPost:
			b.createHandler(w, r, ns)
		case name != "" && r.Method == http.MethodGet:
			b.getHandler(w, r, ns, name)
		case name != "" && r.Method == http.MethodPut:
			b.updateHandler(w, r, ns, name)
		case name != "" && r.Method == http.MethodDelete:
			b.deleteHandler(w, r, ns, name)
		default:
			writeError(w, http.StatusMethodNotAllowed, r.Method+" "+r.URL.Path)
		}
	})

	mux.HandleFunc("/watch/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "GET required")
			return
		}
		ns := strings.TrimPrefix(r.URL.Path, "/watch/")
		// Allow /watch or /watch/ for cluster-scope (future). For now
		// everything is namespaced; ns may be empty string meaning
		// all-namespaces.
		b.watchHandler(w, r, ns)
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintln(w, "ok")
	})

	return mux
}

// ----- helpers --------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(&errorBody{Message: msg})
}

// userLabel flattens the X-Aggexp-User-* headers into a loggable
// string. Not load-bearing; only used by log.Printf calls above.
func userLabel(r *http.Request) string {
	name := r.Header.Get("X-Aggexp-User-Name")
	if name == "" {
		return "<anon>"
	}
	groups := r.Header.Get("X-Aggexp-User-Groups")
	if groups == "" {
		return name
	}
	return name + "[" + groups + "]"
}

// ----- entrypoint -----------------------------------------------------------

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()

	b := newBackend()
	srv := &http.Server{
		Addr:              *addr,
		Handler:           b.mux(),
		ReadHeaderTimeout: 5 * time.Second,
		// Deliberately no WriteTimeout: SSE streams run indefinitely.
	}
	log.Printf("note-backend-http listening on %s", *addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
