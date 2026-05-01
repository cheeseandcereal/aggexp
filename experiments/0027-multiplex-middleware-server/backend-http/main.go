// Command backend-http is the 0027 HTTP backend. It is a stdlib-only
// fork of 0026's backend-note, generalized to serve ANY single
// resource kind — widget, gadget, sprocket — as configured by
// environment variables at pod startup.
//
// This is what the multiplex middleware connects to. Three
// Deployments of this image, one per resource kind, share no state
// and know nothing about Kubernetes.
//
// Env variables:
//
//	RESOURCE        (required) lowercase singular, e.g. "widget"
//	PLURAL          (required) lowercase plural, e.g. "widgets"
//	KIND            (required) CamelCase, e.g. "Widget"
//	GROUP           (default: aggexp.io) APIGroup
//	VERSION         (default: v1)
//	NAMESPACED      (default: true)
//	PORT            (default: 8080) listen addr :$PORT
//	TITLE_MAX_LEN   (default: 64)   used for the spec schema
//	DESCRIPTION     (default: "A <Kind> is...") short description
//
// Wire shape matches 0026 byte-for-byte so the middleware's HTTP
// client (re-implemented here as a minimal component-internal
// httpClient) speaks the same protocol against every backend.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

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

// Object is the generic wire shape: apiVersion/kind/metadata + an
// arbitrary spec map + an arbitrary status map. Since the backend
// serves one kind per process, the spec shape is hard-coded by the
// /schema handler but the in-memory store keeps spec as raw JSON.
type Object struct {
	APIVersion string          `json:"apiVersion"`
	Kind       string          `json:"kind"`
	Metadata   Meta            `json:"metadata"`
	Spec       json.RawMessage `json:"spec,omitempty"`
	Status     json.RawMessage `json:"status,omitempty"`
}

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
	Type   string          `json:"type"`
	Object json.RawMessage `json:"object"`
}

type errorBody struct {
	Message string `json:"message"`
}

type key struct{ ns, name string }

type backend struct {
	mu      sync.Mutex
	items   map[key]*Object
	watches map[int]chan *watchEvent
	nextWID int

	cfg config
}

type config struct {
	Group       string
	Version     string
	Resource    string // plural
	Kind        string
	Singular    string
	Namespaced  bool
	TitleMax    int
	Description string
}

func newBackend(cfg config) *backend {
	return &backend{
		items:   map[key]*Object{},
		watches: map[int]chan *watchEvent{},
		cfg:     cfg,
	}
}

func newUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("rand.Read: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

func (b *backend) schemaHandler(w http.ResponseWriter, r *http.Request) {
	// Plain JSON Schema describing just the business payload. The
	// middleware lifts this to full K8s OpenAPI via 0023 Track B.
	//
	// Per-kind differentiation keeps each backend's schema recognizably
	// distinct in kubectl explain output, so the three-AA demo has
	// genuinely different shapes.
	specProps := map[string]any{
		"title": map[string]any{
			"type":        "string",
			"minLength":   3,
			"maxLength":   b.cfg.TitleMax,
			"description": fmt.Sprintf("Short display title for the %s. 3-%d characters.", b.cfg.Singular, b.cfg.TitleMax),
		},
		"description": map[string]any{
			"type":        "string",
			"description": fmt.Sprintf("Free-form description of the %s.", b.cfg.Singular),
		},
		fmt.Sprintf("%sColor", b.cfg.Singular): map[string]any{
			"type":        "string",
			"enum":        []any{"red", "green", "blue", "yellow"},
			"description": fmt.Sprintf("Color of the %s. Per-kind distinguishing field.", b.cfg.Singular),
		},
	}
	jsonSchema := map[string]any{
		"type":        "object",
		"description": b.cfg.Description,
		"properties": map[string]any{
			"spec": map[string]any{
				"type":        "object",
				"description": fmt.Sprintf("Caller-supplied fields for %s.", b.cfg.Singular),
				"required":    []any{"title"},
				"properties":  specProps,
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
		Group:                   b.cfg.Group,
		Version:                 b.cfg.Version,
		Resource:                b.cfg.Resource,
		Kind:                    b.cfg.Kind,
		Singular:                b.cfg.Singular,
		Namespaced:              b.cfg.Namespaced,
		Writable:                true,
		SupportsServerSideApply: true,
		OpenAPIV3:               raw,
		Columns: []tableColumn{
			{Name: "Name", Type: "string", Description: fmt.Sprintf("Name of the %s.", b.cfg.Singular)},
			{Name: "Title", Type: "string", Description: "Title."},
			{Name: "Color", Type: "string", Description: "Color."},
			{Name: "Age", Type: "string", Description: "Time since creation."},
		},
		RowFields: []string{
			".metadata.name",
			".spec.title",
			fmt.Sprintf(".spec.%sColor", b.cfg.Singular),
			".metadata.creationTimestamp",
		},
	})
}

func (b *backend) getHandler(w http.ResponseWriter, _ *http.Request, ns, name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, ok := b.items[key{ns, name}]
	if !ok {
		writeError(w, http.StatusNotFound,
			fmt.Sprintf("%s.%s %q not found", b.cfg.Resource, b.cfg.Group, name))
		return
	}
	writeJSON(w, http.StatusOK, n)
}

func (b *backend) listHandler(w http.ResponseWriter, _ *http.Request, ns string) {
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
	var n Object
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
	n.APIVersion = b.cfg.Group + "/" + b.cfg.Version
	n.Kind = b.cfg.Kind

	b.mu.Lock()
	defer b.mu.Unlock()
	k := key{n.Metadata.Namespace, n.Metadata.Name}
	if _, exists := b.items[k]; exists {
		writeError(w, http.StatusConflict,
			fmt.Sprintf("%s.%s %q already exists", b.cfg.Resource, b.cfg.Group, n.Metadata.Name))
		return
	}
	n.Metadata.UID = newUID()
	n.Metadata.CreationTimestamp = time.Now().UTC().Format(time.RFC3339)
	n.Status = json.RawMessage(fmt.Sprintf(`{"updatedAt":%q}`, n.Metadata.CreationTimestamp))
	b.items[k] = &n

	raw, _ := json.Marshal(&n)
	b.broadcastLocked("ADDED", raw)
	log.Printf("create user=%s name=%s ns=%s", userLabel(r), n.Metadata.Name, n.Metadata.Namespace)
	writeJSON(w, http.StatusCreated, &n)
}

func (b *backend) updateHandler(w http.ResponseWriter, r *http.Request, ns, name string) {
	var n Object
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
	n.APIVersion = b.cfg.Group + "/" + b.cfg.Version
	n.Kind = b.cfg.Kind

	forceAllowCreate := r.URL.Query().Get("forceAllowCreate") == "true"

	b.mu.Lock()
	defer b.mu.Unlock()
	k := key{n.Metadata.Namespace, name}
	existing, ok := b.items[k]
	if !ok {
		if !forceAllowCreate {
			writeError(w, http.StatusNotFound,
				fmt.Sprintf("%s.%s %q not found", b.cfg.Resource, b.cfg.Group, name))
			return
		}
		n.Metadata.UID = newUID()
		n.Metadata.CreationTimestamp = time.Now().UTC().Format(time.RFC3339)
		n.Status = json.RawMessage(fmt.Sprintf(`{"updatedAt":%q}`, n.Metadata.CreationTimestamp))
		b.items[k] = &n
		raw, _ := json.Marshal(&n)
		b.broadcastLocked("ADDED", raw)
		log.Printf("upsert-create user=%s name=%s", userLabel(r), n.Metadata.Name)
		writeJSON(w, http.StatusCreated, &n)
		return
	}
	n.Metadata.UID = existing.Metadata.UID
	n.Metadata.CreationTimestamp = existing.Metadata.CreationTimestamp
	n.Status = json.RawMessage(fmt.Sprintf(`{"updatedAt":%q}`, time.Now().UTC().Format(time.RFC3339)))
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
			fmt.Sprintf("%s.%s %q not found", b.cfg.Resource, b.cfg.Group, name))
		return
	}
	delete(b.items, k)
	raw, _ := json.Marshal(existing)
	b.broadcastLocked("DELETED", raw)
	log.Printf("delete user=%s name=%s", userLabel(r), name)
	writeJSON(w, http.StatusOK, existing)
}

func (b *backend) watchHandler(w http.ResponseWriter, r *http.Request, ns string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
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
		}
	}
}

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
		b.watchHandler(w, r, ns)
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "ok")
	})
	return mux
}

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

func envOr(name, fallback string) string {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	return v
}

func envBool(name string, fallback bool) bool {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

func envInt(name string, fallback int) int {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func main() {
	addrFlag := flag.String("addr", "", "HTTP listen address (overrides PORT env)")
	flag.Parse()

	cfg := config{
		Group:       envOr("GROUP", "aggexp.io"),
		Version:     envOr("VERSION", "v1"),
		Resource:    envOr("PLURAL", ""),
		Kind:        envOr("KIND", ""),
		Singular:    envOr("RESOURCE", ""),
		Namespaced:  envBool("NAMESPACED", true),
		TitleMax:    envInt("TITLE_MAX_LEN", 64),
		Description: envOr("DESCRIPTION", ""),
	}
	if cfg.Resource == "" || cfg.Kind == "" || cfg.Singular == "" {
		log.Fatalf("backend-http: RESOURCE, PLURAL, and KIND env vars are required (got singular=%q plural=%q kind=%q)",
			cfg.Singular, cfg.Resource, cfg.Kind)
	}
	if cfg.Description == "" {
		cfg.Description = fmt.Sprintf("A %s is an 0027 demo resource backed by an HTTP/JSON + SSE backend.", cfg.Kind)
	}

	addr := *addrFlag
	if addr == "" {
		addr = ":" + envOr("PORT", "8080")
	}

	b := newBackend(cfg)
	srv := &http.Server{
		Addr:              addr,
		Handler:           b.mux(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("backend-http listening on %s; kind=%s group=%s/%s plural=%s",
		addr, cfg.Kind, cfg.Group, cfg.Version, cfg.Resource)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
