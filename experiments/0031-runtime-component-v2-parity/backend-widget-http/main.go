// Command backend-widget-http is the 0031 HTTP/SSE backend for the
// widgets.aggexp.io/v1 API. Stdlib-only; no framework dependency.
// Per-kind boilerplate is baked in (this binary serves widgets and
// widgets only).
//
// Wire shape matches runtime/component/v2/httpbackend (= 0026/0027):
//
//	GET  /schema
//	GET  /objects/{ns}/{name}
//	GET  /objects/{ns}
//	POST /objects/{ns}
//	PUT  /objects/{ns}/{name}  [?forceAllowCreate=true]
//	DELETE /objects/{ns}/{name}
//	GET  /watch/{ns}         (text/event-stream)
//	GET  /healthz
package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	group    = "widgets.aggexp.io"
	version  = "v1"
	resource = "widgets"
	kind     = "Widget"
	singular = "widget"
)

type meta struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace,omitempty"`
	UID               string            `json:"uid,omitempty"`
	CreationTimestamp string            `json:"creationTimestamp,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
}

type object struct {
	APIVersion string          `json:"apiVersion"`
	Kind       string          `json:"kind"`
	Metadata   meta            `json:"metadata"`
	Spec       json.RawMessage `json:"spec,omitempty"`
	Status     json.RawMessage `json:"status,omitempty"`
}

type key struct{ ns, name string }

type backend struct {
	mu      sync.Mutex
	items   map[key]*object
	watches map[int]chan *watchEvent
	nextWID int
}

type watchEvent struct {
	Type   string          `json:"type"`
	Object json.RawMessage `json:"object"`
}

func newUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// JSON schema describing spec+status. Middleware lifts to K8s OpenAPI
// via Track B in openapi.Synthesize. Widget-specific fields: title,
// color (red/green/blue/yellow — intentionally NOT including black,
// which is the declarative-admission denial rule).
var widgetSchema = mustJSON(map[string]any{
	"type":        "object",
	"description": "A Widget is a 0031 demo resource served by runtime/component/v2 over HTTP/SSE.",
	"properties": map[string]any{
		"spec": map[string]any{
			"type":        "object",
			"description": "Caller-supplied widget fields.",
			"required":    []any{},
			"properties": map[string]any{
				"title": map[string]any{
					"type":        "string",
					"description": "Display title.",
					"minLength":   1,
					"maxLength":   128,
				},
				"color": map[string]any{
					"type":        "string",
					"description": "Widget color.",
					"enum":        []any{"red", "green", "blue", "yellow", "black"},
				},
				"size": map[string]any{
					"type":        "integer",
					"description": "Arbitrary integer size.",
					"minimum":     0,
				},
			},
		},
		"status": map[string]any{
			"type":        "object",
			"description": "Server-observed status.",
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

type schemaResp struct {
	Group                   string            `json:"group"`
	Version                 string            `json:"version"`
	Resource                string            `json:"resource"`
	Kind                    string            `json:"kind"`
	Singular                string            `json:"singular"`
	Namespaced              bool              `json:"namespaced"`
	Writable                bool              `json:"writable"`
	SupportsServerSideApply bool              `json:"supportsServerSideApply"`
	SchemaIsOpenAPI         bool              `json:"schemaIsOpenapi"`
	Schema                  json.RawMessage   `json:"schema"`
	Columns                 []col             `json:"columns,omitempty"`
	RowFields               []string          `json:"rowFields,omitempty"`
	ShortNames              []string          `json:"shortNames,omitempty"`
	Categories              []string          `json:"categories,omitempty"`
	WatchCapability         string            `json:"watchCapability,omitempty"`
	ExtraFields             map[string]string `json:"-"`
}

type col struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Format      string `json:"format,omitempty"`
	Description string `json:"description,omitempty"`
	Priority    int32  `json:"priority,omitempty"`
}

func (b *backend) schemaH(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, &schemaResp{
		Group:                   group,
		Version:                 version,
		Resource:                resource,
		Kind:                    kind,
		Singular:                singular,
		Namespaced:              true,
		Writable:                true,
		SupportsServerSideApply: true,
		SchemaIsOpenAPI:         false,
		Schema:                  widgetSchema,
		WatchCapability:         "push",
		Columns: []col{
			{Name: "Name", Type: "string", Description: "Widget name"},
			{Name: "Title", Type: "string", Description: "Title"},
			{Name: "Color", Type: "string", Description: "Color"},
			{Name: "Size", Type: "integer", Description: "Size"},
			{Name: "Age", Type: "string", Description: "Time since creation"},
		},
		RowFields: []string{
			".metadata.name",
			".spec.title",
			".spec.color",
			".spec.size",
			".metadata.creationTimestamp",
		},
	})
}

func (b *backend) getH(w http.ResponseWriter, _ *http.Request, ns, name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	obj, ok := b.items[key{ns, name}]
	if !ok {
		writeErr(w, http.StatusNotFound, fmt.Sprintf("widget %q not found", name))
		return
	}
	writeJSON(w, http.StatusOK, obj)
}

func (b *backend) listH(w http.ResponseWriter, _ *http.Request, ns string) {
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
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}

func (b *backend) createH(w http.ResponseWriter, r *http.Request, ns string) {
	var obj object
	if err := json.NewDecoder(r.Body).Decode(&obj); err != nil {
		writeErr(w, http.StatusBadRequest, "decode: "+err.Error())
		return
	}
	if obj.Metadata.Name == "" {
		writeErr(w, http.StatusBadRequest, "metadata.name required")
		return
	}
	if ns != "" {
		obj.Metadata.Namespace = ns
	}
	obj.APIVersion = group + "/" + version
	obj.Kind = kind
	b.mu.Lock()
	defer b.mu.Unlock()
	k := key{obj.Metadata.Namespace, obj.Metadata.Name}
	if _, exists := b.items[k]; exists {
		writeErr(w, http.StatusConflict, fmt.Sprintf("widget %q exists", obj.Metadata.Name))
		return
	}
	obj.Metadata.UID = newUID()
	obj.Metadata.CreationTimestamp = time.Now().UTC().Format(time.RFC3339)
	obj.Status = json.RawMessage(fmt.Sprintf(`{"updatedAt":%q}`, obj.Metadata.CreationTimestamp))
	b.items[k] = &obj
	raw, _ := json.Marshal(&obj)
	b.broadcastLocked("ADDED", raw)
	log.Printf("widget create ns=%s name=%s", obj.Metadata.Namespace, obj.Metadata.Name)
	writeJSON(w, http.StatusCreated, &obj)
}

func (b *backend) updateH(w http.ResponseWriter, r *http.Request, ns, name string) {
	var obj object
	if err := json.NewDecoder(r.Body).Decode(&obj); err != nil {
		writeErr(w, http.StatusBadRequest, "decode: "+err.Error())
		return
	}
	if ns != "" {
		obj.Metadata.Namespace = ns
	}
	if obj.Metadata.Name == "" {
		obj.Metadata.Name = name
	}
	obj.APIVersion = group + "/" + version
	obj.Kind = kind
	forceCreate := r.URL.Query().Get("forceAllowCreate") == "true"
	b.mu.Lock()
	defer b.mu.Unlock()
	k := key{obj.Metadata.Namespace, name}
	existing, ok := b.items[k]
	if !ok {
		if !forceCreate {
			writeErr(w, http.StatusNotFound, fmt.Sprintf("widget %q not found", name))
			return
		}
		obj.Metadata.UID = newUID()
		obj.Metadata.CreationTimestamp = time.Now().UTC().Format(time.RFC3339)
		obj.Status = json.RawMessage(fmt.Sprintf(`{"updatedAt":%q}`, obj.Metadata.CreationTimestamp))
		b.items[k] = &obj
		raw, _ := json.Marshal(&obj)
		b.broadcastLocked("ADDED", raw)
		writeJSON(w, http.StatusCreated, &obj)
		return
	}
	obj.Metadata.UID = existing.Metadata.UID
	obj.Metadata.CreationTimestamp = existing.Metadata.CreationTimestamp
	obj.Status = json.RawMessage(fmt.Sprintf(`{"updatedAt":%q}`, time.Now().UTC().Format(time.RFC3339)))
	b.items[k] = &obj
	raw, _ := json.Marshal(&obj)
	b.broadcastLocked("MODIFIED", raw)
	log.Printf("widget update ns=%s name=%s", obj.Metadata.Namespace, name)
	writeJSON(w, http.StatusOK, &obj)
}

func (b *backend) deleteH(w http.ResponseWriter, _ *http.Request, ns, name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	k := key{ns, name}
	existing, ok := b.items[k]
	if !ok {
		writeErr(w, http.StatusNotFound, fmt.Sprintf("widget %q not found", name))
		return
	}
	delete(b.items, k)
	raw, _ := json.Marshal(existing)
	b.broadcastLocked("DELETED", raw)
	log.Printf("widget delete ns=%s name=%s", ns, name)
	writeJSON(w, http.StatusOK, existing)
}

func (b *backend) watchH(w http.ResponseWriter, r *http.Request, ns string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "streaming unsupported")
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
	for k, obj := range b.items {
		if ns != "" && k.ns != ns {
			continue
		}
		raw, _ := json.Marshal(obj)
		initial = append(initial, &watchEvent{Type: "ADDED", Object: raw})
	}
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.watches, wid)
		close(ch)
		b.mu.Unlock()
	}()

	for _, ev := range initial {
		if err := sseSend(w, flusher, ev); err != nil {
			return
		}
	}
	ctx := r.Context()
	t := time.NewTicker(20 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, open := <-ch:
			if !open {
				return
			}
			if err := sseSend(w, flusher, ev); err != nil {
				return
			}
		case <-t.C:
			if _, err := fmt.Fprintf(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func sseSend(w http.ResponseWriter, f http.Flusher, ev *watchEvent) error {
	raw, _ := json.Marshal(ev)
	if _, err := fmt.Fprintf(w, "data: %s\n\n", raw); err != nil {
		return err
	}
	f.Flush()
	return nil
}

func (b *backend) broadcastLocked(t string, raw []byte) {
	ev := &watchEvent{Type: t, Object: raw}
	for _, ch := range b.watches {
		select {
		case ch <- ev:
		default:
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"message": msg})
}

func main() {
	b := &backend{
		items:   map[key]*object{},
		watches: map[int]chan *watchEvent{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/schema", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "GET")
			return
		}
		b.schemaH(w, r)
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
			b.listH(w, r, ns)
		case name == "" && r.Method == http.MethodPost:
			b.createH(w, r, ns)
		case name != "" && r.Method == http.MethodGet:
			b.getH(w, r, ns, name)
		case name != "" && r.Method == http.MethodPut:
			b.updateH(w, r, ns, name)
		case name != "" && r.Method == http.MethodDelete:
			b.deleteH(w, r, ns, name)
		default:
			writeErr(w, http.StatusMethodNotAllowed, r.Method+" "+r.URL.Path)
		}
	})
	mux.HandleFunc("/watch/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "GET")
			return
		}
		ns := strings.TrimPrefix(r.URL.Path, "/watch/")
		b.watchH(w, r, ns)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "ok")
	})
	srv := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Printf("widget-http backend listening on :8080")
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
