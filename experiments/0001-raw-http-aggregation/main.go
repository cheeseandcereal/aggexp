// Phase 0 probe: a hand-rolled Go net/http server that pretends to be a
// Kubernetes aggregated API server for hellos.aggexp.io/v1.
//
// This is a probe, not a production server. It does not use k8s.io/apiserver.
// It does not verify client certs. The point is to observe what the kube
// aggregation layer + kubectl actually demand on the wire.
package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// ---------- minimal k8s-shaped types ----------

type typeMeta struct {
	Kind       string `json:"kind,omitempty"`
	APIVersion string `json:"apiVersion,omitempty"`
}

type objectMeta struct {
	Name              string `json:"name,omitempty"`
	Namespace         string `json:"namespace,omitempty"`
	UID               string `json:"uid,omitempty"`
	ResourceVersion   string `json:"resourceVersion,omitempty"`
	CreationTimestamp string `json:"creationTimestamp,omitempty"`
}

type listMeta struct {
	ResourceVersion string `json:"resourceVersion,omitempty"`
	Continue        string `json:"continue,omitempty"`
}

type helloSpec struct {
	Greeting string `json:"greeting"`
}

type hello struct {
	typeMeta
	Metadata objectMeta `json:"metadata"`
	Spec     helloSpec  `json:"spec"`
}

type helloList struct {
	typeMeta
	Metadata listMeta `json:"metadata"`
	Items    []hello  `json:"items"`
}

// APIResourceList / APIGroup / APIGroupList per metav1.

type apiResource struct {
	Name         string   `json:"name"`
	SingularName string   `json:"singularName"`
	Namespaced   bool     `json:"namespaced"`
	Kind         string   `json:"kind"`
	Verbs        []string `json:"verbs"`
	ShortNames   []string `json:"shortNames,omitempty"`
}

type apiResourceList struct {
	typeMeta
	GroupVersion string        `json:"groupVersion"`
	Resources    []apiResource `json:"resources"`
}

type groupVersionForDiscovery struct {
	GroupVersion string `json:"groupVersion"`
	Version      string `json:"version"`
}

type apiGroup struct {
	typeMeta
	Name             string                     `json:"name"`
	Versions         []groupVersionForDiscovery `json:"versions"`
	PreferredVersion groupVersionForDiscovery   `json:"preferredVersion"`
}

type apiGroupList struct {
	typeMeta
	Groups []apiGroup `json:"groups"`
}

// metav1.Status

type statusCause struct {
	Type    string `json:"type,omitempty"`
	Message string `json:"message,omitempty"`
	Field   string `json:"field,omitempty"`
}

type statusDetails struct {
	Name   string        `json:"name,omitempty"`
	Group  string        `json:"group,omitempty"`
	Kind   string        `json:"kind,omitempty"`
	Causes []statusCause `json:"causes,omitempty"`
}

type metaStatus struct {
	typeMeta
	Metadata listMeta       `json:"metadata"`
	Status   string         `json:"status,omitempty"`
	Message  string         `json:"message,omitempty"`
	Reason   string         `json:"reason,omitempty"`
	Details  *statusDetails `json:"details,omitempty"`
	Code     int            `json:"code,omitempty"`
}

// Table output (meta.k8s.io/v1)

type tableColumnDefinition struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Format      string `json:"format,omitempty"`
	Description string `json:"description,omitempty"`
	Priority    int    `json:"priority"`
}

type tableRow struct {
	Cells  []interface{}   `json:"cells"`
	Object json.RawMessage `json:"object,omitempty"`
}

type table struct {
	typeMeta
	Metadata          listMeta                `json:"metadata"`
	ColumnDefinitions []tableColumnDefinition `json:"columnDefinitions"`
	Rows              []tableRow              `json:"rows"`
}

// watch.Event

type watchEvent struct {
	Type   string          `json:"type"`
	Object json.RawMessage `json:"object"`
}

// ---------- static data ----------

var staticHellos = []hello{
	{
		typeMeta: typeMeta{Kind: "Hello", APIVersion: "aggexp.io/v1"},
		Metadata: objectMeta{
			Name:              "world",
			UID:               "00000000-0000-0000-0000-000000000001",
			ResourceVersion:   "1",
			CreationTimestamp: "2024-01-01T00:00:00Z",
		},
		Spec: helloSpec{Greeting: "Hello, world!"},
	},
	{
		typeMeta: typeMeta{Kind: "Hello", APIVersion: "aggexp.io/v1"},
		Metadata: objectMeta{
			Name:              "friend",
			UID:               "00000000-0000-0000-0000-000000000002",
			ResourceVersion:   "2",
			CreationTimestamp: "2024-01-01T00:00:00Z",
		},
		Spec: helloSpec{Greeting: "Hello, friend!"},
	},
}

// Monotonic RV counter used for BOOKMARK events. Seeded at 10 per spec.
var bookmarkRV atomic.Uint64

// bookmarkInterval: how often to emit BOOKMARK during an open watch.
const bookmarkInterval = 10 * time.Second

// ---------- helpers ----------

func logRequest(r *http.Request) {
	// Gather X-Remote-* headers (set by kube-apiserver's aggregator client
	// to convey the end-user identity) plus User-Agent.
	var parts []string
	for name, vals := range r.Header {
		if strings.HasPrefix(strings.ToLower(name), "x-remote-") {
			parts = append(parts, fmt.Sprintf("%s=%v", name, vals))
		}
	}
	log.Printf("request method=%s path=%s ua=%q x-remote=%v accept=%q",
		r.Method, r.URL.RequestURI(), r.UserAgent(), parts, r.Header.Get("Accept"))
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("writeJSON encode error: %v", err)
	}
}

func notFoundStatus(w http.ResponseWriter, resource, name string) {
	s := metaStatus{
		typeMeta: typeMeta{Kind: "Status", APIVersion: "v1"},
		Status:   "Failure",
		Message:  fmt.Sprintf("%s %q not found", resource, name),
		Reason:   "NotFound",
		Details: &statusDetails{
			Name:  name,
			Group: "aggexp.io",
			Kind:  resource,
		},
		Code: http.StatusNotFound,
	}
	writeJSON(w, http.StatusNotFound, s)
}

func genericNotFound(w http.ResponseWriter, r *http.Request) {
	s := metaStatus{
		typeMeta: typeMeta{Kind: "Status", APIVersion: "v1"},
		Status:   "Failure",
		Message:  fmt.Sprintf("the server could not find the requested resource (path=%s)", r.URL.Path),
		Reason:   "NotFound",
		Code:     http.StatusNotFound,
	}
	writeJSON(w, http.StatusNotFound, s)
}

// wantsTable returns true if the client asked for a Table response via the
// Accept header (kubectl's default get path).
func wantsTable(r *http.Request) bool {
	accept := r.Header.Get("Accept")
	// Accept can include multiple media types, comma separated.
	for _, part := range strings.Split(accept, ",") {
		p := strings.TrimSpace(strings.ToLower(part))
		if strings.Contains(p, "as=table") && strings.Contains(p, "meta.k8s.io") {
			return true
		}
	}
	return false
}

func helloAsRaw(h hello) json.RawMessage {
	b, _ := json.Marshal(h)
	return json.RawMessage(b)
}

func helloListAsTable(hs []hello) table {
	cols := []tableColumnDefinition{
		{Name: "Name", Type: "string", Format: "name", Description: "Name of the Hello"},
		{Name: "Greeting", Type: "string", Description: "Greeting spec field"},
	}
	rows := make([]tableRow, 0, len(hs))
	for _, h := range hs {
		rows = append(rows, tableRow{
			Cells:  []interface{}{h.Metadata.Name, h.Spec.Greeting},
			Object: helloAsRaw(h),
		})
	}
	return table{
		typeMeta:          typeMeta{Kind: "Table", APIVersion: "meta.k8s.io/v1"},
		Metadata:          listMeta{ResourceVersion: "1"},
		ColumnDefinitions: cols,
		Rows:              rows,
	}
}

// ---------- handlers ----------

func handleAPIs(w http.ResponseWriter, r *http.Request) {
	logRequest(r)
	gl := apiGroupList{
		typeMeta: typeMeta{Kind: "APIGroupList", APIVersion: "v1"},
		Groups: []apiGroup{{
			Name:             "aggexp.io",
			Versions:         []groupVersionForDiscovery{{GroupVersion: "aggexp.io/v1", Version: "v1"}},
			PreferredVersion: groupVersionForDiscovery{GroupVersion: "aggexp.io/v1", Version: "v1"},
		}},
	}
	writeJSON(w, http.StatusOK, gl)
}

func handleAPIGroup(w http.ResponseWriter, r *http.Request) {
	logRequest(r)
	g := apiGroup{
		typeMeta:         typeMeta{Kind: "APIGroup", APIVersion: "v1"},
		Name:             "aggexp.io",
		Versions:         []groupVersionForDiscovery{{GroupVersion: "aggexp.io/v1", Version: "v1"}},
		PreferredVersion: groupVersionForDiscovery{GroupVersion: "aggexp.io/v1", Version: "v1"},
	}
	writeJSON(w, http.StatusOK, g)
}

func handleAPIResourceList(w http.ResponseWriter, r *http.Request) {
	logRequest(r)
	rl := apiResourceList{
		typeMeta:     typeMeta{Kind: "APIResourceList", APIVersion: "v1"},
		GroupVersion: "aggexp.io/v1",
		Resources: []apiResource{{
			Name:         "hellos",
			SingularName: "hello",
			Namespaced:   false,
			Kind:         "Hello",
			Verbs:        []string{"get", "list", "watch"},
			ShortNames:   []string{"hi"},
		}},
	}
	writeJSON(w, http.StatusOK, rl)
}

func handleHellos(w http.ResponseWriter, r *http.Request) {
	logRequest(r)
	// Watch branch
	if r.URL.Query().Get("watch") == "true" || r.URL.Query().Get("watch") == "1" {
		handleHellosWatch(w, r)
		return
	}
	// Table branch
	if wantsTable(r) {
		writeJSON(w, http.StatusOK, helloListAsTable(staticHellos))
		return
	}
	// Normal list
	hl := helloList{
		typeMeta: typeMeta{Kind: "HelloList", APIVersion: "aggexp.io/v1"},
		Metadata: listMeta{ResourceVersion: "1"},
		Items:    staticHellos,
	}
	writeJSON(w, http.StatusOK, hl)
}

func handleHello(w http.ResponseWriter, r *http.Request, name string) {
	logRequest(r)
	for _, h := range staticHellos {
		if h.Metadata.Name == name {
			if wantsTable(r) {
				writeJSON(w, http.StatusOK, helloListAsTable([]hello{h}))
				return
			}
			writeJSON(w, http.StatusOK, h)
			return
		}
	}
	notFoundStatus(w, "hellos", name)
}

func handleHellosWatch(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		log.Printf("watch: http.Flusher not supported by ResponseWriter")
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	enc := json.NewEncoder(w)

	// Emit initial ADDED events for each static hello.
	for _, h := range staticHellos {
		ev := watchEvent{Type: "ADDED", Object: helloAsRaw(h)}
		if err := enc.Encode(ev); err != nil {
			log.Printf("watch: encode ADDED err: %v", err)
			return
		}
		flusher.Flush()
	}
	log.Printf("watch: sent initial ADDED events (%d items)", len(staticHellos))

	ctx := r.Context()
	ticker := time.NewTicker(bookmarkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("watch: client disconnected: %v", ctx.Err())
			return
		case <-ticker.C:
			rv := bookmarkRV.Add(1)
			bookmark := hello{
				typeMeta: typeMeta{Kind: "Hello", APIVersion: "aggexp.io/v1"},
				Metadata: objectMeta{ResourceVersion: fmt.Sprintf("%d", rv)},
			}
			ev := watchEvent{Type: "BOOKMARK", Object: helloAsRaw(bookmark)}
			if err := enc.Encode(ev); err != nil {
				log.Printf("watch: encode BOOKMARK err: %v", err)
				return
			}
			flusher.Flush()
			log.Printf("watch: sent BOOKMARK rv=%d", rv)
		}
	}
}

// ---------- OpenAPI ----------

func handleOpenAPIv2(w http.ResponseWriter, r *http.Request) {
	logRequest(r)
	doc := map[string]interface{}{
		"swagger": "2.0",
		"info": map[string]interface{}{
			"title":   "aggexp probe",
			"version": "v1",
		},
		"paths": map[string]interface{}{
			"/apis/aggexp.io/v1/hellos": map[string]interface{}{
				"get": map[string]interface{}{
					"summary":     "list Hellos",
					"operationId": "listHellos",
					"produces":    []string{"application/json"},
					"responses": map[string]interface{}{
						"200": map[string]interface{}{
							"description": "OK",
							"schema":      map[string]interface{}{"$ref": "#/definitions/HelloList"},
						},
					},
				},
			},
			"/apis/aggexp.io/v1/hellos/{name}": map[string]interface{}{
				"get": map[string]interface{}{
					"summary":     "read a Hello",
					"operationId": "readHello",
					"produces":    []string{"application/json"},
					"parameters": []map[string]interface{}{
						{"name": "name", "in": "path", "required": true, "type": "string"},
					},
					"responses": map[string]interface{}{
						"200": map[string]interface{}{
							"description": "OK",
							"schema":      map[string]interface{}{"$ref": "#/definitions/Hello"},
						},
					},
				},
			},
		},
		"definitions": map[string]interface{}{
			"Hello": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"apiVersion": map[string]interface{}{"type": "string"},
					"kind":       map[string]interface{}{"type": "string"},
					"metadata":   map[string]interface{}{"type": "object"},
					"spec": map[string]interface{}{
						"type": "object",
						"properties": map[string]interface{}{
							"greeting": map[string]interface{}{"type": "string"},
						},
					},
				},
			},
			"HelloList": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"apiVersion": map[string]interface{}{"type": "string"},
					"kind":       map[string]interface{}{"type": "string"},
					"metadata":   map[string]interface{}{"type": "object"},
					"items": map[string]interface{}{
						"type":  "array",
						"items": map[string]interface{}{"$ref": "#/definitions/Hello"},
					},
				},
			},
		},
	}
	writeJSON(w, http.StatusOK, doc)
}

func handleOpenAPIv3Root(w http.ResponseWriter, r *http.Request) {
	logRequest(r)
	doc := map[string]interface{}{
		"paths": map[string]interface{}{
			"apis/aggexp.io/v1": map[string]interface{}{
				"serverRelativeURL": "/openapi/v3/apis/aggexp.io/v1",
			},
		},
	}
	writeJSON(w, http.StatusOK, doc)
}

func handleOpenAPIv3Group(w http.ResponseWriter, r *http.Request) {
	logRequest(r)
	doc := map[string]interface{}{
		"openapi": "3.0.0",
		"info": map[string]interface{}{
			"title":   "aggexp probe",
			"version": "v1",
		},
		"paths": map[string]interface{}{
			"/apis/aggexp.io/v1/hellos": map[string]interface{}{
				"get": map[string]interface{}{
					"summary":     "list Hellos",
					"operationId": "listHellos",
					"responses": map[string]interface{}{
						"200": map[string]interface{}{
							"description": "OK",
							"content": map[string]interface{}{
								"application/json": map[string]interface{}{
									"schema": map[string]interface{}{
										"$ref": "#/components/schemas/HelloList",
									},
								},
							},
						},
					},
				},
			},
			"/apis/aggexp.io/v1/hellos/{name}": map[string]interface{}{
				"get": map[string]interface{}{
					"summary":     "read a Hello",
					"operationId": "readHello",
					"parameters": []map[string]interface{}{
						{"name": "name", "in": "path", "required": true, "schema": map[string]interface{}{"type": "string"}},
					},
					"responses": map[string]interface{}{
						"200": map[string]interface{}{
							"description": "OK",
							"content": map[string]interface{}{
								"application/json": map[string]interface{}{
									"schema": map[string]interface{}{
										"$ref": "#/components/schemas/Hello",
									},
								},
							},
						},
					},
				},
			},
		},
		"components": map[string]interface{}{
			"schemas": map[string]interface{}{
				"Hello": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"apiVersion": map[string]interface{}{"type": "string"},
						"kind":       map[string]interface{}{"type": "string"},
						"metadata":   map[string]interface{}{"type": "object"},
						"spec": map[string]interface{}{
							"type": "object",
							"properties": map[string]interface{}{
								"greeting": map[string]interface{}{"type": "string"},
							},
						},
					},
				},
				"HelloList": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"apiVersion": map[string]interface{}{"type": "string"},
						"kind":       map[string]interface{}{"type": "string"},
						"metadata":   map[string]interface{}{"type": "object"},
						"items": map[string]interface{}{
							"type":  "array",
							"items": map[string]interface{}{"$ref": "#/components/schemas/Hello"},
						},
					},
				},
			},
		},
	}
	writeJSON(w, http.StatusOK, doc)
}

// ---------- health ----------

func handlePlainOK(w http.ResponseWriter, r *http.Request) {
	logRequest(r)
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// ---------- mux ----------

func newMux() *http.ServeMux {
	mux := http.NewServeMux()

	// Discovery
	mux.HandleFunc("/apis", handleAPIs)
	mux.HandleFunc("/apis/aggexp.io", handleAPIGroup)
	mux.HandleFunc("/apis/aggexp.io/v1", handleAPIResourceList)

	// Health
	mux.HandleFunc("/livez", handlePlainOK)
	mux.HandleFunc("/readyz", handlePlainOK)

	// OpenAPI
	mux.HandleFunc("/openapi/v2", handleOpenAPIv2)
	mux.HandleFunc("/openapi/v3", handleOpenAPIv3Root)
	mux.HandleFunc("/openapi/v3/apis/aggexp.io/v1", handleOpenAPIv3Group)

	// Resources under /apis/aggexp.io/v1/... — use a prefix handler so we
	// can split list vs. single-item path manually (ServeMux's pattern
	// language doesn't do captures in go 1.22 without Go 1.22-style method
	// patterns which we aren't relying on).
	mux.HandleFunc("/apis/aggexp.io/v1/", func(w http.ResponseWriter, r *http.Request) {
		const prefix = "/apis/aggexp.io/v1/"
		rest := strings.TrimPrefix(r.URL.Path, prefix)
		// rest is e.g. "hellos" or "hellos/world" or "" (trailing slash)
		switch {
		case rest == "" || rest == "hellos":
			handleHellos(w, r)
		case strings.HasPrefix(rest, "hellos/"):
			name := strings.TrimPrefix(rest, "hellos/")
			// reject further subpaths — this is a singleton read
			if strings.Contains(name, "/") {
				logRequest(r)
				genericNotFound(w, r)
				return
			}
			handleHello(w, r, name)
		default:
			logRequest(r)
			genericNotFound(w, r)
		}
	})

	// Catch-all
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		logRequest(r)
		genericNotFound(w, r)
	})

	return mux
}

// ---------- main ----------

func main() {
	bindAddr := flag.String("bind-address", "0.0.0.0", "address to bind")
	securePort := flag.Int("secure-port", 8443, "HTTPS port")
	certFile := flag.String("tls-cert-file", "/etc/aggexp/certs/tls.crt", "TLS serving cert")
	keyFile := flag.String("tls-private-key-file", "/etc/aggexp/certs/tls.key", "TLS private key")
	flag.Parse()

	bookmarkRV.Store(10)

	addr := fmt.Sprintf("%s:%d", *bindAddr, *securePort)

	srv := &http.Server{
		Addr:              addr,
		Handler:           newMux(),
		ReadHeaderTimeout: 10 * time.Second,
		TLSConfig: &tls.Config{
			// Deliberate: we do NOT authenticate kube-apiserver's mTLS cert.
			// Probe goal is observation, not authz.
			ClientAuth: tls.NoClientCert,
			MinVersion: tls.VersionTLS12,
		},
	}

	log.Printf("aggexp probe starting on %s (cert=%s key=%s)", addr, *certFile, *keyFile)
	if err := srv.ListenAndServeTLS(*certFile, *keyFile); err != nil {
		log.Printf("server exited: %v", err)
		os.Exit(1)
	}
}
