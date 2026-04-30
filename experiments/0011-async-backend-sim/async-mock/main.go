// Command async-mock is a tiny stdlib HTTP service that simulates a
// cloud backend with async provisioning semantics. It stands in for
// the "async AWS" case (EKS cluster creation, IAM propagation, RDS)
// described in FINDINGS/0009-ack-aggregated-s3.md as the boundary
// where the stateless-AA model starts to strain.
//
// Wire surface:
//
//	GET    /widgets        list widgets (each with computed phase)
//	POST   /widgets        create; returns 202 + Widget{phase=Provisioning}
//	GET    /widgets/{name} get one
//	PUT    /widgets/{name} update; phase resets to Provisioning
//	DELETE /widgets/{name} start deprovision; phase=Deleting for 10s
//
// Phase is computed from the backing record's LastChangeTime:
//
//	< ProvisionDelay from LastChange  -> "Provisioning"
//	>= ProvisionDelay                 -> "Ready"  (ObservedState == DesiredState)
//
// Deletion runs a timer that removes the record after DeleteDelay.
// While the record is marked for deletion its Phase is "Deleting".
//
// All state is in-process and lost on restart. That is fine; the
// experiment is specifically about the AA's behavior when the
// backend takes real time, not about backend durability.
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Widget is the on-the-wire shape the mock serves. The AA's Go
// types mirror this. We keep it dependency-free so the mock can run
// without the apiserver's imports.
type Widget struct {
	Name          string            `json:"name"`
	DesiredState  string            `json:"desiredState"`
	Config        map[string]string `json:"config,omitempty"`
	Phase         string            `json:"phase"`
	ObservedState string            `json:"observedState,omitempty"`
	ReadyAt       *time.Time        `json:"readyAt,omitempty"`
	Message       string            `json:"message,omitempty"`
	CreatedAt     time.Time         `json:"createdAt"`
	UpdatedAt     time.Time         `json:"updatedAt"`
}

type record struct {
	Name          string
	DesiredState  string
	Config        map[string]string
	CreatedAt     time.Time
	LastChange    time.Time // last create/update; anchors the provision window
	Deleting      bool
	DeletionStart time.Time
}

type store struct {
	mu              sync.Mutex
	items           map[string]*record
	provisionDelay  time.Duration
	deleteDelay     time.Duration
	requestLogTimes []time.Time // for lightweight overhead visibility
}

func newStore(provision, del time.Duration) *store {
	return &store{
		items:          map[string]*record{},
		provisionDelay: provision,
		deleteDelay:    del,
	}
}

func (s *store) computePhase(r *record, now time.Time) (phase, observed string, readyAt *time.Time) {
	if r.Deleting {
		return "Deleting", "", nil
	}
	elapsed := now.Sub(r.LastChange)
	if elapsed < s.provisionDelay {
		return "Provisioning", "", nil
	}
	t := r.LastChange.Add(s.provisionDelay)
	return "Ready", r.DesiredState, &t
}

func (s *store) snapshot(r *record, now time.Time) Widget {
	phase, observed, readyAt := s.computePhase(r, now)
	return Widget{
		Name:          r.Name,
		DesiredState:  r.DesiredState,
		Config:        copyMap(r.Config),
		Phase:         phase,
		ObservedState: observed,
		ReadyAt:       readyAt,
		CreatedAt:     r.CreatedAt,
		UpdatedAt:     r.LastChange,
	}
}

func copyMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// Reap removes records whose deletion timer has elapsed. Called
// from every handler so we don't need a separate goroutine.
func (s *store) reapLocked(now time.Time) {
	for name, r := range s.items {
		if r.Deleting && now.Sub(r.DeletionStart) >= s.deleteDelay {
			delete(s.items, name)
		}
	}
}

// ---- handlers ----

type createBody struct {
	Name         string            `json:"name"`
	DesiredState string            `json:"desiredState"`
	Config       map[string]string `json:"config,omitempty"`
}

func (s *store) handleCreate(w http.ResponseWriter, r *http.Request) {
	var body createBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "decode body: "+err.Error())
		return
	}
	if body.Name == "" {
		writeError(w, http.StatusBadRequest, "name required")
		return
	}
	now := time.Now()
	s.mu.Lock()
	s.reapLocked(now)
	if _, exists := s.items[body.Name]; exists {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, "widget "+body.Name+" already exists")
		return
	}
	rec := &record{
		Name:         body.Name,
		DesiredState: body.DesiredState,
		Config:       copyMap(body.Config),
		CreatedAt:    now,
		LastChange:   now,
	}
	s.items[body.Name] = rec
	snap := s.snapshot(rec, now)
	s.mu.Unlock()
	writeJSON(w, http.StatusAccepted, snap)
}

func (s *store) handleList(w http.ResponseWriter, _ *http.Request) {
	now := time.Now()
	s.mu.Lock()
	s.reapLocked(now)
	out := make([]Widget, 0, len(s.items))
	for _, r := range s.items {
		out = append(out, s.snapshot(r, now))
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}

func (s *store) handleGet(w http.ResponseWriter, _ *http.Request, name string) {
	now := time.Now()
	s.mu.Lock()
	s.reapLocked(now)
	r, ok := s.items[name]
	if !ok {
		s.mu.Unlock()
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	snap := s.snapshot(r, now)
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, snap)
}

type updateBody struct {
	DesiredState string            `json:"desiredState"`
	Config       map[string]string `json:"config,omitempty"`
}

func (s *store) handleUpdate(w http.ResponseWriter, r *http.Request, name string) {
	var body updateBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "decode body: "+err.Error())
		return
	}
	now := time.Now()
	s.mu.Lock()
	s.reapLocked(now)
	rec, ok := s.items[name]
	if !ok {
		s.mu.Unlock()
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	changed := rec.DesiredState != body.DesiredState || !mapEqual(rec.Config, body.Config)
	rec.DesiredState = body.DesiredState
	rec.Config = copyMap(body.Config)
	rec.Deleting = false
	if changed {
		rec.LastChange = now
	}
	snap := s.snapshot(rec, now)
	s.mu.Unlock()
	// 202 if we reset the timer, 200 if no change (idempotent).
	status := http.StatusAccepted
	if !changed {
		status = http.StatusOK
	}
	writeJSON(w, status, snap)
}

func (s *store) handleDelete(w http.ResponseWriter, _ *http.Request, name string) {
	now := time.Now()
	s.mu.Lock()
	s.reapLocked(now)
	rec, ok := s.items[name]
	if !ok {
		s.mu.Unlock()
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if !rec.Deleting {
		rec.Deleting = true
		rec.DeletionStart = now
	}
	snap := s.snapshot(rec, now)
	s.mu.Unlock()
	writeJSON(w, http.StatusAccepted, snap)
}

// ---- router ----

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func mapEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	provision := flag.Duration("provision-delay", 30*time.Second,
		"how long a create/update stays in Provisioning before going Ready")
	del := flag.Duration("delete-delay", 10*time.Second,
		"how long a deleted widget lingers in Deleting phase before final removal")
	flag.Parse()

	s := newStore(*provision, *del)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/widgets", func(w http.ResponseWriter, r *http.Request) {
		logReq(r)
		switch r.Method {
		case http.MethodGet:
			s.handleList(w, r)
		case http.MethodPost:
			s.handleCreate(w, r)
		default:
			writeError(w, http.StatusMethodNotAllowed, r.Method+" /widgets not allowed")
		}
	})
	mux.HandleFunc("/widgets/", func(w http.ResponseWriter, r *http.Request) {
		logReq(r)
		name := strings.TrimPrefix(r.URL.Path, "/widgets/")
		if name == "" || strings.Contains(name, "/") {
			writeError(w, http.StatusBadRequest, "bad widget name")
			return
		}
		switch r.Method {
		case http.MethodGet:
			s.handleGet(w, r, name)
		case http.MethodPut:
			s.handleUpdate(w, r, name)
		case http.MethodDelete:
			s.handleDelete(w, r, name)
		default:
			writeError(w, http.StatusMethodNotAllowed, r.Method+" /widgets/"+name+" not allowed")
		}
	})

	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Printf("async-mock listening on %s (provision=%s delete=%s)",
		*addr, *provision, *del)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func logReq(r *http.Request) {
	log.Printf("%s %s", r.Method, r.URL.Path)
}
