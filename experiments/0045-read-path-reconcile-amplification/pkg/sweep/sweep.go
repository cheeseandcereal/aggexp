// Package sweep is experiment 0045's periodic reconcile sweep plus a
// debug HTTP server. The sweep is the 0028 shape: on a fixed
// interval it calls the REST adapter's ReconcileList (the SAME
// reconcile the inline read path uses), so the inline path and the
// sweep agree by construction — they share one code path and one set
// of toggles.
//
// The debug server (default :8444) exposes:
//
//	GET  /metrics        — counters snapshot + reconcile policy
//	POST /sweep          — run one sweep synchronously, return result
//	POST /reset          — zero the counters (between amplification runs)
//	POST /adopt?on=true  — toggle adoption (inline + sweep)
//	POST /gc?on=false    — toggle collection (inline + sweep)
//	POST /negcache?on=true — toggle the backend negative-existence cache
//
// It is unauthenticated; fine for the lab (same posture as 0028's
// :8444 endpoint).
package sweep

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"k8s.io/klog/v2"

	"github.com/cheeseandcereal/aggexp/experiments/0045-read-path-reconcile-amplification/pkg/backend"
	"github.com/cheeseandcereal/aggexp/experiments/0045-read-path-reconcile-amplification/pkg/metrics"
	"github.com/cheeseandcereal/aggexp/experiments/0045-read-path-reconcile-amplification/pkg/widgetrest"
)

// Sweeper runs periodic reconciles and serves the debug endpoint.
type Sweeper struct {
	rest     *widgetrest.REST
	bodies   *backend.Store
	counters *metrics.Counters
	interval time.Duration

	mu   sync.Mutex
	last Result
}

// Result is one sweep's outcome.
type Result struct {
	Trigger      string    `json:"trigger"`
	StartedAt    time.Time `json:"startedAt"`
	DurationMS   float64   `json:"durationMs"`
	BackendCount int       `json:"backendCount"`
	RecordCount  int       `json:"recordCount"`
}

// Options configures a Sweeper.
type Options struct {
	REST     *widgetrest.REST
	Bodies   *backend.Store
	Counters *metrics.Counters
	Interval time.Duration
}

// New constructs a Sweeper.
func New(opts Options) *Sweeper {
	if opts.Interval <= 0 {
		opts.Interval = 2 * time.Minute
	}
	return &Sweeper{
		rest:     opts.REST,
		bodies:   opts.Bodies,
		counters: opts.Counters,
		interval: opts.Interval,
	}
}

// RunPeriodic loops until ctx is done, running a sweep each interval.
func (s *Sweeper) RunPeriodic(ctx context.Context) {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.runOnce(ctx, "periodic")
		}
	}
}

func (s *Sweeper) runOnce(ctx context.Context, trigger string) Result {
	start := time.Now()
	// Cluster-wide reconcile (ns="" → all namespaces). fromSweep=true
	// routes counters to the sweep buckets.
	backendRefs, recByKey := s.rest.ReconcileList(ctx, "", true)
	res := Result{
		Trigger:      trigger,
		StartedAt:    start.UTC(),
		DurationMS:   float64(time.Since(start).Microseconds()) / 1000.0,
		BackendCount: len(backendRefs),
		RecordCount:  len(recByKey),
	}
	klog.InfoS("sweep", "trigger", trigger, "backend", res.BackendCount, "records", res.RecordCount, "durationMs", res.DurationMS)
	s.mu.Lock()
	s.last = res
	s.mu.Unlock()
	return res
}

// ServeDebug starts the debug HTTP server on addr. Blocks until ctx
// is done.
func (s *Sweeper) ServeDebug(ctx context.Context, addr string) error {
	mux := http.NewServeMux()

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		out := struct {
			Counters metrics.Snapshot   `json:"counters"`
			Policy   widgetrest.Policy  `json:"policy"`
			NegCache bool               `json:"negCacheEnabled"`
			LastSweep Result            `json:"lastSweep"`
		}{
			Counters:  s.counters.Snapshot(),
			Policy:    s.rest.GetPolicy(),
			NegCache:  s.bodies.NegCacheEnabled(),
			LastSweep: s.snapshotLast(),
		}
		writeJSON(w, out)
	})

	mux.HandleFunc("/sweep", func(w http.ResponseWriter, req *http.Request) {
		res := s.runOnce(req.Context(), "manual")
		writeJSON(w, res)
	})

	mux.HandleFunc("/reset", func(w http.ResponseWriter, _ *http.Request) {
		s.counters.Reset()
		writeJSON(w, map[string]string{"status": "reset"})
	})

	mux.HandleFunc("/adopt", func(w http.ResponseWriter, req *http.Request) {
		on := req.URL.Query().Get("on") == "true"
		s.rest.SetAdopt(on)
		writeJSON(w, map[string]any{"adoptEnabled": on})
	})

	mux.HandleFunc("/gc", func(w http.ResponseWriter, req *http.Request) {
		on := req.URL.Query().Get("on") == "true"
		s.rest.SetGC(on)
		writeJSON(w, map[string]any{"gcEnabled": on})
	})

	mux.HandleFunc("/negcache", func(w http.ResponseWriter, req *http.Request) {
		on := req.URL.Query().Get("on") == "true"
		s.bodies.SetNegCache(on)
		writeJSON(w, map[string]any{"negCacheEnabled": on})
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	klog.InfoS("sweep-debug-listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("sweep debug server: %w", err)
	}
	return nil
}

func (s *Sweeper) snapshotLast() Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
