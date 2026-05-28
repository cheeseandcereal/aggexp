package server

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"k8s.io/klog/v2"

	"github.com/cheeseandcereal/aggexp/experiments/0033-crd-cas-object-locking/pkg/locking"
)

// startDebug runs a tiny HTTP server exposing CAS-attempt counters
// for the locking layer. The scenarios script scrapes it to record
// retry behavior under contention.
//
// Endpoints:
//   GET  /debug/stats   -> JSON {total, successes, conflicts, ...}
//   POST /debug/reset   -> zeroes the counters; returns the snapshot.
func startDebug(ctx context.Context, addr string, lock *locking.Backend) {
	mux := http.NewServeMux()
	stats := lock.Stats()
	snapshot := func() map[string]any {
		return map[string]any{
			"podName":      lock.PodName(),
			"mode":         string(lock.Mode()),
			"total":        stats.Total.Load(),
			"successes":    stats.Successes.Load(),
			"conflicts":    stats.Conflicts.Load(),
			"renewals":     stats.Renewals.Load(),
			"staleStolen":  stats.StaleStolen.Load(),
		}
	}
	mux.HandleFunc("/debug/stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(snapshot())
	})
	mux.HandleFunc("/debug/reset", func(w http.ResponseWriter, _ *http.Request) {
		stats.Total.Store(0)
		stats.Successes.Store(0)
		stats.Conflicts.Store(0)
		stats.Renewals.Store(0)
		stats.StaleStolen.Store(0)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(snapshot())
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	go func() {
		klog.Infof("debug-server: listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			klog.Warningf("debug-server: %v", err)
		}
	}()
}
