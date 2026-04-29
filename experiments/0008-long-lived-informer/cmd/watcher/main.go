// Watcher is a small client-go dynamic informer probe for the 0008
// experiment. It watches hellos.aggexp.io/v1 and logs every callback
// to stdout in a simple key=value format that's easy to grep. A
// `/status` HTTP endpoint exposes running counters.
//
// Deliberately small (~200 lines). No controller-runtime; the whole
// point is to see what the bare reflector does.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

var (
	adds    atomic.Uint64
	updates atomic.Uint64
	deletes atomic.Uint64
	lastRV  atomic.Value // string
	startT  = time.Now()

	sleepMS = envInt("WATCHER_SLEEP_MS", 0)
	group   = envStr("WATCH_GROUP", "aggexp.io")
	version = envStr("WATCH_VERSION", "v1")
	// resource is the plural, lowercase name. `hellos` for 0002; `repos` for 0004.
	resource   = envStr("WATCH_RESOURCE", "hellos")
	statusAddr = envStr("STATUS_ADDR", ":8080")
)

func envStr(k, def string) string {
	if v, ok := os.LookupEnv(k); ok {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v, ok := os.LookupEnv(k); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// logf is a trivial key=value logger. Prefixing with ts and bare
// event= words makes scenarios reproducible via grep.
func logf(event string, kvs ...any) {
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	out := fmt.Sprintf("ts=%s event=%s", ts, event)
	for i := 0; i+1 < len(kvs); i += 2 {
		out += fmt.Sprintf(" %v=%v", kvs[i], kvs[i+1])
	}
	fmt.Println(out)
}

// extract object name, rv, uid from an unstructured event object.
func objKeys(obj any) (name, rv, uid string) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		if du, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			name = du.Key
			if u2, ok := du.Obj.(*unstructured.Unstructured); ok {
				rv = u2.GetResourceVersion()
				uid = string(u2.GetUID())
			}
			return
		}
		return "?", "?", "?"
	}
	return u.GetName(), u.GetResourceVersion(), string(u.GetUID())
}

func maybeSleep() {
	if sleepMS > 0 {
		time.Sleep(time.Duration(sleepMS) * time.Millisecond)
	}
}

func onAdd(obj any, isInInitialList bool) {
	name, rv, uid := objKeys(obj)
	adds.Add(1)
	lastRV.Store(rv)
	logf("add", "name", name, "rv", rv, "uid", uid, "initial", isInInitialList)
	maybeSleep()
}

func onUpdate(old, cur any) {
	name, rv, uid := objKeys(cur)
	_, oldRV, oldUID := objKeys(old)
	updates.Add(1)
	lastRV.Store(rv)
	logf("update", "name", name, "rv", rv, "uid", uid, "oldrv", oldRV, "olduid", oldUID, "uidchanged", uid != oldUID)
	maybeSleep()
}

func onDelete(obj any) {
	name, rv, uid := objKeys(obj)
	deletes.Add(1)
	if rv != "" {
		lastRV.Store(rv)
	}
	logf("delete", "name", name, "rv", rv, "uid", uid)
	maybeSleep()
}

func heartbeat(ctx context.Context, inf cache.SharedIndexInformer) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rv, _ := lastRV.Load().(string)
			items := len(inf.GetStore().ListKeys())
			logf("heartbeat",
				"adds", adds.Load(),
				"updates", updates.Load(),
				"deletes", deletes.Load(),
				"store_items", items,
				"last_rv", rv,
				"uptime", time.Since(startT).Round(time.Second))
		}
	}
}

func statusServer(ctx context.Context, inf cache.SharedIndexInformer) {
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		rv, _ := lastRV.Load().(string)
		keys := inf.GetStore().ListKeys()
		resp := map[string]any{
			"adds":        adds.Load(),
			"updates":     updates.Load(),
			"deletes":     deletes.Load(),
			"last_rv":     rv,
			"uptime":      time.Since(startT).Round(time.Second).String(),
			"store_items": len(keys),
			"store_keys":  keys,
			"sleep_ms":    sleepMS,
			"watching":    fmt.Sprintf("%s/%s %s", group, version, resource),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	srv := &http.Server{Addr: statusAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	logf("status-listen", "addr", statusAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("status server: %v", err)
	}
}

func main() {
	flag.Parse()
	lastRV.Store("")

	cfg, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("in-cluster config: %v", err)
	}
	// Let the reflector handle retries itself; keep default QPS/Burst.
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("dynamic client: %v", err)
	}

	gvr := schema.GroupVersionResource{Group: group, Version: version, Resource: resource}
	factory := dynamicinformer.NewDynamicSharedInformerFactory(dyn, 30*time.Minute)
	inf := factory.ForResource(gvr).Informer()

	// ErrorHandler is how reflector-level errors (410 Gone, auth, TLS, etc.)
	// surface. Default handler klogs; we want them in our plain stdout stream.
	_ = inf.SetWatchErrorHandler(func(r *cache.Reflector, err error) {
		logf("watch-error", "err", fmt.Sprintf("%q", err.Error()))
	})

	_, err = inf.AddEventHandler(cache.ResourceEventHandlerDetailedFuncs{
		AddFunc:    onAdd,
		UpdateFunc: onUpdate,
		DeleteFunc: onDelete,
	})
	if err != nil {
		log.Fatalf("add event handler: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		logf("shutdown", "reason", "signal")
		cancel()
	}()

	logf("start",
		"group", group,
		"version", version,
		"resource", resource,
		"sleep_ms", sleepMS,
		"status_addr", statusAddr,
	)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); heartbeat(ctx, inf) }()
	go func() { defer wg.Done(); statusServer(ctx, inf) }()

	factory.Start(ctx.Done())
	// Log sync state once.
	if cache.WaitForCacheSync(ctx.Done(), inf.HasSynced) {
		logf("synced", "store_items", len(inf.GetStore().ListKeys()))
	} else {
		logf("sync-failed")
	}

	// Sanity: keep the process alive until ctx closes. factory.Start
	// spawns its own goroutines; we only need to wait for ours.
	<-ctx.Done()
	wg.Wait()
	// Avoid unused imports if someone trims things later.
	_ = metav1.ObjectMeta{}
}
