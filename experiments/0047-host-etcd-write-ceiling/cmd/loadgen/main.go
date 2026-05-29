// Command loadgen is the experiment-0047 load generator and the
// driver of its four scenarios. It is NOT part of the served
// apiserver; a throwaway lab tool.
//
// It composes two pools against aggexp.io/v1 widgets through the host
// kube-apiserver's aggregation layer:
//
//   - a WRITER pool: K client goroutines issuing Create-then-Update at
//     an aggregate target rate of R writes/sec across a fixed working
//     set of objects (each served write drives the embedded-lock
//     acquire + commit-release CR-write amplification under
//     measurement);
//   - a WATCHER pool: N concurrent watches held open for the run, each
//     as an impersonated identity (per-watcher poll on the AA side, or
//     SharedPoll depending on how the AA was deployed).
//
// At the end of a run it prints the achieved write rate and the
// observed watch-event count. The metadata-CR write breakdown by kind
// and the host-etcd latency are scraped separately by
// hack/scrape-metrics.sh (which reads the AA pod logs' aggexp-0047-
// metrics line and the host apiserver's /metrics).
//
// All kubectl/client traffic must target context kind-aggexp-0047; the
// caller is responsible for that (the kubeconfig current-context, or
// -kubecontext).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var widgetGVR = schema.GroupVersionResource{Group: "aggexp.io", Version: "v1", Resource: "widgets"}

func main() {
	var (
		rate       = flag.Float64("rate", 10, "aggregate target write rate (writes/sec) across the writer pool")
		writers    = flag.Int("writers", 4, "number of writer client goroutines (K)")
		objects    = flag.Int("objects", 20, "size of the working set of widgets the writers cycle through")
		watchers   = flag.Int("watchers", 0, "number of concurrent watches to hold open (N)")
		duration   = flag.Duration("duration", 30*time.Second, "how long to drive writes / hold watches")
		ns         = flag.String("namespace", "aggexp-widgets", "namespace")
		identity   = flag.String("as", "loadgen", "impersonated writer identity")
		watchIdent = flag.String("watch-as", "", "impersonated watcher identity (default: same as -as)")
		kubeconfig = flag.String("kubeconfig", os.Getenv("HOME")+"/.kube/config", "kubeconfig")
		kubectx    = flag.String("kubecontext", "kind-aggexp-0047", "kube context (pinned to the 0047 cluster)")
		prefix     = flag.String("prefix", "lw", "widget name prefix for this run")
		cleanup    = flag.Bool("cleanup", true, "delete the working-set widgets at the end of the run")
	)
	flag.Parse()
	if *watchIdent == "" {
		*watchIdent = *identity
	}

	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: *kubeconfig},
		&clientcmd.ConfigOverrides{CurrentContext: *kubectx},
	).ClientConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "kubeconfig:", err)
		os.Exit(1)
	}
	// Generous client-side throughput so the loadgen, not client-go's
	// default QPS=5/burst=10, is the rate gate.
	cfg.QPS = 2000
	cfg.Burst = 4000

	writerCfg := rest.CopyConfig(cfg)
	writerCfg.Impersonate = rest.ImpersonationConfig{UserName: *identity}
	writerDyn, err := dynamic.NewForConfig(writerCfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "writer client:", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration+60*time.Second)
	defer cancel()

	// --- watcher pool ---
	var watchEvents int64
	var watchWG sync.WaitGroup
	watchDeadline := time.Now().Add(*duration)
	for i := 0; i < *watchers; i++ {
		ic := rest.CopyConfig(cfg)
		ic.Impersonate = rest.ImpersonationConfig{UserName: *watchIdent}
		wdyn, derr := dynamic.NewForConfig(ic)
		if derr != nil {
			fmt.Fprintln(os.Stderr, "watch client:", derr)
			os.Exit(1)
		}
		w, werr := wdyn.Resource(widgetGVR).Namespace(*ns).Watch(ctx, metav1.ListOptions{})
		if werr != nil {
			fmt.Fprintf(os.Stderr, "watch %d: %v\n", i, werr)
			os.Exit(1)
		}
		watchWG.Add(1)
		go func() {
			defer watchWG.Done()
			defer w.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case _, ok := <-w.ResultChan():
					if !ok {
						return
					}
					atomic.AddInt64(&watchEvents, 1)
				case <-time.After(time.Until(watchDeadline)):
					return
				}
			}
		}()
	}

	// --- working set: create the objects first (counts as writes) ---
	var totalWrites int64
	var writeErrs int64
	for i := 0; i < *objects; i++ {
		name := fmt.Sprintf("%s-%d", *prefix, i)
		if err := createWidget(ctx, writerDyn, *ns, name, *identity, i); err != nil {
			// AlreadyExists is fine on a re-run; count real errors.
			atomic.AddInt64(&writeErrs, 1)
		} else {
			atomic.AddInt64(&totalWrites, 1)
		}
	}

	// --- writer pool: K goroutines issuing Updates at aggregate R/s ---
	start := time.Now()
	deadline := start.Add(*duration)
	// Per-writer interval so the pool's aggregate is ~R/s.
	perWriterInterval := time.Duration(float64(*writers) / *rate * float64(time.Second))
	if perWriterInterval <= 0 {
		perWriterInterval = time.Microsecond
	}
	var writeWG sync.WaitGroup
	for k := 0; k < *writers; k++ {
		writeWG.Add(1)
		go func(k int) {
			defer writeWG.Done()
			tick := time.NewTicker(perWriterInterval)
			defer tick.Stop()
			n := k
			for time.Now().Before(deadline) {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
				}
				name := fmt.Sprintf("%s-%d", *prefix, n%*objects)
				n += *writers
				if err := updateWidget(ctx, writerDyn, *ns, name); err != nil {
					atomic.AddInt64(&writeErrs, 1)
					continue
				}
				atomic.AddInt64(&totalWrites, 1)
			}
		}(k)
	}
	writeWG.Wait()
	elapsed := time.Since(start)

	if *watchers > 0 {
		time.Sleep(time.Until(watchDeadline))
	}
	cancel()
	watchWG.Wait()

	achieved := float64(atomic.LoadInt64(&totalWrites)) / elapsed.Seconds()
	fmt.Printf("loadgen: targetRate=%.0f/s writers=%d objects=%d watchers=%d duration=%s\n",
		*rate, *writers, *objects, *watchers, *duration)
	fmt.Printf("loadgen: servedWrites=%d (errors=%d) elapsed=%s achievedRate=%.1f writes/s\n",
		atomic.LoadInt64(&totalWrites), atomic.LoadInt64(&writeErrs), elapsed.Round(time.Millisecond), achieved)
	fmt.Printf("loadgen: watchEvents=%d\n", atomic.LoadInt64(&watchEvents))

	if *cleanup {
		dctx, dcancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer dcancel()
		deleted := 0
		for i := 0; i < *objects; i++ {
			name := fmt.Sprintf("%s-%d", *prefix, i)
			if err := writerDyn.Resource(widgetGVR).Namespace(*ns).Delete(dctx, name, metav1.DeleteOptions{}); err == nil {
				deleted++
			}
		}
		fmt.Printf("loadgen: cleaned up %d widgets\n", deleted)
	}
}

func createWidget(ctx context.Context, dyn dynamic.Interface, ns, name, owner string, i int) error {
	w := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "aggexp.io/v1",
		"kind":       "Widget",
		"metadata": map[string]any{
			"name":      name,
			"namespace": ns,
		},
		"spec": map[string]any{
			"color": colorFor(i),
			"size":  int64(i % 100),
		},
	}}
	_, err := dyn.Resource(widgetGVR).Namespace(ns).Create(ctx, w, metav1.CreateOptions{})
	return err
}

// updateWidget does a read-modify-write of spec.size so each Update is
// a genuine body change (the emission filter must NOT suppress it).
func updateWidget(ctx context.Context, dyn dynamic.Interface, ns, name string) error {
	cur, err := dyn.Resource(widgetGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	size, _, _ := unstructured.NestedInt64(cur.Object, "spec", "size")
	_ = unstructured.SetNestedField(cur.Object, (size+1)%1000, "spec", "size")
	_, err = dyn.Resource(widgetGVR).Namespace(ns).Update(ctx, cur, metav1.UpdateOptions{})
	return err
}

func colorFor(i int) string {
	colors := []string{"red", "green", "blue", "yellow", "purple"}
	return colors[i%len(colors)]
}
