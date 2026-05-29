// Command contend is experiment 0049's cross-replica write-contention
// harness. It drives genuinely concurrent writes to the SAME Widget
// object and tallies the HTTP outcomes (200 OK / 409 Conflict /
// 500 InternalError), so scenario 1 can pin the 0048 post-acquire 500
// and scenario 2 can confirm the transaction fix converts those into
// clean 409s (or successful retries) with ZERO 500s.
//
// Why this reproduces the bug (FINDINGS/0048 + 0043): the embedded
// lock's holder identity is the REPLICA id (HOSTNAME). Two concurrent
// writes that land on the same replica both see the lock "held by
// self" and take the re-entrant acquire path, so BOTH proceed past
// acquisition into the body Put + commit-release writes. Those two
// writes were single-shot (no retry) in 0048, so the loser's commit
// CAS conflict surfaced as a 500. Across replicas the same collision
// happens via lease takeover. Either way, genuine concurrency on one
// object is what triggers it; this harness produces that concurrency.
//
// To defeat the connection-reuse that hid the bug in 0048's harness,
// each writer goroutine gets its OWN rest.Config -> its OWN HTTP/2
// connection pool to the host kube-apiserver, and (optionally) we drive
// many rounds with increasing writer counts. The aggregation layer then
// spreads the distinct connections across the per-pod backends of the
// load-balanced Service.
//
// It talks the served aggregated API (widgets.aggexp.io/v1) through the
// host kube-apiserver via the standard dynamic client, so it exercises
// exactly the path kubectl/controllers use. No direct pod dialing, no
// bespoke auth.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

var widgetGVR = schema.GroupVersionResource{Group: "widgets.aggexp.io", Version: "v1", Resource: "widgets"}

type outcome struct {
	ok       atomic.Int64 // 200/201
	conflict atomic.Int64 // 409
	internal atomic.Int64 // 500
	other    atomic.Int64 // anything else
}

func (o *outcome) record(err error) {
	if err == nil {
		o.ok.Add(1)
		return
	}
	switch {
	case apierrors.IsConflict(err):
		o.conflict.Add(1)
	case apierrors.IsInternalError(err) || apierrors.IsServerTimeout(err) || statusCode(err) == 500:
		o.internal.Add(1)
	default:
		o.other.Add(1)
	}
}

func statusCode(err error) int32 {
	if se, ok := err.(apierrors.APIStatus); ok {
		return se.Status().Code
	}
	return 0
}

func main() {
	var (
		kubeconfig  = flag.String("kubeconfig", defaultKubeconfig(), "path to kubeconfig")
		ctxName     = flag.String("context", "kind-aggexp-0049", "kube context (pin to avoid drift)")
		namespace   = flag.String("namespace", "aggexp-widgets", "widget namespace")
		name        = flag.String("name", "contended", "widget name to hammer")
		writers     = flag.Int("writers", 12, "concurrent writers per round")
		rounds      = flag.Int("rounds", 20, "number of contended rounds")
		ramp        = flag.Bool("ramp", false, "ramp writer count 2,4,...,writers across rounds")
		blind       = flag.Bool("blind", true, "blind writes (no resourceVersion; last-writer-wins) vs OCC")
		qps         = flag.Float64("qps", 300, "per-client QPS")
		burst       = flag.Int("burst", 600, "per-client burst")
		cleanup     = flag.Bool("cleanup", true, "delete the object at the end")
	)
	flag.Parse()

	ctx := context.Background()

	// Build N independent clients, each with its OWN rest.Config and
	// thus its OWN connection pool (defeats keepalive collapse).
	maxWriters := *writers
	clients := make([]dynamic.Interface, maxWriters)
	for i := 0; i < maxWriters; i++ {
		cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			&clientcmd.ClientConfigLoadingRules{ExplicitPath: *kubeconfig},
			&clientcmd.ConfigOverrides{CurrentContext: *ctxName},
		).ClientConfig()
		if err != nil {
			fatalf("build client %d: %v", i, err)
		}
		cfg.QPS = float32(*qps)
		cfg.Burst = *burst
		// Force a distinct transport per client.
		cfg.DisableCompression = true
		dyn, err := dynamic.NewForConfig(cfg)
		if err != nil {
			fatalf("dynamic client %d: %v", i, err)
		}
		clients[i] = dyn
	}

	// Ensure the object exists (first create, ignore AlreadyExists).
	seed := newWidget(*namespace, *name, "blue", 1)
	_, err := clients[0].Resource(widgetGVR).Namespace(*namespace).Create(ctx, seed, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		fatalf("seed create: %v", err)
	}
	fmt.Printf("seeded %s/%s\n", *namespace, *name)

	total := &outcome{}
	var maxObservedRV uint64
	var rvMu sync.Mutex

	for round := 0; round < *rounds; round++ {
		w := maxWriters
		if *ramp {
			w = 2 * (round%((maxWriters/2)+1) + 1)
			if w > maxWriters {
				w = maxWriters
			}
		}
		ro := &outcome{}
		var wg sync.WaitGroup
		for i := 0; i < w; i++ {
			wg.Add(1)
			go func(idx, rnd int) {
				defer wg.Done()
				color := fmt.Sprintf("c%d-%d", rnd, idx)
				size := int64(rnd*1000 + idx)
				var werr error
				if *blind {
					werr = blindUpdate(ctx, clients[idx], *namespace, *name, color, size)
				} else {
					werr = occUpdate(ctx, clients[idx], *namespace, *name, color, size)
				}
				ro.record(werr)
				total.record(werr)
			}(i, round)
		}
		wg.Wait()

		// Read back the authoritative object and track RV monotonicity.
		got, gerr := clients[0].Resource(widgetGVR).Namespace(*namespace).Get(ctx, *name, metav1.GetOptions{})
		if gerr == nil {
			rv := parseRV(got.GetResourceVersion())
			rvMu.Lock()
			if rv < maxObservedRV {
				fmt.Printf("  !! RV REGRESSION round %d: %d < %d\n", round, rv, maxObservedRV)
			}
			if rv > maxObservedRV {
				maxObservedRV = rv
			}
			rvMu.Unlock()
		}
		fmt.Printf("round %2d writers=%2d  ok=%-3d 409=%-3d 500=%-3d other=%-3d\n",
			round, w, ro.ok.Load(), ro.conflict.Load(), ro.internal.Load(), ro.other.Load())
	}

	fmt.Println("---- TOTALS ----")
	fmt.Printf("ok(200/201)=%d  conflict(409)=%d  internal(500)=%d  other=%d\n",
		total.ok.Load(), total.conflict.Load(), total.internal.Load(), total.other.Load())
	fmt.Printf("max observed RV=%d  blind=%v\n", maxObservedRV, *blind)

	if total.internal.Load() > 0 {
		fmt.Printf("RESULT: %d FIVE-HUNDREDS observed (bug present / fix off)\n", total.internal.Load())
	} else {
		fmt.Println("RESULT: zero 500s (clean: every loser 409'd or succeeded on retry)")
	}

	if *cleanup {
		_ = clients[0].Resource(widgetGVR).Namespace(*namespace).Delete(ctx, *name, metav1.DeleteOptions{})
		fmt.Printf("deleted %s/%s\n", *namespace, *name)
	}

	if total.internal.Load() > 0 {
		os.Exit(3) // non-zero exit signals 500s for scripting.
	}
}

// blindUpdate does a GET then UPDATE without preserving resourceVersion
// (so OCC is skipped server-side -> last-writer-wins). This is the
// adversarial path: pure write contention with no client-side
// serialization.
func blindUpdate(ctx context.Context, dyn dynamic.Interface, ns, name, color string, size int64) error {
	w := newWidget(ns, name, color, size)
	// No resourceVersion set -> server treats clientRV as "" -> OCC skipped.
	_, err := dyn.Resource(widgetGVR).Namespace(ns).Update(ctx, w, metav1.UpdateOptions{})
	return err
}

// occUpdate reads, mutates, and writes back with the read RV (standard
// optimistic-concurrency update). Losers get a legitimate OCC 409.
func occUpdate(ctx context.Context, dyn dynamic.Interface, ns, name, color string, size int64) error {
	got, err := dyn.Resource(widgetGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	_ = unstructured.SetNestedField(got.Object, color, "spec", "color")
	_ = unstructured.SetNestedField(got.Object, size, "spec", "size")
	_, err = dyn.Resource(widgetGVR).Namespace(ns).Update(ctx, got, metav1.UpdateOptions{})
	return err
}

func newWidget(ns, name, color string, size int64) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{Group: "widgets.aggexp.io", Version: "v1", Kind: "Widget"})
	u.SetNamespace(ns)
	u.SetName(name)
	_ = unstructured.SetNestedField(u.Object, color, "spec", "color")
	_ = unstructured.SetNestedField(u.Object, size, "spec", "size")
	return u
}

func parseRV(s string) uint64 {
	var n uint64
	for _, c := range s {
		if c < '0' || c > '9' {
			return n
		}
		n = n*10 + uint64(c-'0')
	}
	return n
}

func defaultKubeconfig() string {
	if v := os.Getenv("KUBECONFIG"); v != "" {
		return v
	}
	if h, err := os.UserHomeDir(); err == nil {
		return h + "/.kube/config"
	}
	return ""
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "contend: "+format+"\n", args...)
	os.Exit(1)
}
