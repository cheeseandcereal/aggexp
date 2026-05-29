// Command client-go-probe is experiment 0048 Scenario 4: a client-go
// reflector/informer driving the composed AA's widgets.aggexp.io/v1
// resource. It exercises the wire contract a real controller's cache
// depends on — LIST+WATCH, resync, and resume-by-RV — against the
// multi-replica AA (0042 RV authority + 0044 per-watcher emission).
//
// It uses the dynamic informer (the AA serves a custom group, so a
// typed client would need generated clientsets we deliberately do not
// build here). The informer's behavior — initial LIST, live WATCH
// events, periodic resync — is logged so the scenario can read it.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

func main() {
	var (
		kubeconfig = flag.String("kubeconfig", os.Getenv("KUBECONFIG"), "path to kubeconfig")
		ctxName    = flag.String("context", "kind-aggexp-0048", "kube context to use (pin it; the shared kubeconfig drifts under parallel runs)")
		resync     = flag.Duration("resync", 30*time.Second, "informer resync period")
		runFor     = flag.Duration("run-for", 0, "exit after this duration (0 = until signal)")
	)
	flag.Parse()

	gvr := schema.GroupVersionResource{Group: "widgets.aggexp.io", Version: "v1", Resource: "widgets"}

	loader := clientcmd.NewDefaultClientConfigLoadingRules()
	if *kubeconfig != "" {
		loader.ExplicitPath = *kubeconfig
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loader,
		&clientcmd.ConfigOverrides{CurrentContext: *ctxName},
	).ClientConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "client config: %v\n", err)
		os.Exit(1)
	}
	cfg.QPS = 50
	cfg.Burst = 100

	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dynamic client: %v\n", err)
		os.Exit(1)
	}

	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dyn, *resync, metav1.NamespaceAll, nil)
	inf := factory.ForResource(gvr).Informer()

	var adds, updates, deletes int
	_, err = inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			adds++
			u := obj.(*unstructured.Unstructured)
			klog.InfoS("informer-ADD", "name", u.GetName(), "ns", u.GetNamespace(), "uid", u.GetUID(), "rv", u.GetResourceVersion())
		},
		UpdateFunc: func(old, obj interface{}) {
			updates++
			ou := old.(*unstructured.Unstructured)
			nu := obj.(*unstructured.Unstructured)
			// Same-RV update = a resync re-list, not a real change.
			kind := "MODIFIED"
			if ou.GetResourceVersion() == nu.GetResourceVersion() {
				kind = "RESYNC(same-RV)"
			}
			klog.InfoS("informer-UPDATE", "type", kind, "name", nu.GetName(), "uid", nu.GetUID(), "oldRV", ou.GetResourceVersion(), "newRV", nu.GetResourceVersion())
		},
		DeleteFunc: func(obj interface{}) {
			deletes++
			if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tomb.Obj
			}
			u := obj.(*unstructured.Unstructured)
			klog.InfoS("informer-DELETE", "name", u.GetName(), "uid", u.GetUID(), "rv", u.GetResourceVersion())
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "add handler: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if *runFor > 0 {
		go func() { time.Sleep(*runFor); cancel() }()
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sig; cancel() }()

	klog.InfoS("reflector-starting", "gvr", gvr.String(), "resync", resync.String())
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), inf.HasSynced) {
		klog.Error("initial cache sync failed")
		os.Exit(1)
	}
	klog.InfoS("reflector-synced", "store-len", len(inf.GetStore().ListKeys()))

	// Periodic summary so the scenario can read steady-state behavior.
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			klog.InfoS("reflector-exiting", "adds", adds, "updates", updates, "deletes", deletes, "store-len", len(inf.GetStore().ListKeys()))
			return
		case <-tick.C:
			klog.InfoS("reflector-summary", "adds", adds, "updates", updates, "deletes", deletes, "store-len", len(inf.GetStore().ListKeys()))
		}
	}
}
