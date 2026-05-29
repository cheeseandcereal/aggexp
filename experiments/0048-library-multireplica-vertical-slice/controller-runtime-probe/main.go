// Command controller-runtime-probe is experiment 0048 Scenario 5: a
// controller-runtime manager with a reconcile loop and a finalizer
// driving the composed AA's widgets.aggexp.io/v1 Widget. It proves the
// manager's cache, the reconciler, and the finalizer lifecycle all
// work against the multi-replica aggregated API — the heaviest
// ecosystem consumer in the arc.
//
// controller-runtime is intentionally a dependency here (the experiment
// probes its compatibility), an allowed exception to the lab's
// "no heavy frameworks in experiments" anti-pattern.
//
// The Widget is reconciled as an *unstructured.Unstructured (the AA
// serves a custom group with no generated typed clientset). The
// manager's cache must be told to use unstructured (Cache.Unstructured
// is opt-in; default-off silently bypasses the cache — FINDINGS/0012).
package main

import (
	"context"
	"flag"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/clientcmd"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const finalizerName = "lab.aggexp.io/cr-probe"

var widgetGVK = schema.GroupVersionKind{Group: "widgets.aggexp.io", Version: "v1", Kind: "Widget"}

func main() {
	var ctxName string
	flag.StringVar(&ctxName, "context", "kind-aggexp-0048", "kube context (pin it; shared kubeconfig drifts under parallel runs)")
	flag.Parse()

	log.SetLogger(zap.New(zap.UseDevMode(true)))
	logger := log.Log.WithName("cr-probe")

	loader := clientcmd.NewDefaultClientConfigLoadingRules()
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loader,
		&clientcmd.ConfigOverrides{CurrentContext: ctxName},
	).ClientConfig()
	if err != nil {
		logger.Error(err, "client config")
		os.Exit(1)
	}
	cfg.QPS = 50
	cfg.Burst = 100

	mgr, err := manager.New(cfg, manager.Options{
		// The manager's cache must use unstructured for the custom
		// group; default-off bypasses the cache (FINDINGS/0012).
		Cache: cache.Options{},
		Client: client.Options{
			Cache: &client.CacheOptions{Unstructured: true},
		},
	})
	if err != nil {
		logger.Error(err, "manager.New")
		os.Exit(1)
	}

	rec := &widgetReconciler{client: mgr.GetClient(), log: logger}

	proto := &unstructured.Unstructured{}
	proto.SetGroupVersionKind(widgetGVK)

	if err := builder.ControllerManagedBy(mgr).
		For(proto).
		Complete(rec); err != nil {
		logger.Error(err, "build controller")
		os.Exit(1)
	}

	logger.Info("manager starting", "context", ctxName, "gvk", widgetGVK.String())
	if err := mgr.Start(signals.SetupSignalHandler()); err != nil {
		logger.Error(err, "manager exited")
		os.Exit(1)
	}
}

type widgetReconciler struct {
	client client.Client
	log    logr.Logger
}

func (r *widgetReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	w := &unstructured.Unstructured{}
	w.SetGroupVersionKind(widgetGVK)
	if err := r.client.Get(ctx, req.NamespacedName, w); err != nil {
		if client.IgnoreNotFound(err) == nil {
			r.log.Info("reconcile: gone", "name", req.String())
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	color, _, _ := unstructured.NestedString(w.Object, "spec", "color")
	r.log.Info("reconcile", "name", req.String(), "uid", string(w.GetUID()), "rv", w.GetResourceVersion(), "color", color, "deleting", w.GetDeletionTimestamp() != nil)

	// Finalizer lifecycle (FINDINGS/0045: finalizers persist on the
	// metadata CR and are stitched back; this proves the lifecycle).
	if w.GetDeletionTimestamp().IsZero() {
		if !controllerutil.ContainsFinalizer(w, finalizerName) {
			controllerutil.AddFinalizer(w, finalizerName)
			if err := r.client.Update(ctx, w); err != nil {
				return reconcile.Result{}, err
			}
			r.log.Info("finalizer-added", "name", req.String())
		}
		return reconcile.Result{}, nil
	}

	// Being deleted: do cleanup (none, this is a probe), then remove the
	// finalizer so the object can actually go.
	if controllerutil.ContainsFinalizer(w, finalizerName) {
		controllerutil.RemoveFinalizer(w, finalizerName)
		if err := r.client.Update(ctx, w); err != nil {
			return reconcile.Result{}, err
		}
		r.log.Info("finalizer-removed", "name", req.String())
	}
	return reconcile.Result{RequeueAfter: time.Second}, nil
}
