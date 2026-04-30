// Package main is the controller-runtime manager used in
// experiment 0012 to probe what happens when a standard
// controller-runtime Manager is pointed at an aggregated apiserver
// backed by experiment 0007 (files.aggexp.io/v1, read-only,
// stateless).
//
// Every behavior is instrumented with INFO-level logs keyed on the
// scenario being probed ("reconcile-start", "ssa-patch-result",
// "finalizer-add-result", "ownerref-set-result", "leader-elected",
// "cache-resync"). The experiment harness greps those lines.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	fileGroup     = "aggexp.io"
	fileVersion   = "v1"
	fileKind      = "File"
	finalizerName = "aggexp.io/example-finalizer"
	annotationKey = "aggexp.io/last-reconciled"
	parentNS      = "aggexp-system"
	parentCMName  = "aggexp-files-parent"
	ssaFieldMgr   = "aggexp-file-controller"
)

var (
	scheme = runtime.NewScheme()
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
}

// fileGVK returns the GVK for aggexp.io/v1 File.
func fileGVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: fileGroup, Version: fileVersion, Kind: fileKind}
}

func newFile() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(fileGVK())
	return u
}

// reconciler observes File objects and attempts to carry out the
// standard "controller chores": SSA-patch an annotation, add a
// finalizer, set an owner reference to a host-cluster ConfigMap.
// Each attempt's outcome is logged.
type reconciler struct {
	c client.Client
}

// Reconcile is called by controller-runtime for every enqueued key.
func (r *reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := klog.FromContext(ctx).WithValues("file", req.Name)
	log.Info("reconcile-start", "at", time.Now().UTC().Format(time.RFC3339Nano))

	// Fetch current state.
	obj := newFile()
	getErr := r.c.Get(ctx, types.NamespacedName{Name: req.Name}, obj)
	if apierrors.IsNotFound(getErr) {
		log.Info("reconcile-not-found", "msg", "file vanished from cache; nothing to do")
		return reconcile.Result{}, nil
	}
	if getErr != nil {
		log.Error(getErr, "reconcile-get-failed")
		return reconcile.Result{}, getErr
	}
	log.Info("reconcile-observed",
		"rv", obj.GetResourceVersion(),
		"uid", obj.GetUID(),
		"finalizers", obj.GetFinalizers(),
		"deletionTimestamp", obj.GetDeletionTimestamp(),
		"annotations", obj.GetAnnotations())

	// Scenario-5 prep: ensure a parent ConfigMap exists in the host cluster,
	// then record its UID for an ownerReference. We record-only; SSA is
	// what attempts to set it.
	parent := &corev1.ConfigMap{}
	parentKey := client.ObjectKey{Namespace: parentNS, Name: parentCMName}
	if err := r.c.Get(ctx, parentKey, parent); err != nil {
		if apierrors.IsNotFound(err) {
			parent = &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Namespace: parentNS, Name: parentCMName},
				Data:       map[string]string{"note": "parent for File ownerReference experiment 0012"},
			}
			if cerr := r.c.Create(ctx, parent); cerr != nil && !apierrors.IsAlreadyExists(cerr) {
				log.Error(cerr, "parent-configmap-create-failed")
			} else {
				log.Info("parent-configmap-created", "uid", parent.GetUID())
			}
		} else {
			log.Error(err, "parent-configmap-get-failed")
		}
	}

	// If we got here during a delete, try to clear the finalizer.
	if obj.GetDeletionTimestamp() != nil {
		log.Info("reconcile-delete-path",
			"deletionTimestamp", obj.GetDeletionTimestamp(),
			"finalizers", obj.GetFinalizers())
		if containsFinalizer(obj, finalizerName) {
			patched := obj.DeepCopy()
			removeFinalizer(patched, finalizerName)
			if err := r.c.Update(ctx, patched); err != nil {
				log.Error(err, "finalizer-remove-result", "outcome", "error")
			} else {
				log.Info("finalizer-remove-result", "outcome", "ok", "rv", patched.GetResourceVersion())
			}
		}
		return reconcile.Result{}, nil
	}

	// (a) SSA patch: set the last-reconciled annotation. We build a
	// tiny "apply" object that expresses only the fields we own,
	// matching how client.Apply uses SSA.
	applyObj := newFile()
	applyObj.SetName(obj.GetName())
	applyObj.SetAnnotations(map[string]string{annotationKey: time.Now().UTC().Format(time.RFC3339)})
	ssaErr := r.c.Patch(ctx, applyObj, client.Apply,
		client.FieldOwner(ssaFieldMgr), client.ForceOwnership)
	if ssaErr != nil {
		log.Error(ssaErr, "ssa-patch-result", "outcome", "error")
	} else {
		log.Info("ssa-patch-result",
			"outcome", "ok",
			"rv", applyObj.GetResourceVersion(),
			"managedFields-count", len(applyObj.GetManagedFields()))
		// Re-fetch to see what actually landed server-side.
		check := newFile()
		if gErr := r.c.Get(ctx, client.ObjectKey{Name: obj.GetName()}, check); gErr == nil {
			mfJSON, _ := json.Marshal(check.GetManagedFields())
			log.Info("ssa-patch-verify",
				"annotations-after", check.GetAnnotations(),
				"managedFields-count", len(check.GetManagedFields()),
				"managedFields-json", string(mfJSON))
		} else {
			log.Error(gErr, "ssa-patch-verify-get-failed")
		}
	}

	// (b) Finalizer: add if missing using a JSON-merge-ish update.
	// We deliberately use client.Update (PUT) rather than SSA here
	// so we can observe backend persistence behavior separately.
	if !containsFinalizer(obj, finalizerName) {
		mut := obj.DeepCopy()
		addFinalizer(mut, finalizerName)
		if err := r.c.Update(ctx, mut); err != nil {
			log.Error(err, "finalizer-add-result", "outcome", "error")
		} else {
			log.Info("finalizer-add-result",
				"outcome", "ok",
				"rv", mut.GetResourceVersion(),
				"finalizers-after", mut.GetFinalizers())
			verify := newFile()
			if gErr := r.c.Get(ctx, client.ObjectKey{Name: obj.GetName()}, verify); gErr == nil {
				log.Info("finalizer-add-verify", "finalizers", verify.GetFinalizers())
			}
		}
	}

	// (c) OwnerReference: set to the parent ConfigMap.
	if parent.GetUID() != "" {
		cur := newFile()
		if gErr := r.c.Get(ctx, client.ObjectKey{Name: obj.GetName()}, cur); gErr == nil {
			needUpdate := true
			for _, or := range cur.GetOwnerReferences() {
				if or.UID == parent.GetUID() && or.Kind == "ConfigMap" {
					needUpdate = false
					break
				}
			}
			if needUpdate {
				mut := cur.DeepCopy()
				mut.SetOwnerReferences(append(cur.GetOwnerReferences(), metav1.OwnerReference{
					APIVersion: "v1",
					Kind:       "ConfigMap",
					Name:       parent.GetName(),
					UID:        parent.GetUID(),
				}))
				if err := r.c.Update(ctx, mut); err != nil {
					log.Error(err, "ownerref-set-result", "outcome", "error")
				} else {
					log.Info("ownerref-set-result",
						"outcome", "ok",
						"rv", mut.GetResourceVersion(),
						"ownerRefs-after", mut.GetOwnerReferences())
					verify := newFile()
					if gErr := r.c.Get(ctx, client.ObjectKey{Name: obj.GetName()}, verify); gErr == nil {
						log.Info("ownerref-verify", "ownerReferences", verify.GetOwnerReferences())
					}
				}
			}
		}
	}

	return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
}

func containsFinalizer(u *unstructured.Unstructured, name string) bool {
	for _, f := range u.GetFinalizers() {
		if f == name {
			return true
		}
	}
	return false
}

func addFinalizer(u *unstructured.Unstructured, name string) {
	if containsFinalizer(u, name) {
		return
	}
	u.SetFinalizers(append(u.GetFinalizers(), name))
}

func removeFinalizer(u *unstructured.Unstructured, name string) {
	in := u.GetFinalizers()
	out := make([]string, 0, len(in))
	for _, f := range in {
		if f != name {
			out = append(out, f)
		}
	}
	u.SetFinalizers(out)
}

func main() {
	zapOpts := zap.Options{Development: true}
	fs := flag.NewFlagSet("controller", flag.ExitOnError)
	zapOpts.BindFlags(fs)
	_ = fs.Parse(os.Args[1:])
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))

	cfg := ctrl.GetConfigOrDie()

	mgr, err := ctrl.NewManager(cfg, manager.Options{
		Scheme:                     scheme,
		LeaderElection:             true,
		LeaderElectionID:           "aggexp-file-controller",
		LeaderElectionNamespace:    parentNS,
		LeaderElectionResourceLock: "leases",
		Cache: ctrlcache.Options{
			// No scheme registration for File; the cache must build
			// unstructured informers on the fly using the REST
			// mapper. controller-runtime supports this already;
			// nothing extra needed here.
		},
		Client: client.Options{
			Cache: &client.CacheOptions{
				// Let the cache-backed client serve GETs/LISTs for
				// unstructured objects (default is to bypass the
				// cache for those).
				Unstructured: true,
			},
		},
	})
	if err != nil {
		klog.Fatalf("manager-new: %v", err)
	}

	if err := ctrl.NewControllerManagedBy(mgr).
		Named("file-controller").
		For(newFile()).
		Complete(&reconciler{c: mgr.GetClient()}); err != nil {
		klog.Fatalf("controller-build: %v", err)
	}

	// Add a hook that logs when caches are synced.
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		if !mgr.GetCache().WaitForCacheSync(ctx) {
			klog.Error(nil, "cache-sync-failed")
			return fmt.Errorf("cache sync failed")
		}
		klog.InfoS("cache-sync-ok", "gvk", fileGVK().String())
		return nil
	})); err != nil {
		klog.Fatalf("cache-runnable-add: %v", err)
	}

	klog.Info("manager-start")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		klog.Fatalf("manager-start: %v", err)
	}
}
