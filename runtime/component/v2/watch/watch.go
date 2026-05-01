// Package watch holds the v2 substrate's watch-semantics helpers
// that are transport-independent: the initial-events-end BOOKMARK
// object builder and the resourceVersion authority abstraction.
//
// The main watch implementation lives in
// runtime/component/v2/grpcbackend.REST (it wraps
// watch.Broadcaster and drives push or poll loops). This package
// exists so consumers wanting to construct BOOKMARK objects or
// coordinate RV across multiple sources do not need to import the
// REST adapter.
//
// # initial-events-end BOOKMARK
//
// Per FINDINGS/0011 and FINDINGS/0025, the middleware MUST emit a
// BOOKMARK event with the `k8s.io/initial-events-end=true`
// annotation at the tail of the Watch prefix. Both push-capable
// and poll-only backends are covered — the BOOKMARK emission is
// a middleware concern, independent of backend watch capability.
//
// # Unified RV authority
//
// Per FINDINGS/0025, the middleware owns RV end-to-end.
// MetadataStore.Record.RecordResourceVersion (the host-etcd CRD's
// own RV, monotonic by host-apiserver construction) is preferred
// when a Record exists; a local atomic counter is the fallback for
// stitchless flows. The helpers in Authority below wrap that
// policy so consumers at different layers agree on a single RV
// source.
package watch

import (
	"strconv"
	"sync/atomic"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	componentscheme "github.com/cheeseandcereal/aggexp/runtime/component/v2/scheme"
)

// InitialEventsEndAnnotation is the annotation kubectl and
// WatchList-aware clients key off. Constant here so all callers
// emit the same string.
const InitialEventsEndAnnotation = "k8s.io/initial-events-end"

// BookmarkObject builds a single runtime.Object suitable for a
// BOOKMARK event at the tail of the initial-events prefix. The
// object is typed or unstructured depending on typedWrapper, to
// match the REST's outgoing shape.
func BookmarkObject(gvk schema.GroupVersionKind, rv string, typedWrapper bool) runtime.Object {
	if typedWrapper {
		o := &componentscheme.Object{}
		o.TypeMeta = metav1.TypeMeta{APIVersion: gvk.GroupVersion().String(), Kind: gvk.Kind}
		o.Annotations = map[string]string{InitialEventsEndAnnotation: "true"}
		o.ResourceVersion = rv
		return o
	}
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetAnnotations(map[string]string{InitialEventsEndAnnotation: "true"})
	u.SetResourceVersion(rv)
	return u
}

// Authority encapsulates the unified RV policy: prefer a
// Record-supplied RV when available, fall back to a local monotonic
// counter otherwise. Callers that see a Record from the metastore
// should call Observe with the Record's RV so the local counter
// stays aligned. The returned value is the current RV string.
type Authority struct {
	counter atomic.Uint64
}

// New constructs an Authority initialised to RV=1.
func New() *Authority {
	a := &Authority{}
	a.counter.Store(1)
	return a
}

// Next increments and returns a fresh RV. Use for middleware-only
// events where no Record carries authoritative RV.
func (a *Authority) Next() string {
	return strconv.FormatUint(a.counter.Add(1), 10)
}

// Current returns the current RV string without incrementing.
func (a *Authority) Current() string {
	return strconv.FormatUint(a.counter.Load(), 10)
}

// Observe nudges the local counter forward if rv is larger,
// keeping Next monotonic across external (Record-supplied) RVs.
func (a *Authority) Observe(rv string) {
	n, err := strconv.ParseUint(rv, 10, 64)
	if err != nil {
		return
	}
	for {
		cur := a.counter.Load()
		if n <= cur {
			return
		}
		if a.counter.CompareAndSwap(cur, n) {
			return
		}
	}
}

// Resolve returns the appropriate RV for an object: record's RV if
// non-empty, otherwise the local counter's current value. Also
// Observe's the record RV into the local counter for monotonicity.
func (a *Authority) Resolve(recordRV string) string {
	if recordRV != "" {
		a.Observe(recordRV)
		return recordRV
	}
	return a.Current()
}
