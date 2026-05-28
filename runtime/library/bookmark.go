package library

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
)

// bookmarkAnnotationKey is the annotation that signals the end of
// initial events in a WatchList response.
const bookmarkAnnotationKey = "k8s.io/initial-events-end"

// makeBookmarkEvent creates a BOOKMARK watch event with the
// k8s.io/initial-events-end annotation set to "true". The object
// carries the given resourceVersion in its metadata.
//
// This closes the kubectl wait --for=jsonpath gap identified in
// FINDINGS/0011 and validated in FINDINGS/0025 and FINDINGS/0040.
func makeBookmarkEvent(newObj func() runtime.Object, rv string) watch.Event {
	obj := newObj()
	// Use PartialObjectMetadata as the bookmark carrier if the
	// object factory returns a type with ObjectMeta.
	if setter, ok := obj.(metav1.ObjectMetaAccessor); ok {
		om := setter.GetObjectMeta()
		om.SetResourceVersion(rv)
		om.SetAnnotations(map[string]string{
			bookmarkAnnotationKey: "true",
		})
	}
	return watch.Event{
		Type:   watch.Bookmark,
		Object: obj,
	}
}

// shouldEmitBookmark returns true if the watch options indicate
// bookmarks are allowed. When allowWatchBookmarks is false (or opts
// is nil), we do not emit the initial-events-end bookmark.
func shouldEmitBookmark(allowWatchBookmarks bool) bool {
	return allowWatchBookmarks
}
