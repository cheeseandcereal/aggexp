package crdbackend

import (
	"bytes"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// objectMetaToMap converts an ObjectMeta to the map[string]interface{}
// representation the dynamic client expects inside Unstructured.Object.
// This uses the runtime-DefaultUnstructuredConverter so every
// ObjectMeta field (ManagedFields, Finalizers, OwnerReferences,
// Labels, Annotations, UID, ResourceVersion, CreationTimestamp, etc.)
// round-trips without hand-rolling the conversion.
func objectMetaToMap(m *metav1.ObjectMeta) (map[string]interface{}, error) {
	return runtime.DefaultUnstructuredConverter.ToUnstructured(m)
}

// mapToObjectMeta is the inverse of objectMetaToMap.
func mapToObjectMeta(m map[string]interface{}, out *metav1.ObjectMeta) error {
	return runtime.DefaultUnstructuredConverter.FromUnstructured(m, out)
}

// timeAfterSeconds returns a channel that fires after n seconds.
func timeAfterSeconds(n int) <-chan time.Time {
	return time.After(time.Duration(n) * time.Second)
}

// countMF returns the length of metadata.managedFields on an
// unstructured object. Used in debug logs.
func countMF(u *unstructured.Unstructured) int {
	if u == nil {
		return 0
	}
	return len(u.GetManagedFields())
}

// renameFieldsV1 performs a dumb byte-level rename inside a FieldsV1
// payload. FieldsV1 is raw JSON; our renames (f:counter <->
// f:storedCounter) are simple enough that a byte replace is fine.
// Limited to cases where oldKey is not a substring of some other key.
// If the FieldsV1 is nil, returns nil unchanged.
func renameFieldsV1(fv1 *metav1.FieldsV1, oldKey, newKey string) *metav1.FieldsV1 {
	if fv1 == nil || len(fv1.Raw) == 0 {
		return fv1
	}
	oldQuoted := []byte("\"" + oldKey + "\"")
	newQuoted := []byte("\"" + newKey + "\"")
	if !bytes.Contains(fv1.Raw, oldQuoted) {
		return fv1
	}
	return &metav1.FieldsV1{Raw: bytes.ReplaceAll(fv1.Raw, oldQuoted, newQuoted)}
}

