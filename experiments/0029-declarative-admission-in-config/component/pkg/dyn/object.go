// Package dyn defines a typed Go wrapper that can stand in for
// *unstructured.Unstructured in the component server's Scheme, while
// remaining content-agnostic (Spec/Status are bags of interface{}).
//
// Why this exists: the library's SSA path calls
// `scheme.New(gvk)` and then passes the resulting object through
// `scheme.ConvertToVersion`. For `*unstructured.Unstructured`,
// `Scheme.ObjectKinds` reads the GVK off the instance — which a
// zero-value reflect.New never populates. So SSA fails at
// `"unstructured object has no kind"` before any of the TypedValue
// / structured-merge-diff machinery is touched.
//
// For a typed Go struct, `Scheme.ObjectKinds` reads the GVK from
// `typeToGVK[reflect.Type]` — which IS populated when the type is
// registered. So a zero-value reflect.New of a typed struct is
// enough for the scheme to attribute it to a kind.
//
// The tradeoff: structured-merge-diff now needs to walk the Go
// struct fields (via `typed.ParseableType.FromStructured`). We
// expose exactly the fields any Kubernetes object has — TypeMeta,
// ObjectMeta — and then a bag for the resource-specific bits. The
// resource-specific bits show up to smd as a `map[string]interface{}`,
// which smd treats via the deduced-type path. That is strictly
// less precise than a fully-typed struct (list-merge keys, atomic
// vs granular fields, etc.), but it is the best the component
// server can do without generating Go code per resource at startup.
//
// Experiment 0017's finding on this path: it successfully unblocks
// the typeConverter construction and the empty-object-gvk issue,
// but SSA merge semantics degrade to "everything is atomic" for
// fields under spec/status because the typed parser has no Go
// types below the Object level to key on.
package dyn

import (
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Object is the typed stand-in for a dynamically-registered
// unstructured resource. It implements runtime.Object but NOT
// runtime.Unstructured, so the Scheme treats it on the typed
// branch.
type Object struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Content holds every top-level JSON field other than the
	// TypeMeta and ObjectMeta block. For our Note example this is
	// {spec: {...}, status: {...}}. structured-merge-diff walks it
	// via the untyped/deduced parser branch.
	Content map[string]interface{} `json:"-"`
}

// DeepCopyObject is required by runtime.Object.
func (o *Object) DeepCopyObject() runtime.Object {
	out := &Object{}
	out.TypeMeta = o.TypeMeta
	o.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Content = deepCopyMap(o.Content)
	return out
}

// GetObjectKind implements runtime.Object.
func (o *Object) GetObjectKind() schema.ObjectKind { return &o.TypeMeta }

// MarshalJSON renders the Object as the natural Kubernetes JSON:
// {apiVersion, kind, metadata, <all Content keys>}.
func (o *Object) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{}
	for k, v := range o.Content {
		m[k] = v
	}
	if o.APIVersion != "" {
		m["apiVersion"] = o.APIVersion
	}
	if o.Kind != "" {
		m["kind"] = o.Kind
	}
	raw, err := json.Marshal(&o.ObjectMeta)
	if err != nil {
		return nil, err
	}
	var meta map[string]interface{}
	if err := json.Unmarshal(raw, &meta); err != nil {
		return nil, err
	}
	// Drop empty meta keys so output matches kubectl's expectations.
	if len(meta) > 0 {
		m["metadata"] = meta
	}
	return json.Marshal(m)
}

// UnmarshalJSON is the inverse: splits TypeMeta + ObjectMeta out and
// keeps the rest in Content.
func (o *Object) UnmarshalJSON(data []byte) error {
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	if v, ok := m["apiVersion"].(string); ok {
		o.APIVersion = v
	}
	if v, ok := m["kind"].(string); ok {
		o.Kind = v
	}
	if meta, ok := m["metadata"].(map[string]interface{}); ok {
		raw, err := json.Marshal(meta)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(raw, &o.ObjectMeta); err != nil {
			return err
		}
	}
	delete(m, "apiVersion")
	delete(m, "kind")
	delete(m, "metadata")
	o.Content = m
	return nil
}

// ObjectList is the list analog.
type ObjectList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Object `json:"items"`
}

// DeepCopyObject is required by runtime.Object.
func (l *ObjectList) DeepCopyObject() runtime.Object {
	out := &ObjectList{}
	out.TypeMeta = l.TypeMeta
	l.ListMeta.DeepCopyInto(&out.ListMeta)
	out.Items = make([]Object, len(l.Items))
	for i := range l.Items {
		cp := l.Items[i].DeepCopyObject().(*Object)
		out.Items[i] = *cp
	}
	return out
}

// GetObjectKind implements runtime.Object.
func (l *ObjectList) GetObjectKind() schema.ObjectKind { return &l.TypeMeta }

// GetResourceVersion / SetResourceVersion / etc. come from the
// embedded ListMeta automatically (it implements metav1.ListInterface).

// Helpers --------------------------------------------------------------------

func deepCopyMap(m map[string]interface{}) map[string]interface{} {
	if m == nil {
		return nil
	}
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = deepCopyInterface(v)
	}
	return out
}

func deepCopyInterface(v interface{}) interface{} {
	switch vv := v.(type) {
	case map[string]interface{}:
		return deepCopyMap(vv)
	case []interface{}:
		out := make([]interface{}, len(vv))
		for i := range vv {
			out[i] = deepCopyInterface(vv[i])
		}
		return out
	default:
		return vv
	}
}

// Ensure compile-time interface satisfaction.
var (
	_ runtime.Object     = (*Object)(nil)
	_ runtime.Object     = (*ObjectList)(nil)
	_ schema.ObjectKind  = (*metav1.TypeMeta)(nil)
	_ fmt.Stringer       = (*Object)(nil) // via metav1.TypeMeta implementing String
)

// String implements fmt.Stringer to avoid noisy klog output.
func (o *Object) String() string {
	return fmt.Sprintf("%s/%s %s/%s", o.APIVersion, o.Kind, o.Namespace, o.Name)
}

// AsMap materializes the Object as a `map[string]interface{}` form
// suitable for jsonpath-style lookups (used by the table-row
// renderer). Returned map is a deep reconstruction, not a
// reference to Object's internal Content map.
func (o *Object) AsMap() map[string]interface{} {
	m := map[string]interface{}{}
	for k, v := range o.Content {
		m[k] = v
	}
	if o.APIVersion != "" {
		m["apiVersion"] = o.APIVersion
	}
	if o.Kind != "" {
		m["kind"] = o.Kind
	}
	// Project ObjectMeta as nested map via JSON round-trip.
	if raw, err := json.Marshal(&o.ObjectMeta); err == nil {
		var meta map[string]interface{}
		if err := json.Unmarshal(raw, &meta); err == nil && len(meta) > 0 {
			m["metadata"] = meta
		}
	}
	return m
}
