package scheme

import (
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Object is the typed stand-in for a dynamically-registered
// resource. It implements runtime.Object but NOT
// runtime.Unstructured so the Scheme treats it on the typed branch
// (see package doc for the SSA motivation).
//
// TypeMeta and ObjectMeta are first-class struct fields. Every
// other top-level JSON field is collected into Content, walked by
// structured-merge-diff via the deduced/untyped path. The tradeoff
// is documented in FINDINGS/0017: SMD list-key merge strategies
// (`listType: map` / `listMapKeys`) cannot be honored under
// spec/status without a Go type below the Object level.
type Object struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Content holds every top-level JSON field other than the
	// TypeMeta and ObjectMeta block. For a Note example this is
	// {spec: {...}, status: {...}}.
	Content map[string]interface{} `json:"-"`
}

// DeepCopyObject satisfies runtime.Object.
func (o *Object) DeepCopyObject() runtime.Object {
	out := &Object{}
	out.TypeMeta = o.TypeMeta
	o.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Content = deepCopyMap(o.Content)
	return out
}

// GetObjectKind satisfies runtime.Object.
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
	if len(meta) > 0 {
		m["metadata"] = meta
	}
	return json.Marshal(m)
}

// UnmarshalJSON splits TypeMeta + ObjectMeta out and keeps the
// rest in Content.
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

// String implements fmt.Stringer to keep klog output readable.
func (o *Object) String() string {
	return fmt.Sprintf("%s/%s %s/%s", o.APIVersion, o.Kind, o.Namespace, o.Name)
}

// AsMap materializes the Object as a fresh map suitable for
// jsonpath-style lookups.
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
	if raw, err := json.Marshal(&o.ObjectMeta); err == nil {
		var meta map[string]interface{}
		if err := json.Unmarshal(raw, &meta); err == nil && len(meta) > 0 {
			m["metadata"] = meta
		}
	}
	return m
}

// ObjectList is the list analog of Object.
type ObjectList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Object `json:"items"`
}

// DeepCopyObject satisfies runtime.Object.
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

// GetObjectKind satisfies runtime.Object.
func (l *ObjectList) GetObjectKind() schema.ObjectKind { return &l.TypeMeta }

// Compile-time interface assertions.
var (
	_ runtime.Object = (*Object)(nil)
	_ runtime.Object = (*ObjectList)(nil)
)

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
