// Package scheme builds a dynamic runtime.Scheme for a
// single (possibly runtime-selected) resource GVK, and provides the
// typed wrapper Object that is required for Server-Side Apply.
//
// # Why a typed wrapper
//
// The library's SSA path (managedfields.NewTypeConverter plus the
// generic registry's Create/Update) hits TWO typed-scheme
// checkpoints:
//
//  1. `scheme.New(gvk)` — used to construct an empty object for
//     decode. Fails for *unstructured.Unstructured because the
//     scheme cannot attribute a kind to a naked unstructured value.
//  2. managedfields typed converter — requires that the OpenAPI
//     definition for the resource's top-level type be keyed at the
//     Go canonical name of the sample object registered against the
//     GVK.
//
// A typed wrapper (Object) whose content is still an untyped map
// satisfies (1) without forcing the backend to know the resource's
// shape at compile time. (2) is satisfied by feeding the backend's
// OpenAPI through runtime/component/v2/openapi.Compose keyed at the
// Object canonical name. See 0017 for the original finding and 0023
// for the Track B variant.
//
// # What's different from v1
//
// Functionally identical to runtime/component/scheme. Canonical
// names use the v2 import path so a binary importing both
// substrate versions disambiguates (rare but allowed; v2 is meant
// to be used alone in new consumers).
package scheme

import (
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

// Canonical type names used as keys into the OpenAPI defs map.
const (
	UnstructuredCanonicalName     = "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured.Unstructured"
	UnstructuredListCanonicalName = "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured.UnstructuredList"
	ObjectCanonicalName           = "github.com/cheeseandcereal/aggexp/runtime/component/v2/scheme.Object"
	ObjectListCanonicalName       = "github.com/cheeseandcereal/aggexp/runtime/component/v2/scheme.ObjectList"
)

// ResourceDescriptor is the subset of the backend's GetSchema
// response the scheme needs.
type ResourceDescriptor struct {
	GroupVersion schema.GroupVersion
	Resource     string
	Kind         string
	Singular     string
	Namespaced   bool
	// UseTypedWrapper registers *Object / *ObjectList under the GVK
	// (required for SSA). Default true in v2; kept as a knob for
	// experiments that want the plain unstructured path.
	UseTypedWrapper bool
}

// Bundle holds the Scheme, codec factory, and the canonical
// OpenAPI-defs-map key the consumer needs.
type Bundle struct {
	Descriptor        ResourceDescriptor
	Scheme            *runtime.Scheme
	Codecs            serializer.CodecFactory
	ParameterCodec    runtime.ParameterCodec
	ItemCanonicalName string
	ListCanonicalName string
}

// Build constructs a Scheme for d. The typed wrapper path registers
// both the external and internal group-versions so the library's SSA
// flow's `toUnversioned` lookup succeeds.
func Build(d ResourceDescriptor) *Bundle {
	s := runtime.NewScheme()
	gv := d.GroupVersion

	if d.UseTypedWrapper {
		s.AddKnownTypeWithName(gv.WithKind(d.Kind), &Object{})
		s.AddKnownTypeWithName(gv.WithKind(d.Kind+"List"), &ObjectList{})
		internal := schema.GroupVersion{Group: gv.Group, Version: runtime.APIVersionInternal}
		s.AddKnownTypeWithName(internal.WithKind(d.Kind), &Object{})
		s.AddKnownTypeWithName(internal.WithKind(d.Kind+"List"), &ObjectList{})
	} else {
		s.AddKnownTypeWithName(gv.WithKind(d.Kind), &unstructured.Unstructured{})
		s.AddKnownTypeWithName(gv.WithKind(d.Kind+"List"), &unstructured.UnstructuredList{})
	}

	metav1.AddToGroupVersion(s, gv)
	metav1.AddToGroupVersion(s, schema.GroupVersion{Version: "v1"})

	unversioned := schema.GroupVersion{Group: "", Version: "v1"}
	utilruntime.Must(s.SetVersionPriority(unversioned))
	s.AddUnversionedTypes(unversioned,
		&metav1.Status{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	)
	utilruntime.Must(s.SetVersionPriority(gv))

	codecs := serializer.NewCodecFactory(s)

	itemKey := UnstructuredCanonicalName
	listKey := UnstructuredListCanonicalName
	if d.UseTypedWrapper {
		itemKey = ObjectCanonicalName
		listKey = ObjectListCanonicalName
	}
	return &Bundle{
		Descriptor:        d,
		Scheme:            s,
		Codecs:            codecs,
		ParameterCodec:    runtime.NewParameterCodec(s),
		ItemCanonicalName: itemKey,
		ListCanonicalName: listKey,
	}
}

// Object is the typed wrapper. TypeMeta + ObjectMeta are first-class;
// other top-level JSON keys collect into Content. SMD walks Content
// via the deduced/untyped path.
type Object struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

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

// MarshalJSON renders apiVersion, kind, metadata, and all Content
// keys into a single Kubernetes-shaped JSON object.
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
	var metaMap map[string]interface{}
	if err := json.Unmarshal(raw, &metaMap); err != nil {
		return nil, err
	}
	if len(metaMap) > 0 {
		m["metadata"] = metaMap
	}
	return json.Marshal(m)
}

// UnmarshalJSON splits TypeMeta / ObjectMeta out; the rest lands in
// Content.
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

func (o *Object) String() string {
	return fmt.Sprintf("%s/%s %s/%s", o.APIVersion, o.Kind, o.Namespace, o.Name)
}

// AsMap returns Content merged with apiVersion/kind/metadata keys,
// suitable for jsonpath-style lookups.
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
