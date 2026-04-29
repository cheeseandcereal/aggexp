// Package aggexp holds the internal (un-versioned) types for the
// aggexp.io API group. The generic apiserver round-trips external
// objects through their internal representation during SSA and
// strategic-merge-patch; without an internal version registered in
// the scheme, those paths return 500s complaining about
// "no kind ... is registered for the internal version".
//
// Our internal types are byte-identical to v1 because this experiment
// only has a single external version. Conversion functions are
// trivial (field-by-field assignment). When a future experiment adds
// a v2, we split the types and register real converters.
package aggexp

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName matches v1's.
const GroupName = "aggexp.io"

// SchemeGroupVersion is the internal GV. APIVersionInternal == "__internal".
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// Kind returns a group-qualified GroupKind.
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource returns a group-qualified GroupResource.
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder collects funcs that add internal types to a Scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme is the entry point used by install.
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Hello{},
		&HelloList{},
	)
	return nil
}

// Hello is the internal form of aggexp.io/v1.Hello. Fields mirror
// the external type exactly.
type Hello struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	Spec   HelloSpec
	Status HelloStatus
}

// HelloSpec mirrors v1.HelloSpec.
type HelloSpec struct {
	Greeting string
}

// HelloStatus mirrors v1.HelloStatus.
type HelloStatus struct {
	ObservedGreeting string
}

// HelloList mirrors v1.HelloList.
type HelloList struct {
	metav1.TypeMeta
	metav1.ListMeta

	Items []Hello
}

// Object / ListObject markers plus DeepCopy helpers.

// DeepCopy returns a deep copy of the receiver.
func (in *Hello) DeepCopy() *Hello {
	if in == nil {
		return nil
	}
	out := new(Hello)
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
	out.Status = in.Status
	return out
}

// DeepCopyObject implements runtime.Object.
func (in *Hello) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

// DeepCopy returns a deep copy of the receiver.
func (in *HelloList) DeepCopy() *HelloList {
	if in == nil {
		return nil
	}
	out := new(HelloList)
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]Hello, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopy()
			dc := in.Items[i].DeepCopy()
			out.Items[i] = *dc
		}
	}
	return out
}

// DeepCopyObject implements runtime.Object.
func (in *HelloList) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}
