// Package aggexp holds the internal types for the aggexp.io API
// group used by experiment 0033. The shape is deliberately minimal:
// a Gizmo is a cluster-scoped resource with a Spec.Color string and
// a Spec.Counter int. Updates that change Counter are the ones we
// concurrent-test.
package aggexp

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName matches v1's.
const GroupName = "aggexp.io"

// SchemeGroupVersion is the internal GV.
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// Kind returns a GroupKind.
func Kind(kind string) schema.GroupKind { return SchemeGroupVersion.WithKind(kind).GroupKind() }

// Resource returns a GroupResource.
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder collects funcs adding internal types.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme is the entry point used by install.
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion, &Gizmo{}, &GizmoList{})
	return nil
}

// Gizmo is the internal form of aggexp.io/v1.Gizmo.
type Gizmo struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	Spec   GizmoSpec
	Status GizmoStatus
}

// GizmoSpec is small on purpose. Color/Counter are the only fields
// the demos exercise.
type GizmoSpec struct {
	Color   string
	Counter int64
}

// GizmoStatus carries the AA pod that last wrote.
type GizmoStatus struct {
	LastWriter string
}

// GizmoList is the list type.
type GizmoList struct {
	metav1.TypeMeta
	metav1.ListMeta
	Items []Gizmo
}

// DeepCopy helpers.

func (in *Gizmo) DeepCopy() *Gizmo {
	if in == nil {
		return nil
	}
	out := new(Gizmo)
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
	out.Status = in.Status
	return out
}

func (in *Gizmo) DeepCopyObject() runtime.Object { return in.DeepCopy() }

func (in *GizmoList) DeepCopy() *GizmoList {
	if in == nil {
		return nil
	}
	out := new(GizmoList)
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]Gizmo, len(in.Items))
		for i := range in.Items {
			out.Items[i] = *in.Items[i].DeepCopy()
		}
	}
	return out
}

func (in *GizmoList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
