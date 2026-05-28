// Package v1 defines the external aggexp.io/v1 API types for
// experiment 0033.
package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName is the group name used by this API.
const GroupName = "aggexp.io"

// SchemeGroupVersion is the group version used to register these types.
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

// Resource helps express a resource under this group.
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder collects funcs that add types to a runtime.Scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes, addConversionFuncs)
	// AddToScheme registers the types.
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion, &Gizmo{}, &GizmoList{})
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}

// Gizmo is a cluster-scoped, write-able resource. Locking layer is
// the focus; the spec is incidental.
type Gizmo struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   GizmoSpec   `json:"spec,omitempty"`
	Status GizmoStatus `json:"status,omitempty"`
}

// GizmoSpec is the user-mutable part.
type GizmoSpec struct {
	Color   string `json:"color,omitempty"`
	Counter int64  `json:"counter,omitempty"`
}

// GizmoStatus records the writer (pod) that last wrote.
type GizmoStatus struct {
	LastWriter string `json:"lastWriter,omitempty"`
}

// GizmoList is a list of Gizmos.
type GizmoList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Gizmo `json:"items"`
}

// DeepCopy for the external types.

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
