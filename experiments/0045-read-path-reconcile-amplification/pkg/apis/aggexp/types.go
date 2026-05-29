package aggexp

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName is the API group this experiment exposes to clients.
const GroupName = "aggexp.io"

// SchemeGroupVersion is the internal GroupVersion.
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// Kind returns a GroupKind for the given kind under this group.
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource returns a GroupResource for the given resource name.
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder collects the add-to-scheme funcs.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme registers the internal types into a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Widget{},
		&WidgetList{},
	)
	return nil
}

// Widget is the internal hub form of aggexp.io/v1.Widget. The shape
// is incidental to 0042; the experiment is about RV authority on the
// stitched metadata CR, not the business shape.
type Widget struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	Spec   WidgetSpec
	Status WidgetStatus
}

// WidgetSpec is the user-writable portion of a Widget. It lives on
// the in-memory backend only; never on the metadata CR.
type WidgetSpec struct {
	Color string
	Size  int32
}

// WidgetStatus is observation state. Backend-only.
type WidgetStatus struct {
	Phase string
}

// WidgetList is a list of Widget objects.
type WidgetList struct {
	metav1.TypeMeta
	metav1.ListMeta

	Items []Widget
}

// DeepCopy helpers: hand-written for the internal types.

func (in *Widget) DeepCopy() *Widget {
	if in == nil {
		return nil
	}
	out := new(Widget)
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = WidgetSpec{Color: in.Spec.Color, Size: in.Spec.Size}
	out.Status = WidgetStatus{Phase: in.Status.Phase}
	return out
}

// DeepCopyObject implements runtime.Object.
func (in *Widget) DeepCopyObject() runtime.Object { return in.DeepCopy() }

// DeepCopy for WidgetList.
func (in *WidgetList) DeepCopy() *WidgetList {
	if in == nil {
		return nil
	}
	out := new(WidgetList)
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]Widget, len(in.Items))
		for i := range in.Items {
			out.Items[i] = *in.Items[i].DeepCopy()
		}
	}
	return out
}

// DeepCopyObject implements runtime.Object.
func (in *WidgetList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
