// Package aggexp holds the internal types for aggexp.io.
package aggexp

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const GroupName = "aggexp.io"

var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Widget{},
		&WidgetList{},
	)
	return nil
}

// Widget is a cluster-scoped projection of a resource on an async
// backend. The backend simulates minute-scale async provisioning; a
// freshly-created Widget spends 30s in Provisioning before reaching
// Ready. The AA is stateless; phase is computed by the backend.
type Widget struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	Spec   WidgetSpec
	Status WidgetStatus
}

type WidgetSpec struct {
	// DesiredState is the user's intent. Currently the two-value
	// enum ("running" / "stopped"); the mock accepts either.
	DesiredState string
	// Config is an arbitrary key/value map. Editing it triggers
	// a re-provision.
	Config map[string]string
}

type WidgetStatus struct {
	// Phase is one of "Pending", "Provisioning", "Ready",
	// "Deleting", "Failed". Computed by the backend from the
	// widget's last-change time.
	Phase string
	// ObservedState is the mock backend's opinion of where the
	// thing actually is (empty while Provisioning, matches
	// desiredState once Ready).
	ObservedState string
	// ReadyAt is when the backend reached Ready. Unset until then.
	ReadyAt *metav1.Time
	// Message is human-readable detail.
	Message string
}

type WidgetList struct {
	metav1.TypeMeta
	metav1.ListMeta

	Items []Widget
}

func (in *Widget) DeepCopy() *Widget {
	if in == nil {
		return nil
	}
	out := new(Widget)
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = WidgetSpec{DesiredState: in.Spec.DesiredState}
	if in.Spec.Config != nil {
		out.Spec.Config = make(map[string]string, len(in.Spec.Config))
		for k, v := range in.Spec.Config {
			out.Spec.Config[k] = v
		}
	}
	out.Status = in.Status
	if in.Status.ReadyAt != nil {
		t := *in.Status.ReadyAt
		out.Status.ReadyAt = &t
	}
	return out
}

func (in *Widget) DeepCopyObject() runtime.Object { return in.DeepCopy() }

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
			dc := in.Items[i].DeepCopy()
			out.Items[i] = *dc
		}
	}
	return out
}

func (in *WidgetList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
