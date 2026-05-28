package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Widget is a namespace-scoped demo resource served by an aggregated
// apiserver. Multiple replicas of the AA all watch the same backing
// CRD (widgetstorages.aggexpstorage.aggexp.io/v1) on the host
// kube-apiserver via an informer; events fan out to local watchers
// from each replica's own broadcaster. Used by 0034 to probe whether
// load-balanced kubectl watches see all events even when writes hit
// a different replica than the one serving the watch.
type Widget struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WidgetSpec   `json:"spec,omitempty"`
	Status WidgetStatus `json:"status,omitempty"`
}

// WidgetSpec is the user-writable portion of a Widget.
type WidgetSpec struct {
	// Color is free-text.
	Color string `json:"color,omitempty"`

	// Size is an arbitrary int32.
	Size int32 `json:"size,omitempty"`
}

// WidgetStatus is observation state.
type WidgetStatus struct {
	// ObservedSize echoes spec.size at last server observation.
	ObservedSize int32 `json:"observedSize,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WidgetList is a list of Widget objects.
type WidgetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Widget `json:"items"`
}
