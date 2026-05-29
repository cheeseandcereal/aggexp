package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Widget is a namespace-scoped demo resource served by experiment
// 0042's aggregated apiserver. Its business body (spec + status)
// lives in an in-memory backend; its KRM metadata (uid, labels,
// annotations, etc.) lives on a cluster-scoped metadata CR
// (resourcemetadatas.widgetmeta.aggexp.io/v1). The two are stitched
// on every response, and the stitched object's resourceVersion is
// the host etcd RV of the metadata CR.
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
	// Phase is a free-text lifecycle phase.
	Phase string `json:"phase,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WidgetList is a list of Widget objects.
type WidgetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Widget `json:"items"`
}

