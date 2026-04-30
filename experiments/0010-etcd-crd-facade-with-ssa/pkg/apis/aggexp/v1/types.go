package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Widget is a cluster-scoped demo resource. The aggregated apiserver
// exposes it; storage is forwarded to a CRD (widgetstorages.aggexpstorage.aggexp.io)
// on the host kube-apiserver. The AA adds value via an identity-aware
// facade and a small field-rename transformation between the storage
// and exposed shapes.
type Widget struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WidgetSpec   `json:"spec,omitempty"`
	Status WidgetStatus `json:"status,omitempty"`
}

// WidgetSpec is the user-writable portion of a Widget.
type WidgetSpec struct {
	// Description is free-text.
	Description string `json:"description,omitempty"`

	// Counter is an arbitrary int32. On the backing CRD this field
	// is named `stored_counter`; the facade renames it on read/write.
	Counter int32 `json:"counter,omitempty"`

	// Tags is a string-to-string map. When the caller's user name
	// starts with "alice-", the facade filters this map on read to
	// only include keys starting with "alice-".
	Tags map[string]string `json:"tags,omitempty"`
}

// WidgetStatus is observation state echoing the spec counter back.
type WidgetStatus struct {
	// ObservedCounter echoes spec.counter at last server observation.
	ObservedCounter int32 `json:"observedCounter,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WidgetList is a list of Widget objects.
type WidgetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Widget `json:"items"`
}
