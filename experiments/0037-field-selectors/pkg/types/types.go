// Package types defines the Widget resource for experiment 0037.
package types

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const GroupName = "widgets.aggexp.io"

// Widget is the resource type.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type Widget struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              WidgetSpec `json:"spec,omitempty"`
}

type WidgetSpec struct {
	Color    string `json:"color,omitempty"`
	Size     string `json:"size,omitempty"`
	Priority string `json:"priority,omitempty"`
}

// WidgetList is a list of Widget.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type WidgetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Widget `json:"items"`
}

// DeepCopyObject for Widget
func (w *Widget) DeepCopyObject() runtime.Object { c := *w; return &c }

// DeepCopyObject for WidgetList
func (wl *WidgetList) DeepCopyObject() runtime.Object {
	c := *wl
	c.Items = make([]Widget, len(wl.Items))
	copy(c.Items, wl.Items)
	return &c
}
