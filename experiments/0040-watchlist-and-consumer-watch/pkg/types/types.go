// Package types defines Widget and Gadget resources for experiment 0040.
package types

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	WidgetGroupName = "widgets.aggexp.io"
	GadgetGroupName = "gadgets.aggexp.io"
)

// Widget is the push-mode resource.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type Widget struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              WidgetSpec `json:"spec,omitempty"`
}

type WidgetSpec struct {
	Color string `json:"color,omitempty"`
	Size  string `json:"size,omitempty"`
}

// WidgetList is a list of Widget.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type WidgetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Widget `json:"items"`
}

func (w *Widget) DeepCopyObject() runtime.Object     { c := *w; return &c }
func (wl *WidgetList) DeepCopyObject() runtime.Object {
	c := *wl
	c.Items = make([]Widget, len(wl.Items))
	copy(c.Items, wl.Items)
	return &c
}

// Gadget is the poll-mode resource.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type Gadget struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              GadgetSpec `json:"spec,omitempty"`
}

type GadgetSpec struct {
	Model    string `json:"model,omitempty"`
	Firmware string `json:"firmware,omitempty"`
}

// GadgetList is a list of Gadget.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type GadgetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Gadget `json:"items"`
}

func (g *Gadget) DeepCopyObject() runtime.Object     { c := *g; return &c }
func (gl *GadgetList) DeepCopyObject() runtime.Object {
	c := *gl
	c.Items = make([]Gadget, len(gl.Items))
	copy(c.Items, gl.Items)
	return &c
}
