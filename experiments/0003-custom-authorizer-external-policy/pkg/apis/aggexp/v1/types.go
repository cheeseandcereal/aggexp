package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Hello is a tiny cluster-scoped resource used to exercise the
// aggregated apiserver's CRUD + watch paths without an external
// backend. State lives in-memory on the server.
type Hello struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HelloSpec   `json:"spec,omitempty"`
	Status HelloStatus `json:"status,omitempty"`
}

// HelloSpec is the user-writable portion of a Hello.
type HelloSpec struct {
	// Greeting is the string the server echoes back. Arbitrary.
	Greeting string `json:"greeting,omitempty"`
}

// HelloStatus is the server-managed portion of a Hello. The MVP-lab
// does not split spec/status enforcement; this field is present to
// match Kubernetes convention and to show up in the generated
// OpenAPI schema.
type HelloStatus struct {
	// ObservedGreeting echoes back Spec.Greeting at write time. It
	// exists to give us a status field to populate.
	ObservedGreeting string `json:"observedGreeting,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// HelloList is a list of Hello objects.
type HelloList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Hello `json:"items"`
}
