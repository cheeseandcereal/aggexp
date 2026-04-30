package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Widget is a cluster-scoped projection of a resource on an
// async-provisioning backend. After create or update the backend
// simulates a ~30s provisioning wait; status.phase transitions
// Pending -> Provisioning -> Ready. On delete, a ~10s Deleting phase
// precedes removal. This apiserver is stateless beyond a short-lived
// poll-for-watch cache; the mock is the source of truth.
type Widget struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WidgetSpec   `json:"spec,omitempty"`
	Status WidgetStatus `json:"status,omitempty"`
}

// WidgetSpec is the user-writable portion of a Widget.
type WidgetSpec struct {
	// DesiredState is the intent the caller is expressing. The
	// mock accepts any non-empty string; canonical values are
	// "running" and "stopped".
	DesiredState string `json:"desiredState,omitempty"`

	// Config is an arbitrary key/value map whose contents the
	// mock round-trips. Changing any value triggers a new
	// provisioning cycle.
	Config map[string]string `json:"config,omitempty"`
}

// WidgetStatus is the server's observation of the backing resource.
// All fields are derived live from the mock backend at read time.
type WidgetStatus struct {
	// Phase is a coarse state summary, computed from the mock's
	// creation/update time:
	//
	//   "Pending"      — write just accepted, no provisioning yet
	//                    (rarely visible; mostly an internal
	//                    transition)
	//   "Provisioning" — mock is in its 30s provision window
	//   "Ready"        — provisioning complete; observedState
	//                    == desiredState
	//   "Deleting"     — DELETE issued; mock in 10s deprovision
	//                    window
	//   "Failed"       — backend reported failure (not reached
	//                    by the mock; reserved for parity with
	//                    real backends)
	Phase string `json:"phase,omitempty"`

	// ObservedState is the mock's report of where the widget
	// actually is. Empty during Provisioning; matches
	// spec.desiredState once Ready.
	ObservedState string `json:"observedState,omitempty"`

	// ReadyAt is when the backend first reached the Ready phase
	// after the last provision. Unset while Pending /
	// Provisioning.
	ReadyAt *metav1.Time `json:"readyAt,omitempty"`

	// Message is free-form human-readable detail from the backend.
	Message string `json:"message,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WidgetList is a list of Widget objects.
type WidgetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Widget `json:"items"`
}
