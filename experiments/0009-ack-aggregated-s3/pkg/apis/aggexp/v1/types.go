package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Bucket is a cluster-scoped projection of an AWS S3 bucket. State
// lives on AWS. This apiserver is stateless; Get and List are live
// reads against the S3 API. The Kubernetes resource name equals the
// S3 bucket name (globally unique on real AWS).
type Bucket struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BucketSpec   `json:"spec,omitempty"`
	Status BucketStatus `json:"status,omitempty"`
}

// BucketSpec is the user-writable portion of a Bucket. The subset of
// S3's CreateBucket / PutBucketTagging inputs we model.
type BucketSpec struct {
	// Region is the AWS region. If empty on create, the server uses
	// its default region. On read, Status.Region is authoritative.
	Region string `json:"region,omitempty"`

	// Tags is the bucket's tag set. Matches S3 tagging semantics
	// (key/value, both strings).
	Tags map[string]string `json:"tags,omitempty"`
}

// BucketStatus is the server's observation of the backing S3 object.
// All fields are derived from live AWS calls at read time.
type BucketStatus struct {
	// Region is the actual region of the bucket as reported by S3.
	Region string `json:"region,omitempty"`

	// CreationDate is when S3 created the bucket.
	CreationDate *metav1.Time `json:"creationDate,omitempty"`

	// ObservedAt is when the AA last observed this bucket. For live
	// reads, it's the time of the request.
	ObservedAt metav1.Time `json:"observedAt,omitempty"`

	// Phase is a coarse state summary: "Ready" (bucket exists and
	// is reachable) or "Failed" (the last observation returned an
	// error). Failed is not persisted in the usual sense because
	// there is no store; it is surfaced on the error path of
	// operations that partially succeeded.
	Phase string `json:"phase,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// BucketList is a list of Bucket objects.
type BucketList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Bucket `json:"items"`
}
