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
		&Bucket{},
		&BucketList{},
	)
	return nil
}

// Bucket is the internal form of aggexp.io/v1.Bucket.
type Bucket struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	Spec   BucketSpec
	Status BucketStatus
}

type BucketSpec struct {
	Region string
	Tags   map[string]string
}

type BucketStatus struct {
	Region       string
	CreationDate *metav1.Time
	ObservedAt   metav1.Time
	Phase        string
}

type BucketList struct {
	metav1.TypeMeta
	metav1.ListMeta

	Items []Bucket
}

func (in *Bucket) DeepCopy() *Bucket {
	if in == nil {
		return nil
	}
	out := new(Bucket)
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = BucketSpec{Region: in.Spec.Region}
	if in.Spec.Tags != nil {
		out.Spec.Tags = make(map[string]string, len(in.Spec.Tags))
		for k, v := range in.Spec.Tags {
			out.Spec.Tags[k] = v
		}
	}
	out.Status = in.Status
	if in.Status.CreationDate != nil {
		cd := *in.Status.CreationDate
		out.Status.CreationDate = &cd
	}
	return out
}

func (in *Bucket) DeepCopyObject() runtime.Object { return in.DeepCopy() }

func (in *BucketList) DeepCopy() *BucketList {
	if in == nil {
		return nil
	}
	out := new(BucketList)
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]Bucket, len(in.Items))
		for i := range in.Items {
			dc := in.Items[i].DeepCopy()
			out.Items[i] = *dc
		}
	}
	return out
}

func (in *BucketList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
