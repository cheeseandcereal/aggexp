// Package aggexp holds the internal (un-versioned) types for the
// aggexp.io API group in this experiment.
package aggexp

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName matches v1's.
const GroupName = "aggexp.io"

// SchemeGroupVersion is the internal GV. APIVersionInternal == "__internal".
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// Kind returns a group-qualified GroupKind.
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource returns a group-qualified GroupResource.
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder collects funcs that add internal types to a Scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme is the entry point used by install.
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Repo{},
		&RepoList{},
	)
	return nil
}

// Repo is the internal form of aggexp.io/v1.Repo.
type Repo struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	Spec   RepoSpec
	Status RepoStatus
}

// RepoSpec mirrors v1.RepoSpec.
type RepoSpec struct {
	Owner         string
	Name          string
	Description   string
	DefaultBranch string
	Private       bool
	Language      string
	Stars         int32
	HTMLURL       string
}

// RepoStatus mirrors v1.RepoStatus.
type RepoStatus struct {
	ObservedAt metav1.Time
	ETag       string
}

// RepoList mirrors v1.RepoList.
type RepoList struct {
	metav1.TypeMeta
	metav1.ListMeta

	Items []Repo
}

// DeepCopy helpers.

func (in *Repo) DeepCopy() *Repo {
	if in == nil {
		return nil
	}
	out := new(Repo)
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
	out.Status = in.Status
	return out
}

func (in *Repo) DeepCopyObject() runtime.Object { return in.DeepCopy() }

func (in *RepoList) DeepCopy() *RepoList {
	if in == nil {
		return nil
	}
	out := new(RepoList)
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]Repo, len(in.Items))
		for i := range in.Items {
			dc := in.Items[i].DeepCopy()
			out.Items[i] = *dc
		}
	}
	return out
}

func (in *RepoList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
