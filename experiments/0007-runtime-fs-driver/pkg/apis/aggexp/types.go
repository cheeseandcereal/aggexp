// Package aggexp holds the internal types for the aggexp.io API
// group used by experiment 0007.
package aggexp

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName matches v1's.
const GroupName = "aggexp.io"

// SchemeGroupVersion is the internal GV.
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// Kind returns a GroupKind.
func Kind(kind string) schema.GroupKind { return SchemeGroupVersion.WithKind(kind).GroupKind() }

// Resource returns a GroupResource.
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder collects funcs adding internal types.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme is the entry point used by install.
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion, &File{}, &FileList{})
	return nil
}

// File is the internal form of aggexp.io/v1.File.
type File struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	Spec   FileSpec
	Status FileStatus
}

// FileSpec mirrors v1.FileSpec.
type FileSpec struct {
	Path string
	Size int64
	Mode uint32
}

// FileStatus mirrors v1.FileStatus.
type FileStatus struct {
	ObservedAt metav1.Time
}

// FileList mirrors v1.FileList.
type FileList struct {
	metav1.TypeMeta
	metav1.ListMeta
	Items []File
}

// DeepCopy helpers.

func (in *File) DeepCopy() *File {
	if in == nil {
		return nil
	}
	out := new(File)
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
	out.Status = in.Status
	return out
}

func (in *File) DeepCopyObject() runtime.Object { return in.DeepCopy() }

func (in *FileList) DeepCopy() *FileList {
	if in == nil {
		return nil
	}
	out := new(FileList)
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]File, len(in.Items))
		for i := range in.Items {
			out.Items[i] = *in.Items[i].DeepCopy()
		}
	}
	return out
}

func (in *FileList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
