// Package v1 defines the external aggexp.io/v1 API types for
// experiment 0007.
package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName is the group name used by this API.
const GroupName = "aggexp.io"

// SchemeGroupVersion is the group version used to register these types.
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

// Resource helps express a resource under this group.
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder collects funcs that add types to a runtime.Scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes, addConversionFuncs)
	// AddToScheme registers the types.
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion, &File{}, &FileList{})
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}

// File is a cluster-scoped projection of a filesystem file under
// the server's configured root directory. Read-only in this
// experiment.
type File struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   FileSpec   `json:"spec,omitempty"`
	Status FileStatus `json:"status,omitempty"`
}

// FileSpec captures the observed filesystem attributes.
type FileSpec struct {
	// Path is the absolute path on the server.
	Path string `json:"path,omitempty"`
	// Size is the file size in bytes.
	Size int64 `json:"size,omitempty"`
	// Mode is the file mode bits (octal, decimal-encoded).
	Mode uint32 `json:"mode,omitempty"`
}

// FileStatus records when the server last observed the file.
type FileStatus struct {
	// ObservedAt is the wall-clock time of the last scan.
	ObservedAt metav1.Time `json:"observedAt,omitempty"`
}

// FileList is a list of File objects.
type FileList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []File `json:"items"`
}

// DeepCopy for the external types.

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
