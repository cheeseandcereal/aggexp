package v1

import (
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0007-runtime-fs-driver/pkg/apis/aggexp"
)

// addConversionFuncs registers 1:1 converters between aggexp
// (internal) and aggexp/v1 (external). Identical-shape types; the
// conversions are mechanical. Extracted-runtime substrate doesn't
// care about these; they live with the experiment's type scheme.
func addConversionFuncs(scheme *runtime.Scheme) error {
	if err := scheme.AddConversionFunc((*File)(nil), (*aggexp.File)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertV1FileToInternal(a.(*File), b.(*aggexp.File))
	}); err != nil {
		return err
	}
	if err := scheme.AddConversionFunc((*aggexp.File)(nil), (*File)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertInternalFileToV1(a.(*aggexp.File), b.(*File))
	}); err != nil {
		return err
	}
	if err := scheme.AddConversionFunc((*FileList)(nil), (*aggexp.FileList)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertV1FileListToInternal(a.(*FileList), b.(*aggexp.FileList))
	}); err != nil {
		return err
	}
	return scheme.AddConversionFunc((*aggexp.FileList)(nil), (*FileList)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertInternalFileListToV1(a.(*aggexp.FileList), b.(*FileList))
	})
}

func convertV1FileToInternal(in *File, out *aggexp.File) error {
	out.TypeMeta = in.TypeMeta
	out.ObjectMeta = in.ObjectMeta
	out.Spec = aggexp.FileSpec{Path: in.Spec.Path, Size: in.Spec.Size, Mode: in.Spec.Mode}
	out.Status = aggexp.FileStatus{ObservedAt: in.Status.ObservedAt}
	return nil
}

func convertInternalFileToV1(in *aggexp.File, out *File) error {
	out.TypeMeta = in.TypeMeta
	out.ObjectMeta = in.ObjectMeta
	out.Spec = FileSpec{Path: in.Spec.Path, Size: in.Spec.Size, Mode: in.Spec.Mode}
	out.Status = FileStatus{ObservedAt: in.Status.ObservedAt}
	return nil
}

func convertV1FileListToInternal(in *FileList, out *aggexp.FileList) error {
	out.TypeMeta = in.TypeMeta
	out.ListMeta = in.ListMeta
	if in.Items != nil {
		out.Items = make([]aggexp.File, len(in.Items))
		for i := range in.Items {
			if err := convertV1FileToInternal(&in.Items[i], &out.Items[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func convertInternalFileListToV1(in *aggexp.FileList, out *FileList) error {
	out.TypeMeta = in.TypeMeta
	out.ListMeta = in.ListMeta
	if in.Items != nil {
		out.Items = make([]File, len(in.Items))
		for i := range in.Items {
			if err := convertInternalFileToV1(&in.Items[i], &out.Items[i]); err != nil {
				return err
			}
		}
	}
	return nil
}
