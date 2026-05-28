package v1

import (
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0033-crd-cas-object-locking/pkg/apis/aggexp"
)

// addConversionFuncs registers 1:1 converters between aggexp
// (internal) and aggexp/v1 (external). Mechanical and identical-shape.
func addConversionFuncs(scheme *runtime.Scheme) error {
	if err := scheme.AddConversionFunc((*Gizmo)(nil), (*aggexp.Gizmo)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertV1ToInternal(a.(*Gizmo), b.(*aggexp.Gizmo))
	}); err != nil {
		return err
	}
	if err := scheme.AddConversionFunc((*aggexp.Gizmo)(nil), (*Gizmo)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertInternalToV1(a.(*aggexp.Gizmo), b.(*Gizmo))
	}); err != nil {
		return err
	}
	if err := scheme.AddConversionFunc((*GizmoList)(nil), (*aggexp.GizmoList)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertV1ListToInternal(a.(*GizmoList), b.(*aggexp.GizmoList))
	}); err != nil {
		return err
	}
	return scheme.AddConversionFunc((*aggexp.GizmoList)(nil), (*GizmoList)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertInternalListToV1(a.(*aggexp.GizmoList), b.(*GizmoList))
	})
}

func convertV1ToInternal(in *Gizmo, out *aggexp.Gizmo) error {
	out.TypeMeta = in.TypeMeta
	out.ObjectMeta = in.ObjectMeta
	out.Spec = aggexp.GizmoSpec{Color: in.Spec.Color, Counter: in.Spec.Counter}
	out.Status = aggexp.GizmoStatus{LastWriter: in.Status.LastWriter}
	return nil
}

func convertInternalToV1(in *aggexp.Gizmo, out *Gizmo) error {
	out.TypeMeta = in.TypeMeta
	out.ObjectMeta = in.ObjectMeta
	out.Spec = GizmoSpec{Color: in.Spec.Color, Counter: in.Spec.Counter}
	out.Status = GizmoStatus{LastWriter: in.Status.LastWriter}
	return nil
}

func convertV1ListToInternal(in *GizmoList, out *aggexp.GizmoList) error {
	out.TypeMeta = in.TypeMeta
	out.ListMeta = in.ListMeta
	if in.Items != nil {
		out.Items = make([]aggexp.Gizmo, len(in.Items))
		for i := range in.Items {
			if err := convertV1ToInternal(&in.Items[i], &out.Items[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func convertInternalListToV1(in *aggexp.GizmoList, out *GizmoList) error {
	out.TypeMeta = in.TypeMeta
	out.ListMeta = in.ListMeta
	if in.Items != nil {
		out.Items = make([]Gizmo, len(in.Items))
		for i := range in.Items {
			if err := convertInternalToV1(&in.Items[i], &out.Items[i]); err != nil {
				return err
			}
		}
	}
	return nil
}
