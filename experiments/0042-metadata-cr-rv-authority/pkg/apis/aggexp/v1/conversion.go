package v1

import (
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0042-metadata-cr-rv-authority/pkg/apis/aggexp"
)

// addConversionFuncs registers 1:1 converters between internal
// (aggexp.Widget) and external (v1.Widget). The two shapes are
// identical; the conversions are mechanical. The library's PATCH /
// SSA machinery uses the internal hub version, so both directions
// must be registered.
func addConversionFuncs(scheme *runtime.Scheme) error {
	if err := scheme.AddConversionFunc((*Widget)(nil), (*aggexp.Widget)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertV1ToInternal(a.(*Widget), b.(*aggexp.Widget))
	}); err != nil {
		return err
	}
	if err := scheme.AddConversionFunc((*aggexp.Widget)(nil), (*Widget)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertInternalToV1(a.(*aggexp.Widget), b.(*Widget))
	}); err != nil {
		return err
	}
	if err := scheme.AddConversionFunc((*WidgetList)(nil), (*aggexp.WidgetList)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertV1ListToInternal(a.(*WidgetList), b.(*aggexp.WidgetList))
	}); err != nil {
		return err
	}
	return scheme.AddConversionFunc((*aggexp.WidgetList)(nil), (*WidgetList)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertInternalListToV1(a.(*aggexp.WidgetList), b.(*WidgetList))
	})
}

func convertV1ToInternal(in *Widget, out *aggexp.Widget) error {
	out.TypeMeta = in.TypeMeta
	out.ObjectMeta = in.ObjectMeta
	out.Spec = aggexp.WidgetSpec{Color: in.Spec.Color, Size: in.Spec.Size}
	out.Status = aggexp.WidgetStatus{Phase: in.Status.Phase}
	return nil
}

func convertInternalToV1(in *aggexp.Widget, out *Widget) error {
	out.TypeMeta = in.TypeMeta
	out.ObjectMeta = in.ObjectMeta
	out.Spec = WidgetSpec{Color: in.Spec.Color, Size: in.Spec.Size}
	out.Status = WidgetStatus{Phase: in.Status.Phase}
	return nil
}

func convertV1ListToInternal(in *WidgetList, out *aggexp.WidgetList) error {
	out.TypeMeta = in.TypeMeta
	out.ListMeta = in.ListMeta
	if in.Items != nil {
		out.Items = make([]aggexp.Widget, len(in.Items))
		for i := range in.Items {
			if err := convertV1ToInternal(&in.Items[i], &out.Items[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func convertInternalListToV1(in *aggexp.WidgetList, out *WidgetList) error {
	out.TypeMeta = in.TypeMeta
	out.ListMeta = in.ListMeta
	if in.Items != nil {
		out.Items = make([]Widget, len(in.Items))
		for i := range in.Items {
			if err := convertInternalToV1(&in.Items[i], &out.Items[i]); err != nil {
				return err
			}
		}
	}
	return nil
}
