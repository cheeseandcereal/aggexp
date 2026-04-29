package v1

import (
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0003-custom-authorizer-external-policy/pkg/apis/aggexp"
)

// addConversionFuncs registers 1:1 converters between aggexp
// (internal) and aggexp/v1 (external). Hand-rolled because both
// types are byte-identical; a future v2 would require richer logic
// and conversion-gen.
func addConversionFuncs(scheme *runtime.Scheme) error {
	if err := scheme.AddConversionFunc((*Hello)(nil), (*aggexp.Hello)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertV1HelloToInternal(a.(*Hello), b.(*aggexp.Hello))
	}); err != nil {
		return err
	}
	if err := scheme.AddConversionFunc((*aggexp.Hello)(nil), (*Hello)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertInternalHelloToV1(a.(*aggexp.Hello), b.(*Hello))
	}); err != nil {
		return err
	}
	if err := scheme.AddConversionFunc((*HelloList)(nil), (*aggexp.HelloList)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertV1HelloListToInternal(a.(*HelloList), b.(*aggexp.HelloList))
	}); err != nil {
		return err
	}
	return scheme.AddConversionFunc((*aggexp.HelloList)(nil), (*HelloList)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertInternalHelloListToV1(a.(*aggexp.HelloList), b.(*HelloList))
	})
}

func convertV1HelloToInternal(in *Hello, out *aggexp.Hello) error {
	out.TypeMeta = in.TypeMeta
	out.ObjectMeta = in.ObjectMeta
	out.Spec = aggexp.HelloSpec{Greeting: in.Spec.Greeting}
	out.Status = aggexp.HelloStatus{ObservedGreeting: in.Status.ObservedGreeting}
	return nil
}

func convertInternalHelloToV1(in *aggexp.Hello, out *Hello) error {
	out.TypeMeta = in.TypeMeta
	out.ObjectMeta = in.ObjectMeta
	out.Spec = HelloSpec{Greeting: in.Spec.Greeting}
	out.Status = HelloStatus{ObservedGreeting: in.Status.ObservedGreeting}
	return nil
}

func convertV1HelloListToInternal(in *HelloList, out *aggexp.HelloList) error {
	out.TypeMeta = in.TypeMeta
	out.ListMeta = in.ListMeta
	if in.Items != nil {
		out.Items = make([]aggexp.Hello, len(in.Items))
		for i := range in.Items {
			if err := convertV1HelloToInternal(&in.Items[i], &out.Items[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func convertInternalHelloListToV1(in *aggexp.HelloList, out *HelloList) error {
	out.TypeMeta = in.TypeMeta
	out.ListMeta = in.ListMeta
	if in.Items != nil {
		out.Items = make([]Hello, len(in.Items))
		for i := range in.Items {
			if err := convertInternalHelloToV1(&in.Items[i], &out.Items[i]); err != nil {
				return err
			}
		}
	}
	return nil
}
