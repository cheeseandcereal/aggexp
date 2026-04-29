package v1

import (
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0009-ack-aggregated-s3/pkg/apis/aggexp"
)

// addConversionFuncs registers 1:1 converters between internal
// (aggexp.Bucket) and external (v1.Bucket).
func addConversionFuncs(scheme *runtime.Scheme) error {
	if err := scheme.AddConversionFunc((*Bucket)(nil), (*aggexp.Bucket)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertV1ToInternal(a.(*Bucket), b.(*aggexp.Bucket))
	}); err != nil {
		return err
	}
	if err := scheme.AddConversionFunc((*aggexp.Bucket)(nil), (*Bucket)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertInternalToV1(a.(*aggexp.Bucket), b.(*Bucket))
	}); err != nil {
		return err
	}
	if err := scheme.AddConversionFunc((*BucketList)(nil), (*aggexp.BucketList)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertV1ListToInternal(a.(*BucketList), b.(*aggexp.BucketList))
	}); err != nil {
		return err
	}
	return scheme.AddConversionFunc((*aggexp.BucketList)(nil), (*BucketList)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertInternalListToV1(a.(*aggexp.BucketList), b.(*BucketList))
	})
}

func convertV1ToInternal(in *Bucket, out *aggexp.Bucket) error {
	out.TypeMeta = in.TypeMeta
	out.ObjectMeta = in.ObjectMeta
	out.Spec = aggexp.BucketSpec{Region: in.Spec.Region, Tags: copyStringMap(in.Spec.Tags)}
	out.Status = aggexp.BucketStatus{
		Region:       in.Status.Region,
		CreationDate: in.Status.CreationDate,
		ObservedAt:   in.Status.ObservedAt,
		Phase:        in.Status.Phase,
	}
	return nil
}

func convertInternalToV1(in *aggexp.Bucket, out *Bucket) error {
	out.TypeMeta = in.TypeMeta
	out.ObjectMeta = in.ObjectMeta
	out.Spec = BucketSpec{Region: in.Spec.Region, Tags: copyStringMap(in.Spec.Tags)}
	out.Status = BucketStatus{
		Region:       in.Status.Region,
		CreationDate: in.Status.CreationDate,
		ObservedAt:   in.Status.ObservedAt,
		Phase:        in.Status.Phase,
	}
	return nil
}

func convertV1ListToInternal(in *BucketList, out *aggexp.BucketList) error {
	out.TypeMeta = in.TypeMeta
	out.ListMeta = in.ListMeta
	if in.Items != nil {
		out.Items = make([]aggexp.Bucket, len(in.Items))
		for i := range in.Items {
			if err := convertV1ToInternal(&in.Items[i], &out.Items[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func convertInternalListToV1(in *aggexp.BucketList, out *BucketList) error {
	out.TypeMeta = in.TypeMeta
	out.ListMeta = in.ListMeta
	if in.Items != nil {
		out.Items = make([]Bucket, len(in.Items))
		for i := range in.Items {
			if err := convertInternalToV1(&in.Items[i], &out.Items[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func copyStringMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
