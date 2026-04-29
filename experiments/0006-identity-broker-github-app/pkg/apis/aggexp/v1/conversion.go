package v1

import (
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0006-identity-broker-github-app/pkg/apis/aggexp"
)

// addConversionFuncs registers 1:1 converters between aggexp
// (internal) and aggexp/v1 (external). Hand-rolled because both
// types are byte-identical; a future v2 would require richer logic
// and conversion-gen.
func addConversionFuncs(scheme *runtime.Scheme) error {
	if err := scheme.AddConversionFunc((*Repo)(nil), (*aggexp.Repo)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertV1RepoToInternal(a.(*Repo), b.(*aggexp.Repo))
	}); err != nil {
		return err
	}
	if err := scheme.AddConversionFunc((*aggexp.Repo)(nil), (*Repo)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertInternalRepoToV1(a.(*aggexp.Repo), b.(*Repo))
	}); err != nil {
		return err
	}
	if err := scheme.AddConversionFunc((*RepoList)(nil), (*aggexp.RepoList)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertV1RepoListToInternal(a.(*RepoList), b.(*aggexp.RepoList))
	}); err != nil {
		return err
	}
	return scheme.AddConversionFunc((*aggexp.RepoList)(nil), (*RepoList)(nil), func(a, b interface{}, _ conversion.Scope) error {
		return convertInternalRepoListToV1(a.(*aggexp.RepoList), b.(*RepoList))
	})
}

func convertV1RepoToInternal(in *Repo, out *aggexp.Repo) error {
	out.TypeMeta = in.TypeMeta
	out.ObjectMeta = in.ObjectMeta
	out.Spec = aggexp.RepoSpec{
		Owner:         in.Spec.Owner,
		Name:          in.Spec.Name,
		Description:   in.Spec.Description,
		DefaultBranch: in.Spec.DefaultBranch,
		Private:       in.Spec.Private,
		Language:      in.Spec.Language,
		Stars:         in.Spec.Stars,
		HTMLURL:       in.Spec.HTMLURL,
	}
	out.Status = aggexp.RepoStatus{
		ObservedAt: in.Status.ObservedAt,
		ETag:       in.Status.ETag,
	}
	return nil
}

func convertInternalRepoToV1(in *aggexp.Repo, out *Repo) error {
	out.TypeMeta = in.TypeMeta
	out.ObjectMeta = in.ObjectMeta
	out.Spec = RepoSpec{
		Owner:         in.Spec.Owner,
		Name:          in.Spec.Name,
		Description:   in.Spec.Description,
		DefaultBranch: in.Spec.DefaultBranch,
		Private:       in.Spec.Private,
		Language:      in.Spec.Language,
		Stars:         in.Spec.Stars,
		HTMLURL:       in.Spec.HTMLURL,
	}
	out.Status = RepoStatus{
		ObservedAt: in.Status.ObservedAt,
		ETag:       in.Status.ETag,
	}
	return nil
}

func convertV1RepoListToInternal(in *RepoList, out *aggexp.RepoList) error {
	out.TypeMeta = in.TypeMeta
	out.ListMeta = in.ListMeta
	if in.Items != nil {
		out.Items = make([]aggexp.Repo, len(in.Items))
		for i := range in.Items {
			if err := convertV1RepoToInternal(&in.Items[i], &out.Items[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func convertInternalRepoListToV1(in *aggexp.RepoList, out *RepoList) error {
	out.TypeMeta = in.TypeMeta
	out.ListMeta = in.ListMeta
	if in.Items != nil {
		out.Items = make([]Repo, len(in.Items))
		for i := range in.Items {
			if err := convertInternalRepoToV1(&in.Items[i], &out.Items[i]); err != nil {
				return err
			}
		}
	}
	return nil
}
