// Package apiserver holds the shared Scheme, Codecs, and
// ParameterCodec for experiment 0048's served widgets.aggexp.io group.
//
// The served Widget types are the 0046-GENERATED package
// (pkg/apis/widgets/v1) copied in verbatim — the capstone proves the
// OpenAPI-first generator's output composes with the multi-replica
// machinery. v1.AddToScheme registers the external + internal GVs, the
// identity conversions, and the field-label conversion func; nothing
// type-level is hand-written here.
package apiserver

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	widgetsv1 "github.com/cheeseandcereal/aggexp/experiments/0048-library-multireplica-vertical-slice/pkg/apis/widgets/v1"
)

var (
	// Scheme is the shared scheme holding the generated widgets.aggexp.io
	// types plus the meta/v1 unversioned types kube-apiserver's
	// discovery path expects.
	Scheme = runtime.NewScheme()
	// Codecs is a serializer factory derived from Scheme.
	Codecs = serializer.NewCodecFactory(Scheme)
	// ParameterCodec converts URL query parameters.
	ParameterCodec = runtime.NewParameterCodec(Scheme)
)

func init() {
	// Generated AddToScheme: external + internal GVs, identity
	// conversions, field-label conversions.
	utilruntime.Must(widgetsv1.AddToScheme(Scheme))

	metav1.AddToGroupVersion(Scheme, widgetsv1.SchemeGroupVersion)
	metav1.AddToGroupVersion(Scheme, schema.GroupVersion{Version: "v1"})

	unversioned := schema.GroupVersion{Group: "", Version: "v1"}
	utilruntime.Must(Scheme.SetVersionPriority(unversioned))
	Scheme.AddUnversionedTypes(unversioned,
		&metav1.Status{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	)
}
