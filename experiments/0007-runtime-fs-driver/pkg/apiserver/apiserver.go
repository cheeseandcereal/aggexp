// Package apiserver holds the shared Scheme, Codecs, and
// ParameterCodec for experiment 0007. This file mirrors the
// experiment-level pattern established in 0002/0004; it does not
// belong in the substrate because the scheme is per-experiment.
package apiserver

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	aggexpinstall "github.com/cheeseandcereal/aggexp/experiments/0007-runtime-fs-driver/pkg/apis/aggexp/install"
)

var (
	// Scheme is the shared scheme holding the aggexp.io types plus
	// the meta/v1 unversioned types kube-apiserver's discovery path
	// expects to find.
	Scheme = runtime.NewScheme()
	// Codecs is a serializer factory derived from Scheme.
	Codecs = serializer.NewCodecFactory(Scheme)
	// ParameterCodec converts URL query parameters.
	ParameterCodec = runtime.NewParameterCodec(Scheme)
)

func init() {
	aggexpinstall.Install(Scheme)

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
