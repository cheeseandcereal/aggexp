// Package apiserver contains the shared Scheme, Codecs, and
// ParameterCodec used by the aggexp-hello extension apiserver. Any
// type that needs to be (de)serialized must be registered here.
package apiserver

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	aggexpinstall "github.com/cheeseandcereal/aggexp/experiments/0002-hello-aggregated/pkg/apis/aggexp/install"
)

var (
	// Scheme is the shared scheme used to (de)serialize objects. It
	// holds the aggexp.io/v1 types plus the meta/v1 unversioned
	// types required by kube-apiserver's discovery path.
	Scheme = runtime.NewScheme()

	// Codecs is a serializer factory derived from Scheme.
	Codecs = serializer.NewCodecFactory(Scheme)

	// ParameterCodec converts URL query parameters to/from typed
	// forms (e.g. list options).
	ParameterCodec = runtime.NewParameterCodec(Scheme)
)

func init() {
	aggexpinstall.Install(Scheme)

	// Register the unversioned types kube-apiserver expects under
	// v1 during discovery / error responses. Skipping this looks
	// fine locally but breaks aggregation-layer proxying.
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
