// Package install installs the aggexp.io API group into a runtime.Scheme.
package install

import (
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/runtime"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0006-identity-broker-github-app/pkg/apis/aggexp"
	aggexpv1 "github.com/cheeseandcereal/aggexp/experiments/0006-identity-broker-github-app/pkg/apis/aggexp/v1"
)

// Install registers both the internal ("__internal") and external
// (v1) versions of the aggexp.io group into a scheme. Both are
// required: the generic apiserver round-trips through the internal
// version on PATCH / server-side apply.
func Install(scheme *runtime.Scheme) {
	utilruntime.Must(aggexp.AddToScheme(scheme))
	utilruntime.Must(aggexpv1.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(aggexpv1.SchemeGroupVersion))
}
