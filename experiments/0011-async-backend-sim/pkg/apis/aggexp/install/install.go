// Package install installs the aggexp.io API group into a runtime.Scheme.
package install

import (
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0011-async-backend-sim/pkg/apis/aggexp"
	aggexpv1 "github.com/cheeseandcereal/aggexp/experiments/0011-async-backend-sim/pkg/apis/aggexp/v1"
)

// Install registers both the internal and external (v1) versions of
// the aggexp.io group into scheme.
func Install(scheme *runtime.Scheme) {
	utilruntime.Must(aggexp.AddToScheme(scheme))
	utilruntime.Must(aggexpv1.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(aggexpv1.SchemeGroupVersion))
}
