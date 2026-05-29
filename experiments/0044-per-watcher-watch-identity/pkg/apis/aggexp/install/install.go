// Package install registers both the internal and external (v1)
// versions of the aggexp.io group into a shared runtime.Scheme.
package install

import (
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0044-per-watcher-watch-identity/pkg/apis/aggexp"
	aggexpv1 "github.com/cheeseandcereal/aggexp/experiments/0044-per-watcher-watch-identity/pkg/apis/aggexp/v1"
)

// Install registers the aggexp.io internal and v1 types on scheme.
func Install(scheme *runtime.Scheme) {
	utilruntime.Must(aggexp.AddToScheme(scheme))
	utilruntime.Must(aggexpv1.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(aggexpv1.SchemeGroupVersion))
}
