package crdbackend

import (
	"k8s.io/apimachinery/pkg/labels"
)

// everything is a labels.Selector that matches everything.
func everything() labels.Selector {
	return labels.Everything()
}
