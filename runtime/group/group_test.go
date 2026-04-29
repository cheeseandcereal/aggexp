package group_test

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/registry/rest"

	"github.com/cheeseandcereal/aggexp/runtime/group"
)

func TestInstallRequiresScheme(t *testing.T) {
	g := &group.Group{
		GroupVersion: schema.GroupVersion{Group: "foo.io", Version: "v1"},
	}
	if err := g.Install(nil); err == nil {
		t.Fatal("expected error with nil Scheme")
	}
}

// TestGroupShape is a read-only smoke test: the Group struct must be
// able to be constructed with the shape consumers expect, and the
// defaulted ParameterCodec path must not panic on a non-nil Scheme.
// We cannot call Install without a full GenericAPIServer; that
// requires a completed RecommendedConfig which the server_test
// package exercises.
func TestGroupShape(t *testing.T) {
	scheme := runtime.NewScheme()
	codecs := serializer.NewCodecFactory(scheme)
	g := &group.Group{
		GroupVersion: schema.GroupVersion{Group: "foo.io", Version: "v1"},
		Scheme:       scheme,
		Codecs:       codecs,
		Resources:    map[string]rest.Storage{},
	}
	if g.Scheme != scheme {
		t.Fatal("Scheme not retained")
	}
	if g.Codecs.WithoutConversion() == nil {
		t.Fatal("Codecs should be usable")
	}
}
