// Package scheme builds a runtime.Scheme at runtime based on the
// schema response the backend supplied. The scheme registers an
// unstructured type for the target GVK so the generic apiserver's
// endpoint machinery recognizes it.
//
// Compared to a typed-scheme experiment (e.g. 0007), this approach:
//
//   - Has no internal hub version. External-only. The aggregated
//     apiserver's PATCH path still works for Merge/JSON patches
//     (which operate on raw JSON) but Server-Side Apply would fail
//     because it needs type-aware merge.
//   - Uses unstructured's negotiated serializer (plain JSON/YAML)
//     rather than generated codecs.
package scheme

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

// ResourceDescriptor is the subset of GetSchemaResponse the scheme
// needs. Keeping this type in the scheme package decouples it from
// the generated proto package for unit-testability.
type ResourceDescriptor struct {
	GroupVersion schema.GroupVersion
	Resource     string
	Kind         string
	Singular     string
	Namespaced   bool
}

// Bundle holds everything the rest of the component needs to wire a
// dynamic resource into the generic apiserver.
type Bundle struct {
	Descriptor     ResourceDescriptor
	Scheme         *runtime.Scheme
	Codecs         serializer.CodecFactory
	ParameterCodec runtime.ParameterCodec
}

// Build produces a Bundle for the given resource descriptor.
//
// It registers the unstructured type at {descriptor.GroupVersion,
// descriptor.Kind} and again at {descriptor.GroupVersion, descriptor.Kind+"List"},
// plus the usual meta/v1 unversioned types the generic apiserver's
// discovery path expects.
func Build(d ResourceDescriptor) *Bundle {
	s := runtime.NewScheme()

	gv := d.GroupVersion

	// Register unstructured at the target kind and list kind.
	s.AddKnownTypeWithName(gv.WithKind(d.Kind), &unstructured.Unstructured{})
	s.AddKnownTypeWithName(gv.WithKind(d.Kind+"List"), &unstructured.UnstructuredList{})

	// metav1 types in the group's version: WatchEvent, ListOptions,
	// GetOptions, DeleteOptions, CreateOptions, UpdateOptions, PatchOptions.
	metav1.AddToGroupVersion(s, gv)

	// The installer also looks up ListOptions/etc. under the
	// "OptionsExternalVersion" which NewDefaultAPIGroupInfo
	// hardcodes to GroupVersion{Version: "v1"} (no group). Register
	// meta types there too so the installer finds them.
	metav1.AddToGroupVersion(s, schema.GroupVersion{Version: "v1"})

	// meta/v1 unversioned types. The discovery handler in
	// genericapiserver encodes APIResourceList via these.
	unversioned := schema.GroupVersion{Group: "", Version: "v1"}
	utilruntime.Must(s.SetVersionPriority(unversioned))
	s.AddUnversionedTypes(unversioned,
		&metav1.Status{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	)

	// Make sure the target GV is prioritised.
	utilruntime.Must(s.SetVersionPriority(gv))

	codecs := serializer.NewCodecFactory(s)

	return &Bundle{
		Descriptor:     d,
		Scheme:         s,
		Codecs:         codecs,
		ParameterCodec: runtime.NewParameterCodec(s),
	}
}
