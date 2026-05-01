// Package scheme builds a runtime.Scheme at runtime based on the
// schema response the backend supplied. The scheme registers an
// unstructured type for the target GVK so the generic apiserver's
// endpoint machinery recognizes it.
//
// Compared to 0013 (the forked predecessor), the scheme remains
// unstructured-centric. The richer OpenAPI composition and the
// OpenAPICanonicalTypeName synthesis happen in pkg/server, not
// here — this package's job is only to satisfy the Scheme + Codec
// factory contract.
//
// What this path cannot do (documented precisely in FINDINGS/0017):
//
//   - Scheme.New(gvk) on *unstructured.Unstructured returns an
//     object with an empty GVK because reflect.New produces a zero
//     value. The library's SSA path relies on Scheme.New-stamping-GVK
//     via typeToGVK, which only works for typed Go structs. This
//     manifests as "unstructured object has no kind" during the
//     first Apply, even when a well-formed OpenAPI spec is wired
//     through.
package scheme

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	"github.com/cheeseandcereal/aggexp/experiments/0029-declarative-admission-in-config/component/pkg/dyn"
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
	// UseTypedWrapper, when true, registers *dyn.Object and
	// *dyn.ObjectList under the target GVKs instead of
	// *unstructured.Unstructured. This is required for the
	// Server-Side Apply path to progress past the
	// "unstructured object has no kind" error (see FINDINGS/0017).
	UseTypedWrapper bool
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
// It registers the unstructured or dyn.Object type at
// {descriptor.GroupVersion, descriptor.Kind} and again at
// {descriptor.GroupVersion, descriptor.Kind+"List"}, plus the usual
// meta/v1 unversioned types the generic apiserver's discovery path
// expects.
func Build(d ResourceDescriptor) *Bundle {
	s := runtime.NewScheme()

	gv := d.GroupVersion

	if d.UseTypedWrapper {
		s.AddKnownTypeWithName(gv.WithKind(d.Kind), &dyn.Object{})
		s.AddKnownTypeWithName(gv.WithKind(d.Kind+"List"), &dyn.ObjectList{})
		// Register the internal version too. The generic
		// apiserver's installer uses hubVersion =
		// {Group: gv.Group, Version: runtime.APIVersionInternal}
		// as the target of its toUnversioned() conversions inside
		// the SSA path. Without a registration here the SMD
		// manager fails with "no kind Note is registered for the
		// internal version".
		internal := schema.GroupVersion{Group: gv.Group, Version: runtime.APIVersionInternal}
		s.AddKnownTypeWithName(internal.WithKind(d.Kind), &dyn.Object{})
		s.AddKnownTypeWithName(internal.WithKind(d.Kind+"List"), &dyn.ObjectList{})
	} else {
		s.AddKnownTypeWithName(gv.WithKind(d.Kind), &unstructured.Unstructured{})
		s.AddKnownTypeWithName(gv.WithKind(d.Kind+"List"), &unstructured.UnstructuredList{})
	}

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
