package scheme

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

// Canonical type names used as keys into the OpenAPI defs map.
// These MUST match what
// kube-openapi/pkg/util.GetCanonicalTypeName / openapi.NewDefinitionNamer
// return for the registered sample objects. Exported so the
// openapi package can key its composed schemas correctly.
const (
	UnstructuredCanonicalName     = "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured.Unstructured"
	UnstructuredListCanonicalName = "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured.UnstructuredList"
	DynObjectCanonicalName        = "github.com/cheeseandcereal/aggexp/runtime/component/scheme.Object"
	DynObjectListCanonicalName    = "github.com/cheeseandcereal/aggexp/runtime/component/scheme.ObjectList"
)

// ResourceDescriptor is the subset of GetSchemaResponse the scheme
// needs. Kept decoupled from the proto package so the scheme is
// unit-testable without a gRPC conn.
type ResourceDescriptor struct {
	GroupVersion schema.GroupVersion
	Resource     string
	Kind         string
	Singular     string
	Namespaced   bool
	// UseTypedWrapper registers *Object / *ObjectList under the
	// target GVK instead of *unstructured.Unstructured. Required for
	// Server-Side Apply to progress past the
	// "unstructured object has no kind" error; see package doc.
	UseTypedWrapper bool
}

// Bundle holds everything a consumer needs to wire a dynamic
// resource into the generic apiserver.
type Bundle struct {
	Descriptor     ResourceDescriptor
	Scheme         *runtime.Scheme
	Codecs         serializer.CodecFactory
	ParameterCodec runtime.ParameterCodec
	// ItemCanonicalName is the defs-map key under which the
	// resource's item schema must be registered (differs between
	// the unstructured and typed-wrapper paths). Convenience for
	// callers composing the OpenAPI defs map.
	ItemCanonicalName string
	// ListCanonicalName is the analogous key for the list type.
	ListCanonicalName string
}

// Build produces a Bundle for the given resource descriptor.
//
// It registers the chosen item type at {GroupVersion, Kind} and
// {GroupVersion, Kind+"List"}; when UseTypedWrapper is true, it
// also registers the internal group-version with the same Go type
// so the SSA path's toUnversioned call has an entry to find.
//
// metav1 unversioned types are added so generic discovery and
// APIResourceList encoding work.
func Build(d ResourceDescriptor) *Bundle {
	s := runtime.NewScheme()

	gv := d.GroupVersion

	if d.UseTypedWrapper {
		s.AddKnownTypeWithName(gv.WithKind(d.Kind), &Object{})
		s.AddKnownTypeWithName(gv.WithKind(d.Kind+"List"), &ObjectList{})
		internal := schema.GroupVersion{Group: gv.Group, Version: runtime.APIVersionInternal}
		s.AddKnownTypeWithName(internal.WithKind(d.Kind), &Object{})
		s.AddKnownTypeWithName(internal.WithKind(d.Kind+"List"), &ObjectList{})
	} else {
		s.AddKnownTypeWithName(gv.WithKind(d.Kind), &unstructured.Unstructured{})
		s.AddKnownTypeWithName(gv.WithKind(d.Kind+"List"), &unstructured.UnstructuredList{})
	}

	// metav1 types in the group's version (WatchEvent, ListOptions,
	// GetOptions, DeleteOptions, CreateOptions, UpdateOptions,
	// PatchOptions).
	metav1.AddToGroupVersion(s, gv)

	// The installer also looks up ListOptions/etc under the
	// hard-coded {Version: "v1"} options GroupVersion.
	metav1.AddToGroupVersion(s, schema.GroupVersion{Version: "v1"})

	// meta/v1 unversioned types used by the discovery handler.
	unversioned := schema.GroupVersion{Group: "", Version: "v1"}
	utilruntime.Must(s.SetVersionPriority(unversioned))
	s.AddUnversionedTypes(unversioned,
		&metav1.Status{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	)

	utilruntime.Must(s.SetVersionPriority(gv))

	codecs := serializer.NewCodecFactory(s)

	itemKey := UnstructuredCanonicalName
	listKey := UnstructuredListCanonicalName
	if d.UseTypedWrapper {
		itemKey = DynObjectCanonicalName
		listKey = DynObjectListCanonicalName
	}

	return &Bundle{
		Descriptor:        d,
		Scheme:            s,
		Codecs:            codecs,
		ParameterCodec:    runtime.NewParameterCodec(s),
		ItemCanonicalName: itemKey,
		ListCanonicalName: listKey,
	}
}
