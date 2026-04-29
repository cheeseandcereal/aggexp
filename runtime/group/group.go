package group

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
)

// Group describes a single API group + version + set of resources
// to install into the generic apiserver. The shape is intentionally
// flat — experiments that need multi-version support can call
// Install more than once.
type Group struct {
	// GroupVersion is the group/version served by this Group.
	GroupVersion schema.GroupVersion
	// Scheme holds the internal and external types.
	Scheme *runtime.Scheme
	// Codecs is the codec factory derived from Scheme.
	Codecs serializer.CodecFactory
	// ParameterCodec is used to decode URL query parameters. If
	// nil, runtime.NewParameterCodec(Scheme) is constructed.
	ParameterCodec runtime.ParameterCodec
	// Resources is the resource-name -> storage map for the group.
	// Values are typically *runtime/storage.REST.
	Resources map[string]rest.Storage
}

// Install registers the group on s. Safe to call from a
// runtime/server.Options.Run installer slice.
func (g *Group) Install(s *genericapiserver.GenericAPIServer) error {
	if g.Scheme == nil {
		return fmt.Errorf("Group.Scheme is required")
	}
	pc := g.ParameterCodec
	if pc == nil {
		pc = runtime.NewParameterCodec(g.Scheme)
	}
	info := genericapiserver.NewDefaultAPIGroupInfo(
		g.GroupVersion.Group, g.Scheme, pc, g.Codecs,
	)
	if info.VersionedResourcesStorageMap == nil {
		info.VersionedResourcesStorageMap = map[string]map[string]rest.Storage{}
	}
	info.VersionedResourcesStorageMap[g.GroupVersion.Version] = g.Resources
	if err := s.InstallAPIGroup(&info); err != nil {
		return fmt.Errorf("install api group %s: %w", g.GroupVersion, err)
	}
	return nil
}
