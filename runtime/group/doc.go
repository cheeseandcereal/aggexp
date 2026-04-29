// Package group installs an aggregated API group into a generic
// apiserver. A Group bundles:
//
//   - the Scheme + Codecs holding the group's internal and external
//     types (these come from the experiment's pkg/apiserver);
//   - the ParameterCodec used for URL query-parameter decoding;
//   - the GroupVersion to install;
//   - a map of resource-name -> rest.Storage (typically the
//     runtime/storage adapter).
//
// Install performs the NewDefaultAPIGroupInfo + VersionedResources
// StorageMap + InstallAPIGroup dance. Post-start hooks (e.g. poll
// loops) are registered via runtime/server.Options.Run, not here.
package group
