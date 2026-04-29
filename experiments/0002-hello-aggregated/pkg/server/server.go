// Package server wires the etcd-less, aggexp-flavored generic
// apiserver together.  The shape mirrors k8s.io/sample-apiserver but
// drops EtcdOptions, Admission, and RecommendedOptions entirely in
// favor of a hand-rolled options struct — matching the pattern used
// by sigs.k8s.io/metrics-server.
package server

import (
	"context"
	"fmt"
	"net"

	"github.com/spf13/pflag"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/endpoints/openapi"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/client-go/kubernetes"
	utilversion "k8s.io/component-base/version"
	netutils "k8s.io/utils/net"

	aggexpv1 "github.com/cheeseandcereal/aggexp/experiments/0002-hello-aggregated/pkg/apis/aggexp/v1"
	"github.com/cheeseandcereal/aggexp/experiments/0002-hello-aggregated/pkg/apiserver"
	generatedopenapi "github.com/cheeseandcereal/aggexp/experiments/0002-hello-aggregated/pkg/generated/openapi"
	"github.com/cheeseandcereal/aggexp/experiments/0002-hello-aggregated/pkg/registry/hello"
)

// Options bundles the generic apiserver options we actually use. The
// absence of EtcdOptions is deliberate.
type Options struct {
	SecureServing  *genericoptions.SecureServingOptionsWithLoopback
	Authentication *genericoptions.DelegatingAuthenticationOptions
	Authorization  *genericoptions.DelegatingAuthorizationOptions
	Audit          *genericoptions.AuditOptions
	Features       *genericoptions.FeatureOptions
	CoreAPI        *genericoptions.CoreAPIOptions
}

// NewOptions returns the default options, pre-wired for an AA
// deployed into the aggexp-system namespace in-cluster.
func NewOptions() *Options {
	sso := genericoptions.NewSecureServingOptions()
	sso.BindPort = 8443
	return &Options{
		SecureServing:  sso.WithLoopback(),
		Authentication: genericoptions.NewDelegatingAuthenticationOptions(),
		Authorization:  genericoptions.NewDelegatingAuthorizationOptions(),
		Audit:          genericoptions.NewAuditOptions(),
		Features:       genericoptions.NewFeatureOptions(),
		CoreAPI:        genericoptions.NewCoreAPIOptions(),
	}
}

// AddFlags registers CLI flags onto fs. The grouping mirrors
// RecommendedOptions' AddFlags.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.SecureServing.AddFlags(fs)
	o.Authentication.AddFlags(fs)
	o.Authorization.AddFlags(fs)
	o.Audit.AddFlags(fs)
	o.Features.AddFlags(fs)
	o.CoreAPI.AddFlags(fs)
}

// Validate returns any combined validation errors across the
// sub-options.
func (o *Options) Validate() error {
	var errs []error
	errs = append(errs, o.SecureServing.Validate()...)
	errs = append(errs, o.Authentication.Validate()...)
	errs = append(errs, o.Authorization.Validate()...)
	errs = append(errs, o.Audit.Validate()...)
	errs = append(errs, o.Features.Validate()...)
	errs = append(errs, o.CoreAPI.Validate()...)
	return utilerrors.NewAggregate(errs)
}

// Config builds the generic apiserver RecommendedConfig.
func (o *Options) Config() (*genericapiserver.RecommendedConfig, error) {
	if err := o.SecureServing.MaybeDefaultWithSelfSignedCerts(
		"localhost", nil, []net.IP{netutils.ParseIPSloppy("127.0.0.1")},
	); err != nil {
		return nil, fmt.Errorf("creating self-signed certs: %w", err)
	}

	serverConfig := genericapiserver.NewRecommendedConfig(apiserver.Codecs)

	// EffectiveVersion is required in 1.32+; PrepareRun panics without it.
	serverConfig.EffectiveVersion = utilversion.DefaultKubeEffectiveVersion()

	// OpenAPI v2 + v3 are both wired so /openapi/v2 and /openapi/v3
	// are both served. openapi-gen produced GetOpenAPIDefinitions,
	// and the DefinitionNamer derives GVK extensions from the
	// registered Scheme -- the thing 0001 was missing.
	serverConfig.OpenAPIConfig = genericapiserver.DefaultOpenAPIConfig(
		generatedopenapi.GetOpenAPIDefinitions,
		openapi.NewDefinitionNamer(apiserver.Scheme),
	)
	serverConfig.OpenAPIConfig.Info.Title = "aggexp-hello"
	serverConfig.OpenAPIConfig.Info.Version = "0.1"

	serverConfig.OpenAPIV3Config = genericapiserver.DefaultOpenAPIV3Config(
		generatedopenapi.GetOpenAPIDefinitions,
		openapi.NewDefinitionNamer(apiserver.Scheme),
	)
	serverConfig.OpenAPIV3Config.Info.Title = "aggexp-hello"
	serverConfig.OpenAPIV3Config.Info.Version = "0.1"

	if err := o.SecureServing.ApplyTo(&serverConfig.Config.SecureServing, &serverConfig.Config.LoopbackClientConfig); err != nil {
		return nil, fmt.Errorf("applying secure-serving options: %w", err)
	}
	if err := o.Authentication.ApplyTo(&serverConfig.Config.Authentication, serverConfig.SecureServing, serverConfig.OpenAPIConfig); err != nil {
		return nil, fmt.Errorf("applying authn options: %w", err)
	}
	if err := o.Authorization.ApplyTo(&serverConfig.Config.Authorization); err != nil {
		return nil, fmt.Errorf("applying authz options: %w", err)
	}
	if err := o.Audit.ApplyTo(&serverConfig.Config); err != nil {
		return nil, fmt.Errorf("applying audit options: %w", err)
	}
	if err := o.CoreAPI.ApplyTo(serverConfig); err != nil {
		return nil, fmt.Errorf("applying coreapi options: %w", err)
	}
	// Features.ApplyTo needs a kubernetes clientset when
	// --enable-priority-and-fairness is set (which it is by default).
	// CoreAPI.ApplyTo populated ClientConfig + SharedInformerFactory
	// above; derive the clientset from it.
	var kubeClient kubernetes.Interface
	if serverConfig.ClientConfig != nil {
		c, err := kubernetes.NewForConfig(serverConfig.ClientConfig)
		if err != nil {
			return nil, fmt.Errorf("building kubernetes clientset: %w", err)
		}
		kubeClient = c
	}
	if err := o.Features.ApplyTo(&serverConfig.Config, kubeClient, serverConfig.SharedInformerFactory); err != nil {
		return nil, fmt.Errorf("applying feature options: %w", err)
	}

	return serverConfig, nil
}

// Run completes configuration and runs the apiserver until ctx is
// canceled or serving fails.
func (o *Options) Run(ctx context.Context) error {
	cfg, err := o.Config()
	if err != nil {
		return err
	}
	completed := cfg.Complete()

	server, err := completed.New("aggexp-hello-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return fmt.Errorf("creating apiserver: %w", err)
	}

	// Register the Hello storage under aggexp.io/v1.
	helloREST := hello.NewREST()
	if err := server.AddPostStartHook("aggexp-hello-bookmarker", func(hookCtx genericapiserver.PostStartHookContext) error {
		helloREST.Start(hookCtx.Context)
		return nil
	}); err != nil {
		return err
	}

	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(
		aggexpv1.GroupName, apiserver.Scheme, metav1.ParameterCodec, apiserver.Codecs,
	)
	apiGroupInfo.VersionedResourcesStorageMap[aggexpv1.SchemeGroupVersion.Version] = map[string]rest.Storage{
		"hellos": helloREST,
	}
	if err := server.InstallAPIGroup(&apiGroupInfo); err != nil {
		return fmt.Errorf("installing API group: %w", err)
	}

	prepared := server.PrepareRun()
	return prepared.RunWithContext(ctx)
}
