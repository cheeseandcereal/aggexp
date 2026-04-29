// Package server wires the etcd-less aggexp-repos apiserver
// together. The shape mirrors 0002 / 0003 (metrics-server pattern,
// hand-rolled Options struct; no RecommendedOptions; no EtcdOptions)
// and adds GitHub configuration driving a polling Repo storage.
package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/spf13/pflag"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/authorization/union"
	"k8s.io/apiserver/pkg/endpoints/openapi"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/client-go/kubernetes"
	utilversion "k8s.io/component-base/version"
	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"

	aggexpv1 "github.com/cheeseandcereal/aggexp/experiments/0004-github-driver-static-pat/pkg/apis/aggexp/v1"
	"github.com/cheeseandcereal/aggexp/experiments/0004-github-driver-static-pat/pkg/apiserver"
	"github.com/cheeseandcereal/aggexp/experiments/0004-github-driver-static-pat/pkg/authz"
	generatedopenapi "github.com/cheeseandcereal/aggexp/experiments/0004-github-driver-static-pat/pkg/generated/openapi"
	ghclient "github.com/cheeseandcereal/aggexp/experiments/0004-github-driver-static-pat/pkg/github"
	"github.com/cheeseandcereal/aggexp/experiments/0004-github-driver-static-pat/pkg/registry/repo"
)

// Options bundles the generic apiserver options plus GitHub config.
type Options struct {
	SecureServing  *genericoptions.SecureServingOptionsWithLoopback
	Authentication *genericoptions.DelegatingAuthenticationOptions
	Authorization  *genericoptions.DelegatingAuthorizationOptions
	Audit          *genericoptions.AuditOptions
	Features       *genericoptions.FeatureOptions
	CoreAPI        *genericoptions.CoreAPIOptions

	PolicyServiceURL     string
	PolicyServiceTimeout time.Duration

	GitHubBaseURL        string
	GitHubOwner          string
	GitHubTokenFile      string
	GitHubPollInterval   time.Duration
}

// NewOptions returns the default options.
func NewOptions() *Options {
	sso := genericoptions.NewSecureServingOptions()
	sso.BindPort = 8443
	return &Options{
		SecureServing:        sso.WithLoopback(),
		Authentication:       genericoptions.NewDelegatingAuthenticationOptions(),
		Authorization:        genericoptions.NewDelegatingAuthorizationOptions(),
		Audit:                genericoptions.NewAuditOptions(),
		Features:             genericoptions.NewFeatureOptions(),
		CoreAPI:              genericoptions.NewCoreAPIOptions(),
		PolicyServiceTimeout: 2 * time.Second,
		GitHubPollInterval:   60 * time.Second,
	}
}

// AddFlags registers CLI flags onto fs.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.SecureServing.AddFlags(fs)
	o.Authentication.AddFlags(fs)
	o.Authorization.AddFlags(fs)
	o.Audit.AddFlags(fs)
	o.Features.AddFlags(fs)
	o.CoreAPI.AddFlags(fs)
	fs.StringVar(&o.PolicyServiceURL, "policy-service-url", o.PolicyServiceURL,
		"URL of the external identity-based policy service. Empty disables the custom authorizer.")
	fs.DurationVar(&o.PolicyServiceTimeout, "policy-service-timeout", o.PolicyServiceTimeout,
		"Per-request timeout when calling the policy service.")
	fs.StringVar(&o.GitHubBaseURL, "github-base-url", o.GitHubBaseURL,
		"GitHub REST API base URL. Empty uses https://api.github.com.")
	fs.StringVar(&o.GitHubOwner, "github-owner", o.GitHubOwner,
		"GitHub owner (user or org) whose repos the AA projects.")
	fs.StringVar(&o.GitHubTokenFile, "github-token-file", o.GitHubTokenFile,
		"Path to a file containing a GitHub token (PAT). Optional but heavily rate-limited without.")
	fs.DurationVar(&o.GitHubPollInterval, "github-poll-interval", o.GitHubPollInterval,
		"How often to refresh Repos from GitHub.")
}

// Validate returns any combined validation errors.
func (o *Options) Validate() error {
	var errs []error
	errs = append(errs, o.SecureServing.Validate()...)
	errs = append(errs, o.Authentication.Validate()...)
	errs = append(errs, o.Authorization.Validate()...)
	errs = append(errs, o.Audit.Validate()...)
	errs = append(errs, o.Features.Validate()...)
	errs = append(errs, o.CoreAPI.Validate()...)
	if strings.TrimSpace(o.GitHubOwner) == "" {
		errs = append(errs, fmt.Errorf("--github-owner is required"))
	}
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
	serverConfig.EffectiveVersion = utilversion.DefaultKubeEffectiveVersion()

	serverConfig.OpenAPIConfig = genericapiserver.DefaultOpenAPIConfig(
		generatedopenapi.GetOpenAPIDefinitions,
		openapi.NewDefinitionNamer(apiserver.Scheme),
	)
	serverConfig.OpenAPIConfig.Info.Title = "aggexp-repos"
	serverConfig.OpenAPIConfig.Info.Version = "0.1"

	serverConfig.OpenAPIV3Config = genericapiserver.DefaultOpenAPIV3Config(
		generatedopenapi.GetOpenAPIDefinitions,
		openapi.NewDefinitionNamer(apiserver.Scheme),
	)
	serverConfig.OpenAPIV3Config.Info.Title = "aggexp-repos"
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
	if o.PolicyServiceURL != "" {
		ext := authz.New(authz.Options{
			URL:     o.PolicyServiceURL,
			Timeout: o.PolicyServiceTimeout,
			Group:   aggexpv1.GroupName,
		})
		existing := serverConfig.Config.Authorization.Authorizer
		serverConfig.Config.Authorization.Authorizer = union.New(ext, existing)
		klog.Infof("external-policy authorizer chained; URL=%s group=%s", o.PolicyServiceURL, aggexpv1.GroupName)
	}
	if err := o.Audit.ApplyTo(&serverConfig.Config); err != nil {
		return nil, fmt.Errorf("applying audit options: %w", err)
	}
	if err := o.CoreAPI.ApplyTo(serverConfig); err != nil {
		return nil, fmt.Errorf("applying coreapi options: %w", err)
	}
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

	server, err := completed.New("aggexp-repos-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return fmt.Errorf("creating apiserver: %w", err)
	}

	// Build the GitHub client. Token is optional (unauthenticated
	// GitHub requests are rate-limited to 60/hr); if the token file
	// exists we use it.
	token := ""
	if o.GitHubTokenFile != "" {
		b, err := os.ReadFile(o.GitHubTokenFile)
		if err != nil {
			klog.Warningf("github-token-file %s unreadable (%v); continuing unauthenticated", o.GitHubTokenFile, err)
		} else {
			token = strings.TrimSpace(string(b))
		}
	}
	ghc := ghclient.New(o.GitHubBaseURL, token)

	repoREST := repo.NewREST(repo.Options{
		Owner:        o.GitHubOwner,
		Client:       ghc,
		PollInterval: o.GitHubPollInterval,
	})
	if err := server.AddPostStartHook("aggexp-repos-poller", func(hookCtx genericapiserver.PostStartHookContext) error {
		repoREST.Start(hookCtx.Context)
		return nil
	}); err != nil {
		return err
	}

	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(
		aggexpv1.GroupName, apiserver.Scheme, metav1.ParameterCodec, apiserver.Codecs,
	)
	apiGroupInfo.VersionedResourcesStorageMap[aggexpv1.SchemeGroupVersion.Version] = map[string]rest.Storage{
		"repos": repoREST,
	}
	if err := server.InstallAPIGroup(&apiGroupInfo); err != nil {
		return fmt.Errorf("installing API group: %w", err)
	}

	prepared := server.PrepareRun()
	return prepared.RunWithContext(ctx)
}
