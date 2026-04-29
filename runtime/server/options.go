package server

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/spf13/pflag"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/authorization/union"
	"k8s.io/apiserver/pkg/endpoints/openapi"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/client-go/kubernetes"
	utilversion "k8s.io/component-base/version"
	"k8s.io/klog/v2"
	"k8s.io/kube-openapi/pkg/common"
	netutils "k8s.io/utils/net"

	"github.com/cheeseandcereal/aggexp/runtime/authz"
)

// Options is the substrate's generic Options struct. It intentionally
// omits EtcdOptions: the lab's storage-independence invariant is that
// etcd is never in the hot path. Experiments compose their own
// backend-specific flags alongside these.
type Options struct {
	SecureServing  *genericoptions.SecureServingOptionsWithLoopback
	Authentication *genericoptions.DelegatingAuthenticationOptions
	Authorization  *genericoptions.DelegatingAuthorizationOptions
	Audit          *genericoptions.AuditOptions
	Features       *genericoptions.FeatureOptions
	CoreAPI        *genericoptions.CoreAPIOptions

	// External-policy authorizer (optional).
	PolicyServiceURL     string
	PolicyServiceTimeout time.Duration
	// PolicyGroup is the API group the external authorizer opines
	// on. Empty string means "all resource requests".
	PolicyGroup string

	// Title / Version feed the generated OpenAPI metadata.
	Title   string
	Version string
}

// NewOptions returns a freshly-defaulted Options.
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
		Title:                "aggexp",
		Version:              "0.1",
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
		"URL of the external identity-based policy service. Empty disables the external authorizer.")
	fs.DurationVar(&o.PolicyServiceTimeout, "policy-service-timeout", o.PolicyServiceTimeout,
		"Per-request timeout when calling the policy service.")
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
	return utilerrors.NewAggregate(errs)
}

// Input bundles the per-experiment types required to build the
// generic apiserver config. Scheme + Codecs typically come from a
// pkg/apiserver package; OpenAPIDefinitions from code-generator's
// openapi-gen output.
type Input struct {
	// Scheme is the experiment's shared Scheme.
	Scheme *runtime.Scheme
	// Codecs is the experiment's codec factory (derived from Scheme).
	Codecs serializer.CodecFactory
	// OpenAPIDefinitions is the generated openapi-gen function.
	// Optional; nil disables OpenAPI (kubectl explain will not work).
	OpenAPIDefinitions common.GetOpenAPIDefinitions
}

// Config constructs a RecommendedConfig from the generic Options
// plus experiment-specific Input. The returned config is ready to
// Complete() and New() on.
func (o *Options) Config(in Input) (*genericapiserver.RecommendedConfig, error) {
	if in.Scheme == nil {
		return nil, fmt.Errorf("Input.Scheme is required")
	}
	if err := o.SecureServing.MaybeDefaultWithSelfSignedCerts(
		"localhost", nil, []net.IP{netutils.ParseIPSloppy("127.0.0.1")},
	); err != nil {
		return nil, fmt.Errorf("creating self-signed certs: %w", err)
	}

	cfg := genericapiserver.NewRecommendedConfig(in.Codecs)
	cfg.EffectiveVersion = utilversion.DefaultKubeEffectiveVersion()

	if in.OpenAPIDefinitions != nil {
		cfg.OpenAPIConfig = genericapiserver.DefaultOpenAPIConfig(
			in.OpenAPIDefinitions, openapi.NewDefinitionNamer(in.Scheme),
		)
		cfg.OpenAPIConfig.Info.Title = o.Title
		cfg.OpenAPIConfig.Info.Version = o.Version
		cfg.OpenAPIV3Config = genericapiserver.DefaultOpenAPIV3Config(
			in.OpenAPIDefinitions, openapi.NewDefinitionNamer(in.Scheme),
		)
		cfg.OpenAPIV3Config.Info.Title = o.Title
		cfg.OpenAPIV3Config.Info.Version = o.Version
	}

	if err := o.SecureServing.ApplyTo(&cfg.Config.SecureServing, &cfg.Config.LoopbackClientConfig); err != nil {
		return nil, fmt.Errorf("applying secure-serving options: %w", err)
	}
	if err := o.Authentication.ApplyTo(&cfg.Config.Authentication, cfg.SecureServing, cfg.OpenAPIConfig); err != nil {
		return nil, fmt.Errorf("applying authn options: %w", err)
	}
	if err := o.Authorization.ApplyTo(&cfg.Config.Authorization); err != nil {
		return nil, fmt.Errorf("applying authz options: %w", err)
	}
	if o.PolicyServiceURL != "" {
		ext := authz.New(authz.Options{
			URL:     o.PolicyServiceURL,
			Timeout: o.PolicyServiceTimeout,
			Group:   o.PolicyGroup,
			Log: func(req authz.PolicyRequest, d authorizer.Decision, reason string, err error) {
				klog.V(2).InfoS("ext-authz",
					"user", req.User, "groups", req.Groups,
					"verb", req.Verb, "resource", req.Resource, "name", req.Name,
					"decision", decisionLabel(d), "reason", reason, "err", err,
				)
			},
		})
		existing := cfg.Config.Authorization.Authorizer
		cfg.Config.Authorization.Authorizer = union.New(ext, existing)
		klog.Infof("external-policy authorizer chained; URL=%s group=%s", o.PolicyServiceURL, o.PolicyGroup)
	}
	if err := o.Audit.ApplyTo(&cfg.Config); err != nil {
		return nil, fmt.Errorf("applying audit options: %w", err)
	}
	if err := o.CoreAPI.ApplyTo(cfg); err != nil {
		return nil, fmt.Errorf("applying coreapi options: %w", err)
	}
	var kubeClient kubernetes.Interface
	if cfg.ClientConfig != nil {
		c, err := kubernetes.NewForConfig(cfg.ClientConfig)
		if err != nil {
			return nil, fmt.Errorf("building kubernetes clientset: %w", err)
		}
		kubeClient = c
	}
	if err := o.Features.ApplyTo(&cfg.Config, kubeClient, cfg.SharedInformerFactory); err != nil {
		return nil, fmt.Errorf("applying feature options: %w", err)
	}
	return cfg, nil
}

// GroupInstaller is implemented by anything that knows how to add
// itself to a completed generic apiserver. runtime/group.Group is
// the primary implementation; experiments may supply custom ones.
type GroupInstaller interface {
	// Install registers the group's storage on s. It also gets a
	// chance to add post-start hooks.
	Install(s *genericapiserver.GenericAPIServer) error
}

// PostStartFunc is a narrower alias for genericapiserver's
// post-start hook signature, useful when an experiment wants to
// pass a background-loop launcher to Run.
type PostStartFunc = func(ctx context.Context) error

// Run completes configuration, builds the apiserver, lets each
// installer register its API group, optionally adds PostStart
// hooks, and runs the server until ctx is cancelled.
func (o *Options) Run(
	ctx context.Context,
	serverName string,
	in Input,
	installers []GroupInstaller,
	postStartHooks map[string]PostStartFunc,
) error {
	cfg, err := o.Config(in)
	if err != nil {
		return err
	}
	completed := cfg.Complete()
	server, err := completed.New(serverName, genericapiserver.NewEmptyDelegate())
	if err != nil {
		return fmt.Errorf("creating apiserver: %w", err)
	}
	for name, fn := range postStartHooks {
		fn := fn
		if err := server.AddPostStartHook(name, func(hookCtx genericapiserver.PostStartHookContext) error {
			return fn(hookCtx.Context)
		}); err != nil {
			return err
		}
	}
	for _, inst := range installers {
		if err := inst.Install(server); err != nil {
			return fmt.Errorf("installing group: %w", err)
		}
	}
	prepared := server.PrepareRun()
	return prepared.RunWithContext(ctx)
}

func decisionLabel(d authorizer.Decision) string {
	switch d {
	case authorizer.DecisionAllow:
		return "allow"
	case authorizer.DecisionDeny:
		return "deny"
	default:
		return "noopinion"
	}
}
