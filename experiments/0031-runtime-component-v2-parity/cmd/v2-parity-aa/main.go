// Command v2-parity-aa is the first post-promotion consumer of
// runtime/component/v2. One process hosts N aggregated APIs declared
// via APIDefinition CRDs on the host cluster; each one is wired
// through a v2 REST adapter with the shared MetadataStore, optional
// declarative admission, and push-or-poll watch as declared.
//
// Accepts the standard runtime/server flags plus a few multiplex-
// specific ones. Exits cleanly on SIGTERM after sweeping every
// APIService it owns.
//
// Scope comparison:
//
//	0021 (v1 parity, single-AA)         ~38 LOC main
//	0027 (multiplex experiment)        ~807 LOC main + reconciler
//	THIS (v2 multiplex, substrate-form) ~180 LOC (most of 0027's
//	                                    reconciler lives in the
//	                                    substrate's multiplex pkg).
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/dynamic"
	clientgorest "k8s.io/client-go/rest"
	"k8s.io/component-base/cli"
	"k8s.io/component-base/logs"
	"k8s.io/klog/v2"
	genericapiserver "k8s.io/apiserver/pkg/server"

	"github.com/cheeseandcereal/aggexp/runtime/component/v2/gc"
	"github.com/cheeseandcereal/aggexp/runtime/component/v2/httpbackend"
	"github.com/cheeseandcereal/aggexp/runtime/component/v2/metadatastore"
	"github.com/cheeseandcereal/aggexp/runtime/component/v2/multiplex"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"
)

type options struct {
	*runtimeserver.Options

	ServiceName      string
	ServiceNamespace string
	CAPath           string
	ShutdownGrace    time.Duration

	GCGroup    string
	GCResource string
	GCAddress  string // backend address for the GC's list probe (same as the widget backend)
	GCInterval time.Duration
	GCMinAge   time.Duration
}

func newOptions() *options {
	return &options{
		Options:          runtimeserver.NewOptions(),
		ServiceName:      "aggexp",
		ServiceNamespace: "aggexp-system",
		CAPath:           "/etc/aggexp/certs/ca.crt",
		ShutdownGrace:    20 * time.Second,
		GCGroup:          "widgets.aggexp.io",
		GCResource:       "widgets",
		GCInterval:       30 * time.Second,
		GCMinAge:         10 * time.Second,
	}
}

func (o *options) addFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	fs.StringVar(&o.ServiceName, "service-name", o.ServiceName, "Host-cluster Service name fronting this pod.")
	fs.StringVar(&o.ServiceNamespace, "service-namespace", o.ServiceNamespace, "Host-cluster Service namespace.")
	fs.StringVar(&o.CAPath, "ca-path", o.CAPath, "Path to CA cert to stamp into APIService.caBundle.")
	fs.DurationVar(&o.ShutdownGrace, "shutdown-grace", o.ShutdownGrace, "Grace period for the PreShutdown APIService sweep.")
	fs.StringVar(&o.GCGroup, "gc-group", o.GCGroup, "Group the GC reconciler manages.")
	fs.StringVar(&o.GCResource, "gc-resource", o.GCResource, "Resource plural the GC reconciler manages.")
	fs.StringVar(&o.GCAddress, "gc-backend-address", o.GCAddress, "Backend address used by GC for orphan detection.")
	fs.DurationVar(&o.GCInterval, "gc-interval", o.GCInterval, "GC sweep interval.")
	fs.DurationVar(&o.GCMinAge, "gc-min-age", o.GCMinAge, "GC grace window.")
}

func main() {
	opts := newOptions()
	cmd := &cobra.Command{
		Use:   "v2-parity-aa",
		Short: "First runtime/component/v2 consumer: multiplex middleware + metastore + GC + declarative admission.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := opts.Validate(); err != nil {
				return err
			}
			return run(opts, genericapiserver.SetupSignalContext())
		},
	}
	opts.addFlags(cmd.Flags())
	logs.AddFlags(cmd.Flags())
	if code := cli.Run(cmd); code != 0 {
		fmt.Fprintln(os.Stderr, "v2-parity-aa exited with error")
		os.Exit(code)
	}
}

func run(o *options, ctx context.Context) error {
	// Host-cluster clients (for APIDefinition informer + MetadataStore
	// + APIService creates).
	cfg, err := clientgorest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("in-cluster kube config: %w", err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("dynamic client: %w", err)
	}
	caBundle, err := os.ReadFile(o.CAPath)
	if err != nil {
		return fmt.Errorf("read ca %s: %w", o.CAPath, err)
	}

	// MetadataStore — shared by every AA registered in this process.
	store := metadatastore.New(dyn, "aggexp-v2-parity")

	// Multiplex reconciler.
	mx := multiplex.New(dyn, multiplex.Options{
		CABundle:              caBundle,
		ServiceName:           o.ServiceName,
		ServiceNamespace:      o.ServiceNamespace,
		ReconcileResyncPeriod: 60 * time.Second,
		BackendTimeout:        15 * time.Second,
		ShutdownGrace:         o.ShutdownGrace,
		FieldManager:          "aggexp-v2-parity",
		MetadataStore:         store,
	})

	// Bootstrap Scheme: placeholder for the generic apiserver; real
	// groups are attached dynamically by the reconciler.
	bootScheme := runtime.NewScheme()
	metav1.AddToGroupVersion(bootScheme, schema.GroupVersion{Version: "v1"})
	codecs := serializer.NewCodecFactory(bootScheme)

	in := runtimeserver.Input{
		Scheme:             bootScheme,
		Codecs:             codecs,
		OpenAPIDefinitions: mx.OpenAPIClosure(), // live closure; re-reads defs on each library pass.
	}
	recommended, err := o.Config(in)
	if err != nil {
		return err
	}
	// Per multiplex/package doc: nil the pre-materialized Definitions
	// cache so the closure above is actually consulted on each
	// openapi-builder pass. (0027 consequent; left to the consumer
	// because the substrate can't touch the recommended config.)
	if recommended.OpenAPIV3Config != nil {
		recommended.OpenAPIV3Config.Definitions = nil
	}
	if recommended.OpenAPIConfig != nil {
		recommended.OpenAPIConfig.Definitions = nil
	}
	completed := recommended.Complete()
	server, err := completed.New("aggexp-v2-parity-0031", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return fmt.Errorf("new apiserver: %w", err)
	}
	mx.AttachServer(server)

	// GC reconciler against the widget backend's business store. GC
	// is per (group, resource); we wire one to exercise the primitive.
	// A production consumer would spawn one per APIDefinition.
	if o.GCAddress != "" {
		gcClient, err := httpbackend.New(o.GCAddress, 5*time.Second)
		if err != nil {
			return fmt.Errorf("gc backend client: %w", err)
		}
		gcr := gc.New(store, gcClient, gc.Config{
			Group:        o.GCGroup,
			Resource:     o.GCResource,
			Interval:     o.GCInterval,
			MinAge:       o.GCMinAge,
			InitialDelay: 15 * time.Second,
		})
		if err := server.AddPostStartHook("gc-reconciler",
			func(hc genericapiserver.PostStartHookContext) error {
				gcr.Start(hc.Context)
				return nil
			},
		); err != nil {
			return err
		}
	}

	// Seed the ResourceMetadata CRD if absent (embedded in v2/metadatastore).
	if err := server.AddPostStartHook("ensure-metadata-crd",
		func(hc genericapiserver.PostStartHookContext) error {
			return ensureCRD(hc.Context, dyn, metadatastore.CRDYAML)
		},
	); err != nil {
		return err
	}

	// Post-start hook to run the multiplex reconciler.
	if err := server.AddPostStartHook("multiplex-reconciler",
		func(hc genericapiserver.PostStartHookContext) error {
			go mx.Run(hc.Context)
			return nil
		},
	); err != nil {
		return err
	}
	// PreShutdown: sweep every APIService we created so kube-apiserver
	// drops us cleanly.
	if err := server.AddPreShutdownHook("multiplex-sweep",
		func() error {
			ctx, cancel := context.WithTimeout(context.Background(), o.ShutdownGrace)
			defer cancel()
			mx.ShutdownSweep(ctx)
			return nil
		},
	); err != nil {
		return err
	}

	klog.Infof("v2-parity-aa starting; multiplex with MetadataStore=%v GC=(%s/%s)",
		store != nil, o.GCGroup, o.GCResource)
	prepared := server.PrepareRun()
	return prepared.RunWithContext(ctx)
}

// ensureCRD idempotently applies a CRD YAML using the dynamic client.
// Doing it in-process (rather than in deploy.sh) lets the middleware
// come up independently of operator tooling, which is part of the
// substrate's single-binary story.
func ensureCRD(ctx context.Context, dyn dynamic.Interface, raw []byte) error {
	u, err := decodeYAML(raw)
	if err != nil {
		return fmt.Errorf("decode embedded CRD: %w", err)
	}
	gvr := schema.GroupVersionResource{
		Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions",
	}
	_, err = dyn.Resource(gvr).Get(ctx, u.GetName(), metav1.GetOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get crd %s: %w", u.GetName(), err)
	}
	if _, err := dyn.Resource(gvr).Create(ctx, u, metav1.CreateOptions{FieldManager: "aggexp-v2-parity"}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create crd %s: %w", u.GetName(), err)
	}
	klog.Infof("ensureCRD: created %s", u.GetName())
	return nil
}
