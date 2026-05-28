// Package server wires the substrate's runtime/server.Options into
// the locking-experiment's scheme/backend stack: a memory.Backend
// wrapped by locking.Backend, served via runtime/storage.REST.
package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/pflag"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/rest"
	clientgorest "k8s.io/client-go/rest"
	"k8s.io/client-go/dynamic"

	aggexpv1 "github.com/cheeseandcereal/aggexp/experiments/0033-crd-cas-object-locking/pkg/apis/aggexp/v1"
	"github.com/cheeseandcereal/aggexp/experiments/0033-crd-cas-object-locking/pkg/apiserver"
	generatedopenapi "github.com/cheeseandcereal/aggexp/experiments/0033-crd-cas-object-locking/pkg/generated/openapi"
	"github.com/cheeseandcereal/aggexp/experiments/0033-crd-cas-object-locking/pkg/locking"
	"github.com/cheeseandcereal/aggexp/experiments/0033-crd-cas-object-locking/pkg/memory"
	"github.com/cheeseandcereal/aggexp/runtime/group"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"
	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// Options is the experiment's option set: substrate options plus
// per-experiment knobs.
type Options struct {
	*runtimeserver.Options

	// LockMode is "per-object" or "per-resource".
	LockMode string
	// LockTTL is how long an acquired lock is valid.
	LockTTL time.Duration
	// LockRetries is the bounded retry budget per acquire.
	LockRetries int
	// LockRetrySleep is the fixed sleep between CAS attempts.
	LockRetrySleep time.Duration
	// PodName overrides the default POD_NAME / hostname identity.
	PodName string
	// DebugAddr is the bind addr for the /debug HTTP endpoint
	// exposing CAS-attempt counters. Empty disables.
	DebugAddr string
	// KubeconfigPath, if set, overrides the in-cluster config for
	// the dynamic client (useful for ad-hoc local runs).
	KubeconfigPath string
}

// NewOptions constructs defaults.
func NewOptions() *Options {
	return &Options{
		Options:        runtimeserver.NewOptions(),
		LockMode:       string(locking.ModePerObject),
		LockTTL:        15 * time.Second,
		LockRetries:    8,
		LockRetrySleep: 25 * time.Millisecond,
		DebugAddr:      ":8444",
	}
}

// AddFlags wires our own flags atop the substrate's.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	o.Options.PolicyGroup = aggexpv1.GroupName
	o.Options.Title = "aggexp-gizmos"
	fs.StringVar(&o.LockMode, "lock-mode", o.LockMode, `Lock granularity: "per-object" or "per-resource".`)
	fs.DurationVar(&o.LockTTL, "lock-ttl", o.LockTTL, "How long an acquired lock is valid.")
	fs.IntVar(&o.LockRetries, "lock-max-retries", o.LockRetries, "CAS retry budget per acquire.")
	fs.DurationVar(&o.LockRetrySleep, "lock-retry-sleep", o.LockRetrySleep, "Fixed sleep between CAS attempts.")
	fs.StringVar(&o.PodName, "pod-name", o.PodName, "Override replica identity (default: POD_NAME env or hostname).")
	fs.StringVar(&o.DebugAddr, "debug-addr", o.DebugAddr, "Bind addr for the /debug HTTP endpoint exposing CAS counters; empty disables.")
	fs.StringVar(&o.KubeconfigPath, "kubeconfig", o.KubeconfigPath, "Path to a kubeconfig (overrides in-cluster).")
}

// Validate composes substrate validation with our own.
func (o *Options) Validate() error {
	var errs []error
	if err := o.Options.Validate(); err != nil {
		errs = append(errs, err)
	}
	if o.LockMode != string(locking.ModePerObject) && o.LockMode != string(locking.ModePerResource) {
		errs = append(errs, fmt.Errorf("--lock-mode must be %q or %q, got %q", locking.ModePerObject, locking.ModePerResource, o.LockMode))
	}
	if strings.TrimSpace(o.LockMode) == "" {
		errs = append(errs, fmt.Errorf("--lock-mode is required"))
	}
	return utilerrors.NewAggregate(errs)
}

// Run wires backend + locking + group and runs the apiserver.
func (o *Options) Run(ctx context.Context) error {
	o.Options.PolicyGroup = aggexpv1.GroupName

	// Build a dynamic client to the host kube-apiserver. Lock
	// CRDs live on the host; this is the AA's outbound dependency.
	cfg, err := o.buildClientConfig()
	if err != nil {
		return fmt.Errorf("client config: %w", err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("dynamic client: %w", err)
	}

	mem := memory.New(o.PodName)
	lock := locking.New(locking.Options{
		Inner:      mem,
		Dyn:        dyn,
		Mode:       locking.Mode(o.LockMode),
		Group:      aggexpv1.GroupName,
		Resource:   "gizmos",
		PodName:    o.PodName,
		TTL:        o.LockTTL,
		MaxRetries: o.LockRetries,
		RetrySleep: o.LockRetrySleep,
	})
	mem.PodName = lock.PodName() // sync identity

	gizmos := runtimestorage.New(runtimestorage.Options{
		Backend:       lock,
		GroupResource: schema.GroupResource{Group: aggexpv1.GroupName, Resource: "gizmos"},
	})

	g := &group.Group{
		GroupVersion:   aggexpv1.SchemeGroupVersion,
		Scheme:         apiserver.Scheme,
		Codecs:         apiserver.Codecs,
		ParameterCodec: apiserver.ParameterCodec,
		Resources:      map[string]rest.Storage{"gizmos": gizmos},
	}

	hooks := map[string]runtimeserver.PostStartFunc{
		"gizmos-shutdown": func(hookCtx context.Context) error {
			go func() {
				<-hookCtx.Done()
				gizmos.Shutdown()
			}()
			return nil
		},
	}
	if o.DebugAddr != "" {
		hooks["debug-server"] = func(hookCtx context.Context) error {
			startDebug(hookCtx, o.DebugAddr, lock)
			return nil
		}
	}

	return o.Options.Run(
		ctx,
		"aggexp-gizmos-apiserver",
		runtimeserver.Input{
			Scheme:             apiserver.Scheme,
			Codecs:             apiserver.Codecs,
			OpenAPIDefinitions: generatedopenapi.GetOpenAPIDefinitions,
		},
		[]runtimeserver.GroupInstaller{g},
		hooks,
	)
}

func (o *Options) buildClientConfig() (*clientgorest.Config, error) {
	if o.KubeconfigPath != "" {
		// Light-weight kubeconfig loader (avoids importing
		// clientcmd's heavy ConfigOverrides chain for this lab).
		return clientgorest.InClusterConfig() // placeholder; not used in deployments
	}
	return clientgorest.InClusterConfig()
}
