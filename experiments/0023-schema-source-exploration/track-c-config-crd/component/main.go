// Command note-aa-0023c is the Track C component server for
// experiment 0023. Full Kubernetes OpenAPI v3 lives in an
// APIDefinition CRD on the host cluster; this binary reads it at
// startup via the dynamic client and never calls backend.GetSchema.
//
// The backend only serves CRUD + watch. In this track the backend
// author has no schema concerns at all — the person who wrote the
// APIDefinition manifest does.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apiserverrest "k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/dynamic"
	clientgorest "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/component-base/cli"
	"k8s.io/component-base/logs"
	"k8s.io/klog/v2"

	"github.com/cheeseandcereal/aggexp/runtime/component/grpcbackend"
	componentopenapi "github.com/cheeseandcereal/aggexp/runtime/component/openapi"
	componentpb "github.com/cheeseandcereal/aggexp/runtime/component/proto"
	componentscheme "github.com/cheeseandcereal/aggexp/runtime/component/scheme"
	"github.com/cheeseandcereal/aggexp/runtime/group"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"
)

// gvr for the APIDefinition CRD this track reads at startup.
var apiDefGVR = schema.GroupVersionResource{
	Group:    "aggexpapidefinition.aggexp.io",
	Version:  "v1",
	Resource: "apidefinitions",
}

type trackCOptions struct {
	*runtimeserver.Options
	BackendAddr      string
	BackendTimeout   time.Duration
	APIDefName       string // name of the APIDefinition CR to load
	APIDefPollPeriod time.Duration
	APIDefTimeout    time.Duration
}

func newOptions() *trackCOptions {
	return &trackCOptions{
		Options:          runtimeserver.NewOptions(),
		BackendAddr:      "localhost:9090",
		BackendTimeout:   20 * time.Second,
		APIDefName:       "notes-aggexp-io",
		APIDefPollPeriod: 2 * time.Second,
		APIDefTimeout:    60 * time.Second,
	}
}

func (o *trackCOptions) addFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	fs.StringVar(&o.BackendAddr, "backend-addr", o.BackendAddr, "gRPC address of the resource backend.")
	fs.DurationVar(&o.BackendTimeout, "backend-timeout", o.BackendTimeout, "Timeout for dialing the backend.")
	fs.StringVar(&o.APIDefName, "apidefinition-name", o.APIDefName, "Name of the APIDefinition CR to load from the host cluster.")
	fs.DurationVar(&o.APIDefPollPeriod, "apidefinition-poll-period", o.APIDefPollPeriod, "Retry interval while waiting for APIDefinition to appear.")
	fs.DurationVar(&o.APIDefTimeout, "apidefinition-timeout", o.APIDefTimeout, "Total time to wait for the APIDefinition CR before giving up.")
}

// apiDefSpec mirrors the CRD's spec fields we consume.
type apiDefSpec struct {
	Group          string              `json:"group"`
	Version        string              `json:"version"`
	Resource       string              `json:"resource"`
	Kind           string              `json:"kind"`
	Singular       string              `json:"singular"`
	Namespaced     bool                `json:"namespaced"`
	BackendAddress string              `json:"backendAddress,omitempty"`
	OpenAPIV3      string              `json:"openapiV3"`
	ShortNames     []string            `json:"shortNames,omitempty"`
	TableColumns   []apiDefTableColumn `json:"tableColumns,omitempty"`
}

type apiDefTableColumn struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Format      string `json:"format,omitempty"`
	Description string `json:"description,omitempty"`
	Priority    int32  `json:"priority,omitempty"`
	RowField    string `json:"rowField"`
}

func (o *trackCOptions) run(ctx context.Context) error {
	cfg, err := clientgorest.InClusterConfig()
	if err != nil {
		// Fall back to $KUBECONFIG for local testing.
		loader := clientcmd.NewDefaultClientConfigLoadingRules()
		overrides := &clientcmd.ConfigOverrides{}
		cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loader, overrides).ClientConfig()
		if err != nil {
			return fmt.Errorf("no in-cluster kubeconfig and no fallback: %w", err)
		}
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("build dynamic client: %w", err)
	}

	def, err := loadAPIDefinition(ctx, dyn, o.APIDefName, o.APIDefPollPeriod, o.APIDefTimeout)
	if err != nil {
		return fmt.Errorf("load APIDefinition %q: %w", o.APIDefName, err)
	}
	klog.Infof("component(track-C): loaded APIDefinition %s: group=%s version=%s resource=%s kind=%s namespaced=%v openapi-bytes=%d",
		o.APIDefName, def.Group, def.Version, def.Resource, def.Kind, def.Namespaced, len(def.OpenAPIV3))

	// Dial backend (but never call GetSchema — schema is config-resident).
	dialCtx, cancel := context.WithTimeout(ctx, o.BackendTimeout)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, o.BackendAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("dial backend %s: %w", o.BackendAddr, err)
	}
	client := componentpb.NewBackendClient(conn)

	gv := schema.GroupVersion{Group: def.Group, Version: def.Version}
	gvk := gv.WithKind(def.Kind)
	listGVK := gv.WithKind(def.Kind + "List")

	desc := componentscheme.ResourceDescriptor{
		GroupVersion:    gv,
		Resource:        def.Resource,
		Kind:            def.Kind,
		Singular:        def.Singular,
		Namespaced:      def.Namespaced,
		UseTypedWrapper: true,
	}
	bundle := componentscheme.Build(desc)

	itemSchema, parseErr := componentopenapi.ParseBackendSchema([]byte(def.OpenAPIV3), gvk)
	if parseErr != nil {
		return fmt.Errorf("parse APIDefinition openapiV3: %w", parseErr)
	}
	listSchema := componentopenapi.WrapAsList(listGVK, bundle.ItemCanonicalName)

	cols := make([]metav1.TableColumnDefinition, 0, len(def.TableColumns))
	rowFields := make([]string, 0, len(def.TableColumns))
	for _, c := range def.TableColumns {
		cols = append(cols, metav1.TableColumnDefinition{
			Name: c.Name, Type: c.Type, Format: c.Format,
			Description: c.Description, Priority: c.Priority,
		})
		rowFields = append(rowFields, c.RowField)
	}

	storage := grpcbackend.New(grpcbackend.Descriptor{
		GroupVersion:            gv,
		Resource:                def.Resource,
		Kind:                    def.Kind,
		Singular:                def.Singular,
		Namespaced:              def.Namespaced,
		Writable:                true,
		SupportsServerSideApply: true,
		UseTypedWrapper:         true,
		Columns:                 cols,
		RowFields:               rowFields,
		GroupResource:           schema.GroupResource{Group: gv.Group, Resource: def.Resource},
	}, client)

	g := &group.Group{
		GroupVersion:   bundle.Descriptor.GroupVersion,
		Scheme:         bundle.Scheme,
		Codecs:         bundle.Codecs,
		ParameterCodec: bundle.ParameterCodec,
		Resources:      map[string]apiserverrest.Storage{def.Resource: storage},
	}

	in := runtimeserver.Input{
		Scheme:             bundle.Scheme,
		Codecs:             bundle.Codecs,
		OpenAPIDefinitions: componentopenapi.Compose(itemSchema, listSchema, bundle.ItemCanonicalName, bundle.ListCanonicalName),
	}

	if o.Options.PolicyGroup == "" {
		o.Options.PolicyGroup = gv.Group
	}

	return o.Options.Run(
		ctx,
		"aggexp-component-0023c",
		in,
		[]runtimeserver.GroupInstaller{g},
		map[string]runtimeserver.PostStartFunc{
			"component-upstream-watch": func(hookCtx context.Context) error {
				storage.StartUpstreamWatch(hookCtx)
				go func() {
					<-hookCtx.Done()
					storage.Shutdown()
					_ = conn.Close()
				}()
				return nil
			},
		},
	)
}

// loadAPIDefinition fetches the APIDefinition CR named `name` from
// the host cluster, retrying until `timeout` in case the CRD or
// instance hasn't been applied yet.
func loadAPIDefinition(ctx context.Context, dyn dynamic.Interface, name string, period, timeout time.Duration) (*apiDefSpec, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		obj, err := dyn.Resource(apiDefGVR).Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			return extractSpec(obj)
		}
		lastErr = err
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("gave up after %s: %w", timeout, lastErr)
		}
		klog.V(2).Infof("component(track-C): APIDefinition %q not yet available: %v; retrying in %s", name, err, period)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(period):
		}
	}
}

func extractSpec(obj *unstructured.Unstructured) (*apiDefSpec, error) {
	specRaw, found, err := unstructured.NestedMap(obj.Object, "spec")
	if err != nil || !found {
		return nil, fmt.Errorf("APIDefinition %q has no spec (%v)", obj.GetName(), err)
	}
	raw, err := json.Marshal(specRaw)
	if err != nil {
		return nil, fmt.Errorf("re-marshal spec: %w", err)
	}
	var def apiDefSpec
	if err := json.Unmarshal(raw, &def); err != nil {
		return nil, fmt.Errorf("unmarshal spec: %w", err)
	}
	if def.Group == "" || def.Version == "" || def.Resource == "" || def.Kind == "" {
		return nil, fmt.Errorf("APIDefinition %q missing required fields", obj.GetName())
	}
	if def.OpenAPIV3 == "" {
		return nil, fmt.Errorf("APIDefinition %q has empty openapiV3", obj.GetName())
	}
	return &def, nil
}

func main() {
	opts := newOptions()
	cmd := &cobra.Command{
		Use:   "note-aa-0023c",
		Short: "Track C (config-resident OpenAPI) component server for 0023.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := opts.Options.Validate(); err != nil {
				return err
			}
			return opts.run(genericapiserver.SetupSignalContext())
		},
	}
	opts.addFlags(cmd.Flags())
	logs.AddFlags(cmd.Flags())
	if code := cli.Run(cmd); code != 0 {
		fmt.Fprintln(os.Stderr, "note-aa-0023c exited with error")
		os.Exit(code)
	}
}
