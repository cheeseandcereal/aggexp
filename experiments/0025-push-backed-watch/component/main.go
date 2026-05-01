// Command note-aa-0025 is the aggregated apiserver component for
// experiment 0025. At startup it dials the backend, asks for its
// schema (Track B JSON Schema — middleware synthesizes OpenAPI),
// probes its Watch capability, and wires a custom REST storage that
// either forwards backend.Watch events (push mode) or runs its own
// list-poll loop (poll mode). Either way, the Watch HTTP path on the
// apiserver side emits a BOOKMARK event with
// metadata.annotations["k8s.io/initial-events-end"]="true" after the
// initial snapshot, closing the gap 0011 identified.
//
// Deliberately does not use runtime/component.Run(): the substrate's
// grpcbackend.REST does not emit the initial-events-end bookmark and
// does not support a poll-mode fallback. The substrate will grow
// these under the 0030 v2 promotion.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"
	"k8s.io/component-base/logs"
	"k8s.io/klog/v2"

	localrest "github.com/cheeseandcereal/aggexp/experiments/0025-push-backed-watch/component/rest"
	"github.com/cheeseandcereal/aggexp/experiments/0025-push-backed-watch/component/synthesis"
	componentopenapi "github.com/cheeseandcereal/aggexp/runtime/component/openapi"
	componentpb "github.com/cheeseandcereal/aggexp/runtime/component/proto"
	componentscheme "github.com/cheeseandcereal/aggexp/runtime/component/scheme"
	"github.com/cheeseandcereal/aggexp/runtime/group"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"
)

type options struct {
	*runtimeserver.Options
	BackendAddr    string
	BackendTimeout time.Duration
	PollInterval   time.Duration
	ForceMode      string // "", "push", "poll"
}

func newOptions() *options {
	return &options{
		Options:        runtimeserver.NewOptions(),
		BackendAddr:    "localhost:9090",
		BackendTimeout: 20 * time.Second,
		PollInterval:   15 * time.Second,
	}
}

func (o *options) addFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	fs.StringVar(&o.BackendAddr, "backend-addr", o.BackendAddr, "gRPC address of the resource backend.")
	fs.DurationVar(&o.BackendTimeout, "backend-timeout", o.BackendTimeout, "Timeout for startup backend RPCs.")
	fs.DurationVar(&o.PollInterval, "poll-interval", o.PollInterval, "Poll interval when backend does not support Watch.")
	fs.StringVar(&o.ForceMode, "watch-mode", o.ForceMode,
		`Force watch mode: "push", "poll", or "" for capability probe (default).`)
}

func (o *options) run(ctx context.Context) error {
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

	schemaCtx, cancel2 := context.WithTimeout(ctx, o.BackendTimeout)
	defer cancel2()
	resp, err := client.GetSchema(schemaCtx, &componentpb.GetSchemaRequest{})
	if err != nil {
		return fmt.Errorf("GetSchema: %w", err)
	}
	klog.Infof("component(0025): backend schema: group=%s version=%s resource=%s kind=%s",
		resp.GetGroup(), resp.GetVersion(), resp.GetResource(), resp.GetKind())

	gv := schema.GroupVersion{Group: resp.GetGroup(), Version: resp.GetVersion()}
	gvk := gv.WithKind(resp.GetKind())
	listGVK := gv.WithKind(resp.GetKind() + "List")

	// Track B schema lift (from 0023). Backend ships plain JSON
	// Schema; middleware synthesizes full Kubernetes OpenAPI v3.
	lifted, err := synthesis.LiftJSONSchemaToOpenAPI(gvk, resp.GetOpenapiV3())
	if err != nil {
		return fmt.Errorf("synthesis.LiftJSONSchemaToOpenAPI: %w", err)
	}

	desc := componentscheme.ResourceDescriptor{
		GroupVersion:    gv,
		Resource:        resp.GetResource(),
		Kind:            resp.GetKind(),
		Singular:        resp.GetSingular(),
		Namespaced:      resp.GetNamespaced(),
		UseTypedWrapper: true,
	}
	bundle := componentscheme.Build(desc)

	itemSchema, parseErr := componentopenapi.ParseBackendSchema(lifted, gvk)
	if parseErr != nil {
		return fmt.Errorf("parse lifted OpenAPI: %w", parseErr)
	}
	listSchema := componentopenapi.WrapAsList(listGVK, bundle.ItemCanonicalName)

	cols := make([]metav1.TableColumnDefinition, 0, len(resp.GetColumns()))
	for _, c := range resp.GetColumns() {
		cols = append(cols, metav1.TableColumnDefinition{
			Name:        c.GetName(),
			Type:        c.GetType(),
			Format:      c.GetFormat(),
			Description: c.GetDescription(),
			Priority:    c.GetPriority(),
		})
	}

	// Watch-capability probe.
	mode, err := resolveMode(ctx, client, o)
	if err != nil {
		klog.Warningf("component(0025): watch-capability probe error: %v; defaulting to poll", err)
		mode = localrest.ModePoll
	}
	klog.Infof("component(0025): watch mode = %s (force=%q, poll-interval=%s)",
		mode, o.ForceMode, o.PollInterval)

	storage := localrest.New(localrest.Descriptor{
		GroupVersion:            gv,
		Resource:                resp.GetResource(),
		Kind:                    resp.GetKind(),
		Singular:                resp.GetSingular(),
		Namespaced:              resp.GetNamespaced(),
		Writable:                resp.GetWritable(),
		SupportsServerSideApply: resp.GetSupportsServerSideApply(),
		Columns:                 cols,
		RowFields:               resp.GetRowFields(),
		GroupResource:           schema.GroupResource{Group: gv.Group, Resource: resp.GetResource()},
		PollInterval:            o.PollInterval,
	}, client, mode)

	g := &group.Group{
		GroupVersion:   bundle.Descriptor.GroupVersion,
		Scheme:         bundle.Scheme,
		Codecs:         bundle.Codecs,
		ParameterCodec: bundle.ParameterCodec,
		Resources:      map[string]rest.Storage{resp.GetResource(): storage},
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
		"aggexp-component-0025",
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

func resolveMode(ctx context.Context, client componentpb.BackendClient, o *options) (localrest.Mode, error) {
	switch o.ForceMode {
	case "push":
		return localrest.ModePush, nil
	case "poll":
		return localrest.ModePoll, nil
	case "":
		return localrest.ProbeMode(ctx, client, 3*time.Second)
	default:
		return localrest.ModePoll, fmt.Errorf("invalid --watch-mode=%q (want push, poll, or \"\")", o.ForceMode)
	}
}

func main() {
	opts := newOptions()
	cmd := &cobra.Command{
		Use:   "note-aa-0025",
		Short: "0025 component server: probes backend Watch capability and switches between push-forwarding and list-polling.",
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
		fmt.Fprintln(os.Stderr, "note-aa-0025 exited with error")
		os.Exit(code)
	}
}
