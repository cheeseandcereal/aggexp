// Command aggexp-multiplex is the 0027 multiplex middleware.
//
// One process hosts an aggregated apiserver. It watches
// APIDefinition CRDs on the host cluster; for each desired
// definition it registers an API group (via InstallAPIGroup, called
// at reconcile time AFTER the apiserver is running) and creates a
// corresponding APIService object on the host. It writes status
// back to the APIDefinition.
//
// On SIGTERM the middleware deletes every APIService it owns, then
// shuts down. The internal API groups stay installed in-process
// (go-restful has no deregister) but are unreachable from the
// outside because the APIService is gone. This is the 0027
// experiment-scoped trade-off documented in the README's Decisions.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	clientgorest "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/component-base/cli"
	"k8s.io/component-base/logs"
	"k8s.io/klog/v2"
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"

	"github.com/cheeseandcereal/aggexp/runtime/component/grpcbackend"
	componentopenapi "github.com/cheeseandcereal/aggexp/runtime/component/openapi"
	componentpb "github.com/cheeseandcereal/aggexp/runtime/component/proto"
	componentscheme "github.com/cheeseandcereal/aggexp/runtime/component/scheme"
	"github.com/cheeseandcereal/aggexp/runtime/group"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"
)

// ---------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------

type options struct {
	*runtimeserver.Options

	ReconcileResyncPeriod time.Duration
	BackendTimeout        time.Duration
	ServiceName           string
	ServiceNamespace      string
	CAPath                string
	ShutdownGrace         time.Duration
}

func newOptions() *options {
	return &options{
		Options:               runtimeserver.NewOptions(),
		ReconcileResyncPeriod: 60 * time.Second,
		BackendTimeout:        15 * time.Second,
		ServiceName:           "aggexp",
		ServiceNamespace:      "aggexp-system",
		CAPath:                "/etc/aggexp/certs/ca.crt",
		ShutdownGrace:         25 * time.Second,
	}
}

func (o *options) addFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	fs.DurationVar(&o.ReconcileResyncPeriod, "reconcile-resync", o.ReconcileResyncPeriod,
		"APIDefinition informer resync period.")
	fs.DurationVar(&o.BackendTimeout, "backend-timeout", o.BackendTimeout,
		"Timeout for startup/reconcile calls to a backend.")
	fs.StringVar(&o.ServiceName, "service-name", o.ServiceName,
		"Name of the host-cluster Service fronting this middleware pod.")
	fs.StringVar(&o.ServiceNamespace, "service-namespace", o.ServiceNamespace,
		"Namespace of the host-cluster Service fronting this middleware pod.")
	fs.StringVar(&o.CAPath, "ca-path", o.CAPath,
		"Path to the CA cert used as APIService.caBundle.")
	fs.DurationVar(&o.ShutdownGrace, "shutdown-grace", o.ShutdownGrace,
		"How long to wait after sending APIService deletes during graceful shutdown.")
}

// ---------------------------------------------------------------------
// main entry
// ---------------------------------------------------------------------

func main() {
	opts := newOptions()
	cmd := &cobra.Command{
		Use:   "aggexp-multiplex",
		Short: "0027 multiplex middleware: one process, many AAs, reconciled from APIDefinition CRDs.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := opts.Validate(); err != nil {
				return err
			}
			return runMultiplex(opts, genericapiserver.SetupSignalContext())
		},
	}
	opts.addFlags(cmd.Flags())
	logs.AddFlags(cmd.Flags())
	if code := cli.Run(cmd); code != 0 {
		fmt.Fprintln(os.Stderr, "aggexp-multiplex exited with error")
		os.Exit(code)
	}
}

// runMultiplex is the bootstrapping path.
func runMultiplex(o *options, ctx context.Context) error {
	cfg, err := clientgorest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("build in-cluster kube client config: %w", err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("dynamic client: %w", err)
	}

	caBundle, err := os.ReadFile(o.CAPath)
	if err != nil {
		return fmt.Errorf("read ca bundle %s: %w", o.CAPath, err)
	}

	mx := newMultiplex(o, dyn, caBundle)

	// Boot scheme is a placeholder — we register real groups at
	// reconcile time. metav1 is needed for list/watch options.
	bootScheme := runtime.NewScheme()
	metav1.AddToGroupVersion(bootScheme, schema.GroupVersion{Version: "v1"})
	codecs := serializer.NewCodecFactory(bootScheme)

	// OpenAPI closure re-reads per-AA defs on each library refresh.
	openAPIFunc := func(refCallback common.ReferenceCallback) map[string]common.OpenAPIDefinition {
		defs := componentopenapi.GeneratedDefinitions(refCallback)
		for k, v := range mx.currentOpenAPIDefs() {
			defs[k] = v
		}
		return defs
	}

	in := runtimeserver.Input{
		Scheme:             bootScheme,
		Codecs:             codecs,
		OpenAPIDefinitions: openAPIFunc,
	}

	cfgRecommended, err := o.Config(in)
	if err != nil {
		return err
	}
	completed := cfgRecommended.Complete()
	server, err := completed.New("aggexp-multiplex-0027", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return fmt.Errorf("creating apiserver: %w", err)
	}

	mx.attachServer(server)

	if err := server.AddPostStartHook("multiplex-reconciler",
		func(hookCtx genericapiserver.PostStartHookContext) error {
			go mx.runReconciler(hookCtx.Context)
			return nil
		},
	); err != nil {
		return err
	}
	if err := server.AddPreShutdownHook("multiplex-sweep-apiservices",
		func() error {
			sweepCtx, cancel := context.WithTimeout(context.Background(), o.ShutdownGrace)
			defer cancel()
			mx.shutdownSweep(sweepCtx)
			return nil
		},
	); err != nil {
		return err
	}

	prepared := server.PrepareRun()
	return prepared.RunWithContext(ctx)
}

// ---------------------------------------------------------------------
// Multiplex core
// ---------------------------------------------------------------------

type installed struct {
	key string // APIDefinition CR name

	groupVersion schema.GroupVersion
	kind         string
	plural       string
	singular     string
	namespaced   bool

	apiServiceName string

	rest           *grpcbackend.REST
	groupInstaller *group.Group

	cancelWatch context.CancelFunc

	openapiItemKey string
	openapiListKey string
	itemSchema     spec.Schema
	listSchema     spec.Schema

	apiServiceAlive bool
}

type multiplex struct {
	opts     *options
	dyn      dynamic.Interface
	caBundle []byte

	serverMu sync.Mutex
	server   *genericapiserver.GenericAPIServer

	installedMu sync.RWMutex
	installed   map[string]*installed // key: "<group>/<version>"

	queue    workqueue.TypedRateLimitingInterface[string]
	informer cache.SharedIndexInformer
	stopCh   chan struct{}
}

func newMultiplex(o *options, dyn dynamic.Interface, ca []byte) *multiplex {
	return &multiplex{
		opts:      o,
		dyn:       dyn,
		caBundle:  ca,
		installed: map[string]*installed{},
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "apidefinitions"},
		),
		stopCh: make(chan struct{}),
	}
}

func (m *multiplex) attachServer(s *genericapiserver.GenericAPIServer) {
	m.serverMu.Lock()
	defer m.serverMu.Unlock()
	m.server = s
}

func (m *multiplex) currentOpenAPIDefs() map[string]common.OpenAPIDefinition {
	m.installedMu.RLock()
	defer m.installedMu.RUnlock()
	out := map[string]common.OpenAPIDefinition{}
	for _, i := range m.installed {
		out[i.openapiItemKey] = common.OpenAPIDefinition{Schema: i.itemSchema}
		out[i.openapiListKey] = common.OpenAPIDefinition{Schema: i.listSchema}
	}
	return out
}

// ---------------------------------------------------------------------
// GVRs used as host-cluster client handles
// ---------------------------------------------------------------------

var apiDefGVR = schema.GroupVersionResource{Group: "aggexp.io", Version: "v1", Resource: "apidefinitions"}
var apiServiceGVR = schema.GroupVersionResource{Group: "apiregistration.k8s.io", Version: "v1", Resource: "apiservices"}

// ---------------------------------------------------------------------
// Informer + reconcile loop
// ---------------------------------------------------------------------

func (m *multiplex) runReconciler(ctx context.Context) {
	klog.Infof("multiplex: starting reconciler; informer GVR=%s", apiDefGVR)
	f := dynamicinformer.NewFilteredDynamicSharedInformerFactory(m.dyn, m.opts.ReconcileResyncPeriod, metav1.NamespaceAll, nil)
	inf := f.ForResource(apiDefGVR).Informer()
	m.informer = inf

	_, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if k := keyFor(obj); k != "" {
				m.queue.Add(k)
			}
		},
		UpdateFunc: func(_, obj interface{}) {
			if k := keyFor(obj); k != "" {
				m.queue.Add(k)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tomb.Obj
			}
			if k := keyFor(obj); k != "" {
				m.queue.Add(k)
			}
		},
	})
	if err != nil {
		klog.Errorf("multiplex: AddEventHandler: %v", err)
		return
	}

	f.Start(m.stopCh)
	if !cache.WaitForCacheSync(ctx.Done(), inf.HasSynced) {
		klog.Error("multiplex: informer cache did not sync")
		return
	}
	klog.Info("multiplex: informer cache synced; reconciler live")

	go wait.UntilWithContext(ctx, func(ctx context.Context) {
		for m.processNext(ctx) {
		}
	}, time.Second)

	<-ctx.Done()
	klog.Info("multiplex: reconciler exiting")
	close(m.stopCh)
	m.queue.ShutDown()
}

func keyFor(obj interface{}) string {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return ""
	}
	return u.GetName()
}

func (m *multiplex) processNext(ctx context.Context) bool {
	name, shutdown := m.queue.Get()
	if shutdown {
		return false
	}
	defer m.queue.Done(name)
	if err := m.reconcile(ctx, name); err != nil {
		klog.Errorf("multiplex: reconcile %s: %v", name, err)
		m.queue.AddRateLimited(name)
		return true
	}
	m.queue.Forget(name)
	return true
}

func (m *multiplex) reconcile(ctx context.Context, name string) error {
	obj, err := m.dyn.Resource(apiDefGVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("get apidefinition %s: %w", name, err)
	}
	if apierrors.IsNotFound(err) {
		return m.reconcileDelete(ctx, name)
	}
	return m.reconcileUpsert(ctx, obj)
}

func (m *multiplex) reconcileDelete(ctx context.Context, name string) error {
	m.installedMu.Lock()
	defer m.installedMu.Unlock()
	for key, inst := range m.installed {
		if inst.key == name {
			klog.Infof("multiplex: deleting APIService for %s (apidef=%s)", key, name)
			if inst.apiServiceAlive {
				if err := m.dyn.Resource(apiServiceGVR).Delete(ctx, inst.apiServiceName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
					return fmt.Errorf("delete apiservice %s: %w", inst.apiServiceName, err)
				}
				inst.apiServiceAlive = false
			}
			if inst.cancelWatch != nil {
				inst.cancelWatch()
			}
			if inst.rest != nil {
				inst.rest.Shutdown()
			}
			// Intentionally keep entry in m.installed so the same
			// group/version can't be reinstalled via a new APIDefinition.
			break
		}
	}
	return nil
}

func (m *multiplex) reconcileUpsert(ctx context.Context, u *unstructured.Unstructured) error {
	name := u.GetName()
	spec, err := parseAPIDefSpec(u)
	if err != nil {
		m.recordCondition(ctx, name, "Ready", "False", "SchemaInvalid", err.Error())
		return err
	}
	key := spec.group + "/" + spec.version

	m.installedMu.RLock()
	existing := m.installed[key]
	m.installedMu.RUnlock()

	if existing != nil && existing.key != name {
		msg := fmt.Sprintf("group/version %s/%s is already registered by APIDefinition %q", spec.group, spec.version, existing.key)
		m.recordCondition(ctx, name, "Ready", "False", "Conflict", msg)
		return errors.New(msg)
	}
	if existing != nil && existing.key == name {
		if !existing.apiServiceAlive {
			if err := m.ensureAPIService(ctx, existing); err != nil {
				m.recordCondition(ctx, name, "Available", "False", "APIServiceError", err.Error())
				return err
			}
		}
		m.recordSuccess(ctx, u, existing)
		return nil
	}

	inst, err := m.buildInstall(ctx, name, spec)
	if err != nil {
		m.recordCondition(ctx, name, "Ready", "False", classifyReason(err), err.Error())
		return err
	}
	if err := m.installGroup(inst); err != nil {
		if inst.rest != nil {
			inst.rest.Shutdown()
		}
		m.recordCondition(ctx, name, "Ready", "False", "InstallGroupFailed", err.Error())
		return err
	}
	if err := m.ensureAPIService(ctx, inst); err != nil {
		m.recordCondition(ctx, name, "Available", "False", "APIServiceError", err.Error())
		return err
	}

	m.installedMu.Lock()
	m.installed[key] = inst
	m.installedMu.Unlock()

	watchCtx, cancel := context.WithCancel(context.Background())
	inst.cancelWatch = cancel
	inst.rest.StartUpstreamWatch(watchCtx)

	m.recordSuccess(ctx, u, inst)
	klog.Infof("multiplex: reconciled APIDefinition %s -> group=%s/%s resource=%s APIService=%s",
		name, spec.group, spec.version, spec.plural, inst.apiServiceName)
	return nil
}

type apidefSpec struct {
	group           string
	version         string
	kind            string
	plural          string
	singular        string
	namespaced      bool
	schemaSource    string
	embeddedSchema  []byte
	backendTrans    string
	backendAddress  string
	watchCapability string
}

func parseAPIDefSpec(u *unstructured.Unstructured) (apidefSpec, error) {
	var out apidefSpec
	sp, found, err := unstructured.NestedMap(u.Object, "spec")
	if err != nil || !found {
		return out, fmt.Errorf("spec missing: %v", err)
	}
	out.group, _ = sp["group"].(string)
	out.version, _ = sp["version"].(string)
	out.kind, _ = sp["kind"].(string)
	out.plural, _ = sp["plural"].(string)
	out.singular, _ = sp["singular"].(string)
	scope, _ := sp["scope"].(string)
	out.namespaced = scope == "Namespaced"
	out.schemaSource, _ = sp["schemaSource"].(string)
	if out.schemaSource == "" {
		out.schemaSource = "backendJSONSchema"
	}
	if out.schemaSource == "configEmbedded" {
		if sch, ok := sp["schema"]; ok && sch != nil {
			raw, err := json.Marshal(sch)
			if err != nil {
				return out, fmt.Errorf("marshal embedded schema: %w", err)
			}
			out.embeddedSchema = raw
		} else {
			return out, fmt.Errorf("schemaSource=configEmbedded but .spec.schema is missing")
		}
	}
	if b, ok := sp["backend"].(map[string]interface{}); ok {
		out.backendTrans, _ = b["transport"].(string)
		out.backendAddress, _ = b["address"].(string)
	} else {
		return out, fmt.Errorf(".spec.backend is missing")
	}
	out.watchCapability, _ = sp["watchCapability"].(string)
	if out.backendTrans == "" {
		out.backendTrans = "http"
	}
	if out.backendTrans != "http" {
		return out, fmt.Errorf("backend.transport %q not supported in 0027 (only http)", out.backendTrans)
	}
	for _, req := range []struct{ k, v string }{
		{"group", out.group}, {"version", out.version}, {"kind", out.kind},
		{"plural", out.plural}, {"singular", out.singular},
		{"backend.address", out.backendAddress},
	} {
		if req.v == "" {
			return out, fmt.Errorf("spec.%s is required", req.k)
		}
	}
	return out, nil
}

func (m *multiplex) buildInstall(ctx context.Context, name string, s apidefSpec) (*installed, error) {
	hc := newHTTPBackendClient(s.backendAddress, m.opts.BackendTimeout)

	var jsonSchema []byte
	var backendColumns []*componentpb.TableColumn
	var backendRowFields []string
	switch s.schemaSource {
	case "backendJSONSchema":
		fetchCtx, cancel := context.WithTimeout(ctx, m.opts.BackendTimeout)
		defer cancel()
		resp, err := hc.GetSchema(fetchCtx, &componentpb.GetSchemaRequest{})
		if err != nil {
			return nil, fmt.Errorf("backend GetSchema: %w", err)
		}
		jsonSchema = resp.GetOpenapiV3()
		backendColumns = resp.GetColumns()
		backendRowFields = resp.GetRowFields()
	case "configEmbedded":
		jsonSchema = s.embeddedSchema
	case "backendOpenAPI":
		return nil, fmt.Errorf("schemaSource=backendOpenAPI not implemented in 0027")
	default:
		return nil, fmt.Errorf("unknown schemaSource %q", s.schemaSource)
	}

	gv := schema.GroupVersion{Group: s.group, Version: s.version}
	gvk := gv.WithKind(s.kind)
	listGVK := gv.WithKind(s.kind + "List")

	liftedOpenAPI, err := LiftJSONSchemaToOpenAPI(gvk, jsonSchema)
	if err != nil {
		return nil, fmt.Errorf("synthesis lift: %w", err)
	}

	desc := componentscheme.ResourceDescriptor{
		GroupVersion:    gv,
		Resource:        s.plural,
		Kind:            s.kind,
		Singular:        s.singular,
		Namespaced:      s.namespaced,
		UseTypedWrapper: true,
	}
	bundle := componentscheme.Build(desc)

	itemSchema, perr := componentopenapi.ParseBackendSchema(liftedOpenAPI, gvk)
	if perr != nil {
		return nil, fmt.Errorf("parse lifted OpenAPI: %w", perr)
	}
	listSchema := componentopenapi.WrapAsList(listGVK, bundle.ItemCanonicalName)

	var cols []metav1.TableColumnDefinition
	for _, c := range backendColumns {
		cols = append(cols, metav1.TableColumnDefinition{
			Name: c.GetName(), Type: c.GetType(), Format: c.GetFormat(),
			Description: c.GetDescription(), Priority: c.GetPriority(),
		})
	}
	rowFields := backendRowFields
	if len(cols) == 0 {
		cols = []metav1.TableColumnDefinition{
			{Name: "Name", Type: "string", Description: "Name"},
			{Name: "Age", Type: "string", Description: "Time since creation"},
		}
		rowFields = []string{".metadata.name", ".metadata.creationTimestamp"}
	}

	restStorage := grpcbackend.New(grpcbackend.Descriptor{
		GroupVersion:            gv,
		Resource:                s.plural,
		Kind:                    s.kind,
		Singular:                s.singular,
		Namespaced:              s.namespaced,
		Writable:                true,
		SupportsServerSideApply: true,
		UseTypedWrapper:         true,
		Columns:                 cols,
		RowFields:               rowFields,
		GroupResource:           schema.GroupResource{Group: s.group, Resource: s.plural},
	}, hc)

	g := &group.Group{
		GroupVersion:   gv,
		Scheme:         bundle.Scheme,
		Codecs:         bundle.Codecs,
		ParameterCodec: bundle.ParameterCodec,
		Resources:      map[string]rest.Storage{s.plural: restStorage},
	}

	return &installed{
		key:            name,
		groupVersion:   gv,
		kind:           s.kind,
		plural:         s.plural,
		singular:       s.singular,
		namespaced:     s.namespaced,
		apiServiceName: fmt.Sprintf("%s.%s", s.version, s.group),
		rest:           restStorage,
		groupInstaller: g,
		openapiItemKey: bundle.ItemCanonicalName,
		openapiListKey: bundle.ListCanonicalName,
		itemSchema:     itemSchema,
		listSchema:     listSchema,
	}, nil
}

func (m *multiplex) installGroup(inst *installed) error {
	m.serverMu.Lock()
	s := m.server
	m.serverMu.Unlock()
	if s == nil {
		return fmt.Errorf("apiserver not yet constructed")
	}
	if err := inst.groupInstaller.Install(s); err != nil {
		return fmt.Errorf("InstallAPIGroup: %w", err)
	}
	klog.Infof("multiplex: installed API group %s resource=%s", inst.groupVersion, inst.plural)
	return nil
}

// ensureAPIService creates-or-updates the APIService object using
// the dynamic client (no typed kube-aggregator dependency).
func (m *multiplex) ensureAPIService(ctx context.Context, inst *installed) error {
	u := buildAPIServiceUnstructured(inst.apiServiceName, inst.groupVersion, m.opts.ServiceName, m.opts.ServiceNamespace, m.caBundle)
	existing, err := m.dyn.Resource(apiServiceGVR).Get(ctx, inst.apiServiceName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("get apiservice: %w", err)
	}
	if apierrors.IsNotFound(err) {
		if _, cerr := m.dyn.Resource(apiServiceGVR).Create(ctx, u, metav1.CreateOptions{FieldManager: "aggexp-multiplex"}); cerr != nil {
			return fmt.Errorf("create apiservice: %w", cerr)
		}
		inst.apiServiceAlive = true
		return nil
	}
	u.SetResourceVersion(existing.GetResourceVersion())
	if _, uerr := m.dyn.Resource(apiServiceGVR).Update(ctx, u, metav1.UpdateOptions{FieldManager: "aggexp-multiplex"}); uerr != nil {
		return fmt.Errorf("update apiservice: %w", uerr)
	}
	inst.apiServiceAlive = true
	return nil
}

func (m *multiplex) shutdownSweep(ctx context.Context) {
	m.installedMu.RLock()
	snap := make([]*installed, 0, len(m.installed))
	for _, i := range m.installed {
		snap = append(snap, i)
	}
	m.installedMu.RUnlock()
	klog.Infof("multiplex: shutdownSweep: %d APIService(s) to remove", len(snap))
	for _, i := range snap {
		if !i.apiServiceAlive {
			continue
		}
		if err := m.dyn.Resource(apiServiceGVR).Delete(ctx, i.apiServiceName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			klog.Warningf("multiplex: shutdownSweep: delete %s: %v", i.apiServiceName, err)
			continue
		}
		i.apiServiceAlive = false
		klog.Infof("multiplex: shutdownSweep: deleted APIService %s", i.apiServiceName)
	}
}

// ---------------------------------------------------------------------
// Status writer
// ---------------------------------------------------------------------

func (m *multiplex) recordSuccess(ctx context.Context, u *unstructured.Unstructured, inst *installed) {
	name := u.GetName()
	gen := u.GetGeneration()
	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"observedGeneration":   gen,
			"registeredAPIService": inst.apiServiceName,
			"conditions": []interface{}{
				newCondition("Ready", "True", "Reconciled",
					fmt.Sprintf("APIDefinition reconciled; APIService %s active", inst.apiServiceName), gen),
				newCondition("Available", "True", "APIServiceRegistered",
					fmt.Sprintf("APIService %s exists on host cluster", inst.apiServiceName), gen),
			},
		},
	}
	writeStatus(ctx, m.dyn, name, patch)
}

func (m *multiplex) recordCondition(ctx context.Context, name, condType, status, reason, msg string) {
	obj, err := m.dyn.Resource(apiDefGVR).Get(ctx, name, metav1.GetOptions{})
	var gen int64 = 0
	if err == nil {
		gen = obj.GetGeneration()
	}
	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"observedGeneration": gen,
			"conditions": []interface{}{
				newCondition(condType, status, reason, msg, gen),
			},
		},
	}
	writeStatus(ctx, m.dyn, name, patch)
}

func newCondition(t, s, reason, msg string, gen int64) map[string]interface{} {
	return map[string]interface{}{
		"type":               t,
		"status":             s,
		"reason":             reason,
		"message":            msg,
		"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
		"observedGeneration": gen,
	}
}

const typeMergeJSON = "application/merge-patch+json"

func writeStatus(ctx context.Context, dyn dynamic.Interface, name string, patch map[string]interface{}) {
	raw, err := json.Marshal(patch)
	if err != nil {
		klog.Warningf("writeStatus: marshal: %v", err)
		return
	}
	_, err = dyn.Resource(apiDefGVR).Patch(ctx, name, typeMergeJSON, raw, metav1.PatchOptions{FieldManager: "aggexp-multiplex"}, "status")
	if err != nil && !apierrors.IsNotFound(err) {
		klog.Warningf("writeStatus: patch %s: %v", name, err)
	}
}

func classifyReason(err error) string {
	s := err.Error()
	switch {
	case strings.Contains(s, "GetSchema"), strings.Contains(s, "dial"), strings.Contains(s, "connect"):
		return "BackendUnreachable"
	case strings.Contains(s, "synthesis"), strings.Contains(s, "parse lifted OpenAPI"), strings.Contains(s, "schemaSource"):
		return "SchemaInvalid"
	default:
		return "ReconcileError"
	}
}

// buildAPIServiceUnstructured composes an unstructured APIService
// object suitable for dynamic-client Create/Update.
func buildAPIServiceUnstructured(name string, gv schema.GroupVersion, svcName, svcNs string, caBundle []byte) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "apiregistration.k8s.io", Version: "v1", Kind: "APIService",
	})
	u.SetName(name)
	u.SetLabels(map[string]string{"app.kubernetes.io/managed-by": "aggexp-multiplex"})
	u.Object["spec"] = map[string]interface{}{
		"group":                gv.Group,
		"version":              gv.Version,
		"groupPriorityMinimum": int64(1000),
		"versionPriority":      int64(15),
		// caBundle is wire-encoded as base64 in JSON. metav1 handles
		// this for typed clients; unstructured uses the raw JSON
		// shape, which is base64 for []byte.
		"caBundle": caBundle,
		"service": map[string]interface{}{
			"name":      svcName,
			"namespace": svcNs,
			"port":      int64(443),
		},
	}
	return u
}
