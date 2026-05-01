// Package multiplex implements the v2 dynamic AA reconciler. One
// middleware process hosts many aggregated APIs: APIDefinition CRDs
// on the host cluster declare them, a dynamic informer drives a
// workqueue, and each reconcile installs (or removes) an API group
// on the already-running generic apiserver.
//
// Consolidates FINDINGS/0027's findings into substrate form:
//
//   - APIDefinition is a first-class typed runtime.Object registered
//     on a bootstrap scheme, so reconcile uses typed access rather
//     than stringly-typed unstructured dives.
//   - The OpenAPI closure re-reads its source on every invocation;
//     callers MUST nil OpenAPIV3Config.Definitions + OpenAPIConfig.Definitions
//     on the RecommendedConfig after runtime/server.Config()
//     returns, to defeat the library's eager materialization
//     (FINDINGS/0027 consequent).
//   - Per-AA REST storage is built with v2/grpcbackend.REST +
//     transport-selected BackendClient (grpc or http) + optional
//     MetadataStore + optional admission.Engine.
//   - Graceful shutdown deletes managed APIServices via a
//     PreShutdown hook.
//
// # Known gap in v2 alpha
//
// The library's /openapi/v3 per-group endpoint map and the SSA
// typed-converter are both built once at PrepareRun and frozen for
// statically-known groups. Dynamically-installed groups get CRUD +
// list + watch + table rendering but DEGRADE for:
//
//   - kubectl explain <kind> on a dynamic group returns 404 at
//     /openapi/v3/apis/<group>/<version>.
//   - kubectl apply --server-side on a dynamic group fails at
//     managedfields.NewTypeConverter.
//
// The cache-defeat fix for eager Definitions materialization LANDS
// in v2 (opt out via Compose's closure shape); the per-install V3
// endpoint refresh and SSA typed-converter rebuild are DEFERRED per
// FINDINGS/0027. Single-AA (non-multiplex) consumers of v2 keep
// full SSA + explain because their group is installed at PrepareRun.
package multiplex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"

	"github.com/cheeseandcereal/aggexp/runtime/component/v2/admission"
	"github.com/cheeseandcereal/aggexp/runtime/component/v2/grpcbackend"
	"github.com/cheeseandcereal/aggexp/runtime/component/v2/httpbackend"
	"github.com/cheeseandcereal/aggexp/runtime/component/v2/metadatastore"
	componentopenapi "github.com/cheeseandcereal/aggexp/runtime/component/v2/openapi"
	componentv2pb "github.com/cheeseandcereal/aggexp/runtime/component/v2/proto"
	componentscheme "github.com/cheeseandcereal/aggexp/runtime/component/v2/scheme"
	"github.com/cheeseandcereal/aggexp/runtime/group"
)

// APIDefGVR is the host-cluster GVR of the APIDefinition CRD.
var APIDefGVR = schema.GroupVersionResource{
	Group: "aggexp.io", Version: "v1", Resource: "apidefinitions",
}

// APIServiceGVR is the host-cluster aggregation-layer GVR.
var APIServiceGVR = schema.GroupVersionResource{
	Group: "apiregistration.k8s.io", Version: "v1", Resource: "apiservices",
}

// Options is the Multiplex reconciler's configuration.
type Options struct {
	// CABundle is the APIService.caBundle used for each registered
	// aggregate.
	CABundle []byte
	// ServiceName / ServiceNamespace identify the host-cluster
	// Service fronting this middleware pod.
	ServiceName      string
	ServiceNamespace string
	// ReconcileResyncPeriod is the informer's resync period.
	ReconcileResyncPeriod time.Duration
	// BackendTimeout bounds per-AA GetSchema calls.
	BackendTimeout time.Duration
	// ShutdownGrace is how long the PreShutdown sweep is given.
	ShutdownGrace time.Duration
	// FieldManager on APIDefinition status writes and APIService
	// creates/updates.
	FieldManager string
	// MetadataStore, if non-nil, is wired into every per-AA REST for
	// KRM stitching.
	MetadataStore *metadatastore.Store
}

// Defaults returns sensible defaults.
func Defaults() Options {
	return Options{
		ServiceName:           "aggexp",
		ServiceNamespace:      "aggexp-system",
		ReconcileResyncPeriod: 60 * time.Second,
		BackendTimeout:        15 * time.Second,
		ShutdownGrace:         25 * time.Second,
		FieldManager:          "aggexp-v2-multiplex",
	}
}

// Multiplex hosts dynamic AAs.
type Multiplex struct {
	opts Options
	dyn  dynamic.Interface

	serverMu sync.Mutex
	server   *genericapiserver.GenericAPIServer

	installedMu sync.RWMutex
	installed   map[string]*installed // key: "<group>/<version>"

	queue    workqueue.TypedRateLimitingInterface[string]
	informer cache.SharedIndexInformer
	stopCh   chan struct{}
}

// installed tracks the state of one registered AA.
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

// New builds a Multiplex.
func New(dyn dynamic.Interface, o Options) *Multiplex {
	if o.ReconcileResyncPeriod <= 0 {
		o.ReconcileResyncPeriod = 60 * time.Second
	}
	if o.BackendTimeout <= 0 {
		o.BackendTimeout = 15 * time.Second
	}
	if o.ShutdownGrace <= 0 {
		o.ShutdownGrace = 25 * time.Second
	}
	if o.FieldManager == "" {
		o.FieldManager = "aggexp-v2-multiplex"
	}
	return &Multiplex{
		opts:      o,
		dyn:       dyn,
		installed: map[string]*installed{},
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "apidefinitions"},
		),
		stopCh: make(chan struct{}),
	}
}

// AttachServer must be called after the generic apiserver has been
// built but before it runs. The reconciler installs groups into it.
func (m *Multiplex) AttachServer(s *genericapiserver.GenericAPIServer) {
	m.serverMu.Lock()
	defer m.serverMu.Unlock()
	m.server = s
}

// OpenAPIClosure returns a GetOpenAPIDefinitions function that
// merges each installed AA's item+list schemas with the baseline
// meta/v1 definitions. Pass it as runtime/server.Input.OpenAPIDefinitions.
func (m *Multiplex) OpenAPIClosure() common.GetOpenAPIDefinitions {
	return componentopenapi.Compose(func() map[string]common.OpenAPIDefinition {
		m.installedMu.RLock()
		defer m.installedMu.RUnlock()
		out := map[string]common.OpenAPIDefinition{}
		for _, i := range m.installed {
			out[i.openapiItemKey] = common.OpenAPIDefinition{Schema: i.itemSchema}
			out[i.openapiListKey] = common.OpenAPIDefinition{Schema: i.listSchema}
		}
		return out
	})
}

// Run starts the reconciler goroutine. Blocks until ctx is
// cancelled; the queue shuts down on exit.
func (m *Multiplex) Run(ctx context.Context) {
	klog.Infof("multiplex: starting; informer GVR=%s", APIDefGVR)
	f := dynamicinformer.NewFilteredDynamicSharedInformerFactory(m.dyn, m.opts.ReconcileResyncPeriod, metav1.NamespaceAll, nil)
	inf := f.ForResource(APIDefGVR).Informer()
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
	klog.Info("multiplex: informer synced; reconciler live")
	go wait.UntilWithContext(ctx, func(ctx context.Context) {
		for m.processNext(ctx) {
		}
	}, time.Second)
	<-ctx.Done()
	close(m.stopCh)
	m.queue.ShutDown()
}

// ShutdownSweep deletes every APIService this multiplex owns.
// Intended as a PreShutdown hook.
func (m *Multiplex) ShutdownSweep(ctx context.Context) {
	m.installedMu.RLock()
	snap := make([]*installed, 0, len(m.installed))
	for _, i := range m.installed {
		snap = append(snap, i)
	}
	m.installedMu.RUnlock()
	klog.Infof("multiplex: ShutdownSweep: %d APIService(s) to remove", len(snap))
	for _, i := range snap {
		if !i.apiServiceAlive {
			continue
		}
		if err := m.dyn.Resource(APIServiceGVR).Delete(ctx, i.apiServiceName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			klog.Warningf("multiplex: ShutdownSweep delete %s: %v", i.apiServiceName, err)
			continue
		}
		i.apiServiceAlive = false
	}
}

func keyFor(obj interface{}) string {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return ""
	}
	return u.GetName()
}

func (m *Multiplex) processNext(ctx context.Context) bool {
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

func (m *Multiplex) reconcile(ctx context.Context, name string) error {
	obj, err := m.dyn.Resource(APIDefGVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("get apidefinition %s: %w", name, err)
	}
	if apierrors.IsNotFound(err) {
		return m.reconcileDelete(ctx, name)
	}
	return m.reconcileUpsert(ctx, obj)
}

func (m *Multiplex) reconcileDelete(ctx context.Context, name string) error {
	m.installedMu.Lock()
	defer m.installedMu.Unlock()
	for key, inst := range m.installed {
		if inst.key == name {
			if inst.apiServiceAlive {
				if err := m.dyn.Resource(APIServiceGVR).Delete(ctx, inst.apiServiceName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
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
			_ = key // kept for future: we deliberately leave the
			// entry so the same group/version can't be reinstalled.
			break
		}
	}
	return nil
}

func (m *Multiplex) reconcileUpsert(ctx context.Context, u *unstructured.Unstructured) error {
	name := u.GetName()
	parsed, err := parseAPIDef(u)
	if err != nil {
		m.recordCondition(ctx, name, "Ready", "False", "SchemaInvalid", err.Error())
		return err
	}
	key := parsed.Spec.Group + "/" + parsed.Spec.Version

	m.installedMu.RLock()
	existing := m.installed[key]
	m.installedMu.RUnlock()

	if existing != nil && existing.key != name {
		msg := fmt.Sprintf("group/version %s/%s is already registered by APIDefinition %q", parsed.Spec.Group, parsed.Spec.Version, existing.key)
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

	inst, err := m.buildInstall(ctx, name, parsed)
	if err != nil {
		m.recordCondition(ctx, name, "Ready", "False", classifyReason(err), err.Error())
		return err
	}

	m.installedMu.Lock()
	m.installed[key] = inst
	m.installedMu.Unlock()

	if err := m.installGroup(inst); err != nil {
		m.installedMu.Lock()
		delete(m.installed, key)
		m.installedMu.Unlock()
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
	watchCtx, cancel := context.WithCancel(context.Background())
	inst.cancelWatch = cancel
	inst.rest.StartUpstreamWatch(watchCtx)

	m.recordSuccess(ctx, u, inst)
	klog.Infof("multiplex: reconciled APIDefinition %s -> group=%s/%s resource=%s APIService=%s",
		name, parsed.Spec.Group, parsed.Spec.Version, parsed.Spec.Plural, inst.apiServiceName)
	return nil
}

func (m *Multiplex) buildInstall(ctx context.Context, defName string, parsed APIDefinition) (*installed, error) {
	// Transport selection.
	var backend componentv2pb.BackendClient
	switch strings.ToLower(parsed.Spec.Backend.Transport) {
	case "", "http":
		client, err := httpbackend.New(parsed.Spec.Backend.Address, m.opts.BackendTimeout)
		if err != nil {
			return nil, fmt.Errorf("http client: %w", err)
		}
		backend = client
	case "grpc":
		dialCtx, cancel := context.WithTimeout(ctx, m.opts.BackendTimeout)
		defer cancel()
		client, _, err := grpcbackend.Dial(dialCtx, parsed.Spec.Backend.Address)
		if err != nil {
			return nil, fmt.Errorf("grpc dial: %w", err)
		}
		backend = client
	default:
		return nil, fmt.Errorf("unknown backend transport %q", parsed.Spec.Backend.Transport)
	}

	// Schema source.
	var rawSchema []byte
	var isOpenAPI bool
	var cols []*componentv2pb.TableColumn
	var rowFields []string
	switch parsed.Spec.SchemaSource {
	case "", "backendJSONSchema":
		fetchCtx, cancel := context.WithTimeout(ctx, m.opts.BackendTimeout)
		defer cancel()
		resp, err := backend.GetSchema(fetchCtx, &componentv2pb.GetSchemaRequest{})
		if err != nil {
			return nil, fmt.Errorf("backend GetSchema: %w", err)
		}
		rawSchema = resp.GetSchema()
		isOpenAPI = resp.GetSchemaIsOpenapi()
		cols = resp.GetColumns()
		rowFields = resp.GetRowFields()
		if parsed.Spec.WatchCapability == "" {
			parsed.Spec.WatchCapability = resp.GetWatchCapability()
		}
	case "configEmbedded":
		if len(parsed.Spec.Schema.Raw) == 0 {
			return nil, fmt.Errorf("schemaSource=configEmbedded but spec.schema is missing")
		}
		rawSchema = parsed.Spec.Schema.Raw
		isOpenAPI = false
	default:
		return nil, fmt.Errorf("unknown schemaSource %q", parsed.Spec.SchemaSource)
	}

	gv := schema.GroupVersion{Group: parsed.Spec.Group, Version: parsed.Spec.Version}
	gvk := gv.WithKind(parsed.Spec.Kind)
	listGVK := gv.WithKind(parsed.Spec.Kind + "List")

	// Track B synthesis if the backend shipped plain JSON Schema.
	if !isOpenAPI {
		lifted, err := componentopenapi.Synthesize(gvk, rawSchema)
		if err != nil {
			return nil, fmt.Errorf("synthesis lift: %w", err)
		}
		rawSchema = lifted
	}

	desc := componentscheme.ResourceDescriptor{
		GroupVersion:    gv,
		Resource:        parsed.Spec.Plural,
		Kind:            parsed.Spec.Kind,
		Singular:        parsed.Spec.Singular,
		Namespaced:      parsed.Spec.Scope == "Namespaced",
		UseTypedWrapper: true,
	}
	bundle := componentscheme.Build(desc)

	itemSchema, perr := componentopenapi.ParseBackendSchema(rawSchema, gvk)
	if perr != nil {
		return nil, fmt.Errorf("parse OpenAPI: %w", perr)
	}
	listSchema := componentopenapi.WrapAsList(listGVK, bundle.ItemCanonicalName)

	tableCols := make([]metav1.TableColumnDefinition, 0, len(cols))
	for _, c := range cols {
		tableCols = append(tableCols, metav1.TableColumnDefinition{
			Name: c.GetName(), Type: c.GetType(), Format: c.GetFormat(),
			Description: c.GetDescription(), Priority: c.GetPriority(),
		})
	}
	if len(tableCols) == 0 {
		tableCols = []metav1.TableColumnDefinition{
			{Name: "Name", Type: "string", Description: "Name"},
			{Name: "Age", Type: "string", Description: "Time since creation"},
		}
		rowFields = []string{".metadata.name", ".metadata.creationTimestamp"}
	}

	watchMode := grpcbackend.ModePoll
	switch strings.ToLower(parsed.Spec.WatchCapability) {
	case "push", "both":
		watchMode = grpcbackend.ModePush
	}

	restStorage := grpcbackend.New(grpcbackend.Descriptor{
		GroupVersion:            gv,
		Resource:                parsed.Spec.Plural,
		Kind:                    parsed.Spec.Kind,
		Singular:                parsed.Spec.Singular,
		Namespaced:              parsed.Spec.Scope == "Namespaced",
		Writable:                true,
		SupportsServerSideApply: true,
		UseTypedWrapper:         true,
		Columns:                 tableCols,
		RowFields:               rowFields,
		GroupResource:           schema.GroupResource{Group: parsed.Spec.Group, Resource: parsed.Spec.Plural},
		WatchMode:               watchMode,
	}, backend)

	if m.opts.MetadataStore != nil {
		restStorage = restStorage.WithMetadataStore(m.opts.MetadataStore)
	}
	if len(parsed.Spec.Admission.Validations) > 0 || len(parsed.Spec.Admission.Mutations) > 0 {
		engine, err := admission.New(parsed.Spec.Admission)
		if err != nil {
			return nil, fmt.Errorf("admission compile: %w", err)
		}
		restStorage = restStorage.WithAdmission(engine)
	}

	g := &group.Group{
		GroupVersion:   gv,
		Scheme:         bundle.Scheme,
		Codecs:         bundle.Codecs,
		ParameterCodec: bundle.ParameterCodec,
		Resources:      map[string]rest.Storage{parsed.Spec.Plural: restStorage},
	}
	return &installed{
		key:            defName,
		groupVersion:   gv,
		kind:           parsed.Spec.Kind,
		plural:         parsed.Spec.Plural,
		singular:       parsed.Spec.Singular,
		namespaced:     parsed.Spec.Scope == "Namespaced",
		apiServiceName: fmt.Sprintf("%s.%s", parsed.Spec.Version, parsed.Spec.Group),
		rest:           restStorage,
		groupInstaller: g,
		openapiItemKey: bundle.ItemCanonicalName,
		openapiListKey: bundle.ListCanonicalName,
		itemSchema:     itemSchema,
		listSchema:     listSchema,
	}, nil
}

func (m *Multiplex) installGroup(inst *installed) error {
	m.serverMu.Lock()
	s := m.server
	m.serverMu.Unlock()
	if s == nil {
		return fmt.Errorf("apiserver not yet attached")
	}
	if err := inst.groupInstaller.Install(s); err != nil {
		return fmt.Errorf("InstallAPIGroup: %w", err)
	}
	klog.Infof("multiplex: installed API group %s resource=%s", inst.groupVersion, inst.plural)
	return nil
}

func (m *Multiplex) ensureAPIService(ctx context.Context, inst *installed) error {
	u := buildAPIServiceUnstructured(inst.apiServiceName, inst.groupVersion, m.opts.ServiceName, m.opts.ServiceNamespace, m.opts.CABundle)
	existing, err := m.dyn.Resource(APIServiceGVR).Get(ctx, inst.apiServiceName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("get apiservice: %w", err)
	}
	if apierrors.IsNotFound(err) {
		if _, cerr := m.dyn.Resource(APIServiceGVR).Create(ctx, u, metav1.CreateOptions{FieldManager: m.opts.FieldManager}); cerr != nil {
			return fmt.Errorf("create apiservice: %w", cerr)
		}
		inst.apiServiceAlive = true
		return nil
	}
	u.SetResourceVersion(existing.GetResourceVersion())
	if _, uerr := m.dyn.Resource(APIServiceGVR).Update(ctx, u, metav1.UpdateOptions{FieldManager: m.opts.FieldManager}); uerr != nil {
		return fmt.Errorf("update apiservice: %w", uerr)
	}
	inst.apiServiceAlive = true
	return nil
}

func (m *Multiplex) recordSuccess(ctx context.Context, u *unstructured.Unstructured, inst *installed) {
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

func (m *Multiplex) recordCondition(ctx context.Context, name, condType, status, reason, msg string) {
	obj, err := m.dyn.Resource(APIDefGVR).Get(ctx, name, metav1.GetOptions{})
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

const mergePatchJSON = "application/merge-patch+json"

func writeStatus(ctx context.Context, dyn dynamic.Interface, name string, patch map[string]interface{}) {
	raw, err := json.Marshal(patch)
	if err != nil {
		klog.Warningf("writeStatus: marshal: %v", err)
		return
	}
	_, err = dyn.Resource(APIDefGVR).Patch(ctx, name, "application/merge-patch+json", raw, metav1.PatchOptions{FieldManager: "aggexp-v2-multiplex"}, "status")
	if err != nil && !apierrors.IsNotFound(err) {
		klog.Warningf("writeStatus: patch %s: %v", name, err)
	}
	_ = mergePatchJSON // keep exported constant used in case callers override
}

func classifyReason(err error) string {
	s := err.Error()
	switch {
	case strings.Contains(s, "GetSchema"), strings.Contains(s, "dial"), strings.Contains(s, "connect"):
		return "BackendUnreachable"
	case strings.Contains(s, "synthesis"), strings.Contains(s, "parse OpenAPI"), strings.Contains(s, "schemaSource"):
		return "SchemaInvalid"
	case strings.Contains(s, "admission compile"):
		return "AdmissionInvalid"
	default:
		return "ReconcileError"
	}
}

func buildAPIServiceUnstructured(name string, gv schema.GroupVersion, svcName, svcNs string, caBundle []byte) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "apiregistration.k8s.io", Version: "v1", Kind: "APIService",
	})
	u.SetName(name)
	u.SetLabels(map[string]string{"app.kubernetes.io/managed-by": "aggexp-v2-multiplex"})
	u.Object["spec"] = map[string]interface{}{
		"group":                gv.Group,
		"version":              gv.Version,
		"groupPriorityMinimum": int64(1000),
		"versionPriority":      int64(15),
		"caBundle":             caBundle,
		"service": map[string]interface{}{
			"name": svcName, "namespace": svcNs, "port": int64(443),
		},
	}
	return u
}

// --- APIDefinition typed representation ---

// APIDefinition is the v2 APIDefinition resource, registered as a
// typed runtime.Object on the host-cluster scheme so reconcile uses
// typed access rather than stringly-typed unstructured dives.
//
// The concrete schema is defined by the CRD at the v2/multiplex
// package's apidefinition-crd.yaml (embedded below for operator
// convenience).
type APIDefinition struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   APIDefinitionSpec   `json:"spec,omitempty"`
	Status APIDefinitionStatus `json:"status,omitempty"`
}

// APIDefinitionList is the list form.
type APIDefinitionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []APIDefinition `json:"items"`
}

// APIDefinitionSpec captures everything the thesis committed to
// declaring per-AA in config.
type APIDefinitionSpec struct {
	Group    string `json:"group"`
	Version  string `json:"version"`
	Kind     string `json:"kind"`
	Plural   string `json:"plural"`
	Singular string `json:"singular"`
	Scope    string `json:"scope"` // "Namespaced" or "Cluster"

	SchemaSource string               `json:"schemaSource,omitempty"`
	Schema       runtime.RawExtension `json:"schema,omitempty"`

	Backend         BackendSpec      `json:"backend"`
	WatchCapability string           `json:"watchCapability,omitempty"`
	Admission       admission.Config `json:"admission,omitempty"`
}

// BackendSpec is how the middleware reaches the backend.
type BackendSpec struct {
	Transport string `json:"transport"` // "http" or "grpc"; default "http"
	Address   string `json:"address"`   // URL for http, hostport for grpc
}

// APIDefinitionStatus is what the reconciler writes back.
type APIDefinitionStatus struct {
	ObservedGeneration   int64                `json:"observedGeneration,omitempty"`
	RegisteredAPIService string               `json:"registeredAPIService,omitempty"`
	Conditions           []metav1.Condition   `json:"conditions,omitempty"`
}

// DeepCopyObject satisfies runtime.Object.
func (a *APIDefinition) DeepCopyObject() runtime.Object {
	out := &APIDefinition{}
	out.TypeMeta = a.TypeMeta
	a.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = a.Spec.deepCopy()
	out.Status = a.Status.deepCopy()
	return out
}

// DeepCopyObject satisfies runtime.Object.
func (a *APIDefinitionList) DeepCopyObject() runtime.Object {
	out := &APIDefinitionList{}
	out.TypeMeta = a.TypeMeta
	a.ListMeta.DeepCopyInto(&out.ListMeta)
	out.Items = make([]APIDefinition, len(a.Items))
	for i := range a.Items {
		out.Items[i] = *(a.Items[i].DeepCopyObject().(*APIDefinition))
	}
	return out
}

func (s APIDefinitionSpec) deepCopy() APIDefinitionSpec {
	cp := s
	if len(s.Schema.Raw) > 0 {
		cp.Schema.Raw = append([]byte(nil), s.Schema.Raw...)
	}
	cp.Admission = s.Admission // shallow; Config is a value type
	return cp
}

func (s APIDefinitionStatus) deepCopy() APIDefinitionStatus {
	cp := s
	if len(s.Conditions) > 0 {
		cp.Conditions = append([]metav1.Condition(nil), s.Conditions...)
	}
	return cp
}

// parseAPIDef converts an unstructured informer object to the
// typed APIDefinition.
func parseAPIDef(u *unstructured.Unstructured) (APIDefinition, error) {
	raw, err := u.MarshalJSON()
	if err != nil {
		return APIDefinition{}, err
	}
	var out APIDefinition
	if err := json.Unmarshal(raw, &out); err != nil {
		return APIDefinition{}, fmt.Errorf("decode APIDefinition: %w", err)
	}
	// Validate required fields early.
	for _, req := range []struct{ k, v string }{
		{"group", out.Spec.Group}, {"version", out.Spec.Version},
		{"kind", out.Spec.Kind}, {"plural", out.Spec.Plural}, {"singular", out.Spec.Singular},
		{"backend.address", out.Spec.Backend.Address},
	} {
		if req.v == "" {
			return APIDefinition{}, fmt.Errorf("spec.%s is required", req.k)
		}
	}
	return out, nil
}
