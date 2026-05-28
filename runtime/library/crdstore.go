package library

import (
	"context"
	"fmt"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// CRDStoreEventSink receives events from the CRD informer and fans
// them out to watch clients. The CRD's resourceVersion is preserved
// as the RV authority (not a per-replica counter).
type CRDStoreEventSink interface {
	// Action publishes a watch event. The object's RV is the host
	// CRD's etcd-assigned RV.
	Action(et watch.EventType, obj runtime.Object)
	// CurrentResourceVersion returns the last observed host CRD RV.
	CurrentResourceVersion() string
	// SetCurrentResourceVersion records the highest observed RV.
	SetCurrentResourceVersion(rv string)
}

// CRDStoreConverter translates between unstructured CRD objects and
// the consumer's typed objects. Consumers must implement both directions.
type CRDStoreConverter interface {
	// ToTyped converts an unstructured CRD object to the consumer's type.
	ToTyped(u *unstructured.Unstructured) runtime.Object
	// ToUnstructured converts a typed object to unstructured for CRD write.
	ToUnstructured(obj runtime.Object, namespace string) *unstructured.Unstructured
}

// CRDStoreOptions configures a CRDStore.
type CRDStoreOptions struct {
	// Dynamic is a dynamic client against the host kube-apiserver.
	Dynamic dynamic.Interface
	// GVR is the GroupVersionResource of the backing CRD.
	GVR schema.GroupVersionResource
	// Namespace for the informer and writes. Required for namespace-scoped CRDs.
	Namespace string
	// Converter translates between unstructured and typed objects.
	Converter CRDStoreConverter
	// ReplicaID identifies this replica in logs.
	ReplicaID string
	// ResyncPeriod for the shared informer. 0 means no resync.
	ResyncPeriod time.Duration
	// GroupResource for error messages.
	GroupResource schema.GroupResource
	// Backend identity methods (New, NewList, Kind, etc.) — delegate to the
	// consumer's types. This is the Backend that provides type identity.
	TypeBackend runtimestorage.Backend
}

// CRDStore implements runtimestorage.WritableBackend by forwarding all
// operations to a CRD on the host kube-apiserver. It uses a shared
// dynamic informer for Get/List and re-broadcasts informer events
// through an EventSink for cross-replica watch consistency.
//
// The host CRD's resourceVersion is the unified RV authority.
type CRDStore struct {
	client    dynamic.ResourceInterface
	namespace string
	replicaID string
	converter CRDStoreConverter
	gr        schema.GroupResource
	tb        runtimestorage.Backend

	factory dynamicinformer.DynamicSharedInformerFactory
	lister  cache.GenericLister
	gvr     schema.GroupVersionResource

	mu   sync.RWMutex
	sink CRDStoreEventSink
}

// NewCRDStore constructs a CRDStore.
func NewCRDStore(opts CRDStoreOptions) *CRDStore {
	if opts.Dynamic == nil {
		panic("CRDStore: Dynamic client is required")
	}
	if opts.Converter == nil {
		panic("CRDStore: Converter is required")
	}
	if opts.TypeBackend == nil {
		panic("CRDStore: TypeBackend is required")
	}
	s := &CRDStore{
		namespace: opts.Namespace,
		replicaID: opts.ReplicaID,
		converter: opts.Converter,
		gr:        opts.GroupResource,
		tb:        opts.TypeBackend,
		gvr:       opts.GVR,
	}
	if opts.Namespace != "" {
		s.client = opts.Dynamic.Resource(opts.GVR).Namespace(opts.Namespace)
	} else {
		s.client = opts.Dynamic.Resource(opts.GVR)
	}
	s.factory = dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		opts.Dynamic, opts.ResyncPeriod, opts.Namespace, nil,
	)
	return s
}

// SetSink wires the event sink. Must be called before Start.
func (s *CRDStore) SetSink(sink CRDStoreEventSink) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sink = sink
}

// Start spins up the shared informer. Blocks until initial sync completes.
func (s *CRDStore) Start(ctx context.Context) error {
	informer := s.factory.ForResource(s.gvr)
	s.lister = informer.Lister()

	_, err := informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { s.handleEvent(watch.Added, obj) },
		UpdateFunc: func(_, obj interface{}) { s.handleEvent(watch.Modified, obj) },
		DeleteFunc: func(obj interface{}) { s.handleDelete(obj) },
	})
	if err != nil {
		return fmt.Errorf("add event handler: %w", err)
	}

	s.factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.Informer().HasSynced) {
		return fmt.Errorf("informer cache sync failed")
	}
	klog.InfoS("crdstore-informer-synced", "replica", s.replicaID, "gvr", s.gvr.String())
	return nil
}

func (s *CRDStore) handleEvent(et watch.EventType, obj interface{}) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	typed := s.converter.ToTyped(u)
	rv := u.GetResourceVersion()

	s.mu.RLock()
	sink := s.sink
	s.mu.RUnlock()
	if sink == nil {
		return
	}
	sink.SetCurrentResourceVersion(rv)
	sink.Action(et, typed)
}

func (s *CRDStore) handleDelete(obj interface{}) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	s.handleEvent(watch.Deleted, obj)
}

// --- runtimestorage.Backend ---

func (s *CRDStore) New() runtime.Object                                                  { return s.tb.New() }
func (s *CRDStore) NewList() runtime.Object                                              { return s.tb.NewList() }
func (s *CRDStore) Kind() string                                                         { return s.tb.Kind() }
func (s *CRDStore) SingularName() string                                                 { return s.tb.SingularName() }
func (s *CRDStore) NamespaceScoped() bool                                                { return s.tb.NamespaceScoped() }
func (s *CRDStore) TableColumns() []metav1.TableColumnDefinition                         { return s.tb.TableColumns() }
func (s *CRDStore) RowsFor(obj runtime.Object) ([]metav1.TableRow, error)                { return s.tb.RowsFor(obj) }

func (s *CRDStore) Get(ctx context.Context, _ user.Info, name string) (runtime.Object, error) {
	var obj runtime.Object
	var err error
	if s.namespace != "" {
		obj, err = s.lister.ByNamespace(s.namespace).Get(name)
	} else {
		obj, err = s.lister.Get(name)
	}
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, apierrors.NewNotFound(s.gr, name)
		}
		return nil, apierrors.NewInternalError(fmt.Errorf("informer Get %s: %w", name, err))
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, apierrors.NewInternalError(fmt.Errorf("unexpected object %T", obj))
	}
	return s.converter.ToTyped(u), nil
}

func (s *CRDStore) List(_ context.Context, _ user.Info, _ runtimestorage.ListOptions) (runtime.Object, error) {
	var objs []runtime.Object
	var err error
	if s.namespace != "" {
		objs, err = s.lister.ByNamespace(s.namespace).List(everythingSelector())
	} else {
		objs, err = s.lister.List(everythingSelector())
	}
	if err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("informer List: %w", err))
	}
	list := s.tb.NewList()
	items := make([]runtime.Object, 0, len(objs))
	for _, o := range objs {
		u, ok := o.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		items = append(items, s.converter.ToTyped(u))
	}
	if err := setListItems(list, items); err != nil {
		return nil, err
	}
	// Stamp list RV from sink.
	s.mu.RLock()
	sink := s.sink
	s.mu.RUnlock()
	if sink != nil {
		setListRV(list, sink.CurrentResourceVersion())
	}
	return list, nil
}

// --- runtimestorage.WritableBackend ---

func (s *CRDStore) Create(ctx context.Context, _ user.Info, obj runtime.Object) (runtime.Object, error) {
	u := s.converter.ToUnstructured(obj, s.namespace)
	created, err := s.client.Create(ctx, u, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			name := u.GetName()
			return nil, apierrors.NewAlreadyExists(s.gr, name)
		}
		return nil, apierrors.NewInternalError(fmt.Errorf("crdstore Create: %w", err))
	}
	return s.converter.ToTyped(created), nil
}

func (s *CRDStore) Update(ctx context.Context, _ user.Info, name string, obj runtime.Object, forceAllowCreate bool) (runtime.Object, bool, error) {
	u := s.converter.ToUnstructured(obj, s.namespace)
	if u.GetName() == "" {
		u.SetName(name)
	}

	// Get existing to preserve RV for CAS.
	existing, err := s.client.Get(ctx, name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, false, apierrors.NewInternalError(fmt.Errorf("crdstore Get %s: %w", name, err))
	}
	if err != nil { // not found
		if !forceAllowCreate {
			return nil, false, apierrors.NewNotFound(s.gr, name)
		}
		created, cerr := s.client.Create(ctx, u, metav1.CreateOptions{})
		if cerr != nil {
			return nil, false, apierrors.NewInternalError(fmt.Errorf("crdstore Create %s: %w", name, cerr))
		}
		return s.converter.ToTyped(created), true, nil
	}

	u.SetResourceVersion(existing.GetResourceVersion())
	updated, uerr := s.client.Update(ctx, u, metav1.UpdateOptions{})
	if uerr != nil {
		if apierrors.IsConflict(uerr) {
			return nil, false, uerr
		}
		return nil, false, apierrors.NewInternalError(fmt.Errorf("crdstore Update %s: %w", name, uerr))
	}
	return s.converter.ToTyped(updated), false, nil
}

func (s *CRDStore) Delete(ctx context.Context, _ user.Info, name string) (runtime.Object, bool, error) {
	existing, err := s.client.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, apierrors.NewNotFound(s.gr, name)
		}
		return nil, false, apierrors.NewInternalError(fmt.Errorf("crdstore Get %s: %w", name, err))
	}
	if derr := s.client.Delete(ctx, name, metav1.DeleteOptions{}); derr != nil {
		if apierrors.IsNotFound(derr) {
			return nil, false, apierrors.NewNotFound(s.gr, name)
		}
		return nil, false, apierrors.NewInternalError(fmt.Errorf("crdstore Delete %s: %w", name, derr))
	}
	return s.converter.ToTyped(existing), true, nil
}

// Compile-time assertions.
var (
	_ runtimestorage.Backend         = (*CRDStore)(nil)
	_ runtimestorage.WritableBackend = (*CRDStore)(nil)
)
