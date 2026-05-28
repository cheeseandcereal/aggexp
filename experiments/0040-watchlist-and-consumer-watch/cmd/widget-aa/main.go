// Command widget-aa is experiment 0040: library-mode AA demonstrating
// WatchList BOOKMARK emission and push vs poll consumer watch modes.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apimachinery/pkg/conversion"
	kubetypes "k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/component-base/cli"
	"k8s.io/klog/v2"

	"github.com/cheeseandcereal/aggexp/experiments/0040-watchlist-and-consumer-watch/pkg/backend"
	genopenapi "github.com/cheeseandcereal/aggexp/experiments/0040-watchlist-and-consumer-watch/pkg/openapi"
	"github.com/cheeseandcereal/aggexp/experiments/0040-watchlist-and-consumer-watch/pkg/pollwatch"
	"github.com/cheeseandcereal/aggexp/experiments/0040-watchlist-and-consumer-watch/pkg/types"
	"github.com/cheeseandcereal/aggexp/runtime/group"
	runtimeserver "github.com/cheeseandcereal/aggexp/runtime/server"
	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

var (
	scheme         = runtime.NewScheme()
	codecs         = serializer.NewCodecFactory(scheme)
	parameterCodec = runtime.NewParameterCodec(scheme)
)

func init() {
	// ---- Widget (push mode) ----
	widgetGV := schema.GroupVersion{Group: types.WidgetGroupName, Version: "v1"}
	widgetInternalGV := schema.GroupVersion{Group: types.WidgetGroupName, Version: runtime.APIVersionInternal}
	scheme.AddKnownTypes(widgetGV, &types.Widget{}, &types.WidgetList{})
	metav1.AddToGroupVersion(scheme, widgetGV)
	scheme.AddKnownTypes(widgetInternalGV, &types.Widget{}, &types.WidgetList{})
	utilruntime.Must(scheme.AddConversionFunc((*types.Widget)(nil), (*types.Widget)(nil),
		func(a, b interface{}, _ conversion.Scope) error {
			*b.(*types.Widget) = *a.(*types.Widget)
			return nil
		}))
	utilruntime.Must(scheme.AddConversionFunc((*types.WidgetList)(nil), (*types.WidgetList)(nil),
		func(a, b interface{}, _ conversion.Scope) error {
			*b.(*types.WidgetList) = *a.(*types.WidgetList)
			return nil
		}))

	// ---- Gadget (poll mode) ----
	gadgetGV := schema.GroupVersion{Group: types.GadgetGroupName, Version: "v1"}
	gadgetInternalGV := schema.GroupVersion{Group: types.GadgetGroupName, Version: runtime.APIVersionInternal}
	scheme.AddKnownTypes(gadgetGV, &types.Gadget{}, &types.GadgetList{})
	metav1.AddToGroupVersion(scheme, gadgetGV)
	scheme.AddKnownTypes(gadgetInternalGV, &types.Gadget{}, &types.GadgetList{})
	utilruntime.Must(scheme.AddConversionFunc((*types.Gadget)(nil), (*types.Gadget)(nil),
		func(a, b interface{}, _ conversion.Scope) error {
			*b.(*types.Gadget) = *a.(*types.Gadget)
			return nil
		}))
	utilruntime.Must(scheme.AddConversionFunc((*types.GadgetList)(nil), (*types.GadgetList)(nil),
		func(a, b interface{}, _ conversion.Scope) error {
			*b.(*types.GadgetList) = *a.(*types.GadgetList)
			return nil
		}))

	// Unversioned types for the machinery
	metav1.AddToGroupVersion(scheme, schema.GroupVersion{Version: "v1"})
	unversioned := schema.GroupVersion{Group: "", Version: "v1"}
	utilruntime.Must(scheme.SetVersionPriority(unversioned))
	scheme.AddUnversionedTypes(unversioned,
		&metav1.Status{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	)
}

// ---- BookmarkWatch: wraps the upstream Watch to inject initial-events-end BOOKMARK ----

// BookmarkWatchREST wraps a *runtimestorage.REST and overrides Watch to
// emit the k8s.io/initial-events-end BOOKMARK after the initial prefix,
// respecting allowWatchBookmarks.
type BookmarkWatchREST struct {
	*runtimestorage.REST
	gvk     schema.GroupVersionKind
	newFunc func() runtime.Object
}

func (bw *BookmarkWatchREST) Watch(ctx context.Context, opts *metainternalversion.ListOptions) (watch.Interface, error) {
	// Determine if we should emit bookmark
	allowBookmarks := false
	if opts != nil {
		allowBookmarks = opts.AllowWatchBookmarks
	}

	// Delegate to the underlying REST's Watch — this calls WatchWithPrefix
	// which synchronously places all prefix ADDED events into the channel
	// before subscribing to live events.
	w, err := bw.REST.Watch(ctx, opts)
	if err != nil {
		return nil, err
	}

	if !allowBookmarks {
		return w, nil
	}

	// Wrap: drain all prefix events already in the channel, inject BOOKMARK,
	// then forward live events.
	return newBookmarkInjector(w, bw.gvk, bw.REST.CurrentResourceVersion(), bw.newFunc), nil
}

// bookmarkInjector wraps a watch.Interface and injects a BOOKMARK event
// after all prefix (ADDED) events have been delivered but before the
// first live event.
type bookmarkInjector struct {
	upstream watch.Interface
	ch       chan watch.Event
	gvk      schema.GroupVersionKind
	rv       string
	newFunc  func() runtime.Object
	stopCh   chan struct{}
}

func newBookmarkInjector(upstream watch.Interface, gvk schema.GroupVersionKind, rv string, newFunc func() runtime.Object) *bookmarkInjector {
	bi := &bookmarkInjector{
		upstream: upstream,
		ch:       make(chan watch.Event, 100),
		gvk:      gvk,
		rv:       rv,
		newFunc:  newFunc,
		stopCh:   make(chan struct{}),
	}
	go bi.run()
	return bi
}

func (bi *bookmarkInjector) run() {
	defer close(bi.ch)
	upCh := bi.upstream.ResultChan()

	// Phase 1: Drain prefix events. WatchWithPrefix places them all into
	// the channel synchronously before subscribing to live events.
	// We drain everything immediately available, then inject the bookmark.
	// A small sleep ensures the prefix is fully queued before we check.
	time.Sleep(5 * time.Millisecond)
	for {
		select {
		case ev, ok := <-upCh:
			if !ok {
				bi.injectBookmark()
				return
			}
			select {
			case bi.ch <- ev:
			case <-bi.stopCh:
				return
			}
		default:
			// No more buffered events — prefix is done
			goto prefixDone
		}
	}

prefixDone:
	bi.injectBookmark()

	// Phase 2: Forward all subsequent (live) events
	for {
		select {
		case <-bi.stopCh:
			return
		case ev, ok := <-upCh:
			if !ok {
				return
			}
			select {
			case bi.ch <- ev:
			case <-bi.stopCh:
				return
			}
		}
	}
}

func (bi *bookmarkInjector) injectBookmark() {
	// Use the resource's own type for the bookmark object so the watch
	// stream serializer (which is GVK-specific) can encode it.
	obj := bi.newFunc()
	acc, err := meta.Accessor(obj)
	if err != nil {
		return
	}
	acc.SetResourceVersion(bi.rv)
	acc.SetAnnotations(map[string]string{
		"k8s.io/initial-events-end": "true",
	})
	ev := watch.Event{Type: watch.Bookmark, Object: obj}
	select {
	case bi.ch <- ev:
	case <-bi.stopCh:
	}
}

func (bi *bookmarkInjector) Stop() {
	close(bi.stopCh)
	bi.upstream.Stop()
}

func (bi *bookmarkInjector) ResultChan() <-chan watch.Event {
	return bi.ch
}

// ---- main ----

type options struct {
	*runtimeserver.Options
	pollInterval time.Duration
}

func newOptions() *options {
	return &options{
		Options:      runtimeserver.NewOptions(),
		pollInterval: 5 * time.Second,
	}
}

func (o *options) addFlags(fs *pflag.FlagSet) {
	o.Options.AddFlags(fs)
	o.Options.PolicyGroup = types.WidgetGroupName
	o.Options.Title = "aggexp-0040-widget-aa"
	fs.DurationVar(&o.pollInterval, "poll-interval", o.pollInterval, "Poll interval for gadget poll-mode watch")
}

func (o *options) run(ctx context.Context) error {
	// ---- Widget: push mode (writable backend, events via Publisher) ----
	widgetBackend := backend.NewWidgetBackend()
	widgetGR := schema.GroupResource{Group: types.WidgetGroupName, Resource: "widgets"}
	widgetStore := runtimestorage.New(runtimestorage.Options{
		Backend:       widgetBackend,
		GroupResource: widgetGR,
	})
	widgetWatchStore := &BookmarkWatchREST{
		REST:    widgetStore,
		gvk:     schema.GroupVersionKind{Group: types.WidgetGroupName, Version: "v1", Kind: "Widget"},
		newFunc: func() runtime.Object { return &types.Widget{} },
	}

	// ---- Gadget: poll mode (read-only list, poll wrapper emits events) ----
	gadgetSource := backend.NewGadgetSource()
	gadgetBackend := backend.NewGadgetBackend(gadgetSource)
	gadgetGR := schema.GroupResource{Group: types.GadgetGroupName, Resource: "gadgets"}
	gadgetStore := runtimestorage.New(runtimestorage.Options{
		Backend:       gadgetBackend,
		GroupResource: gadgetGR,
	})
	gadgetWatchStore := &BookmarkWatchREST{
		REST:    gadgetStore,
		gvk:     schema.GroupVersionKind{Group: types.GadgetGroupName, Version: "v1", Kind: "Gadget"},
		newFunc: func() runtime.Object { return &types.Gadget{} },
	}

	// Poll wrapper: wraps gadgetSource.ListAll into watch events
	lister := pollwatch.ListerFunc(func(_ context.Context) ([]runtime.Object, error) {
		items := gadgetSource.ListAll()
		out := make([]runtime.Object, len(items))
		for i := range items {
			c := items[i]
			out[i] = &c
		}
		return out, nil
	})
	poller := pollwatch.New(lister, gadgetStore, o.pollInterval)

	// Seed some gadgets for demonstration
	seedGadgets(gadgetSource)

	widgetGroup := &group.Group{
		GroupVersion:   schema.GroupVersion{Group: types.WidgetGroupName, Version: "v1"},
		Scheme:         scheme,
		Codecs:         codecs,
		ParameterCodec: parameterCodec,
		Resources:      map[string]rest.Storage{"widgets": widgetWatchStore},
	}

	gadgetGroup := &group.Group{
		GroupVersion:   schema.GroupVersion{Group: types.GadgetGroupName, Version: "v1"},
		Scheme:         scheme,
		Codecs:         codecs,
		ParameterCodec: parameterCodec,
		Resources:      map[string]rest.Storage{"gadgets": gadgetWatchStore},
	}

	return o.Options.Run(
		ctx,
		"aggexp-0040-widget-aa",
		runtimeserver.Input{
			Scheme:             scheme,
			Codecs:             codecs,
			OpenAPIDefinitions: genopenapi.GetOpenAPIDefinitions,
		},
		[]runtimeserver.GroupInstaller{widgetGroup, gadgetGroup},
		map[string]runtimeserver.PostStartFunc{
			"poll-gadgets": func(hookCtx context.Context) error {
				go poller.Run(hookCtx)
				klog.Infof("gadget poll-watcher started (interval=%v)", o.pollInterval)
				return nil
			},
			"gadget-mutator": func(hookCtx context.Context) error {
				// Background goroutine that mutates gadgets to demonstrate poll diffs
				go gadgetMutator(hookCtx, gadgetSource)
				return nil
			},
			"shutdown": func(hookCtx context.Context) error {
				go func() {
					<-hookCtx.Done()
					widgetStore.Shutdown()
					gadgetStore.Shutdown()
				}()
				return nil
			},
		},
	)
}

// seedGadgets puts some initial gadgets in the source.
func seedGadgets(source *backend.GadgetSource) {
	source.Put(&types.Gadget{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "gizmo-alpha",
			Namespace:         "default",
			UID:               kubetypes.UID(uuid.New().String()),
			CreationTimestamp: metav1.Time{Time: time.Now()},
		},
		Spec: types.GadgetSpec{Model: "X100", Firmware: "1.0.0"},
	})
	source.Put(&types.Gadget{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "gizmo-beta",
			Namespace:         "default",
			UID:               kubetypes.UID(uuid.New().String()),
			CreationTimestamp: metav1.Time{Time: time.Now()},
		},
		Spec: types.GadgetSpec{Model: "Y200", Firmware: "2.1.0"},
	})
}

// gadgetMutator simulates external changes to gadgets every 15s.
func gadgetMutator(ctx context.Context, source *backend.GadgetSource) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	cycle := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cycle++
			switch cycle % 3 {
			case 1:
				// Update firmware on gizmo-alpha
				source.Put(&types.Gadget{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "gizmo-alpha",
						Namespace:         "default",
						UID:               kubetypes.UID(uuid.New().String()),
						CreationTimestamp: metav1.Time{Time: time.Now()},
					},
					Spec: types.GadgetSpec{Model: "X100", Firmware: "1." + strconv.Itoa(cycle) + ".0"},
				})
				klog.Infof("gadget-mutator: updated gizmo-alpha firmware to 1.%d.0", cycle)
			case 2:
				// Add a new gadget
				name := "gizmo-gamma-" + strconv.Itoa(cycle)
				source.Put(&types.Gadget{
					ObjectMeta: metav1.ObjectMeta{
						Name:              name,
						Namespace:         "default",
						UID:               kubetypes.UID(uuid.New().String()),
						CreationTimestamp: metav1.Time{Time: time.Now()},
					},
					Spec: types.GadgetSpec{Model: "Z300", Firmware: "0.1.0"},
				})
				klog.Infof("gadget-mutator: added %s", name)
			case 0:
				// Remove the previously added gadget
				name := "gizmo-gamma-" + strconv.Itoa(cycle-1)
				source.Remove(name)
				klog.Infof("gadget-mutator: removed %s", name)
			}
		}
	}
}

func main() {
	opts := newOptions()
	cmd := &cobra.Command{
		Use:   "widget-aa",
		Short: "0040: WatchList BOOKMARK + push/poll consumer watch",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := opts.Options.Validate(); err != nil {
				return err
			}
			ctx := genericapiserver.SetupSignalContext()
			return opts.run(ctx)
		},
	}
	opts.addFlags(cmd.Flags())
	if code := cli.Run(cmd); code != 0 {
		fmt.Fprintln(os.Stderr, "widget-aa exited with error")
		os.Exit(code)
	}
}
