// Package asyncbackend implements a runtime/storage.Backend +
// WritableBackend that talks to an async HTTP mock whose lifecycle
// takes real time (30s provision, 10s deprovision). The key design
// choice this experiment probes:
//
//  1. Create / Update return IMMEDIATELY with phase=Provisioning.
//     They do NOT block waiting for the backend to finish. This
//     keeps the HTTP request model honest; minute-scale provisions
//     would hang kubectl otherwise.
//
//  2. A polling loop at PollInterval re-reads the backend and
//     diffs against previous state; transitions (Provisioning ->
//     Ready, Ready -> Deleting, Deleting -> gone) emit watch events
//     through runtime/storage.Publisher. This is what gives
//     kubectl wait / informers their "live update" behavior without
//     the AA itself holding authoritative state.
//
//  3. Reads (Get / List) are LIVE against the backend. The
//     in-memory cache exists only to drive watch diffs and to
//     preserve UIDs across reads within the AA's lifetime. It is
//     not consulted on GET.
//
// FINDINGS/0009-ack-aggregated-s3.md flagged "async backends force
// state back into the picture." This backend is the deliberate test
// of that thesis. See FINDINGS/0011-async-backend-sim.md for what
// we found.
package asyncbackend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/klog/v2"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0011-async-backend-sim/pkg/apis/aggexp"
	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// Options configures a Backend.
type Options struct {
	// MockURL is the base URL of the async mock, e.g.
	// http://async-mock.aggexp-system.svc. Required.
	MockURL string
	// HTTPClient is used for calls to the mock. Defaults to a
	// client with a 10s timeout.
	HTTPClient *http.Client
	// PollInterval is how often the poll loop re-reads the mock
	// to drive watch events. Defaults to 5s if zero.
	PollInterval time.Duration
}

// Backend implements runtime/storage.Backend +
// runtime/storage.WritableBackend against the async mock.
type Backend struct {
	base     *url.URL
	client   *http.Client
	interval time.Duration

	mu        sync.RWMutex
	uids      map[string]types.UID
	created   map[string]time.Time      // first-observed creation timestamp
	seen      map[string]*aggexp.Widget // last snapshot per widget; diff source
	publisher runtimestorage.Publisher
}

// mockWidget mirrors async-mock/main.go's Widget type on the wire.
// Duplicated here to keep the backend's imports clean; the shape is
// small enough that a shared types package is not worth it.
type mockWidget struct {
	Name          string            `json:"name"`
	DesiredState  string            `json:"desiredState"`
	Config        map[string]string `json:"config,omitempty"`
	Phase         string            `json:"phase"`
	ObservedState string            `json:"observedState,omitempty"`
	ReadyAt       *time.Time        `json:"readyAt,omitempty"`
	Message       string            `json:"message,omitempty"`
	CreatedAt     time.Time         `json:"createdAt"`
	UpdatedAt     time.Time         `json:"updatedAt"`
}

type mockList struct {
	Items []mockWidget `json:"items"`
}

// New constructs a Backend. SetPublisher must be called before Start.
func New(opts Options) (*Backend, error) {
	if opts.MockURL == "" {
		return nil, fmt.Errorf("asyncbackend: MockURL required")
	}
	u, err := url.Parse(opts.MockURL)
	if err != nil {
		return nil, fmt.Errorf("asyncbackend: parse MockURL: %w", err)
	}
	c := opts.HTTPClient
	if c == nil {
		c = &http.Client{Timeout: 10 * time.Second}
	}
	iv := opts.PollInterval
	if iv <= 0 {
		iv = 5 * time.Second
	}
	return &Backend{
		base:     u,
		client:   c,
		interval: iv,
		uids:     map[string]types.UID{},
		created:  map[string]time.Time{},
		seen:     map[string]*aggexp.Widget{},
	}, nil
}

// SetPublisher wires the adapter's publisher for watch events.
func (b *Backend) SetPublisher(p runtimestorage.Publisher) { b.publisher = p }

// Start launches the poll loop.
func (b *Backend) Start(ctx context.Context) {
	go func() {
		b.pollOnce(ctx)
		t := time.NewTicker(b.interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				b.pollOnce(ctx)
			}
		}
	}()
}

// ---- runtime/storage.Backend identity ----

func (b *Backend) New() runtime.Object     { return &aggexp.Widget{} }
func (b *Backend) NewList() runtime.Object { return &aggexp.WidgetList{} }
func (b *Backend) Kind() string            { return "Widget" }
func (b *Backend) SingularName() string    { return "widget" }
func (b *Backend) NamespaceScoped() bool   { return false }

// ---- Get / List: LIVE reads ----

func (b *Backend) Get(ctx context.Context, u user.Info, name string) (runtime.Object, error) {
	logUser("get", u, "name", name)
	mw, err := b.doGetWidget(ctx, name)
	if err != nil {
		return nil, err
	}
	obj := b.toWidget(mw)
	return obj, nil
}

func (b *Backend) List(ctx context.Context, u user.Info, _ runtimestorage.ListOptions) (runtime.Object, error) {
	logUser("list", u)
	ml, err := b.doListWidgets(ctx)
	if err != nil {
		return nil, err
	}
	list := &aggexp.WidgetList{Items: make([]aggexp.Widget, 0, len(ml.Items))}
	for i := range ml.Items {
		list.Items = append(list.Items, *b.toWidget(&ml.Items[i]))
	}
	sort.Slice(list.Items, func(i, j int) bool {
		return list.Items[i].Name < list.Items[j].Name
	})
	return list, nil
}

// ---- Create: POST; return IMMEDIATELY with the Provisioning object ----

func (b *Backend) Create(ctx context.Context, u user.Info, obj runtime.Object) (runtime.Object, error) {
	wid, ok := obj.(*aggexp.Widget)
	if !ok {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("expected Widget, got %T", obj))
	}
	if wid.Name == "" {
		return nil, apierrors.NewBadRequest("metadata.name is required")
	}
	logUser("create", u, "name", wid.Name)

	body := map[string]any{
		"name":         wid.Name,
		"desiredState": wid.Spec.DesiredState,
		"config":       wid.Spec.Config,
	}
	mw, status, err := b.doPost(ctx, "/widgets", body)
	if err != nil {
		return nil, err
	}
	_ = status // 202 expected

	result := b.toWidget(mw)
	preserveManagedMeta(result, wid)

	// Eager watch event so clients that just wrote see their own
	// Provisioning object without waiting a poll cycle.
	if b.publisher != nil {
		b.publisher.PublishAdded(result.DeepCopy())
	}
	return result, nil
}

// ---- Update: PUT; return IMMEDIATELY ----

func (b *Backend) Update(ctx context.Context, u user.Info, name string, obj runtime.Object, forceAllowCreate bool) (runtime.Object, bool, error) {
	wid, ok := obj.(*aggexp.Widget)
	if !ok {
		return nil, false, apierrors.NewBadRequest(fmt.Sprintf("expected Widget, got %T", obj))
	}
	if wid.Name == "" {
		wid.Name = name
	}
	if wid.Name != name {
		return nil, false, apierrors.NewBadRequest(fmt.Sprintf("body name %q != path name %q", wid.Name, name))
	}
	logUser("update", u, "name", name)

	// Check existence.
	_, err := b.doGetWidget(ctx, name)
	exists := err == nil
	if err != nil && !isNotFoundAPIErr(err) {
		return nil, false, err
	}

	created := false
	if !exists {
		if !forceAllowCreate {
			return nil, false, apierrors.NewNotFound(aggexp.Resource("widgets"), name)
		}
		// SSA / apply path: delegate to Create.
		obj2, err := b.Create(ctx, u, obj)
		if err != nil {
			return nil, false, err
		}
		return obj2, true, nil
	}

	body := map[string]any{
		"desiredState": wid.Spec.DesiredState,
		"config":       wid.Spec.Config,
	}
	mw, _, err := b.doPut(ctx, "/widgets/"+name, body)
	if err != nil {
		return nil, false, err
	}
	result := b.toWidget(mw)
	preserveManagedMeta(result, wid)

	if b.publisher != nil {
		b.publisher.PublishModified(result.DeepCopy())
	}
	return result, created, nil
}

// ---- Delete: DELETE; return with phase=Deleting ----

func (b *Backend) Delete(ctx context.Context, u user.Info, name string) (runtime.Object, bool, error) {
	logUser("delete", u, "name", name)

	mw, _, err := b.doDelete(ctx, "/widgets/"+name)
	if err != nil {
		return nil, false, err
	}
	result := b.toWidget(mw)

	// The backend is still there (Deleting phase). Emit a Modified
	// event so clients see the phase transition immediately; the
	// final Deleted event comes from the poll loop when the mock
	// actually reaps the record.
	if b.publisher != nil {
		b.publisher.PublishModified(result.DeepCopy())
	}
	return result, true, nil
}

// ---- Table ----

func (b *Backend) TableColumns() []metav1.TableColumnDefinition {
	return []metav1.TableColumnDefinition{
		{Name: "Name", Type: "string", Format: "name"},
		{Name: "Desired", Type: "string"},
		{Name: "Observed", Type: "string"},
		{Name: "Phase", Type: "string"},
		{Name: "Age", Type: "date"},
	}
}

func (b *Backend) RowsFor(obj runtime.Object) ([]metav1.TableRow, error) {
	row := func(w *aggexp.Widget) metav1.TableRow {
		age := "<unknown>"
		if !w.CreationTimestamp.IsZero() {
			age = translateTimestampSince(w.CreationTimestamp)
		}
		return metav1.TableRow{
			Cells: []interface{}{
				w.Name,
				w.Spec.DesiredState,
				w.Status.ObservedState,
				w.Status.Phase,
				age,
			},
			Object: runtime.RawExtension{Object: w},
		}
	}
	switch v := obj.(type) {
	case *aggexp.Widget:
		return []metav1.TableRow{row(v)}, nil
	case *aggexp.WidgetList:
		rs := make([]metav1.TableRow, 0, len(v.Items))
		for i := range v.Items {
			rs = append(rs, row(&v.Items[i]))
		}
		return rs, nil
	}
	return nil, fmt.Errorf("unexpected object %T", obj)
}

// ---- poll loop ----

func (b *Backend) pollOnce(ctx context.Context) {
	t0 := time.Now()
	ml, err := b.doListWidgets(ctx)
	if err != nil {
		klog.V(2).InfoS("async-poll-failed", "err", err)
		return
	}
	next := make(map[string]*aggexp.Widget, len(ml.Items))
	for i := range ml.Items {
		w := b.toWidget(&ml.Items[i])
		next[w.Name] = w.DeepCopy()
	}

	b.mu.Lock()
	prev := b.seen
	var added, modified, deleted []*aggexp.Widget
	for name, cur := range next {
		old, existed := prev[name]
		if !existed {
			added = append(added, cur)
		} else if !widgetEqual(old, cur) {
			modified = append(modified, cur)
		}
	}
	for name, old := range prev {
		if _, still := next[name]; !still {
			deleted = append(deleted, old.DeepCopy())
			delete(b.uids, name)
			delete(b.created, name)
		}
	}
	b.seen = next
	b.mu.Unlock()

	if b.publisher != nil {
		for _, o := range added {
			b.publisher.PublishAdded(o.DeepCopy())
		}
		for _, o := range modified {
			b.publisher.PublishModified(o.DeepCopy())
		}
		for _, o := range deleted {
			b.publisher.PublishDeleted(o.DeepCopy())
		}
	}

	klog.V(2).InfoS("async-poll",
		"count", len(next), "added", len(added), "modified", len(modified),
		"deleted", len(deleted), "took", time.Since(t0))
}

// ---- HTTP plumbing to the mock ----

func (b *Backend) doGetWidget(ctx context.Context, name string) (*mockWidget, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, b.resolve("/widgets/"+name), nil)
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("mock GET %s: %w", name, err))
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, apierrors.NewNotFound(aggexp.Resource("widgets"), name)
	}
	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(resp.Body)
		return nil, apierrors.NewInternalError(fmt.Errorf("mock GET %s: status %d: %s", name, resp.StatusCode, msg))
	}
	var mw mockWidget
	if err := json.NewDecoder(resp.Body).Decode(&mw); err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("mock GET %s: decode: %w", name, err))
	}
	return &mw, nil
}

func (b *Backend) doListWidgets(ctx context.Context) (*mockList, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, b.resolve("/widgets"), nil)
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("mock LIST: %w", err))
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(resp.Body)
		return nil, apierrors.NewInternalError(fmt.Errorf("mock LIST: status %d: %s", resp.StatusCode, msg))
	}
	var ml mockList
	if err := json.NewDecoder(resp.Body).Decode(&ml); err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("mock LIST: decode: %w", err))
	}
	return &ml, nil
}

func (b *Backend) doPost(ctx context.Context, path string, body any) (*mockWidget, int, error) {
	return b.doBody(ctx, http.MethodPost, path, body)
}

func (b *Backend) doPut(ctx context.Context, path string, body any) (*mockWidget, int, error) {
	return b.doBody(ctx, http.MethodPut, path, body)
}

func (b *Backend) doBody(ctx context.Context, method, path string, body any) (*mockWidget, int, error) {
	buf := &bytes.Buffer{}
	if body != nil {
		if err := json.NewEncoder(buf).Encode(body); err != nil {
			return nil, 0, apierrors.NewInternalError(err)
		}
	}
	req, _ := http.NewRequestWithContext(ctx, method, b.resolve(path), buf)
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, 0, apierrors.NewInternalError(fmt.Errorf("mock %s %s: %w", method, path, err))
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, resp.StatusCode, apierrors.NewNotFound(aggexp.Resource("widgets"), trimWidgetsPath(path))
	}
	if resp.StatusCode == http.StatusConflict {
		return nil, resp.StatusCode, apierrors.NewAlreadyExists(aggexp.Resource("widgets"), trimWidgetsPath(path))
	}
	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(resp.Body)
		return nil, resp.StatusCode, apierrors.NewInternalError(fmt.Errorf("mock %s %s: status %d: %s", method, path, resp.StatusCode, msg))
	}
	var mw mockWidget
	if err := json.NewDecoder(resp.Body).Decode(&mw); err != nil {
		return nil, resp.StatusCode, apierrors.NewInternalError(fmt.Errorf("mock %s %s: decode: %w", method, path, err))
	}
	return &mw, resp.StatusCode, nil
}

func (b *Backend) doDelete(ctx context.Context, path string) (*mockWidget, int, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, b.resolve(path), nil)
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, 0, apierrors.NewInternalError(fmt.Errorf("mock DELETE %s: %w", path, err))
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, resp.StatusCode, apierrors.NewNotFound(aggexp.Resource("widgets"), trimWidgetsPath(path))
	}
	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(resp.Body)
		return nil, resp.StatusCode, apierrors.NewInternalError(fmt.Errorf("mock DELETE %s: status %d: %s", path, resp.StatusCode, msg))
	}
	var mw mockWidget
	if err := json.NewDecoder(resp.Body).Decode(&mw); err != nil {
		return nil, resp.StatusCode, apierrors.NewInternalError(fmt.Errorf("mock DELETE %s: decode: %w", path, err))
	}
	return &mw, resp.StatusCode, nil
}

func (b *Backend) resolve(path string) string {
	u := *b.base
	u.Path = path
	return u.String()
}

func trimWidgetsPath(p string) string {
	if len(p) > len("/widgets/") && p[:len("/widgets/")] == "/widgets/" {
		return p[len("/widgets/"):]
	}
	return p
}

// ---- conversion: mock JSON -> internal Widget ----

func (b *Backend) toWidget(mw *mockWidget) *aggexp.Widget {
	w := &aggexp.Widget{
		Spec: aggexp.WidgetSpec{
			DesiredState: mw.DesiredState,
			Config:       copyStringMap(mw.Config),
		},
		Status: aggexp.WidgetStatus{
			Phase:         mw.Phase,
			ObservedState: mw.ObservedState,
			Message:       mw.Message,
		},
	}
	w.Name = mw.Name
	w.TypeMeta.Kind = "Widget"
	w.TypeMeta.APIVersion = "aggexp.io/v1"
	if mw.ReadyAt != nil {
		t := metav1.NewTime(*mw.ReadyAt)
		w.Status.ReadyAt = &t
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if uid, ok := b.uids[mw.Name]; ok {
		w.UID = uid
	} else {
		uid := types.UID(uuid.New().String())
		b.uids[mw.Name] = uid
		w.UID = uid
	}
	if ct, ok := b.created[mw.Name]; ok {
		w.CreationTimestamp = metav1.NewTime(ct)
	} else {
		// First time we see this widget: use the mock's CreatedAt.
		b.created[mw.Name] = mw.CreatedAt
		w.CreationTimestamp = metav1.NewTime(mw.CreatedAt)
	}
	return w
}

func copyStringMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// preserveManagedMeta mirrors s3backend's helper: library-layer
// metadata (managedFields, labels, annotations) comes from the
// request; backend-authoritative data (phase, timestamps) stays as
// returned by the mock. See 0009 for the stateless-AA caveats on
// field ownership.
func preserveManagedMeta(dst, src *aggexp.Widget) {
	if src == nil {
		return
	}
	if src.UID != "" {
		dst.UID = src.UID
	}
	if !src.CreationTimestamp.IsZero() {
		dst.CreationTimestamp = src.CreationTimestamp
	}
	if src.ResourceVersion != "" {
		dst.ResourceVersion = src.ResourceVersion
	}
	if len(src.ManagedFields) > 0 {
		dst.ManagedFields = append([]metav1.ManagedFieldsEntry(nil), src.ManagedFields...)
	}
	if len(src.Labels) > 0 {
		if dst.Labels == nil {
			dst.Labels = map[string]string{}
		}
		for k, v := range src.Labels {
			dst.Labels[k] = v
		}
	}
	if len(src.Annotations) > 0 {
		if dst.Annotations == nil {
			dst.Annotations = map[string]string{}
		}
		for k, v := range src.Annotations {
			dst.Annotations[k] = v
		}
	}
}

func widgetEqual(a, b *aggexp.Widget) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Status.Phase != b.Status.Phase {
		return false
	}
	if a.Status.ObservedState != b.Status.ObservedState {
		return false
	}
	if a.Spec.DesiredState != b.Spec.DesiredState {
		return false
	}
	if !mapEqual(a.Spec.Config, b.Spec.Config) {
		return false
	}
	aReady := a.Status.ReadyAt
	bReady := b.Status.ReadyAt
	if (aReady == nil) != (bReady == nil) {
		return false
	}
	if aReady != nil && !aReady.Equal(bReady) {
		return false
	}
	return true
}

func mapEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func isNotFoundAPIErr(err error) bool {
	return apierrors.IsNotFound(err)
}

func translateTimestampSince(t metav1.Time) string {
	if t.IsZero() {
		return "<unknown>"
	}
	d := time.Since(t.Time)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

func logUser(verb string, u user.Info, fields ...interface{}) {
	if u == nil {
		klog.V(2).InfoS("async-backend", append([]interface{}{"verb", verb}, fields...)...)
		return
	}
	kv := append([]interface{}{"verb", verb, "user", u.GetName(), "groups", u.GetGroups()}, fields...)
	klog.V(2).InfoS("async-backend", kv...)
}

// Compile-time assertions.
var (
	_ runtimestorage.Backend         = (*Backend)(nil)
	_ runtimestorage.WritableBackend = (*Backend)(nil)
)
