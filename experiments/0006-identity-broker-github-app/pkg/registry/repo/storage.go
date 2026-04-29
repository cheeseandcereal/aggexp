// Package repo provides the rest.Storage implementation for
// aggexp.io/v1 Repo resources under experiment 0006.
//
// Unlike 0004 (a shared poll loop with a process-wide cache), this
// storage fetches per-request, on behalf of the caller, via the
// identity broker. Each Get / List / Watch pulls the caller's
// user.Info off the request context, hands it to the broker for a
// caller-scoped token, and uses that token to call the mock-github
// service. Nothing is cached across callers.
//
// Read-only (no Create/Update/Delete/Patch).
//
// The resource name convention stays `<owner>.<repo-name>`. Since
// we do not maintain a cross-request cache, the resource namespace
// is whatever the broker is willing to hand the caller a token for,
// projected into kubernetes-name shape.
package repo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/klog/v2"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0006-identity-broker-github-app/pkg/apis/aggexp"
	brokerclient "github.com/cheeseandcereal/aggexp/experiments/0006-identity-broker-github-app/pkg/broker"
	ghclient "github.com/cheeseandcereal/aggexp/experiments/0006-identity-broker-github-app/pkg/github"
)

// REST is the read-only rest.Storage for Repo, backed by
// per-request broker-mediated GitHub-ish calls.
type REST struct {
	owner  string
	client *ghclient.Client
	rv     atomic.Uint64
}

// Options configures the REST storage.
type Options struct {
	Owner  string
	Client *ghclient.Client
}

// NewREST constructs the REST storage.
func NewREST(opts Options) *REST {
	r := &REST{
		owner:  opts.Owner,
		client: opts.Client,
	}
	r.rv.Store(1)
	return r
}

// Start is a no-op: there's no poll loop to run. Kept for symmetry
// with 0004 and in case a later iteration needs background work.
func (r *REST) Start(_ context.Context) {}

// ---- identity / shape interfaces ----

func (r *REST) New() runtime.Object     { return &aggexp.Repo{} }
func (r *REST) NewList() runtime.Object { return &aggexp.RepoList{} }
func (r *REST) Destroy()                {}
func (r *REST) NamespaceScoped() bool   { return false }
func (r *REST) Kind() string            { return "Repo" }
func (r *REST) GetSingularName() string { return "repo" }

// ---- rv helpers ----

func (r *REST) currentRV() string {
	return strconv.FormatUint(r.rv.Load(), 10)
}

// ---- Getter ----

func (r *REST) Get(ctx context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	u, ok := genericapirequest.UserFrom(ctx)
	if !ok || u == nil {
		return nil, k8serrors.NewUnauthorized("no user in request context")
	}
	owner, repoName, ok := splitRepoName(name)
	if !ok {
		return nil, k8serrors.NewNotFound(aggexp.Resource("repos"), name)
	}
	// The experiment's AA is configured with a single owner scope;
	// a request for any other owner is 404 rather than forwarded,
	// to keep the projection behavior clean.
	if r.owner != "" && owner != r.owner {
		return nil, k8serrors.NewNotFound(aggexp.Resource("repos"), name)
	}
	klog.V(2).InfoS("repo-get", "user", u.GetName(), "groups", u.GetGroups(), "owner", owner, "name", repoName)

	ghRepo, err := r.client.GetRepo(ctx, u, owner, repoName)
	if err != nil {
		var denied *brokerclient.ErrDenied
		if errors.As(err, &denied) {
			// Quiet denial: pretend the object doesn't exist.
			// Observable signal: the broker logs the deny.
			klog.V(2).InfoS("repo-get-denied", "user", u.GetName(), "name", name, "reason", denied.Reason)
			return nil, k8serrors.NewNotFound(aggexp.Resource("repos"), name)
		}
		if errors.Is(err, ghclient.ErrNotFound) {
			return nil, k8serrors.NewNotFound(aggexp.Resource("repos"), name)
		}
		return nil, k8serrors.NewInternalError(err)
	}
	return r.toAggexp(ghRepo), nil
}

// ---- Lister ----

func (r *REST) List(ctx context.Context, opts *metainternalversion.ListOptions) (runtime.Object, error) {
	u, ok := genericapirequest.UserFrom(ctx)
	if !ok || u == nil {
		return nil, k8serrors.NewUnauthorized("no user in request context")
	}
	sel := labels.Everything()
	if opts != nil && opts.LabelSelector != nil {
		sel = opts.LabelSelector
	}
	klog.V(2).InfoS("repo-list", "user", u.GetName(), "groups", u.GetGroups(), "owner", r.owner)

	t0 := time.Now()
	ghRepos, err := r.client.ListRepos(ctx, u, r.owner, 100, 4)
	list := &aggexp.RepoList{}
	list.ResourceVersion = r.currentRV()

	if err != nil {
		var denied *brokerclient.ErrDenied
		if errors.As(err, &denied) {
			// Quiet denial: return an empty list. The caller sees
			// "you have no repos" rather than a 403; this is the
			// fail-closed shape chosen for this experiment.
			klog.V(1).InfoS("repo-list-denied",
				"user", u.GetName(), "reason", denied.Reason, "took", time.Since(t0))
			return list, nil
		}
		return nil, k8serrors.NewInternalError(err)
	}

	list.Items = make([]aggexp.Repo, 0, len(ghRepos))
	for i := range ghRepos {
		obj := r.toAggexp(&ghRepos[i])
		if sel.Matches(labels.Set(obj.Labels)) {
			list.Items = append(list.Items, *obj)
		}
	}
	klog.V(2).InfoS("repo-list-ok", "user", u.GetName(), "count", len(list.Items), "took", time.Since(t0))
	return list, nil
}

// ---- Watcher ----
//
// Watch is per-caller: when a client opens a watch, we do one fetch
// on their behalf, emit an Added event per returned repo, then
// block (emitting periodic bookmarks) until the client disconnects
// or the context ends. No cross-caller event plumbing — the mock
// backend doesn't push change events and the experiment isn't
// probing live-update semantics, only identity flow.

func (r *REST) Watch(ctx context.Context, opts *metainternalversion.ListOptions) (watch.Interface, error) {
	u, ok := genericapirequest.UserFrom(ctx)
	if !ok || u == nil {
		return nil, k8serrors.NewUnauthorized("no user in request context")
	}
	sel := labels.Everything()
	if opts != nil && opts.LabelSelector != nil {
		sel = opts.LabelSelector
	}
	klog.V(2).InfoS("repo-watch", "user", u.GetName(), "groups", u.GetGroups())

	ghRepos, err := r.client.ListRepos(ctx, u, r.owner, 100, 4)
	if err != nil {
		var denied *brokerclient.ErrDenied
		if errors.As(err, &denied) {
			// Open a watch with no prefix events. Client will see
			// only bookmarks until they disconnect.
			klog.V(1).InfoS("repo-watch-denied", "user", u.GetName(), "reason", denied.Reason)
			return newPerCallerWatch(ctx, nil), nil
		}
		return nil, k8serrors.NewInternalError(err)
	}
	events := make([]watch.Event, 0, len(ghRepos))
	for i := range ghRepos {
		o := r.toAggexp(&ghRepos[i])
		if !sel.Matches(labels.Set(o.Labels)) {
			continue
		}
		events = append(events, watch.Event{Type: watch.Added, Object: o})
	}
	return newPerCallerWatch(ctx, events), nil
}

// ---- TableConvertor ----

func (r *REST) ConvertToTable(_ context.Context, object runtime.Object, _ runtime.Object) (*metav1.Table, error) {
	t := &metav1.Table{
		ColumnDefinitions: []metav1.TableColumnDefinition{
			{Name: "Name", Type: "string", Format: "name", Description: "Resource name (<owner>.<repo>)."},
			{Name: "Owner", Type: "string", Description: "GitHub owner."},
			{Name: "Repo", Type: "string", Description: "GitHub repo name."},
			{Name: "Stars", Type: "integer", Description: "Stargazer count."},
			{Name: "Language", Type: "string", Description: "Primary language."},
			{Name: "Age", Type: "date", Description: "Time since observation."},
		},
	}
	row := func(o *aggexp.Repo) metav1.TableRow {
		return metav1.TableRow{
			Cells: []interface{}{
				o.Name,
				o.Spec.Owner,
				o.Spec.Name,
				int64(o.Spec.Stars),
				o.Spec.Language,
				translateTimestampSince(o.Status.ObservedAt),
			},
			Object: runtime.RawExtension{Object: o},
		}
	}
	switch obj := object.(type) {
	case *aggexp.Repo:
		t.Rows = []metav1.TableRow{row(obj)}
	case *aggexp.RepoList:
		for i := range obj.Items {
			t.Rows = append(t.Rows, row(&obj.Items[i]))
		}
		t.ListMeta.ResourceVersion = obj.ResourceVersion
	default:
		return nil, fmt.Errorf("unexpected object type %T", object)
	}
	return t, nil
}

// ---- helpers ----

func (r *REST) toAggexp(gh *ghclient.Repo) *aggexp.Repo {
	name := gh.Owner.Login + "." + gh.Name
	obj := &aggexp.Repo{
		Spec: aggexp.RepoSpec{
			Owner:         gh.Owner.Login,
			Name:          gh.Name,
			Description:   gh.Description,
			DefaultBranch: gh.DefaultBranch,
			Private:       gh.Private,
			Language:      gh.Language,
			Stars:         int32(gh.Stars),
			HTMLURL:       gh.HTMLURL,
		},
		Status: aggexp.RepoStatus{
			ObservedAt: metav1.NewTime(time.Now()),
		},
	}
	obj.Name = name
	obj.TypeMeta.Kind = "Repo"
	obj.TypeMeta.APIVersion = "aggexp.io/v1"
	// Per-caller UID: deterministic across a single request (so
	// List items are stable within the call), but not persisted
	// across calls — mirrors the 0004 pod-restart amnesia since
	// the AA here has no global cache at all.
	obj.UID = types.UID(uuid.NewSHA1(uuid.NameSpaceOID, []byte(name)).String())
	obj.ResourceVersion = r.currentRV()
	obj.CreationTimestamp = obj.Status.ObservedAt
	return obj
}

func splitRepoName(name string) (owner, repo string, ok bool) {
	idx := strings.IndexByte(name, '.')
	if idx <= 0 || idx == len(name)-1 {
		return "", "", false
	}
	return name[:idx], name[idx+1:], true
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

// perCallerWatch is a tiny watch.Interface that emits prefix events
// then blocks on ctx. It's not backed by a broadcaster because the
// experiment does not exercise live change events.
type perCallerWatch struct {
	ch   chan watch.Event
	done chan struct{}
}

func newPerCallerWatch(ctx context.Context, prefix []watch.Event) *perCallerWatch {
	w := &perCallerWatch{
		ch:   make(chan watch.Event, len(prefix)+4),
		done: make(chan struct{}),
	}
	for _, ev := range prefix {
		w.ch <- ev
	}
	go func() {
		// Bookmark every 10s until the client goes away; mirrors
		// the cadence the 0001 probe used and is accepted by
		// kubectl.
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				close(w.ch)
				return
			case <-w.done:
				close(w.ch)
				return
			case <-t.C:
				// No bookmark object plumbed; do nothing. kubectl
				// is happy holding an idle stream.
			}
		}
	}()
	return w
}

func (w *perCallerWatch) Stop()                       { close(w.done) }
func (w *perCallerWatch) ResultChan() <-chan watch.Event { return w.ch }

// Compile-time interface assertions.
var (
	_ rest.Storage              = (*REST)(nil)
	_ rest.Scoper               = (*REST)(nil)
	_ rest.KindProvider         = (*REST)(nil)
	_ rest.SingularNameProvider = (*REST)(nil)
	_ rest.Getter               = (*REST)(nil)
	_ rest.Lister               = (*REST)(nil)
	_ rest.Watcher              = (*REST)(nil)
	_ rest.TableConvertor       = (*REST)(nil)
	_ watch.Interface           = (*perCallerWatch)(nil)
)
