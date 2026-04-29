// Package repo provides the rest.Storage implementation for
// aggexp.io/v1 Repo resources. It is backed by a polling GitHub
// client; resourceVersion is a monotonic atomic counter; watch
// events are fanned out via watch.Broadcaster.
//
// Read-only (no Create/Update/Delete/Patch). Writes to the backing
// GitHub API are left to a later experiment.
//
// The resource name convention: `<owner>.<repo-name>`. Dots are
// legal in Kubernetes names; slashes are not. This is a pragmatic
// choice for the MVP; the .N naming may need revisiting if a
// specific github repo's name contains dots (rare but possible).
package repo

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/klog/v2"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0004-github-driver-static-pat/pkg/apis/aggexp"
	ghclient "github.com/cheeseandcereal/aggexp/experiments/0004-github-driver-static-pat/pkg/github"
)

// REST is the read-only rest.Storage for Repo. It implements the
// read + watch subset:
//
//	Storage, Scoper, KindProvider, SingularNameProvider,
//	Getter, Lister, Watcher, TableConvertor
type REST struct {
	owner  string
	client *ghclient.Client

	mu     sync.RWMutex
	items  map[string]*aggexp.Repo // keyed by metadata.name (<owner>.<reponame>)
	synced bool

	rv       atomic.Uint64
	bcaster  *watch.Broadcaster
	interval time.Duration
}

// Options configures the Repo REST storage.
type Options struct {
	// Owner is the GitHub user or org whose repos we project.
	Owner string
	// Client is the GitHub REST client.
	Client *ghclient.Client
	// PollInterval is the cadence at which we refresh from GitHub.
	PollInterval time.Duration
}

// NewREST constructs the REST storage.
func NewREST(opts Options) *REST {
	if opts.PollInterval <= 0 {
		opts.PollInterval = 60 * time.Second
	}
	r := &REST{
		owner:    opts.Owner,
		client:   opts.Client,
		items:    make(map[string]*aggexp.Repo),
		bcaster:  watch.NewBroadcaster(100, watch.DropIfChannelFull),
		interval: opts.PollInterval,
	}
	r.rv.Store(1)
	return r
}

// Start launches the poll loop and, until the first poll completes,
// List/Get return a NotFound / empty list. This is pragmatic for a
// lab: the first kubectl get might race the initial poll and see
// nothing; clients should retry.
func (r *REST) Start(ctx context.Context) {
	go r.pollLoop(ctx)
	go func() {
		<-ctx.Done()
		r.bcaster.Shutdown()
	}()
}

// ---- identity / shape interfaces ----

func (r *REST) New() runtime.Object     { return &aggexp.Repo{} }
func (r *REST) NewList() runtime.Object { return &aggexp.RepoList{} }
func (r *REST) Destroy()                {}
func (r *REST) NamespaceScoped() bool   { return false }
func (r *REST) Kind() string            { return "Repo" }
func (r *REST) GetSingularName() string { return "repo" }

// ---- rv helpers ----

func (r *REST) nextRV() string {
	return strconv.FormatUint(r.rv.Add(1), 10)
}

func (r *REST) currentRV() string {
	return strconv.FormatUint(r.rv.Load(), 10)
}

// ---- Getter ----

func (r *REST) Get(ctx context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	r.mu.RLock()
	obj, ok := r.items[name]
	r.mu.RUnlock()
	if !ok {
		// Try GitHub directly in case we haven't seen this repo in
		// the cache yet (rare but possible before the first poll
		// completes). This keeps single-object reads working out of
		// the gate.
		owner, rn, ok := splitRepoName(name)
		if !ok {
			return nil, errors.NewNotFound(aggexp.Resource("repos"), name)
		}
		ghRepo, err := r.client.GetRepo(ctx, owner, rn)
		if err == ghclient.ErrNotFound {
			return nil, errors.NewNotFound(aggexp.Resource("repos"), name)
		}
		if err != nil {
			klog.V(2).InfoS("github-get-failed", "name", name, "err", err)
			return nil, errors.NewInternalError(err)
		}
		return r.toAggexp(ghRepo), nil
	}
	return obj.DeepCopy(), nil
}

// ---- Lister ----

func (r *REST) List(ctx context.Context, opts *metainternalversion.ListOptions) (runtime.Object, error) {
	sel := labels.Everything()
	if opts != nil && opts.LabelSelector != nil {
		sel = opts.LabelSelector
	}

	r.mu.RLock()
	list := &aggexp.RepoList{Items: make([]aggexp.Repo, 0, len(r.items))}
	for _, o := range r.items {
		if !sel.Matches(labels.Set(o.Labels)) {
			continue
		}
		list.Items = append(list.Items, *o.DeepCopy())
	}
	list.ListMeta.ResourceVersion = r.currentRV()
	r.mu.RUnlock()
	return list, nil
}

// ---- Watcher ----

func (r *REST) Watch(ctx context.Context, opts *metainternalversion.ListOptions) (watch.Interface, error) {
	sel := labels.Everything()
	if opts != nil && opts.LabelSelector != nil {
		sel = opts.LabelSelector
	}

	requested := ""
	if opts != nil {
		requested = opts.ResourceVersion
	}
	if requested != "" && requested != "0" {
		reqN, err := strconv.ParseUint(requested, 10, 64)
		if err != nil || reqN != r.rv.Load() {
			// No event buffer; anything other than "current" or
			// "any" is unsatisfiable. The reflector will relist.
			return nil, errors.NewResourceExpired(fmt.Sprintf(
				"too old resource version: %s (current %s)", requested, r.currentRV()))
		}
	}

	r.mu.RLock()
	prefix := make([]watch.Event, 0, len(r.items))
	for _, o := range r.items {
		if !sel.Matches(labels.Set(o.Labels)) {
			continue
		}
		prefix = append(prefix, watch.Event{Type: watch.Added, Object: o.DeepCopy()})
	}
	r.mu.RUnlock()

	w, err := r.bcaster.WatchWithPrefix(prefix)
	if err != nil {
		return nil, err
	}
	return watch.Filter(w, func(ev watch.Event) (watch.Event, bool) {
		r2, ok := ev.Object.(*aggexp.Repo)
		if !ok {
			return ev, true
		}
		return ev, sel.Matches(labels.Set(r2.Labels))
	}), nil
}

// ---- TableConvertor ----

func (r *REST) ConvertToTable(ctx context.Context, object runtime.Object, _ runtime.Object) (*metav1.Table, error) {
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

// ---- poll loop ----

func (r *REST) pollLoop(ctx context.Context) {
	// First poll immediately; then tick.
	r.refreshOnce(ctx)
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.refreshOnce(ctx)
		}
	}
}

func (r *REST) refreshOnce(ctx context.Context) {
	t0 := time.Now()
	repos, err := r.client.ListRepos(ctx, r.owner, 100, 4)
	if err != nil {
		klog.V(2).InfoS("github-list-failed", "owner", r.owner, "err", err)
		return
	}
	now := metav1.NewTime(time.Now())

	r.mu.Lock()
	prev := r.items
	next := make(map[string]*aggexp.Repo, len(repos))

	for i := range repos {
		gh := &repos[i]
		obj := r.toAggexp(gh)
		obj.Status.ObservedAt = now
		next[obj.Name] = obj
	}

	// Added / Modified.
	for name, cur := range next {
		old, existed := prev[name]
		if !existed {
			cur.ResourceVersion = r.nextRV()
			cur.UID = types.UID(uuid.New().String())
			cur.CreationTimestamp = now
			r.bcaster.Action(watch.Added, cur.DeepCopy())
		} else if !repoEqual(old, cur) {
			// Preserve UID + CreationTimestamp; bump RV.
			cur.UID = old.UID
			cur.CreationTimestamp = old.CreationTimestamp
			cur.ResourceVersion = r.nextRV()
			r.bcaster.Action(watch.Modified, cur.DeepCopy())
		} else {
			// Unchanged; keep prior object identity.
			next[name] = old
		}
	}
	// Deleted.
	for name, old := range prev {
		if _, still := next[name]; !still {
			tomb := old.DeepCopy()
			tomb.ResourceVersion = r.nextRV()
			r.bcaster.Action(watch.Deleted, tomb)
		}
	}
	r.items = next
	r.synced = true
	r.mu.Unlock()

	klog.V(2).InfoS("github-refresh", "owner", r.owner, "count", len(repos), "took", time.Since(t0))
}

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
	}
	obj.Name = name
	obj.TypeMeta.Kind = "Repo"
	obj.TypeMeta.APIVersion = "aggexp.io/v1"
	return obj
}

// ---- helpers ----

func splitRepoName(name string) (owner, repo string, ok bool) {
	idx := strings.IndexByte(name, '.')
	if idx <= 0 || idx == len(name)-1 {
		return "", "", false
	}
	return name[:idx], name[idx+1:], true
}

func repoEqual(a, b *aggexp.Repo) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Spec == b.Spec
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

// requestUser is retained for parity with 0002/0003's logging style;
// Repo does not mutate state, so we don't log the user per request
// here (the authorizer already logs identity decisions). Kept as a
// helper for future experiments that add writes.
func requestUser(ctx context.Context) (name string, groups []string) {
	u, _ := genericapirequest.UserFrom(ctx)
	if u == nil {
		return "?", nil
	}
	return u.GetName(), u.GetGroups()
}

var _ = requestUser // silence "declared and not used"

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
)
