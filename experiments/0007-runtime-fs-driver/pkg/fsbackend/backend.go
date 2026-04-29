// Package fsbackend implements a runtime/storage.Backend that
// projects the contents of a server-side directory as aggexp.io/v1
// File resources. It is read-only by design (writes to user-owned
// directories are a later experiment).
//
// The backend runs a poll loop (ticker-driven, no fsnotify scope
// cut) that scans the root directory, builds File objects, and
// publishes Added/Modified/Deleted events to the runtime/storage
// adapter's watch broadcaster.
package fsbackend

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/klog/v2"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0007-runtime-fs-driver/pkg/apis/aggexp"
	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// Options configures the Backend.
type Options struct {
	// Root is the absolute path the backend projects. Only
	// top-level non-hidden entries are listed; subdirectories are
	// ignored in this experiment.
	Root string
	// PollInterval is how often the scan loop runs. Defaults to
	// 5s if zero.
	PollInterval time.Duration
}

// Backend is the runtime/storage.Backend implementation.
type Backend struct {
	root     string
	interval time.Duration

	mu    sync.RWMutex
	items map[string]*aggexp.File

	publisher runtimestorage.Publisher // set via SetPublisher before Start
}

// New constructs a Backend.
func New(opts Options) *Backend {
	if opts.PollInterval <= 0 {
		opts.PollInterval = 5 * time.Second
	}
	return &Backend{
		root:     opts.Root,
		interval: opts.PollInterval,
		items:    map[string]*aggexp.File{},
	}
}

// SetPublisher hands the backend a Publisher so Start can emit
// watch events. Must be called before Start.
func (b *Backend) SetPublisher(p runtimestorage.Publisher) { b.publisher = p }

// Start launches the scan loop until ctx is canceled. Safe to call
// from a post-start hook; non-blocking.
func (b *Backend) Start(ctx context.Context) {
	go func() {
		b.scanOnce(ctx)
		t := time.NewTicker(b.interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				b.scanOnce(ctx)
			}
		}
	}()
}

// --- runtime/storage.Backend ---

func (b *Backend) New() runtime.Object     { return &aggexp.File{} }
func (b *Backend) NewList() runtime.Object { return &aggexp.FileList{} }
func (b *Backend) Kind() string            { return "File" }
func (b *Backend) SingularName() string    { return "file" }
func (b *Backend) NamespaceScoped() bool   { return false }

func (b *Backend) Get(_ context.Context, _ user.Info, name string) (runtime.Object, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	f, ok := b.items[name]
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: "aggexp.io", Resource: "files"}, name)
	}
	return f.DeepCopy(), nil
}

func (b *Backend) List(_ context.Context, _ user.Info, _ runtimestorage.ListOptions) (runtime.Object, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	list := &aggexp.FileList{Items: make([]aggexp.File, 0, len(b.items))}
	names := make([]string, 0, len(b.items))
	for n := range b.items {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		list.Items = append(list.Items, *b.items[n].DeepCopy())
	}
	return list, nil
}

func (b *Backend) TableColumns() []metav1.TableColumnDefinition {
	return []metav1.TableColumnDefinition{
		{Name: "Name", Type: "string", Format: "name", Description: "Basename of the file."},
		{Name: "Path", Type: "string", Description: "Absolute path on the server."},
		{Name: "Size", Type: "integer", Description: "Size in bytes."},
		{Name: "Mode", Type: "string", Description: "File mode (octal)."},
		{Name: "Age", Type: "date", Description: "Time since observation."},
	}
}

func (b *Backend) RowsFor(obj runtime.Object) ([]metav1.TableRow, error) {
	row := func(f *aggexp.File) metav1.TableRow {
		return metav1.TableRow{
			Cells: []interface{}{
				f.Name,
				f.Spec.Path,
				f.Spec.Size,
				fmt.Sprintf("%#o", f.Spec.Mode),
				translateTimestampSince(f.Status.ObservedAt),
			},
			Object: runtime.RawExtension{Object: f},
		}
	}
	switch v := obj.(type) {
	case *aggexp.File:
		return []metav1.TableRow{row(v)}, nil
	case *aggexp.FileList:
		rs := make([]metav1.TableRow, 0, len(v.Items))
		for i := range v.Items {
			rs = append(rs, row(&v.Items[i]))
		}
		return rs, nil
	}
	return nil, fmt.Errorf("unexpected object %T", obj)
}

// --- scan loop ---

func (b *Backend) scanOnce(_ context.Context) {
	entries, err := os.ReadDir(b.root)
	if err != nil {
		klog.V(2).InfoS("fs-scan-failed", "root", b.root, "err", err)
		return
	}
	now := metav1.NewTime(time.Now())

	next := make(map[string]*aggexp.File, len(entries))
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.IsDir() {
			continue
		}
		// Kubernetes resource names are lowercase; lowercase the
		// basename. Non-DNS characters get replaced.
		name := sanitize(e.Name())
		f := &aggexp.File{
			Spec: aggexp.FileSpec{
				Path: filepath.Join(b.root, e.Name()),
				Size: info.Size(),
				Mode: uint32(info.Mode().Perm()),
			},
			Status: aggexp.FileStatus{ObservedAt: now},
		}
		f.Name = name
		f.TypeMeta.Kind = "File"
		f.TypeMeta.APIVersion = "aggexp.io/v1"
		next[name] = f
	}

	b.mu.Lock()
	prev := b.items
	// Compute diff.
	added := []*aggexp.File{}
	modified := []*aggexp.File{}
	deleted := []*aggexp.File{}
	for name, cur := range next {
		if old, existed := prev[name]; !existed {
			cur.UID = types.UID(uuid.New().String())
			cur.CreationTimestamp = now
			added = append(added, cur)
		} else {
			if old.Spec == cur.Spec {
				// Unchanged; keep prior object identity so a
				// restart doesn't churn the watch.
				next[name] = old
				continue
			}
			cur.UID = old.UID
			cur.CreationTimestamp = old.CreationTimestamp
			modified = append(modified, cur)
		}
	}
	for name, old := range prev {
		if _, still := next[name]; !still {
			deleted = append(deleted, old.DeepCopy())
		}
	}
	b.items = next
	b.mu.Unlock()

	if b.publisher != nil {
		for _, f := range added {
			b.publisher.PublishAdded(f.DeepCopy())
		}
		for _, f := range modified {
			b.publisher.PublishModified(f.DeepCopy())
		}
		for _, f := range deleted {
			b.publisher.PublishDeleted(f.DeepCopy())
		}
	}

	klog.V(2).InfoS("fs-scan", "root", b.root, "count", len(next), "added", len(added), "modified", len(modified), "deleted", len(deleted))
}

// sanitize lowercases the basename and replaces underscores and
// non-alphanumeric characters with '-' so kubernetes names stay
// valid. Collisions can occur but are unlikely for typical files.
func sanitize(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	for _, r := range strings.ToLower(name) {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return b.String()
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
	default:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
}

// Compile-time assertion.
var _ runtimestorage.Backend = (*Backend)(nil)
