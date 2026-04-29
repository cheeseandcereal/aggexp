package fsbackend_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0007-runtime-fs-driver/pkg/apis/aggexp"
	"github.com/cheeseandcereal/aggexp/experiments/0007-runtime-fs-driver/pkg/fsbackend"
	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

type recorder struct {
	mu    sync.Mutex
	added []string
	rv    uint64
}

func (r *recorder) PublishAdded(o runtime.Object) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if f, ok := o.(*aggexp.File); ok {
		r.added = append(r.added, f.Name)
	}
}
func (r *recorder) PublishModified(o runtime.Object) {}
func (r *recorder) PublishDeleted(o runtime.Object) {}
func (r *recorder) CurrentResourceVersion() string    { return "0" }

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.added)
}

func TestScanListsAndPublishes(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a.txt", "b.txt", "c.log"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	b := fsbackend.New(fsbackend.Options{Root: dir, PollInterval: 50 * time.Millisecond})
	rec := &recorder{}
	b.SetPublisher(rec)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b.Start(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rec.count() >= 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if rec.count() < 3 {
		t.Fatalf("expected 3 Added events, got %d", rec.count())
	}

	obj, err := b.List(context.Background(), nil, runtimestorage.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	list, ok := obj.(*aggexp.FileList)
	if !ok {
		t.Fatalf("expected *FileList, got %T", obj)
	}
	if len(list.Items) != 3 {
		t.Fatalf("expected 3 files listed, got %d", len(list.Items))
	}
}

func TestHiddenFilesIgnored(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".secret"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "visible.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	b := fsbackend.New(fsbackend.Options{Root: dir, PollInterval: 50 * time.Millisecond})
	rec := &recorder{}
	b.SetPublisher(rec)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b.Start(ctx)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if rec.count() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	obj, err := b.List(context.Background(), nil, runtimestorage.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	list := obj.(*aggexp.FileList)
	if len(list.Items) != 1 {
		t.Fatalf("expected 1 visible file, got %d", len(list.Items))
	}
	if list.Items[0].Name != "visible.txt" {
		t.Fatalf("expected visible.txt, got %s", list.Items[0].Name)
	}
}
