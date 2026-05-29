// Package backend is experiment 0042's in-memory Widget body store.
// It holds ONLY the business body (spec + status) for each Widget,
// keyed by namespace/name. It never sees, stores, or assigns KRM
// metadata or resourceVersion — those are the metadata CR's job (see
// pkg/metastore). This is the "body lives on a separate backend"
// half of the 0024 stitch, kept deliberately RV-blind so the
// experiment can prove the metadata-CR RV is the sole authority.
package backend

import (
	"sort"
	"sync"

	aggexp "github.com/cheeseandcereal/aggexp/experiments/0042-metadata-cr-rv-authority/pkg/apis/aggexp"
)

// Body is the spec+status of one Widget. No metadata, no RV.
type Body struct {
	Color string
	Size  int32
	Phase string
}

// InMem is a concurrency-safe in-memory body store.
type InMem struct {
	mu     sync.RWMutex
	bodies map[string]Body // key: namespace/name
}

// New returns an empty InMem store.
func New() *InMem {
	return &InMem{bodies: map[string]Body{}}
}

func key(namespace, name string) string { return namespace + "/" + name }

// Get returns the body for (namespace, name) and whether it exists.
func (m *InMem) Get(namespace, name string) (Body, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.bodies[key(namespace, name)]
	return b, ok
}

// Put stores (overwrites) the body for (namespace, name).
func (m *InMem) Put(namespace, name string, b Body) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bodies[key(namespace, name)] = b
}

// Delete removes the body. Idempotent.
func (m *InMem) Delete(namespace, name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.bodies, key(namespace, name))
}

// Ref pairs a body with its identity for listing.
type Ref struct {
	Namespace string
	Name      string
	Body      Body
}

// List returns all bodies, optionally filtered to a namespace
// (empty namespace = all namespaces), sorted by namespace/name.
func (m *InMem) List(namespace string) []Ref {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Ref, 0, len(m.bodies))
	for k, b := range m.bodies {
		ns, nm := splitKey(k)
		if namespace != "" && ns != namespace {
			continue
		}
		out = append(out, Ref{Namespace: ns, Name: nm, Body: b})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func splitKey(k string) (namespace, name string) {
	for i := 0; i < len(k); i++ {
		if k[i] == '/' {
			return k[:i], k[i+1:]
		}
	}
	return "", k
}

// BodyFromWidget extracts the body from a Widget.
func BodyFromWidget(w *aggexp.Widget) Body {
	return Body{Color: w.Spec.Color, Size: w.Spec.Size, Phase: w.Status.Phase}
}

// ApplyBody overlays a Body onto a Widget's spec/status.
func ApplyBody(w *aggexp.Widget, b Body) {
	w.Spec.Color = b.Color
	w.Spec.Size = b.Size
	w.Status.Phase = b.Phase
}
