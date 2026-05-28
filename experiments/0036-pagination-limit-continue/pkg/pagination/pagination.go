// Package pagination implements limit+continue pagination as a wrapper
// around runtime/storage.REST. It intercepts List() to apply pagination
// semantics without any backend support.
package pagination

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/registry/rest"

	runtimestorage "github.com/cheeseandcereal/aggexp/runtime/storage"
)

// PaginatedREST wraps a runtime/storage.REST and adds limit+continue
// pagination to the List operation.
type PaginatedREST struct {
	inner         *runtimestorage.REST
	groupResource schema.GroupResource
}

// New creates a PaginatedREST wrapping the given storage.REST.
func New(inner *runtimestorage.REST, gr schema.GroupResource) *PaginatedREST {
	return &PaginatedREST{inner: inner, groupResource: gr}
}

// Inner returns the wrapped REST for shutdown, publish, etc.
func (p *PaginatedREST) Inner() *runtimestorage.REST { return p.inner }

// --- Delegated interfaces ---

func (p *PaginatedREST) New() runtime.Object                    { return p.inner.New() }
func (p *PaginatedREST) NewList() runtime.Object                { return p.inner.NewList() }
func (p *PaginatedREST) Destroy()                               { p.inner.Destroy() }
func (p *PaginatedREST) NamespaceScoped() bool                  { return p.inner.NamespaceScoped() }
func (p *PaginatedREST) Kind() string                           { return p.inner.Kind() }
func (p *PaginatedREST) GetSingularName() string                { return p.inner.GetSingularName() }

func (p *PaginatedREST) Get(ctx context.Context, name string, opts *metav1.GetOptions) (runtime.Object, error) {
	return p.inner.Get(ctx, name, opts)
}

func (p *PaginatedREST) Watch(ctx context.Context, opts *metainternalversion.ListOptions) (watch.Interface, error) {
	return p.inner.Watch(ctx, opts)
}

func (p *PaginatedREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	t, err := p.inner.ConvertToTable(ctx, object, tableOptions)
	if err != nil {
		return nil, err
	}
	// The apiserver's response handler expects item.Object.Object to be
	// set on each row so it can extract PartialObjectMetadata. Populate
	// it from the source object's items.
	if meta.IsListType(object) {
		items, extractErr := meta.ExtractList(object)
		if extractErr == nil && len(items) == len(t.Rows) {
			for i := range t.Rows {
				t.Rows[i].Object = runtime.RawExtension{Object: items[i]}
			}
		}
		// Copy pagination metadata that the inner ConvertToTable doesn't propagate.
		if li, ok := object.(metav1.ListInterface); ok {
			t.ListMeta.Continue = li.GetContinue()
			t.ListMeta.RemainingItemCount = li.GetRemainingItemCount()
		}
	} else {
		if len(t.Rows) == 1 {
			t.Rows[0].Object = runtime.RawExtension{Object: object}
		}
	}
	return t, nil
}

func (p *PaginatedREST) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, opts *metav1.CreateOptions) (runtime.Object, error) {
	return p.inner.Create(ctx, obj, createValidation, opts)
}

func (p *PaginatedREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, opts *metav1.UpdateOptions) (runtime.Object, bool, error) {
	return p.inner.Update(ctx, name, objInfo, createValidation, updateValidation, forceAllowCreate, opts)
}

func (p *PaginatedREST) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, opts *metav1.DeleteOptions) (runtime.Object, bool, error) {
	return p.inner.Delete(ctx, name, deleteValidation, opts)
}

// --- Paginated List ---

// List implements rest.Lister with limit+continue pagination.
func (p *PaginatedREST) List(ctx context.Context, opts *metainternalversion.ListOptions) (runtime.Object, error) {
	// Get limit and continue from the incoming ListOptions.
	// metainternalversion.ListOptions has Limit and Continue fields
	// populated from the query parameters.
	var limit int64
	var continueToken string
	if opts != nil {
		limit = opts.Limit
		continueToken = opts.Continue
	}

	// If no pagination requested, delegate directly.
	if limit == 0 && continueToken == "" {
		return p.inner.List(ctx, opts)
	}

	// Strip limit/continue before passing to inner list (it doesn't
	// understand pagination) — but preserve label selector.
	innerOpts := opts.DeepCopy()
	innerOpts.Limit = 0
	innerOpts.Continue = ""

	// Fetch all items from the inner storage.
	fullList, err := p.inner.List(ctx, innerOpts)
	if err != nil {
		return nil, err
	}

	// Extract items and sort by name for deterministic ordering.
	items, err := meta.ExtractList(fullList)
	if err != nil {
		return nil, fmt.Errorf("extracting list items: %w", err)
	}
	sort.Slice(items, func(i, j int) bool {
		ai, _ := meta.Accessor(items[i])
		aj, _ := meta.Accessor(items[j])
		return ai.GetName() < aj.GetName()
	})

	// Get current RV from the list metadata.
	currentRV := p.inner.CurrentResourceVersion()

	// Determine offset from continue token.
	offset := 0
	if continueToken != "" {
		tokenRV, tokenOffset, decErr := decodeContinueToken(continueToken)
		if decErr != nil {
			return nil, apierrors.NewBadRequest(fmt.Sprintf("invalid continue token: %v", decErr))
		}
		// Validate RV matches — if data has changed, the token is stale.
		if tokenRV != currentRV {
			return nil, apierrors.NewResourceExpired(fmt.Sprintf(
				"the provided continue token is expired: resource version changed from %s to %s",
				tokenRV, currentRV))
		}
		offset = tokenOffset
	}

	// Apply offset and limit.
	total := len(items)
	if offset > total {
		offset = total
	}
	end := total
	if limit > 0 && offset+int(limit) < total {
		end = offset + int(limit)
	}
	page := items[offset:end]

	// Build the continue token for the next page if there are more items.
	var nextContinue string
	if end < total {
		nextContinue = encodeContinueToken(currentRV, end)
	}

	// Set items directly on the list to avoid type-assertion issues
	// with meta.SetList (value-typed Items slice vs pointer items).
	if err := meta.SetList(fullList, page); err != nil {
		return nil, fmt.Errorf("setting list items: %w", err)
	}

	// Update list metadata via the ListMeta directly.
	if li, ok := fullList.(metav1.ListInterface); ok {
		li.SetResourceVersion(currentRV)
		li.SetContinue(nextContinue)
		if end < total {
			remaining := int64(total - end)
			li.SetRemainingItemCount(&remaining)
		} else {
			li.SetRemainingItemCount(nil)
		}
	}

	return fullList, nil
}

// encodeContinueToken encodes rv and offset into a base64 token.
// Format: base64("rv:offset")
func encodeContinueToken(rv string, offset int) string {
	raw := fmt.Sprintf("%s:%d", rv, offset)
	return base64.StdEncoding.EncodeToString([]byte(raw))
}

// decodeContinueToken decodes a continue token into rv and offset.
func decodeContinueToken(token string) (rv string, offset int, err error) {
	raw, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return "", 0, fmt.Errorf("base64 decode: %w", err)
	}
	parts := strings.SplitN(string(raw), ":", 2)
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("expected rv:offset format, got %q", string(raw))
	}
	offset, err = strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, fmt.Errorf("invalid offset %q: %w", parts[1], err)
	}
	return parts[0], offset, nil
}

// Compile-time interface assertions.
var (
	_ rest.Storage              = (*PaginatedREST)(nil)
	_ rest.Scoper               = (*PaginatedREST)(nil)
	_ rest.KindProvider         = (*PaginatedREST)(nil)
	_ rest.SingularNameProvider = (*PaginatedREST)(nil)
	_ rest.Getter               = (*PaginatedREST)(nil)
	_ rest.Lister               = (*PaginatedREST)(nil)
	_ rest.Watcher              = (*PaginatedREST)(nil)
	_ rest.TableConvertor       = (*PaginatedREST)(nil)
	_ rest.Creater              = (*PaginatedREST)(nil)
	_ rest.Updater              = (*PaginatedREST)(nil)
	_ rest.GracefulDeleter      = (*PaginatedREST)(nil)
)
