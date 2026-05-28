package library

import (
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
)

// FieldAccessor extracts a string value for a given field path from obj.
// Returns ("", false) if the field is unrecognized. Consumers register
// one to support custom field selectors beyond metadata.name and
// metadata.namespace (which are always supported).
type FieldAccessor func(obj runtime.Object, field string) (string, bool)

// fieldSelectorConfig holds the field-selector configuration for a REST adapter.
type fieldSelectorConfig struct {
	selectableFields []string
	accessor         FieldAccessor
}

// validateFieldSelector checks that all fields in the selector are known.
func (c *fieldSelectorConfig) validate(sel fields.Selector) error {
	if sel == nil || sel.Empty() {
		return nil
	}
	known := map[string]bool{
		"metadata.name":      true,
		"metadata.namespace": true,
	}
	for _, f := range c.selectableFields {
		known[f] = true
	}
	reqs := sel.Requirements()
	for _, req := range reqs {
		if !known[req.Field] {
			return apierrors.NewBadRequest(
				fmt.Sprintf("field label not supported: %s", req.Field))
		}
	}
	return nil
}

// matchesField returns true if obj satisfies the field selector.
func (c *fieldSelectorConfig) matchesField(obj runtime.Object, sel fields.Selector) bool {
	if sel == nil || sel.Empty() {
		return true
	}
	reqs := sel.Requirements()
	acc, _ := meta.Accessor(obj)
	for _, req := range reqs {
		var val string
		switch req.Field {
		case "metadata.name":
			if acc != nil {
				val = acc.GetName()
			}
		case "metadata.namespace":
			if acc != nil {
				val = acc.GetNamespace()
			}
		default:
			if c.accessor != nil {
				v, ok := c.accessor(obj, req.Field)
				if !ok {
					return false
				}
				val = v
			} else {
				return false
			}
		}
		switch req.Operator {
		case "=", "==":
			if val != req.Value {
				return false
			}
		case "!=":
			if val == req.Value {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// filterListByField removes items from list that don't match sel.
func (c *fieldSelectorConfig) filterListByField(list runtime.Object, sel fields.Selector) {
	items, err := meta.ExtractList(list)
	if err != nil {
		return
	}
	kept := items[:0]
	for _, o := range items {
		if c.matchesField(o, sel) {
			kept = append(kept, o)
		}
	}
	if err := meta.SetList(list, kept); err != nil {
		setListItemsReflect(list, kept)
	}
}
