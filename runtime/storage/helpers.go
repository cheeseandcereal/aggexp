package storage

import (
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
)

// stampRV sets the ResourceVersion on obj if it implements
// metav1.Object. Used by PublishAdded/Modified/Deleted.
func stampRV(obj runtime.Object, rv string) {
	if obj == nil {
		return
	}
	if acc, err := meta.Accessor(obj); err == nil {
		acc.SetResourceVersion(rv)
	}
}

// setListRV sets ResourceVersion on a list object's ListMeta. Uses
// the meta.List helper when available; falls back to reflection on
// the embedded ListMeta field.
func setListRV(list runtime.Object, rv string) {
	if list == nil {
		return
	}
	if li, ok := list.(metav1.ListInterface); ok {
		li.SetResourceVersion(rv)
		return
	}
	if lm, err := meta.ListAccessor(list); err == nil {
		lm.SetResourceVersion(rv)
	}
}

// listItems returns a slice of items from a list object. It uses
// meta.ExtractList which handles both typed and unstructured lists.
// The returned objects are the same pointer values the list
// contains; mutating them mutates the list.
func listItems(list runtime.Object) ([]runtime.Object, error) {
	if list == nil {
		return nil, nil
	}
	items, err := meta.ExtractList(list)
	if err != nil {
		return nil, fmt.Errorf("list items: %w", err)
	}
	return items, nil
}

// filterList mutates the list in place, removing items whose labels
// do not match sel. If the list's Items field cannot be reached via
// reflection, this is a no-op.
func filterList(list runtime.Object, sel labels.Selector) {
	items, err := meta.ExtractList(list)
	if err != nil {
		return
	}
	kept := items[:0]
	for _, o := range items {
		if matchesLabels(o, sel) {
			kept = append(kept, o)
		}
	}
	if err := meta.SetList(list, kept); err != nil {
		// Fall back to direct reflection; meta.SetList requires
		// the slice type to match the list's Items type exactly.
		setListItemsReflect(list, kept)
	}
}

// setListItemsReflect replaces list.Items via reflection with the
// filtered set, converting between []runtime.Object and the list's
// concrete slice type. It is a defensive fallback; most well-formed
// list types are handled by meta.SetList directly.
func setListItemsReflect(list runtime.Object, kept []runtime.Object) {
	v := reflect.ValueOf(list)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return
	}
	v = v.Elem()
	field := v.FieldByName("Items")
	if !field.IsValid() || !field.CanSet() || field.Kind() != reflect.Slice {
		return
	}
	elemType := field.Type().Elem()
	newSlice := reflect.MakeSlice(field.Type(), len(kept), len(kept))
	for i, o := range kept {
		ov := reflect.ValueOf(o)
		// Items may be a slice of structs or a slice of pointers.
		if elemType.Kind() == reflect.Ptr {
			newSlice.Index(i).Set(ov)
			continue
		}
		if ov.Kind() == reflect.Ptr {
			ov = ov.Elem()
		}
		newSlice.Index(i).Set(ov)
	}
	field.Set(newSlice)
}

// matchesLabels returns true if obj's metadata labels match sel.
// Non-metadata-bearing objects match unconditionally (the selector
// applies only to labeled objects).
func matchesLabels(obj runtime.Object, sel labels.Selector) bool {
	if sel == nil || sel.Empty() {
		return true
	}
	acc, err := meta.Accessor(obj)
	if err != nil {
		return true
	}
	return sel.Matches(labels.Set(acc.GetLabels()))
}
