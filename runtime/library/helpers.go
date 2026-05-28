package library

import (
	"reflect"
	"strconv"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
)

// stampRV sets the ResourceVersion on obj.
func stampRV(obj runtime.Object, rv string) {
	if obj == nil {
		return
	}
	if acc, err := meta.Accessor(obj); err == nil {
		acc.SetResourceVersion(rv)
	}
}

// setListRV sets ResourceVersion on a list object's ListMeta.
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

// listItems returns a slice of items from a list object.
func listItems(list runtime.Object) ([]runtime.Object, error) {
	if list == nil {
		return nil, nil
	}
	return meta.ExtractList(list)
}

// setListItems sets items on a list object.
func setListItems(list runtime.Object, items []runtime.Object) error {
	if err := meta.SetList(list, items); err != nil {
		setListItemsReflect(list, items)
	}
	return nil
}

// filterList mutates the list in place, removing items whose labels
// do not match sel.
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
		setListItemsReflect(list, kept)
	}
}

// setListItemsReflect replaces list.Items via reflection.
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

// objectNameFromObj extracts the name from an object's metadata.
func objectNameFromObj(obj runtime.Object) string {
	if obj == nil {
		return ""
	}
	acc, err := meta.Accessor(obj)
	if err != nil {
		return ""
	}
	return acc.GetName()
}

// objectNamespaceFromObj extracts the namespace from an object's metadata.
func objectNamespaceFromObj(obj runtime.Object) string {
	if obj == nil {
		return ""
	}
	acc, err := meta.Accessor(obj)
	if err != nil {
		return ""
	}
	return acc.GetNamespace()
}

// rvLess returns true when a < b numerically.
func rvLess(a, b string) bool {
	if a == "" {
		return b != ""
	}
	if b == "" {
		return false
	}
	an, aerr := strconv.ParseUint(a, 10, 64)
	bn, berr := strconv.ParseUint(b, 10, 64)
	if aerr != nil || berr != nil {
		return a < b
	}
	return an < bn
}

// everythingSelector returns a labels.Selector that matches everything.
func everythingSelector() labels.Selector {
	return labels.Everything()
}
