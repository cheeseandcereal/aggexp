package main

import (
	"fmt"
	"strings"
)

// emitFieldLabels generates the AddFieldLabelConversionFunc registration
// (the hard gate for field selectors, FINDINGS/0037) plus a FieldAccessor
// usable by runtime/library's FieldSelectorOptions.
func emitFieldLabels(m *Model) string {
	c := m.Config
	var b strings.Builder
	b.WriteString(generatedHeader)
	b.WriteString("\n")
	fmt.Fprintf(&b, "package %s\n\n", c.Package)
	b.WriteString("import (\n")
	b.WriteString("\t\"fmt\"\n")
	if needsStrconv(m) {
		b.WriteString("\t\"strconv\"\n")
	}
	b.WriteString("\n")
	b.WriteString("\t\"k8s.io/apimachinery/pkg/runtime\"\n")
	b.WriteString("\t\"k8s.io/apimachinery/pkg/runtime/schema\"\n")
	b.WriteString(")\n\n")

	// SelectableFields constant.
	b.WriteString("// SelectableFields are the fields (beyond metadata.name and\n")
	b.WriteString("// metadata.namespace) that may be used in --field-selector queries.\n")
	b.WriteString("var SelectableFields = []string{\n")
	for _, fa := range m.FieldAccessors {
		fmt.Fprintf(&b, "\t%q,\n", fa.Path)
	}
	b.WriteString("}\n\n")

	// addFieldLabelConversions: the scheme registration.
	b.WriteString("// addFieldLabelConversions registers the field-label conversion func\n")
	b.WriteString("// on the scheme. Without it, the apiserver library rejects all custom\n")
	b.WriteString("// field selectors with a 400 before the storage handler runs\n")
	b.WriteString("// (FINDINGS/0037).\n")
	b.WriteString("func addFieldLabelConversions(scheme *runtime.Scheme) error {\n")
	b.WriteString("\treturn scheme.AddFieldLabelConversionFunc(\n")
	fmt.Fprintf(&b, "\t\tschema.GroupVersionKind{Group: GroupName, Version: %q, Kind: %q},\n", c.Version, c.Kind)
	b.WriteString("\t\tfunc(label, value string) (string, string, error) {\n")
	b.WriteString("\t\t\tswitch label {\n")
	b.WriteString("\t\t\tcase \"metadata.name\", \"metadata.namespace\":\n")
	b.WriteString("\t\t\t\treturn label, value, nil\n")
	for _, fa := range m.FieldAccessors {
		fmt.Fprintf(&b, "\t\t\tcase %q:\n\t\t\t\treturn label, value, nil\n", fa.Path)
	}
	b.WriteString("\t\t\tdefault:\n")
	b.WriteString("\t\t\t\treturn \"\", \"\", fmt.Errorf(\"%q is not a known field selector\", label)\n")
	b.WriteString("\t\t\t}\n\t\t},\n\t)\n}\n\n")

	// FieldAccessor: extracts a string value for a selectable field.
	fmt.Fprintf(&b, "// FieldAccessor extracts the string value of a selectable field from a\n")
	fmt.Fprintf(&b, "// *%s. Pass it to runtime/library's FieldSelectorOptions.Accessor.\n", c.Kind)
	b.WriteString("func FieldAccessor(obj runtime.Object, field string) (string, bool) {\n")
	fmt.Fprintf(&b, "\tw, ok := obj.(*%s)\n", c.Kind)
	b.WriteString("\tif !ok {\n\t\treturn \"\", false\n\t}\n")
	b.WriteString("\tswitch field {\n")
	for _, fa := range m.FieldAccessors {
		fmt.Fprintf(&b, "\tcase %q:\n\t\treturn %s, true\n", fa.Path, fa.Expr)
	}
	b.WriteString("\tdefault:\n\t\treturn \"\", false\n\t}\n}\n")
	return b.String()
}

func needsStrconv(m *Model) bool {
	for _, fa := range m.FieldAccessors {
		if strings.Contains(fa.Expr, "strconv.") {
			return true
		}
	}
	return false
}
