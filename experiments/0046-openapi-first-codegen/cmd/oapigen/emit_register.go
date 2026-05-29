package main

import (
	"fmt"
	"strings"
)

// ---- register.go (external + internal GV registration) ----
//
// Following the 0037 model: the generated Go types are registered under
// BOTH the external GroupVersion and the internal (runtime.APIVersionInternal)
// GroupVersion, with identity conversions between them. This is the
// dual-version registration the library's PATCH/SSA machinery requires
// (FINDINGS/0002, FINDINGS/0017): the internal hub must exist and be
// convertible, even when the two shapes are byte-identical.

func emitRegister(m *Model) string {
	c := m.Config
	var b strings.Builder
	b.WriteString(generatedHeader)
	b.WriteString("\n")
	fmt.Fprintf(&b, "package %s\n\n", c.Package)
	b.WriteString("import (\n")
	b.WriteString("\tmetav1 \"k8s.io/apimachinery/pkg/apis/meta/v1\"\n")
	b.WriteString("\t\"k8s.io/apimachinery/pkg/runtime\"\n")
	b.WriteString("\t\"k8s.io/apimachinery/pkg/runtime/schema\"\n")
	b.WriteString(")\n\n")

	fmt.Fprintf(&b, "// GroupName is the API group served by this package.\n")
	fmt.Fprintf(&b, "const GroupName = %q\n\n", c.Group)
	fmt.Fprintf(&b, "// SchemeGroupVersion is the external GroupVersion (%s/%s).\n", c.Group, c.Version)
	fmt.Fprintf(&b, "var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: %q}\n\n", c.Version)
	b.WriteString("// InternalGroupVersion is the internal (hub) GroupVersion. The\n")
	b.WriteString("// library's PATCH/SSA machinery routes conversions through it.\n")
	b.WriteString("var InternalGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}\n\n")

	b.WriteString("// Resource expresses a resource under this group.\n")
	b.WriteString("func Resource(resource string) schema.GroupResource {\n")
	b.WriteString("\treturn SchemeGroupVersion.WithResource(resource).GroupResource()\n}\n\n")

	b.WriteString("var (\n")
	b.WriteString("\t// SchemeBuilder collects the add-to-scheme funcs.\n")
	b.WriteString("\tSchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes, addConversionFuncs, addFieldLabelConversions)\n")
	b.WriteString("\t// AddToScheme registers the types (external + internal) on a scheme.\n")
	b.WriteString("\tAddToScheme = SchemeBuilder.AddToScheme\n")
	b.WriteString(")\n\n")

	b.WriteString("func addKnownTypes(scheme *runtime.Scheme) error {\n")
	b.WriteString("\tscheme.AddKnownTypes(SchemeGroupVersion,\n")
	fmt.Fprintf(&b, "\t\t&%s{},\n", c.Kind)
	fmt.Fprintf(&b, "\t\t&%s{},\n", c.listKind())
	b.WriteString("\t)\n")
	b.WriteString("\tmetav1.AddToGroupVersion(scheme, SchemeGroupVersion)\n\n")
	b.WriteString("\t// Internal (hub) version registration.\n")
	b.WriteString("\tscheme.AddKnownTypes(InternalGroupVersion,\n")
	fmt.Fprintf(&b, "\t\t&%s{},\n", c.Kind)
	fmt.Fprintf(&b, "\t\t&%s{},\n", c.listKind())
	b.WriteString("\t)\n")
	b.WriteString("\treturn nil\n}\n")
	return b.String()
}

// ---- conversion.go (identity conversions) ----

func emitConversion(m *Model) string {
	c := m.Config
	k := c.Kind
	lk := c.listKind()
	var b strings.Builder
	b.WriteString(generatedHeader)
	b.WriteString("\n")
	fmt.Fprintf(&b, "package %s\n\n", c.Package)
	b.WriteString("import (\n")
	b.WriteString("\t\"k8s.io/apimachinery/pkg/conversion\"\n")
	b.WriteString("\t\"k8s.io/apimachinery/pkg/runtime\"\n")
	b.WriteString(")\n\n")

	b.WriteString("// addConversionFuncs registers identity conversions for the\n")
	b.WriteString("// external<->internal hub round-trip. The two versions share the\n")
	b.WriteString("// same Go type here, so the conversions are deep copies. They must\n")
	b.WriteString("// still be registered: the library routes PATCH/SSA through the\n")
	b.WriteString("// internal hub (FINDINGS/0002, FINDINGS/0017).\n")
	b.WriteString("func addConversionFuncs(scheme *runtime.Scheme) error {\n")
	fmt.Fprintf(&b, "\tif err := scheme.AddConversionFunc((*%s)(nil), (*%s)(nil), func(a, b interface{}, _ conversion.Scope) error {\n", k, k)
	fmt.Fprintf(&b, "\t\ta.(*%s).DeepCopyInto(b.(*%s))\n", k, k)
	b.WriteString("\t\treturn nil\n\t}); err != nil {\n\t\treturn err\n\t}\n")
	fmt.Fprintf(&b, "\treturn scheme.AddConversionFunc((*%s)(nil), (*%s)(nil), func(a, b interface{}, _ conversion.Scope) error {\n", lk, lk)
	fmt.Fprintf(&b, "\t\ta.(*%s).DeepCopyInto(b.(*%s))\n", lk, lk)
	b.WriteString("\t\treturn nil\n\t})\n}\n")
	return b.String()
}
