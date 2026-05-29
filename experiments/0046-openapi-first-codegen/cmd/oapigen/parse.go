package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
)

// parse loads the OpenAPI document, validates the supported subset, and
// builds the Model. openapiBytes is the raw input (for the SHA-256).
func parse(openapiBytes []byte, cfg *Config) (*Model, error) {
	loader := openapi3.NewLoader()
	// External $refs are rejected in v1: leave IsExternalRefsAllowed
	// at its default (false). LoadFromData will fail if the document
	// references another file.
	loader.IsExternalRefsAllowed = false

	doc, err := loader.LoadFromData(openapiBytes)
	if err != nil {
		return nil, fmt.Errorf("load OpenAPI document: %w", err)
	}
	if doc.Components == nil || len(doc.Components.Schemas) == 0 {
		return nil, fmt.Errorf("OpenAPI document has no components.schemas")
	}

	sum := sha256.Sum256(openapiBytes)
	m := &Model{
		Config:        cfg,
		OpenAPISHA256: hex.EncodeToString(sum[:]),
	}

	p := &parser{
		cfg:       cfg,
		schemas:   doc.Components.Schemas,
		structs:   map[string]*GoStruct{},
		enums:     map[string]*GoEnum{},
		structSeq: nil,
		enumSeq:   nil,
	}

	// Resolve the spec component into a named struct equal to
	// "<Kind>Spec" (or use the component name directly — we name the
	// emitted Go type after the component so the author controls it).
	if cfg.SpecComponent != "" {
		ref, ok := p.schemas[cfg.SpecComponent]
		if !ok {
			return nil, fmt.Errorf("specComponent %q not found in components.schemas", cfg.SpecComponent)
		}
		if err := p.buildNamedStruct(cfg.SpecComponent, ref); err != nil {
			return nil, fmt.Errorf("spec component %q: %w", cfg.SpecComponent, err)
		}
		m.SpecType = cfg.SpecComponent
	}
	if cfg.StatusComponent != "" {
		ref, ok := p.schemas[cfg.StatusComponent]
		if !ok {
			return nil, fmt.Errorf("statusComponent %q not found in components.schemas", cfg.StatusComponent)
		}
		if err := p.buildNamedStruct(cfg.StatusComponent, ref); err != nil {
			return nil, fmt.Errorf("status component %q: %w", cfg.StatusComponent, err)
		}
		m.StatusType = cfg.StatusComponent
	}

	// Emit structs and enums in deterministic (sorted) order so the
	// output is byte-identical across runs.
	names := make([]string, 0, len(p.structs))
	for n := range p.structs {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		m.Structs = append(m.Structs, p.structs[n])
	}
	enames := make([]string, 0, len(p.enums))
	for n := range p.enums {
		enames = append(enames, n)
	}
	sort.Strings(enames)
	for _, n := range enames {
		m.Enums = append(m.Enums, p.enums[n])
	}

	accessors, err := p.buildFieldAccessors()
	if err != nil {
		return nil, err
	}
	m.FieldAccessors = accessors

	return m, nil
}

type parser struct {
	cfg     *Config
	schemas openapi3.Schemas

	structs   map[string]*GoStruct
	enums     map[string]*GoEnum
	structSeq []string
	enumSeq   []string
}

// rejectComposition fails on oneOf/anyOf/arbitrary allOf, which are out
// of the v1 supported subset.
func rejectComposition(where string, s *openapi3.Schema) error {
	if len(s.OneOf) > 0 {
		return fmt.Errorf("%s: oneOf is not supported in the v1 subset", where)
	}
	if len(s.AnyOf) > 0 {
		return fmt.Errorf("%s: anyOf is not supported in the v1 subset", where)
	}
	if len(s.AllOf) > 0 {
		return fmt.Errorf("%s: allOf is not supported in the v1 subset", where)
	}
	if s.Not != nil {
		return fmt.Errorf("%s: not is not supported in the v1 subset", where)
	}
	return nil
}

// buildNamedStruct resolves an object schema into a GoStruct registered
// under name. Idempotent: a struct built once is not rebuilt.
func (p *parser) buildNamedStruct(name string, ref *openapi3.SchemaRef) error {
	if _, done := p.structs[name]; done {
		return nil
	}
	if ref == nil || ref.Value == nil {
		return fmt.Errorf("struct %q has no schema value", name)
	}
	s := ref.Value
	if err := rejectComposition("struct "+name, s); err != nil {
		return err
	}
	if !s.Type.Is("object") {
		return fmt.Errorf("struct %q must be type object, got %v", name, s.Type.Slice())
	}

	gs := &GoStruct{
		Name:        name,
		Description: strings.TrimSpace(s.Description),
	}
	// Reserve the name before recursing so $ref cycles terminate.
	p.structs[name] = gs

	required := map[string]bool{}
	for _, r := range s.Required {
		required[r] = true
	}

	// Deterministic field order: sort property names.
	propNames := make([]string, 0, len(s.Properties))
	for pn := range s.Properties {
		propNames = append(propNames, pn)
	}
	sort.Strings(propNames)

	for _, pn := range propNames {
		pref := s.Properties[pn]
		field, err := p.buildField(name, pn, pref, required[pn])
		if err != nil {
			return err
		}
		gs.Fields = append(gs.Fields, field)
	}
	return nil
}

// buildField maps one property schema into a GoField per the type table.
func (p *parser) buildField(owner, jsonName string, ref *openapi3.SchemaRef, isRequired bool) (GoField, error) {
	f := GoField{
		GoName:   goExportName(jsonName),
		JSONName: jsonName,
		Required: isRequired,
	}

	// $ref → named struct.
	if ref.Ref != "" {
		refName, err := localRefName(ref.Ref)
		if err != nil {
			return f, fmt.Errorf("%s.%s: %w", owner, jsonName, err)
		}
		target, ok := p.schemas[refName]
		if !ok {
			return f, fmt.Errorf("%s.%s: $ref %q not found in components.schemas", owner, jsonName, ref.Ref)
		}
		if err := p.buildNamedStruct(refName, target); err != nil {
			return f, err
		}
		f.GoType = refName
		f.RefName = refName
		f.OAPIType = "object"
		f.Description = strings.TrimSpace(target.Value.Description)
		// Optional ref (not required) becomes a pointer.
		if !isRequired {
			f.GoType = "*" + refName
			f.Pointer = true
			f.OmitEmpty = true
		}
		return f, nil
	}

	s := ref.Value
	if s == nil {
		return f, fmt.Errorf("%s.%s: empty schema", owner, jsonName)
	}
	if err := rejectComposition(fmt.Sprintf("%s.%s", owner, jsonName), s); err != nil {
		return f, err
	}
	f.Description = strings.TrimSpace(s.Description)

	typ := firstType(s)
	switch typ {
	case "string":
		if len(s.Enum) > 0 {
			enumName := goExportName(owner) + goExportName(jsonName)
			vals, err := stringEnumValues(s.Enum)
			if err != nil {
				return f, fmt.Errorf("%s.%s: %w", owner, jsonName, err)
			}
			p.enums[enumName] = &GoEnum{
				Name:        enumName,
				Description: fmt.Sprintf("%s is an enumerated string for %s.%s.", enumName, owner, jsonName),
				Values:      vals,
			}
			f.GoType = enumName
			f.OAPIType = "string"
		} else if s.Format == "date-time" {
			f.GoType = "metav1.Time"
			f.OAPIType = "string"
			f.OAPIFormat = "date-time"
		} else {
			f.GoType = "string"
			f.OAPIType = "string"
		}
	case "integer":
		if s.Format == "int32" {
			f.GoType = "int32"
			f.OAPIType = "integer"
			f.OAPIFormat = "int32"
		} else {
			// int64 default
			f.GoType = "int64"
			f.OAPIType = "integer"
			f.OAPIFormat = "int64"
		}
	case "number":
		if s.Format == "float" {
			f.GoType = "float32"
			f.OAPIType = "number"
			f.OAPIFormat = "float"
		} else {
			// double default
			f.GoType = "float64"
			f.OAPIType = "number"
			f.OAPIFormat = "double"
		}
	case "boolean":
		f.GoType = "bool"
		f.OAPIType = "boolean"
	case "array":
		elem, err := p.buildArrayElem(owner, jsonName, s)
		if err != nil {
			return f, err
		}
		f = mergeArray(f, elem)
	case "object":
		// additionalProperties<T> → map[string]T
		if ap := s.AdditionalProperties.Schema; ap != nil {
			mapElem, err := p.buildMapValue(owner, jsonName, ap)
			if err != nil {
				return f, err
			}
			f = mergeMap(f, mapElem)
		} else if len(s.Properties) > 0 {
			// inline object with properties → named struct
			structName := goExportName(owner) + goExportName(jsonName)
			if err := p.buildInlineStruct(structName, s); err != nil {
				return f, err
			}
			f.GoType = structName
			f.RefName = structName
			f.OAPIType = "object"
			if !isRequired {
				f.GoType = "*" + structName
				f.Pointer = true
				f.OmitEmpty = true
			}
		} else {
			// free-form object → map[string]interface{}
			f.GoType = "map[string]interface{}"
			f.OAPIType = "object"
			f.FreeForm = true
		}
	default:
		return f, fmt.Errorf("%s.%s: unsupported or missing type %q (v1 subset)", owner, jsonName, typ)
	}

	// Optional scalar (not required, no default) → pointer.
	// Arrays, maps, free-form objects, and named structs are handled
	// above; only the simple scalar/enum/time cases get pointerized
	// here.
	if !isRequired && !f.Pointer {
		switch f.GoType {
		case "string", "int32", "int64", "float32", "float64", "bool", "metav1.Time":
			if s.Default == nil {
				f.GoType = "*" + f.GoType
				f.Pointer = true
			}
			f.OmitEmpty = true
		default:
			// enums (named string), maps, arrays, free-form: emit
			// ,omitempty but no pointer (zero value is fine).
			f.OmitEmpty = true
		}
	}

	return f, nil
}

// buildInlineStruct builds a struct from an inline object schema (one
// that has properties but is not a top-level component $ref).
func (p *parser) buildInlineStruct(name string, s *openapi3.Schema) error {
	if _, done := p.structs[name]; done {
		return nil
	}
	gs := &GoStruct{
		Name:        name,
		Description: fmt.Sprintf("%s is a nested object type.", name),
	}
	p.structs[name] = gs
	required := map[string]bool{}
	for _, r := range s.Required {
		required[r] = true
	}
	propNames := make([]string, 0, len(s.Properties))
	for pn := range s.Properties {
		propNames = append(propNames, pn)
	}
	sort.Strings(propNames)
	for _, pn := range propNames {
		field, err := p.buildField(name, pn, s.Properties[pn], required[pn])
		if err != nil {
			return err
		}
		gs.Fields = append(gs.Fields, field)
	}
	return nil
}

// arrayElem carries the resolved element kind for an array property.
type arrayElem struct {
	goType      string
	refName     string
	oapiType    string
	oapiFormat  string
	isRef       bool
}

func (p *parser) buildArrayElem(owner, jsonName string, s *openapi3.Schema) (arrayElem, error) {
	if s.Items == nil {
		return arrayElem{}, fmt.Errorf("%s.%s: array has no items schema", owner, jsonName)
	}
	item := s.Items
	if item.Ref != "" {
		refName, err := localRefName(item.Ref)
		if err != nil {
			return arrayElem{}, fmt.Errorf("%s.%s: %w", owner, jsonName, err)
		}
		target, ok := p.schemas[refName]
		if !ok {
			return arrayElem{}, fmt.Errorf("%s.%s: array item $ref %q not found", owner, jsonName, item.Ref)
		}
		if err := p.buildNamedStruct(refName, target); err != nil {
			return arrayElem{}, err
		}
		return arrayElem{goType: refName, refName: refName, isRef: true, oapiType: "object"}, nil
	}
	if err := rejectComposition(fmt.Sprintf("%s.%s[]", owner, jsonName), item.Value); err != nil {
		return arrayElem{}, err
	}
	switch firstType(item.Value) {
	case "string":
		return arrayElem{goType: "string", oapiType: "string", oapiFormat: item.Value.Format}, nil
	case "integer":
		if item.Value.Format == "int32" {
			return arrayElem{goType: "int32", oapiType: "integer", oapiFormat: "int32"}, nil
		}
		return arrayElem{goType: "int64", oapiType: "integer", oapiFormat: "int64"}, nil
	case "number":
		if item.Value.Format == "float" {
			return arrayElem{goType: "float32", oapiType: "number", oapiFormat: "float"}, nil
		}
		return arrayElem{goType: "float64", oapiType: "number", oapiFormat: "double"}, nil
	case "boolean":
		return arrayElem{goType: "bool", oapiType: "boolean"}, nil
	default:
		return arrayElem{}, fmt.Errorf("%s.%s: unsupported array element type (v1 subset supports scalar and $ref items)", owner, jsonName)
	}
}

func mergeArray(f GoField, e arrayElem) GoField {
	f.GoType = "[]" + e.goType
	f.OAPIType = "array"
	if e.isRef {
		f.ElemKind = "ref"
		f.ElemRefName = e.refName
		f.RefName = e.refName // for Dependencies
	} else {
		f.ElemKind = "scalar"
		f.ElemOAPIType = e.oapiType
		f.ElemOAPIFormat = e.oapiFormat
	}
	return f
}

func (p *parser) buildMapValue(owner, jsonName string, ap *openapi3.SchemaRef) (arrayElem, error) {
	// Only scalar map values are supported in the v1 subset.
	if ap.Ref != "" {
		return arrayElem{}, fmt.Errorf("%s.%s: map value $ref is not supported in the v1 subset (scalar values only)", owner, jsonName)
	}
	if err := rejectComposition(fmt.Sprintf("%s.%s{}", owner, jsonName), ap.Value); err != nil {
		return arrayElem{}, err
	}
	switch firstType(ap.Value) {
	case "string":
		return arrayElem{goType: "string", oapiType: "string", oapiFormat: ap.Value.Format}, nil
	case "integer":
		if ap.Value.Format == "int32" {
			return arrayElem{goType: "int32", oapiType: "integer", oapiFormat: "int32"}, nil
		}
		return arrayElem{goType: "int64", oapiType: "integer", oapiFormat: "int64"}, nil
	case "number":
		if ap.Value.Format == "float" {
			return arrayElem{goType: "float32", oapiType: "number", oapiFormat: "float"}, nil
		}
		return arrayElem{goType: "float64", oapiType: "number", oapiFormat: "double"}, nil
	case "boolean":
		return arrayElem{goType: "bool", oapiType: "boolean"}, nil
	default:
		return arrayElem{}, fmt.Errorf("%s.%s: unsupported map value type (v1 subset supports scalar values)", owner, jsonName)
	}
}

func mergeMap(f GoField, e arrayElem) GoField {
	f.GoType = "map[string]" + e.goType
	f.OAPIType = "object"
	f.MapValueOAPIType = e.oapiType
	f.MapValueOAPIFormat = e.oapiFormat
	return f
}

// buildFieldAccessors resolves the configured selectable field paths
// into Go extraction expressions over a *<Kind> receiver named "w".
func (p *parser) buildFieldAccessors() ([]FieldAccessor, error) {
	var out []FieldAccessor
	for _, path := range p.cfg.ListSelectableFields {
		if path == "metadata.name" || path == "metadata.namespace" {
			// Always supported by the library; no accessor needed.
			continue
		}
		parts := strings.Split(path, ".")
		if len(parts) != 2 {
			return nil, fmt.Errorf("listSelectableFields: %q must be a two-segment dotted path (e.g. spec.color)", path)
		}
		var rootType, rootField string
		switch parts[0] {
		case "spec":
			rootType = p.cfg.SpecComponent
			rootField = "Spec"
		case "status":
			rootType = p.cfg.StatusComponent
			rootField = "Status"
		default:
			return nil, fmt.Errorf("listSelectableFields: %q root must be spec or status", path)
		}
		if rootType == "" {
			return nil, fmt.Errorf("listSelectableFields: %q references %s but no %s component is configured", path, parts[0], parts[0])
		}
		gs := p.structs[rootType]
		if gs == nil {
			return nil, fmt.Errorf("listSelectableFields: %q: %s struct not built", path, rootType)
		}
		var field *GoField
		for i := range gs.Fields {
			if gs.Fields[i].JSONName == parts[1] {
				field = &gs.Fields[i]
				break
			}
		}
		if field == nil {
			return nil, fmt.Errorf("listSelectableFields: %q: field %q not found on %s", path, parts[1], rootType)
		}
		expr, err := accessorExpr(rootField, field)
		if err != nil {
			return nil, fmt.Errorf("listSelectableFields: %q: %w", path, err)
		}
		out = append(out, FieldAccessor{Path: path, Expr: expr})
	}
	return out, nil
}

// accessorExpr returns a Go string expression that yields the field's
// value as a string, given a receiver "w" and the root field name
// (Spec/Status).
func accessorExpr(root string, f *GoField) (string, error) {
	sel := "w." + root + "." + f.GoName
	base := f.GoType
	if f.Pointer {
		base = strings.TrimPrefix(base, "*")
		// dereference guard handled in emit; here assume non-nil path
		sel = "*" + sel
	}
	switch base {
	case "string":
		return sel, nil
	case "int32", "int64":
		return "strconv.FormatInt(int64(" + sel + "), 10)", nil
	case "float32", "float64":
		return "strconv.FormatFloat(float64(" + sel + "), 'g', -1, 64)", nil
	case "bool":
		return "strconv.FormatBool(" + sel + ")", nil
	default:
		// named string enum
		return "string(" + sel + ")", nil
	}
}

// ---- small helpers ----

func firstType(s *openapi3.Schema) string {
	if s == nil || s.Type == nil {
		return ""
	}
	sl := s.Type.Slice()
	if len(sl) == 0 {
		return ""
	}
	return sl[0]
}

func localRefName(ref string) (string, error) {
	const prefix = "#/components/schemas/"
	if !strings.HasPrefix(ref, prefix) {
		return "", fmt.Errorf("only intra-document #/components/schemas/ refs are supported in the v1 subset; got %q", ref)
	}
	return strings.TrimPrefix(ref, prefix), nil
}

func stringEnumValues(enum []any) ([]string, error) {
	out := make([]string, 0, len(enum))
	for _, e := range enum {
		s, ok := e.(string)
		if !ok {
			return nil, fmt.Errorf("enum values must be strings in the v1 subset; got %T", e)
		}
		out = append(out, s)
	}
	return out, nil
}

// goExportName converts a json field name into an exported Go
// identifier (CamelCase). Handles a few common acronyms minimally.
func goExportName(name string) string {
	if name == "" {
		return ""
	}
	// Split on non-alphanumeric boundaries and camelCase humps.
	var b strings.Builder
	upNext := true
	for _, r := range name {
		if r == '_' || r == '-' || r == '.' || r == ' ' {
			upNext = true
			continue
		}
		if upNext {
			b.WriteRune(toUpper(r))
			upNext = false
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func toUpper(r rune) rune {
	if r >= 'a' && r <= 'z' {
		return r - ('a' - 'A')
	}
	return r
}
