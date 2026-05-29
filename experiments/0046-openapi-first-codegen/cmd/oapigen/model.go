package main

// The intermediate representation (IR). The parse stage produces a
// Model from the OpenAPI document + config; the emit stages consume the
// Model. Keeping parse and emit separate makes the type-mapping logic
// testable and the emitters dumb.

// Model is the fully-resolved set of Go types to emit for one resource.
type Model struct {
	Config *Config

	// Structs are the named struct types to emit, in deterministic
	// (sorted-by-name) order. Includes WidgetSpec, WidgetStatus, and
	// any nested object/$ref components, but NOT the top-level
	// <Kind>/<Kind>List (those are synthesized by the emitters since
	// their shape is fixed).
	Structs []*GoStruct

	// SpecType is the Go type name used for the resource's .spec
	// field, or "" if the resource has no spec.
	SpecType string
	// StatusType is the Go type name used for the resource's .status
	// field, or "" if the resource has no status.
	StatusType string

	// Enums are named string-enum types (e.g. WidgetColor) with their
	// allowed constant values, in deterministic order.
	Enums []*GoEnum

	// FieldAccessors maps a selectable field path (e.g. "spec.color")
	// to the Go expression that extracts it as a string from a
	// *<Kind> receiver named "w". Built from listSelectableFields.
	FieldAccessors []FieldAccessor

	// OpenAPISHA256 is the hex SHA-256 of the input OpenAPI document
	// bytes; recorded in doc.go for reproducibility auditing.
	OpenAPISHA256 string
}

// GoStruct is a named Go struct type.
type GoStruct struct {
	Name        string
	Description string
	Fields      []GoField
}

// GoField is one field of a GoStruct.
type GoField struct {
	// GoName is the exported Go field name (CamelCase).
	GoName string
	// JSONName is the wire/json key.
	JSONName string
	// GoType is the Go type expression (e.g. "int32", "[]string",
	// "map[string]string", "*int32", "Coordinates", "WidgetColor").
	GoType string
	// Description is the field doc comment (from OpenAPI description).
	Description string
	// Pointer indicates the field is a pointer (optional scalar/ref).
	Pointer bool
	// OmitEmpty controls the ,omitempty json tag suffix.
	OmitEmpty bool

	// --- openapi-gen emission metadata ---
	// OAPIType is the OpenAPI primitive type ("string", "integer",
	// "number", "boolean", "array", "object") for the def emission.
	OAPIType string
	// OAPIFormat is the OpenAPI format ("int32", "int64", "float",
	// "double", "date-time", "") for the def emission.
	OAPIFormat string
	// RefName, if non-empty, is the name of a struct/$ref this field
	// references (for the def Ref + Dependencies).
	RefName string
	// ElemKind describes array element / map value kinds for the def
	// emission: one of "", "scalar", "ref".
	ElemKind string
	// ElemRefName is the referenced struct name for array<ref>.
	ElemRefName string
	// ElemOAPIType / ElemOAPIFormat for array<scalar>.
	ElemOAPIType   string
	ElemOAPIFormat string
	// MapValueOAPIType for object+additionalProperties<scalar>.
	MapValueOAPIType   string
	MapValueOAPIFormat string
	// FreeForm marks a free-form object (map[string]interface{}).
	FreeForm bool
	// Required marks the field as required (no ,omitempty, no pointer).
	Required bool
}

// GoEnum is a named string type with constant values.
type GoEnum struct {
	Name        string
	Description string
	Values      []string
}

// FieldAccessor maps a selectable field path to a Go extraction
// expression.
type FieldAccessor struct {
	Path string // e.g. "spec.color"
	Expr string // e.g. "string(w.Spec.Color)"
}
