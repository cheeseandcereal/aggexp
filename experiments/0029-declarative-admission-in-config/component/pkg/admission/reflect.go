package admission

import "reflect"

// reflectTypeOf is a tiny helper kept in its own file to avoid
// importing reflect at the top of cel.go just for this one call.
func reflectTypeOf(v any) reflect.Type {
	return reflect.TypeOf(v)
}
