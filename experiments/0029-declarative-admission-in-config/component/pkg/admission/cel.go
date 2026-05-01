package admission

import (
	"encoding/json"
	"fmt"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// Engine holds compiled CEL programs and the pre-parsed mutation
// rules. Compiled at config-load time so every admission call is
// just an Eval.
type Engine struct {
	mutations   []Mutation
	validations []compiledValidation
}

type compiledValidation struct {
	Rule    Validation
	Program cel.Program
}

// NewEngine compiles the config. Returns an error if any CEL
// expression fails to compile; admission rules are a startup-time
// contract, not a runtime one.
func NewEngine(cfg Config) (*Engine, error) {
	env, err := cel.NewEnv(
		cel.Variable("object", cel.DynType),
		cel.Variable("oldObject", cel.DynType),
	)
	if err != nil {
		return nil, fmt.Errorf("cel env: %w", err)
	}
	programs := make([]compiledValidation, 0, len(cfg.Validations))
	for i, v := range cfg.Validations {
		ast, iss := env.Compile(v.Expression)
		if iss != nil && iss.Err() != nil {
			return nil, fmt.Errorf("validation[%d] %q: %w", i, v.Expression, iss.Err())
		}
		if !ast.OutputType().IsExactType(cel.BoolType) {
			return nil, fmt.Errorf("validation[%d] %q: expression must return bool, got %s",
				i, v.Expression, ast.OutputType())
		}
		prg, err := env.Program(ast)
		if err != nil {
			return nil, fmt.Errorf("validation[%d] %q: build program: %w", i, v.Expression, err)
		}
		programs = append(programs, compiledValidation{Rule: v, Program: prg})
	}
	return &Engine{
		mutations:   cfg.Mutations,
		validations: programs,
	}, nil
}

// Mutate applies all configured mutations to raw (an object encoded
// as JSON). Returns the possibly-modified JSON; the boolean reports
// whether any byte actually changed.
func (e *Engine) Mutate(op string, raw []byte) ([]byte, bool, error) {
	if e == nil || len(e.mutations) == 0 {
		return raw, false, nil
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, false, fmt.Errorf("mutate: decode object: %w", err)
	}
	changed := false
	for _, m := range e.mutations {
		if !appliesTo(m.Operations, op) {
			continue
		}
		before, _ := json.Marshal(obj)
		if err := mutatePath(obj, m.JSONPath, m.Op, m.Value); err != nil {
			return nil, false, err
		}
		after, _ := json.Marshal(obj)
		if string(before) != string(after) {
			changed = true
		}
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return nil, false, fmt.Errorf("mutate: reencode: %w", err)
	}
	return out, changed, nil
}

// ValidationFailure describes one failed validation.
type ValidationFailure struct {
	Message   string
	FieldPath string
}

// Validate runs every configured CEL expression. Returns the list
// of failures; empty means "allowed".
func (e *Engine) Validate(op string, raw, oldRaw []byte) ([]ValidationFailure, error) {
	if e == nil || len(e.validations) == 0 {
		return nil, nil
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("validate: decode object: %w", err)
	}
	var oldObj any // left as nil if oldRaw is empty
	if len(oldRaw) > 0 {
		var om map[string]any
		if err := json.Unmarshal(oldRaw, &om); err != nil {
			return nil, fmt.Errorf("validate: decode oldObject: %w", err)
		}
		oldObj = om
	}
	var failures []ValidationFailure
	for _, v := range e.validations {
		if !appliesTo(v.Rule.Operations, op) {
			continue
		}
		val, _, err := v.Program.Eval(map[string]any{
			"object":    obj,
			"oldObject": oldObj,
		})
		if err != nil {
			// Runtime errors (e.g. `has(object.spec.title)` against
			// a non-existent parent) are treated as "rule cannot
			// decide" → deny. The alternative (allow on error)
			// fails open and is worse.
			failures = append(failures, ValidationFailure{
				Message:   fmt.Sprintf("%s [cel eval error: %v]", v.Rule.Message, err),
				FieldPath: v.Rule.FieldPath,
			})
			continue
		}
		b, ok := toBool(val)
		if !ok {
			failures = append(failures, ValidationFailure{
				Message:   fmt.Sprintf("%s [non-bool result: %v]", v.Rule.Message, val),
				FieldPath: v.Rule.FieldPath,
			})
			continue
		}
		if !b {
			msg := v.Rule.Message
			if msg == "" {
				msg = "validation failed: " + v.Rule.Expression
			}
			failures = append(failures, ValidationFailure{
				Message:   msg,
				FieldPath: v.Rule.FieldPath,
			})
		}
	}
	return failures, nil
}

func toBool(v ref.Val) (bool, bool) {
	if v == nil {
		return false, false
	}
	if bv, ok := v.(types.Bool); ok {
		return bool(bv), true
	}
	// Fallback via ConvertToNative, in case cel-go's types surface a
	// different concrete type.
	out, err := v.ConvertToNative(boolType)
	if err == nil {
		if b, ok := out.(bool); ok {
			return b, true
		}
	}
	return false, false
}

// boolType is a reflect.Type cache used by ConvertToNative; pulled
// out so we don't allocate per eval.
var boolType = reflectTypeOf(true)
