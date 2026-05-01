// Package admission is the v2 declarative admission engine. It
// evaluates CEL validation expressions and JSONPath-style mutations
// entirely in the middleware; no backend round-trip.
//
// Composes additively with backend-side admission RPCs (v2/proto's
// Validate / Mutate, from 0020). Middleware rules run FIRST:
// the middleware mutates the object, the middleware validates,
// backend.Mutate runs if supports_mutation, backend.Validate runs
// if supports_validation. A denial at any layer stops the write.
// See FINDINGS/0029.
//
// Wire shape on denial: apierrors.NewInvalid(GK, name,
// field.ErrorList{...}) → HTTP 422 with multi-cause body. Identical
// to kube-apiserver's built-in validation, ValidatingAdmissionPolicy,
// and backend-RPC denials.
//
// # Supported primitives
//
// Validations are CEL expressions returning bool. Two variables
// bound: `object` (the request object as map[string]any) and
// `oldObject` (nil on CREATE). A runtime evaluation error is a
// denial with the error text — fail-closed.
//
// Mutations are JSONPath-addressed set/default ops. Path grammar is
// dotted; annotation and label names (which may contain "." and "/")
// are handled via a special case once the walker enters
// metadata.annotations or metadata.labels.
package admission

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"sigs.k8s.io/yaml"
)

// Config is the admission block of an APIDefinition (the shape
// mirrors the 0022 thesis's AdmissionConfig; v2's multiplex package
// loads it from the APIDefinition CRD).
type Config struct {
	Mutations   []Mutation   `json:"mutations,omitempty" yaml:"mutations,omitempty"`
	Validations []Validation `json:"validations,omitempty" yaml:"validations,omitempty"`
}

// Mutation is a JSONPath-addressed defaulting rule.
type Mutation struct {
	JSONPath   string   `json:"jsonPath" yaml:"jsonPath"`
	Op         string   `json:"op" yaml:"op"` // "set" or "default"
	Value      any      `json:"value,omitempty" yaml:"value,omitempty"`
	Operations []string `json:"operations,omitempty" yaml:"operations,omitempty"` // "CREATE","UPDATE"; empty = both
}

// Validation is one CEL expression + denial message.
type Validation struct {
	Expression string   `json:"expression" yaml:"expression"`
	Message    string   `json:"message" yaml:"message"`
	FieldPath  string   `json:"fieldPath,omitempty" yaml:"fieldPath,omitempty"`
	Operations []string `json:"operations,omitempty" yaml:"operations,omitempty"`
}

// LoadFromFile reads a YAML file containing only an admission
// Config. Convenience for static-config consumers.
func LoadFromFile(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read admission config %q: %w", path, err)
	}
	var out struct {
		Admission Config `json:"admission,omitempty" yaml:"admission,omitempty"`
	}
	if err := yaml.Unmarshal(raw, &out); err != nil {
		return Config{}, fmt.Errorf("parse admission config %q: %w", path, err)
	}
	return normalize(out.Admission), nil
}

// Normalize uppercases per-rule operation filters.
func normalize(c Config) Config {
	for i := range c.Mutations {
		c.Mutations[i].Operations = normalizeOps(c.Mutations[i].Operations)
	}
	for i := range c.Validations {
		c.Validations[i].Operations = normalizeOps(c.Validations[i].Operations)
	}
	return c
}

func normalizeOps(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for _, op := range in {
		out = append(out, strings.ToUpper(strings.TrimSpace(op)))
	}
	return out
}

func appliesTo(ops []string, op string) bool {
	if len(ops) == 0 {
		return true
	}
	for _, o := range ops {
		if o == op {
			return true
		}
	}
	return false
}

// Engine holds compiled CEL programs and raw mutation rules.
type Engine struct {
	mutations   []Mutation
	validations []compiledValidation
}

type compiledValidation struct {
	Rule    Validation
	Program cel.Program
}

// New compiles cfg; returns startup error on bad CEL.
func New(cfg Config) (*Engine, error) {
	cfg = normalize(cfg)
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
	return &Engine{mutations: cfg.Mutations, validations: programs}, nil
}

// Mutate applies all configured mutations to raw JSON. Returns the
// possibly-modified bytes and a changed flag. No-op safe when e is
// nil or the engine has no mutations.
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

// Failure is one failed validation.
type Failure struct {
	Message   string
	FieldPath string
}

// Validate runs all CEL expressions. Empty return means allowed.
func (e *Engine) Validate(op string, raw, oldRaw []byte) ([]Failure, error) {
	if e == nil || len(e.validations) == 0 {
		return nil, nil
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("validate: decode object: %w", err)
	}
	var oldObj any
	if len(oldRaw) > 0 {
		var om map[string]any
		if err := json.Unmarshal(oldRaw, &om); err != nil {
			return nil, fmt.Errorf("validate: decode oldObject: %w", err)
		}
		oldObj = om
	}
	var failures []Failure
	for _, v := range e.validations {
		if !appliesTo(v.Rule.Operations, op) {
			continue
		}
		val, _, err := v.Program.Eval(map[string]any{
			"object":    obj,
			"oldObject": oldObj,
		})
		if err != nil {
			failures = append(failures, Failure{
				Message:   fmt.Sprintf("%s [cel eval error: %v]", v.Rule.Message, err),
				FieldPath: v.Rule.FieldPath,
			})
			continue
		}
		b, ok := toBool(val)
		if !ok {
			failures = append(failures, Failure{
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
			failures = append(failures, Failure{
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
	out, err := v.ConvertToNative(reflect.TypeOf(true))
	if err == nil {
		if b, ok := out.(bool); ok {
			return b, true
		}
	}
	return false, false
}

// --- JSONPath mutator ---

func mutatePath(obj map[string]any, path, op string, value any) error {
	if path == "" {
		return fmt.Errorf("mutation: empty jsonPath")
	}
	parts := splitPath(path)
	return walkAndApply(obj, parts, op, value)
}

func splitPath(p string) []string {
	raw := strings.Split(p, ".")
	if len(raw) >= 3 && raw[0] == "metadata" &&
		(raw[1] == "annotations" || raw[1] == "labels") {
		tail := strings.Join(raw[2:], ".")
		return []string{raw[0], raw[1], tail}
	}
	return raw
}

func walkAndApply(obj map[string]any, parts []string, op string, value any) error {
	if len(parts) == 0 {
		return fmt.Errorf("mutation: empty path parts")
	}
	cur := obj
	for i, part := range parts {
		if i == len(parts)-1 {
			return applyLeaf(cur, part, op, value)
		}
		next, ok := cur[part]
		if !ok || next == nil {
			nm := map[string]any{}
			cur[part] = nm
			cur = nm
			continue
		}
		nm, ok := next.(map[string]any)
		if !ok {
			return fmt.Errorf("mutation: path %q: segment %q is not an object (got %T)", strings.Join(parts, "."), part, next)
		}
		cur = nm
	}
	return nil
}

func applyLeaf(parent map[string]any, key, op string, value any) error {
	switch op {
	case "set":
		parent[key] = value
		return nil
	case "default":
		existing, present := parent[key]
		if present && existing != nil {
			switch v := existing.(type) {
			case string:
				if v != "" {
					return nil
				}
			case map[string]any:
				if len(v) > 0 {
					return nil
				}
			default:
				return nil
			}
		}
		parent[key] = value
		return nil
	default:
		return fmt.Errorf("mutation: unknown op %q (supported: set, default)", op)
	}
}
