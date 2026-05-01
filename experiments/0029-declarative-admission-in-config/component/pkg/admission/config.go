// Package admission implements declarative admission evaluated
// entirely inside the middleware (component server). It composes
// additively with the 0020-style backend Validate/Mutate RPCs: the
// middleware rules run first, the backend RPCs run second.
//
// The config is a YAML file loaded at startup. Shape (simplified
// APIDefinition-ish; no CRD, no reconciler — see experiment README):
//
//	apiVersion: aggexp.io/v1alpha1
//	kind: APIDefinition
//	backend:
//	  addr: "..."
//	  # (0020 RPC admission happens if backend advertises supports_*)
//	admission:
//	  mutations:
//	    - jsonPath: spec.body
//	      op: default
//	      value: "(empty body)"
//	  validations:
//	    - expression: 'has(object.spec.title) && size(object.spec.title) >= 3'
//	      message: "spec.title must be at least 3 characters"
//	      fieldPath: spec.title
//
// Mutation ops supported (subset):
//
//   - set:     always write the value at jsonPath, creating parents.
//   - default: write the value only if the path is missing or null.
//
// We deliberately do NOT implement remove / merge / json-patch;
// decision recorded in the experiment README.
//
// Validations are CEL expressions evaluated with two bindings:
//
//   - object:    the current (possibly-mutated) object as map[string]any.
//   - oldObject: the previous object on UPDATE, or nil on CREATE.
//
// An expression returning false is a denial; the message is returned
// to the client verbatim via apierrors.NewInvalid. A CEL compile
// error at config-load time aborts startup; a runtime evaluation
// error is treated as a denial with the error text.
package admission

import (
	"fmt"
	"os"
	"strings"

	"sigs.k8s.io/yaml"
)

// Config is the admission section of the APIDefinition-ish config.
type Config struct {
	Mutations   []Mutation   `json:"mutations,omitempty" yaml:"mutations,omitempty"`
	Validations []Validation `json:"validations,omitempty" yaml:"validations,omitempty"`
}

// Mutation applies an op to a JSONPath-like dotted path. Value is
// arbitrary JSON/YAML.
type Mutation struct {
	JSONPath string         `json:"jsonPath" yaml:"jsonPath"`
	Op       string         `json:"op" yaml:"op"`
	Value    any            `json:"value,omitempty" yaml:"value,omitempty"`
	// Operations is the list of operations this mutation applies
	// to; empty means both CREATE and UPDATE.
	Operations []string `json:"operations,omitempty" yaml:"operations,omitempty"`
}

// Validation is one CEL expression + denial message + optional field
// path.
type Validation struct {
	Expression string   `json:"expression" yaml:"expression"`
	Message    string   `json:"message" yaml:"message"`
	FieldPath  string   `json:"fieldPath,omitempty" yaml:"fieldPath,omitempty"`
	Operations []string `json:"operations,omitempty" yaml:"operations,omitempty"`
}

// APIDefinition is the outer shape. Only the admission section is
// load-bearing for this experiment; the backend block is informational.
type APIDefinition struct {
	APIVersion string        `json:"apiVersion,omitempty" yaml:"apiVersion,omitempty"`
	Kind       string        `json:"kind,omitempty" yaml:"kind,omitempty"`
	Backend    BackendBlock  `json:"backend,omitempty" yaml:"backend,omitempty"`
	Admission  Config        `json:"admission,omitempty" yaml:"admission,omitempty"`
}

// BackendBlock is informational; the actual backend address is still
// passed via CLI flag in this experiment.
type BackendBlock struct {
	Addr string `json:"addr,omitempty" yaml:"addr,omitempty"`
}

// Load reads and parses the YAML config.
func Load(path string) (*APIDefinition, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read admission config %q: %w", path, err)
	}
	var out APIDefinition
	if err := yaml.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("parse admission config %q: %w", path, err)
	}
	// Normalize ops to upper-case.
	for i := range out.Admission.Mutations {
		out.Admission.Mutations[i].Operations = normalizeOps(out.Admission.Mutations[i].Operations)
	}
	for i := range out.Admission.Validations {
		out.Admission.Validations[i].Operations = normalizeOps(out.Admission.Validations[i].Operations)
	}
	return &out, nil
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

// appliesTo is true if the per-rule operation filter matches op, or
// the filter is empty (meaning "all ops").
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
