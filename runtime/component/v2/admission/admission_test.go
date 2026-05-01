package admission

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMutate_Set(t *testing.T) {
	eng, err := New(Config{
		Mutations: []Mutation{
			{JSONPath: "metadata.annotations.aggexp.io/stamped", Op: "set", Value: "yes"},
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	in := []byte(`{"metadata":{"name":"x"},"spec":{"title":"t"}}`)
	out, changed, err := eng.Mutate("CREATE", in)
	if err != nil {
		t.Fatalf("Mutate: %v", err)
	}
	if !changed {
		t.Errorf("expected changed=true")
	}
	var m map[string]any
	_ = json.Unmarshal(out, &m)
	md, _ := m["metadata"].(map[string]any)
	ann, _ := md["annotations"].(map[string]any)
	if ann["aggexp.io/stamped"] != "yes" {
		t.Errorf("annotation not set: %v", ann)
	}
}

func TestMutate_Default(t *testing.T) {
	eng, _ := New(Config{
		Mutations: []Mutation{
			{JSONPath: "spec.priority", Op: "default", Value: "normal"},
		},
	})
	// Missing -> defaulted.
	out1, _, _ := eng.Mutate("CREATE", []byte(`{"spec":{}}`))
	var m map[string]any
	_ = json.Unmarshal(out1, &m)
	if m["spec"].(map[string]any)["priority"] != "normal" {
		t.Errorf("default not applied: %v", m)
	}
	// Present -> not overridden.
	out2, _, _ := eng.Mutate("CREATE", []byte(`{"spec":{"priority":"high"}}`))
	_ = json.Unmarshal(out2, &m)
	if m["spec"].(map[string]any)["priority"] != "high" {
		t.Errorf("default overrode existing value: %v", m)
	}
}

func TestValidate_Pass(t *testing.T) {
	eng, err := New(Config{
		Validations: []Validation{
			{
				Expression: `has(object.spec.title) && size(object.spec.title) >= 3`,
				Message:    "spec.title must be at least 3 characters",
				FieldPath:  "spec.title",
			},
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	fs, err := eng.Validate("CREATE", []byte(`{"spec":{"title":"hello"}}`), nil)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if len(fs) > 0 {
		t.Errorf("unexpected failures: %v", fs)
	}
}

func TestValidate_MultiFailure(t *testing.T) {
	eng, err := New(Config{
		Validations: []Validation{
			{Expression: `has(object.spec.title)`, Message: "title required", FieldPath: "spec.title"},
			{Expression: `has(object.spec.body)`, Message: "body required", FieldPath: "spec.body"},
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	fs, _ := eng.Validate("CREATE", []byte(`{"spec":{}}`), nil)
	if len(fs) != 2 {
		t.Errorf("want 2 failures, got %d: %v", len(fs), fs)
	}
}

func TestValidate_NonBoolRejects(t *testing.T) {
	_, err := New(Config{
		Validations: []Validation{
			{Expression: `"not bool"`, Message: "should be bool"},
		},
	})
	if err == nil {
		t.Errorf("expected compile-time rejection for non-bool expression")
	}
}

func TestOperationsFilter(t *testing.T) {
	eng, _ := New(Config{
		Validations: []Validation{
			{Expression: `false`, Message: "never create", Operations: []string{"CREATE"}},
		},
	})
	// CREATE hits.
	fs, _ := eng.Validate("CREATE", []byte(`{}`), nil)
	if len(fs) != 1 {
		t.Errorf("CREATE: want 1 failure, got %d", len(fs))
	}
	// UPDATE skipped.
	fs2, _ := eng.Validate("UPDATE", []byte(`{}`), nil)
	if len(fs2) != 0 {
		t.Errorf("UPDATE: want 0 failures, got %d", len(fs2))
	}
}

func TestMutate_AnnotationKeySpecialCase(t *testing.T) {
	// Annotation names containing "." and "/" must not be split
	// further by the path walker.
	eng, _ := New(Config{
		Mutations: []Mutation{
			{JSONPath: "metadata.annotations.argocd.argoproj.io/tracking-id", Op: "set", Value: "abc"},
		},
	})
	out, _, err := eng.Mutate("CREATE", []byte(`{"metadata":{}}`))
	if err != nil {
		t.Fatalf("Mutate: %v", err)
	}
	s := string(out)
	if !strings.Contains(s, `"argocd.argoproj.io/tracking-id":"abc"`) {
		t.Errorf("annotation key lost: %s", s)
	}
}

func TestLoadFromFile_Roundtrip(t *testing.T) {
	// Small smoke test of the YAML loader.
	dir := t.TempDir()
	path := dir + "/cfg.yaml"
	content := []byte(`admission:
  validations:
    - expression: "true"
      message: ok
      fieldPath: spec.x
  mutations:
    - jsonPath: spec.y
      op: set
      value: hello
`)
	if err := writeFile(path, content); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Validations) != 1 || cfg.Validations[0].Message != "ok" {
		t.Errorf("validations wrong: %+v", cfg.Validations)
	}
	if len(cfg.Mutations) != 1 || cfg.Mutations[0].Value != "hello" {
		t.Errorf("mutations wrong: %+v", cfg.Mutations)
	}
}
