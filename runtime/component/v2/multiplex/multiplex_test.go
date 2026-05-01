package multiplex

import (
	"encoding/json"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/cheeseandcereal/aggexp/runtime/component/v2/admission"
)

func TestParseAPIDef_Happy(t *testing.T) {
	u := &unstructured.Unstructured{}
	raw := []byte(`{
		"apiVersion":"aggexp.io/v1",
		"kind":"APIDefinition",
		"metadata":{"name":"widgets.aggexp.io"},
		"spec":{
			"group":"widgets.aggexp.io","version":"v1","kind":"Widget",
			"plural":"widgets","singular":"widget","scope":"Namespaced",
			"schemaSource":"backendJSONSchema",
			"backend":{"transport":"http","address":"http://widget-backend.svc:8080"},
			"watchCapability":"push",
			"admission":{
				"validations":[
					{"expression":"true","message":"ok","fieldPath":"spec.x"}
				]
			}
		}
	}`)
	_ = json.Unmarshal(raw, &u.Object)
	parsed, err := parseAPIDef(u)
	if err != nil {
		t.Fatalf("parseAPIDef: %v", err)
	}
	if parsed.Spec.Group != "widgets.aggexp.io" {
		t.Errorf("group=%s", parsed.Spec.Group)
	}
	if parsed.Spec.Backend.Transport != "http" {
		t.Errorf("backend.transport=%s", parsed.Spec.Backend.Transport)
	}
	if parsed.Spec.WatchCapability != "push" {
		t.Errorf("watchCapability=%s", parsed.Spec.WatchCapability)
	}
	if len(parsed.Spec.Admission.Validations) != 1 {
		t.Errorf("admission.validations: %+v", parsed.Spec.Admission)
	}
}

func TestParseAPIDef_MissingRequired(t *testing.T) {
	u := &unstructured.Unstructured{}
	_ = json.Unmarshal([]byte(`{
		"apiVersion":"aggexp.io/v1","kind":"APIDefinition",
		"metadata":{"name":"x"},"spec":{"group":"g"}
	}`), &u.Object)
	_, err := parseAPIDef(u)
	if err == nil {
		t.Errorf("expected error on incomplete spec")
	}
}

func TestAPIDefinition_DeepCopy(t *testing.T) {
	a := &APIDefinition{
		Spec: APIDefinitionSpec{
			Group: "g", Version: "v", Kind: "K", Plural: "ks", Singular: "k",
			Backend: BackendSpec{Transport: "http", Address: "http://x"},
			Admission: admission.Config{
				Validations: []admission.Validation{{Expression: "true", Message: "ok"}},
			},
		},
	}
	cp := a.DeepCopyObject().(*APIDefinition)
	cp.Spec.Group = "other"
	if a.Spec.Group != "g" {
		t.Errorf("DeepCopy leaked group: %s", a.Spec.Group)
	}
}

func TestEmbeddedCRD(t *testing.T) {
	if !strings.Contains(string(APIDefinitionCRDYAML), "apidefinitions.aggexp.io") {
		t.Errorf("embedded CRD YAML missing expected content")
	}
}

func TestBuildAPIServiceUnstructured(t *testing.T) {
	u := buildAPIServiceUnstructured("v1.widgets.aggexp.io",
		schemaGV("widgets.aggexp.io", "v1"),
		"svc", "ns", []byte("ca"))
	if u.GetName() != "v1.widgets.aggexp.io" {
		t.Errorf("name=%s", u.GetName())
	}
	sp, _ := u.Object["spec"].(map[string]interface{})
	if sp["group"] != "widgets.aggexp.io" {
		t.Errorf("spec.group=%v", sp["group"])
	}
	if svc, _ := sp["service"].(map[string]interface{}); svc["name"] != "svc" {
		t.Errorf("spec.service.name=%v", svc["name"])
	}
}

// schemaGV is a tiny helper to avoid importing schema in the test.
func schemaGV(g, v string) schema.GroupVersion { return schema.GroupVersion{Group: g, Version: v} }
