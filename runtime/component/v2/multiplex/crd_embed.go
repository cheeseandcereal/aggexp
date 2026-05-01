package multiplex

import _ "embed"

// APIDefinitionCRDYAML is the embedded CRD manifest for
// apidefinitions.aggexp.io. Consumers can apply it programmatically
// or extract with `kubectl apply -f <(...) `.
//
//go:embed apidefinition-crd.yaml
var APIDefinitionCRDYAML []byte
