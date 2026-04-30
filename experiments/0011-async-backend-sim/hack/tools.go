//go:build tools
// +build tools

// Package tools pins build-time tool dependencies (code-generator,
// openapi-gen) so `go mod tidy` keeps them in go.sum and the module
// cache, even though no production code imports them.
package tools

import (
	_ "k8s.io/code-generator"
	_ "k8s.io/kube-openapi/cmd/openapi-gen"
)
