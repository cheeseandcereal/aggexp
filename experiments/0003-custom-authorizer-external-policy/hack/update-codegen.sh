#!/usr/bin/env bash
# Regenerate deepcopy and openapi for experiment 0002.
#
# Hard-requires:
#   - go (1.23+)
#   - kube_codegen.sh from k8s.io/code-generator in the module cache
#     (pulled by go mod once code-generator is a tool dep).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULE="github.com/cheeseandcereal/aggexp/experiments/0003-custom-authorizer-external-policy"
BOILERPLATE="${HERE}/hack/boilerplate.go.txt"

# Resolve the code-generator package from the module cache. This picks
# the exact version pinned in go.mod (v0.32.x), which matches the
# k8s.io/apiserver version we build against.
cd "${HERE}"
CODEGEN_PKG="$(go list -m -f '{{.Dir}}' k8s.io/code-generator)"

# shellcheck disable=SC1091
source "${CODEGEN_PKG}/kube_codegen.sh"

kube::codegen::gen_helpers \
	--boilerplate "${BOILERPLATE}" \
	"${HERE}/pkg/apis"

kube::codegen::gen_openapi \
	--output-dir "${HERE}/pkg/generated/openapi" \
	--output-pkg "${MODULE}/pkg/generated/openapi" \
	--report-filename "${HERE}/pkg/generated/openapi/violation_exceptions.list" \
	--update-report \
	--boilerplate "${BOILERPLATE}" \
	"${HERE}/pkg/apis"

echo "codegen complete"
