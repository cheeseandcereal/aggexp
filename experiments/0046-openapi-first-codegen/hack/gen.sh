#!/usr/bin/env bash
set -euo pipefail

# Re-run the oapigen generator and confirm the working tree is clean
# afterward. A clean tree proves the generated output in
# pkg/apis/widgets/v1 is byte-identical to a fresh generation from the
# same input + config + tool version (the reproducibility invariant).
#
# Run from the experiment directory (or anywhere; we cd to the script's
# parent).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${EXP_DIR}"

echo "==> running oapigen"
go run ./cmd/oapigen \
  --config testdata/oapigen.yaml \
  --openapi testdata/widget.openapi.yaml

echo "==> checking generated tree is clean"
# Limit the diff check to the generated package so unrelated edits in
# the worktree don't trip the check.
if ! git diff --quiet -- pkg/apis/widgets/v1; then
  echo "ERROR: generated output changed; not reproducible. Diff:" >&2
  git --no-pager diff -- pkg/apis/widgets/v1 >&2
  exit 1
fi

# Also fail if the generator left any *.broken debug file.
if ls pkg/apis/widgets/v1/*.broken >/dev/null 2>&1; then
  echo "ERROR: generator emitted a .broken file (format failure)" >&2
  exit 1
fi

echo "OK: generated output is byte-identical (reproducible)."
