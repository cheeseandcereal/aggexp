#!/usr/bin/env bash
set -euo pipefail

# Self-check the "spine" of the repo: the set of files and structural
# invariants that every experiment relies on. Agents should run this
# before opening a PR. It's intentionally cheap and offline.

ISSUES=()

fail() { ISSUES+=("FAIL: $*"); }

# --- 1. required spine files ------------------------------------------------
SPINE_FILES=(
  VISION.md
  ETHOS.md
  ARCHITECTURE.md
  SYNTHESIS.md
  EXPERIMENTS.md
  AGENTS.md
  README.md
  UNLICENSE
  FINDINGS/0000-goals.md
  go.mod
  .gitignore
)
for f in "${SPINE_FILES[@]}"; do
  if [[ ! -f "${f}" ]]; then
    fail "missing spine file: ${f}"
  elif [[ ! -s "${f}" ]]; then
    fail "empty spine file: ${f}"
  fi
done

# --- 2. compat findings directory -------------------------------------------
if [[ ! -d "FINDINGS/compat" ]]; then
  fail "missing directory: FINDINGS/compat/"
fi

# --- 3. experiment template -------------------------------------------------
if [[ ! -d "experiments/TEMPLATE" ]]; then
  fail "missing directory: experiments/TEMPLATE/"
elif [[ ! -f "experiments/TEMPLATE/README.md" ]]; then
  fail "missing file: experiments/TEMPLATE/README.md"
fi

# --- 4. experiment <-> findings cross-check --------------------------------
# For each experiments/NNNN-* dir, require a README. If its Status line says
# complete/abandoned, require the matching FINDINGS file by NNNN- prefix.
if [[ -d experiments ]]; then
  # Portable listing: find one level deep, name-filtered. We sort for
  # deterministic output.
  while IFS= read -r dir; do
    [[ -z "${dir}" ]] && continue
    base="$(basename "${dir}")"
    # Skip the template dir explicitly.
    [[ "${base}" == "TEMPLATE" ]] && continue
    # Only dirs matching NNNN-<slug>.
    if [[ ! "${base}" =~ ^[0-9]{4}-.+ ]]; then
      continue
    fi
    readme="${dir}/README.md"
    if [[ ! -f "${readme}" ]]; then
      fail "experiment ${base} has no README.md"
      continue
    fi
    # Status line: grep for "Status" at the start of a line, case-insensitive.
    status_line="$(grep -i -m1 '^[[:space:]]*[*_-]*[[:space:]]*Status' "${readme}" || true)"
    prefix="${base%%-*}"  # NNNN
    needs_findings=0
    if [[ -n "${status_line}" ]]; then
      lower="$(printf '%s' "${status_line}" | tr '[:upper:]' '[:lower:]')"
      if [[ "${lower}" == *complete* || "${lower}" == *abandoned* ]]; then
        needs_findings=1
      fi
    fi
    if [[ "${needs_findings}" -eq 1 ]]; then
      # Any FINDINGS/<prefix>-*.md file counts.
      matches=( FINDINGS/"${prefix}"-*.md )
      # Bash globs leave the literal pattern when nothing matches; guard that.
      if [[ ! -e "${matches[0]}" ]]; then
        fail "experiment ${base} is complete/abandoned but FINDINGS/${prefix}-*.md is missing"
      fi
    fi
  done < <(find experiments -mindepth 1 -maxdepth 1 -type d 2>/dev/null | sort)
fi

# --- 5. no orphan FINDINGS --------------------------------------------------
# Every FINDINGS/NNNN-*.md (ignoring the reserved 0000-goals.md) must have a
# matching experiments/NNNN-*/ directory.
if [[ -d FINDINGS ]]; then
  while IFS= read -r f; do
    [[ -z "${f}" ]] && continue
    base="$(basename "${f}" .md)"
    prefix="${base%%-*}"
    # 0000 is reserved for the goals doc, not an experiment.
    [[ "${prefix}" == "0000" ]] && continue
    # Skip anything not matching NNNN-<slug>.
    if [[ ! "${base}" =~ ^[0-9]{4}-.+ ]]; then
      continue
    fi
    matches=( experiments/"${prefix}"-*/ )
    if [[ ! -e "${matches[0]}" ]]; then
      fail "orphan findings: ${f} has no experiments/${prefix}-*/ dir"
    fi
  done < <(find FINDINGS -mindepth 1 -maxdepth 1 -type f -name '*.md' 2>/dev/null | sort)
fi

# --- 6. hack scripts are executable ----------------------------------------
if [[ -d hack ]]; then
  while IFS= read -r s; do
    [[ -z "${s}" ]] && continue
    if [[ ! -x "${s}" ]]; then
      fail "script not executable: ${s}"
    fi
  done < <(find hack -maxdepth 1 -type f -name '*.sh' 2>/dev/null | sort)
fi

# --- report -----------------------------------------------------------------
if [[ ${#ISSUES[@]} -gt 0 ]]; then
  printf '%s\n' "${ISSUES[@]}"
  exit 1
fi
echo "OK: spine invariants hold"
