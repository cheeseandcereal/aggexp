# AGENTS.md

This repo is an experimentation lab around Kubernetes aggregate APIs.
Most code is written by AI agents. The ethos is unusual; default
agent habits (over-engineering, deduplication, completing tasks and
stopping) will actively fight it. Read this file carefully before
touching anything.

## Before doing anything: required reading

At the start of every task, read these files. They are short. This is
not optional.

1. `VISION.md` — why this repo exists.
2. `ETHOS.md` — how work happens here.
3. `SYNTHESIS.md` — current best understanding of the problem space.
4. Any `FINDINGS/NNNN-*.md` files related to fundamentals your task
   touches. If you're unsure, skim titles; they are named by slug.
5. `EXPERIMENTS.md` — the menu of candidate experiments.

Only then begin the task.

The rediscovery failure mode — an agent re-derives something a previous
agent already figured out but didn't read — is the single biggest risk
in a multi-session, agent-authored repo. SYNTHESIS exists to prevent
it. Use it.

## Mental model

- **Spine** (stable): `VISION.md`, `ETHOS.md`, `ARCHITECTURE.md`,
  `SYNTHESIS.md`, `EXPERIMENTS.md`, `AGENTS.md`, `README.md`,
  `FINDINGS/*`, `hack/*`, `deploy/manifests/*`, `go.mod`,
  `.gitignore`, `UNLICENSE`. Changes here are deliberate.
- **Experiments** (`experiments/NNNN-slug/`): disposable. Any
  language, any style, no tests required. Do not refactor across
  experiment boundaries.
- **Substrate** (`runtime/`, `drivers/`): disciplined. Tests and
  package docs required. Code only arrives here via explicit
  promotion tasks.

The two deliverables:

- **MVP-lab**: the repo functions as a lab. See `ETHOS.md`.
- **MVP-example**: `kubectl get repos` returns the caller's GitHub
  repos with identity-aware authz and live watch. See
  `EXPERIMENTS.md`.

## The six named fundamentals

Experiments probe these; FINDINGS reference them by name; SYNTHESIS
organizes around them.

1. Wire protocol fidelity
2. Identity handoff
3. Storage independence
4. Per-request authorization
5. Resource modeling freedom
6. Watch and consistency semantics

## Fundamental vs. consequent

Every observation is one of:

- **Fundamental**: flows from the architecture of aggregated APIs
  itself. Generalizes.
- **Consequent**: tied to specific library versions, kube-apiserver
  quirks, tool behavior, environmental specifics. Real, worth
  recording, but does not generalize.

FINDINGS files should call consequents out explicitly (as prose —
there is no template field).

## What each living document is for

Agents confuse these. Do not.

- `FINDINGS/NNNN-*.md`: immutable, per-experiment, what happened and
  what it taught at the time. Silent about the broader problem space;
  silent about code structure.
- `ARCHITECTURE.md`: current architectural state of the **substrate**
  (`runtime/`, `drivers/`). Silent about individual experiments.
  Silent about the broader problem space. Rewritten when the substrate
  actually shifts.
- `SYNTHESIS.md`: current best understanding of the **problem space**
  (aggregated APIs and their boundaries). Silent about code structure.
  Rewritten when the author's mental model shifts meaningfully.
- `EXPERIMENTS.md`: the menu of candidate experiments grouped by
  fundamental. No required ordering. Updated when the menu changes.

## Hard rules

- Never promote experiment code into `runtime/` or `drivers/` unless
  your task explicitly says "promote ... to substrate".
- Never refactor across experiment boundaries.
- Experiments with a FINDINGS file are **frozen**. Do not touch them
  unless your task explicitly names that experiment.
- Experiments may be ugly, duplicative, polyglot. Do not "improve"
  them.
- `SYNTHESIS.md` is rewritten, not appended. Only when understanding
  shifts.
- `ARCHITECTURE.md` is rewritten, not appended. Only when the
  substrate's architecture actually shifts.
- `FINDINGS/NNNN-*.md` files are immutable once written. New learnings
  go in SYNTHESIS or in a new FINDINGS file referenced from a new
  experiment.
- Run `hack/test-compat.sh` at phase boundaries or when asked. The
  output is committed as a dated file in `FINDINGS/compat/`.
- Run `hack/verify-spine.sh` before declaring any task complete.
- All git commits must use `git -c commit.gpgsign=false commit ...` —
  the ambient git config requires GPG signing which is interactive
  and will fail in agent sessions.

## Task-completion checklists

Tasks are not complete until the applicable checklist is satisfied.

### Start an experiment

- Create `experiments/NNNN-<kebab-case-slug>/` with zero-padded
  4-digit number.
- Copy contents of `experiments/TEMPLATE/` as the starting skeleton.
- Fill out the README: hypothesis, how to run, status = in-progress,
  decisions made (starts empty), what we're looking to learn.
- Commit with message `experiment(NNNN): start <slug>`.

### Complete an experiment

- Code runs from a clean checkout following the steps in the
  experiment's README.
- README "Status" updated to `complete` (or `abandoned` with a brief
  reason).
- `FINDINGS/NNNN-<slug>.md` written with free-text prose describing:
  what you were trying to learn, what you did, what you observed, what
  surprised you, what fundamentals were touched and what was learned
  about them, what consequents were noted, what this changes for
  SYNTHESIS and EXPERIMENTS, what open questions this raised. Omit
  sections that don't apply. Don't pad. Don't leave "N/A"
  placeholders.
- If this is a phase boundary, run `hack/test-compat.sh` and commit
  the resulting `FINDINGS/compat/<date>.md`.
- Update `SYNTHESIS.md` if your understanding shifted. Rewrite, don't
  append.
- Update `EXPERIMENTS.md` if the menu changed.
- Run `hack/verify-spine.sh` and confirm it exits 0.
- Commit with message `findings(NNNN): complete <slug>` (or similar).

### Promote to substrate

Only execute if your task explicitly authorizes promotion.

- Move code to `runtime/` or `drivers/`.
- Add tests (real tests, not smoke tests).
- Add package documentation (`doc.go` or equivalent).
- The source experiment stays in place; its FINDINGS file references
  the promotion.
- Update `ARCHITECTURE.md` to reflect the new substrate shape.
- Run `hack/verify-spine.sh`.
- Commit with message `substrate: promote <what> from experiment NNNN`.

### Update SYNTHESIS

- Rewrite affected sections; do not append.
- Organize around the six fundamentals.
- Reference specific FINDINGS files where claims are evidenced.
- Commit with message `spine: update SYNTHESIS (<short why>)`.

### Update ARCHITECTURE

- Rewrite when substrate architecture has actually shifted.
- Keep ASCII diagrams if they help. Don't add diagrams for their own
  sake.
- Commit with message `spine: update ARCHITECTURE (<short why>)`.

## Arbitrary tactical decisions

During an experiment you will make many small, arbitrary choices
(buffer sizes, cache TTLs, page sizes, default ports). You are
authorized to make these decisions yourself. Record each one in the
experiment's README under "Decisions made" with a one-line rationale:

```
- Chose 60s cache TTL arbitrarily; GitHub rate limits may require tuning.
- Default BOOKMARK interval 10s; no basis, see how kubectl reacts.
```

If the decision later affects a finding, the FINDINGS file references
the decision. This preserves experimental signal without forcing you
to ask the user for every tuning knob.

## FINDINGS guidance (prose, no template)

A good FINDINGS file covers, as prose not as filled-in form fields:

- What you were trying to learn or break.
- What you did (minimally — not a tutorial).
- What you observed.
- What surprised you.
- Which fundamentals this touched, by name, and what was learned about
  them.
- Which observations are consequents (explicitly called out as such).
- What this changes for SYNTHESIS and EXPERIMENTS (even if the change
  is "nothing").
- What open questions this raised.

Omit what doesn't apply. Don't write empty sections. Don't pad. Prior
FINDINGS files are better references than any template would be.

## Anti-patterns

Concrete things agents do by default that fight this ethos. Do not do
these:

- **Don't add tests to experiment code.** Experiments are scratch.
- **Don't add CI, GitHub Actions, pre-commit hooks, or linter configs**
  unless a task explicitly says so.
- **Don't introduce a shared helper package "just in case."**
  Duplicate between experiments until two experiments demand the same
  abstraction.
- **Don't apply Kubernetes API conventions** (spec/status split,
  conditions, finalizers, generation) to experiments unless the
  experiment is specifically probing those conventions.
- **Don't silently improve older experiments** while working on a
  newer one. Experiments with FINDINGS are frozen.
- **Don't append to SYNTHESIS** or timestamp FINDINGS after the fact.
  SYNTHESIS is rewritten; FINDINGS are immutable.
- **Don't write a FINDINGS file that reads as a filled-in form** with
  empty sections. Omit what doesn't apply.
- **Don't assume a need for `fieldmanager`, server-side apply, or full
  OpenAPI schemas** unless an experiment is exercising them. They are
  observe-only on the compat scoreboard.
- **Don't pull in controller-runtime, kubebuilder scaffolding, or
  other heavy frameworks** into experiments unless that's what the
  experiment is probing.
- **Don't version, tag, or release anything.** No semver, no
  CHANGELOG, no release tags.

## Naming and structural conventions

- Experiment slugs: `NNNN-<kebab-case-slug>` with zero-padded 4-digit
  number. Numbering is sequential by start time; gaps are fine.
- FINDINGS files mirror experiment slugs:
  `FINDINGS/NNNN-<slug>.md`.
- Compat files: `FINDINGS/compat/YYYY-MM-DD.md` (ISO date, UTC);
  `-NN` suffix if multiple in one day.
- Commit messages:
  - `experiment(NNNN): <what>` — work inside an experiment.
  - `spine: <what>` — `VISION`, `ETHOS`, `README`, `AGENTS`,
    `ARCHITECTURE`, `SYNTHESIS`, `EXPERIMENTS`.
  - `substrate: <what>` — `runtime/` and `drivers/`.
  - `findings(NNNN): <what>` — FINDINGS file changes (rare; most are
    write-once).
  - `hack: <what>` — scripts.
  - `deploy: <what>` — manifests, certs, kind config.
  - `chore: <what>` — housekeeping (gitignore, go.mod bumps).

## Go module layout

The root `go.mod` has module path `github.com/cheeseandcereal/aggexp`.
Experiments share that module by default.

An experiment may opt into its own `go.mod` if it needs to pin
different library versions or isolate dependencies. Document the
reason in the experiment's README. The first experiment (Phase 0
probe) does this because it deliberately uses stdlib only.

## Abandoned experiments

Experiments that don't pan out stay in the repo. Update the README's
Status to `abandoned`, and write a short FINDINGS file (1–3
paragraphs) explaining why. This preserves the "we tried X" signal.
Deleting abandoned experiments loses the signal.

## External state dependencies

If an experiment needs a secret (PAT, GitHub App key, etc.):

- List required environment variables in a `.env.example` in the
  experiment directory.
- Name external setup in a README "Prerequisites" section.
- Never commit a real secret. `.env` files are gitignored.

## Process is mutable

If the rules in this file or in ETHOS.md are visibly failing an
experiment, note the observation in a "Process observations" section
at the bottom of SYNTHESIS.md. When a pattern emerges, rewrite this
file. Process changes are spine-level commits with a `spine:` prefix.

## When to ask vs. when to proceed

**Proceed without asking:**
- Tactical choices inside an experiment (record in README's "Decisions
  made").
- Small refactors *within* the current experiment.
- Choosing which experiment to start next from `EXPERIMENTS.md` when
  no specific experiment was named.
- Writing FINDINGS and updating SYNTHESIS after an experiment.

**Ask the user:**
- Spine changes (VISION, ETHOS, the fundamentals list).
- Promotions to `runtime/` or `drivers/`.
- New dependencies in the substrate.
- Deleting any file.
- When you disagree with something in this file.

## Working with git

- Use `git -c commit.gpgsign=false commit -m "..."` for every commit.
  The ambient git config requires GPG signing.
- Never force-push.
- Never skip hooks (`--no-verify`).
- Worktrees may be used for parallel work, under `/tmp/aggexp-wt/`.

## Last resort: when in doubt

When in doubt, ask. A question is cheaper than a wrong implementation
that has to be unwound. This is especially true for spine changes and
promotions.
