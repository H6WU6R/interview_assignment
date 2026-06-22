# Documentation Site Design

Date: 2026-06-23

## Context

The repository already contains most of the explanatory raw material for the
Calais execution algorithm assignment:

- `README.md` explains the project layout, architecture, risk invariant, API
  usage, simulator scripts, Binance Testnet path, verification, and limitations.
- `reports/report_draft.md` expands the assignment requirements into a detailed
  correctness narrative and requirement matrix.
- `tests/simulation/test_required_scenarios.py` contains named scenario tests
  that map directly to the assignment's pass/fail risks.
- `scripts/run_sim_chase.py`, `scripts/run_sim_twap.py`,
  `scripts/run_sim_cancel_race.py`, and `scripts/run_sim_create_timeout.py`
  provide deterministic, runnable examples.

The target style is similar to skfolio: a Python documentation site with a
short landing page, a User Guide, Examples, and an API Reference. This project
should adapt that structure to the assignment rather than copying skfolio's
domain organization.

## Audience And Priority

The primary audience is assignment evaluators. The documentation should help
them quickly answer whether the project satisfies the Calais brief and whether
the candidate understands the implementation.

The secondary audience is a future developer or reviewer who wants to navigate
the codebase, run examples, and inspect public interfaces.

This means the guide should be organized around execution correctness,
assignment requirements, and evidence. It should not read like a generic package
manual or only list modules.

## Goals

1. Provide a skfolio-like documentation site with navigation, search, guide
   pages, examples, and API reference.
2. Explain the modular design system: API/runtime, service/engine, algorithms,
   exchange adapters, risk validation, and observability.
3. Make the safety story explicit: target-position math, price bounds, Decimal
   handling, exposure reservation, UNKNOWN create outcomes, cancel/fill races,
   cumulative fill monotonicity, and TWAP scheduling.
4. Turn simulator scripts and required scenario tests into evaluator-friendly
   examples with commands, expected results, and artifact descriptions.
5. Keep docs buildable locally with a single command.
6. Link the documentation from `README.md` without bloating the README.

## Non-Goals

- Do not build a polished hosted website pipeline in the first pass.
- Do not add notebook-based or plot-heavy examples unless they become useful
  later.
- Do not use Sphinx Gallery initially. The current examples are command-driven
  simulator demos and scenario explanations, so curated Markdown pages are
  clearer.
- Do not document every private helper. The API reference should focus on
  module-level public classes and functions that help reviewers understand the
  design.
- Do not replace the final assignment PDF report. The docs complement the PDF
  and can supply deeper navigable explanation.

## Tooling

Use Sphinx with the PyData Sphinx Theme because it matches the skfolio-style
documentation model and supports Python API docs well.

Add development documentation dependencies:

- `sphinx`
- `pydata-sphinx-theme`
- `myst-parser`
- `sphinx-autodoc-typehints`

Use MyST Markdown for guide and example pages. Use reStructuredText only where
Sphinx API directives are simpler.

The local build command should be:

```bash
uv run sphinx-build -b html docs/source docs/_build/html
```

## Proposed Directory Structure

```text
docs/
  source/
    conf.py
    index.md
    user_guide/
      index.md
      assignment_requirements.md
      architecture.md
      execution_lifecycle.md
      safety_invariants.md
      chase.md
      twap.md
      binance_testnet.md
      observability.md
      limitations.md
    examples/
      index.md
      normal_chase.md
      normal_twap.md
      cancel_fill_race.md
      create_timeout_reconciliation.md
      price_outside_range.md
      duplicate_fill_events.md
      cross_zero_position.md
    api/
      index.rst
      algorithms.rst
      api_runtime.rst
      exchanges.rst
      execution.rst
      observability.rst
      risk.rst
```

`docs/superpowers/` remains planning-only material and should not be included in
the public documentation toctree.

## Site Information Architecture

### Landing Page

The landing page should answer four questions quickly:

1. What is this project?
2. How do I run the local verification?
3. Where is the correctness explanation?
4. Where are the runnable examples and API reference?

It should include a compact quickstart:

```bash
uv sync
uv run pytest -q
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py
uv run python scripts/run_sim_create_timeout.py
```

### User Guide

The User Guide should be the main evaluator path.

- `assignment_requirements.md`: map Calais brief requirements to repository
  coverage. This can adapt the existing requirement matrix from
  `reports/report_draft.md`.
- `architecture.md`: explain the layered design:
  `ExecutionRuntime`, `ExecutionService`, `ExecutionEngine`, algorithms,
  exchange adapters, risk validation, and observability.
- `execution_lifecycle.md`: describe execution states, child order states,
  terminal states, and per-execution serialization.
- `safety_invariants.md`: explain the exposure invariant and why each reserved
  bucket exists.
- `chase.md`: explain passive pricing, repricing threshold, minimum interval,
  `ADVERSE_ONLY` default, `TWO_SIDED` option, and replacement sizing after
  partial fills.
- `twap.md`: explain absolute schedule, cumulative deficit, carry-forward
  behavior, safe child sizing, and rounding remainder.
- `binance_testnet.md`: explain Testnet adapter behavior, credentials,
  explicit send confirmation, stream supervision, reconciliation, and mainnet
  guardrails.
- `observability.md`: explain JSONL/JSON/CSV artifacts, sanitized logs,
  summaries, TWAP slice ledger, and evidence bundles.
- `limitations.md`: explain in-memory persistence, one-symbol scope, one-way
  mode, compact runtime supervision, Testnet evidence limitations, and hard
  disabled mainnet mutations.

### Examples

Examples should be proof pages, not just tutorials. Each page should include:

- The scenario and assignment risk it covers.
- The command or test reference.
- The expected important result.
- The artifacts or response fields a reviewer should inspect.
- Links to related User Guide and API pages.

Initial examples:

- `normal_chase.md`: `scripts/run_sim_chase.py`; passive best-bid order,
  traceable client order ID, exposure reservation.
- `normal_twap.md`: `scripts/run_sim_twap.py`; absolute schedule and first
  slice behavior.
- `cancel_fill_race.md`: `scripts/run_sim_cancel_race.py` and
  `test_t3_cancel_fill_race_updates_confirmed_fills_before_replacement_sizing`;
  fill during cancel reduces replacement size.
- `create_timeout_reconciliation.md`: `scripts/run_sim_create_timeout.py`,
  `test_t4a_create_timeout_reconciles_to_open_order_without_new_client_order_id`,
  and `test_t4b_create_timeout_not_found_releases_unknown_exposure_before_safe_retry`;
  UNKNOWN exposure blocks retries until exact reconciliation.
- `price_outside_range.md`:
  `test_t7_price_outside_range_waits_then_expires_without_invalid_order`;
  no invalid order is submitted when price is outside bounds.
- `duplicate_fill_events.md`:
  `test_t9_duplicate_fill_event_does_not_double_count_cumulative_fill`;
  cumulative fill monotonicity avoids double counting.
- `cross_zero_position.md`:
  `test_t10_cross_zero_position_uses_target_minus_current_absolute_quantity`;
  target position is not treated as target order quantity.

### API Reference

Use `autodoc` and `autosummary` style pages for public modules. Because the
source currently has minimal docstrings, implementation should add concise
docstrings to high-value public classes and functions instead of documenting
private internals.

Initial API pages:

- Algorithms: `algorithms.chase`, `algorithms.twap`
- API/runtime: `api.app`, `api.runtime`, `api.schemas`
- Execution: `execution.models`, `execution.engine`, `execution.service`,
  `execution.state_machine`, `execution.ids`, `execution.clock`
- Exchanges: `exchanges.base`, `exchanges.simulator`,
  `exchanges.binance_usdm`
- Risk: `risk.decimal_math`, `risk.validation`
- Observability: `observability.artifacts`, `observability.logging`,
  `observability.summary`

## Source Content Reuse

The first implementation pass should mostly restructure existing text:

- Pull architecture and invariants from `README.md` and `reports/report_draft.md`.
- Pull requirement mapping from `reports/report_draft.md`.
- Pull example scenario names and expected outcomes from
  `tests/simulation/test_required_scenarios.py`.
- Pull command examples from `README.md` and `scripts/`.
- Keep `AI_USAGE.md` as a separate submission artifact and link to it from the
  landing page or limitations page only if useful.

Avoid duplicating large blocks verbatim in multiple pages. Prefer one canonical
guide section and cross-link from examples.

## Documentation Tone

The docs should be direct and evaluator-focused:

- State what the system guarantees.
- State where the guarantee is implemented.
- State which test or script proves it.
- State the known limitation when the guarantee is intentionally scoped.

Avoid marketing language. Avoid claiming production readiness. Use the phrase
"small and correct" only where it describes the assignment scope.

## Build And Verification

Implementation is complete when these commands pass:

```bash
uv run sphinx-build -b html docs/source docs/_build/html
uv run pytest -q
```

If docs dependencies are not installed by default, `uv sync --group dev` should
install them after `pyproject.toml` is updated.

The build should fail on broken references where practical. Warnings should be
kept low enough that reviewers can trust the docs.

## Acceptance Criteria

1. `README.md` links to the generated documentation source and build command.
2. `docs/source/index.md` provides the quickstart and navigation into User
   Guide, Examples, and API Reference.
3. User Guide pages explain every assignment-critical behavior listed in this
   spec.
4. Examples pages cover at least Chase, TWAP, cancel/fill race, create-timeout
   reconciliation, price-outside-range, duplicate fills, and cross-zero target
   position.
5. API Reference pages build and include the high-value public modules.
6. Key public classes/functions have concise docstrings where the generated API
   reference would otherwise be empty or unclear.
7. `uv run sphinx-build -b html docs/source docs/_build/html` succeeds.
8. Existing tests continue to pass.

## Risks And Mitigations

- Risk: API reference is too sparse because current source files have almost no
  docstrings.
  Mitigation: add concise public docstrings only to high-value classes and
  functions, avoiding a broad docstring campaign.

- Risk: Documentation drifts from the final PDF report.
  Mitigation: treat `reports/report_draft.md` as the source for the report and
  the docs as a navigable explanation. Cross-link concepts but do not duplicate
  every report paragraph.

- Risk: Example pages become stale if outputs include generated IDs.
  Mitigation: show stable output fields and explain generated IDs as examples,
  rather than hard-coding exact IDs.

- Risk: Adding Sphinx dependencies changes the development environment.
  Mitigation: keep them in the dev dependency group only.

- Risk: Sphinx build warnings hide real broken docs.
  Mitigation: keep the first pass small and fix warnings during implementation.

## Implementation Sequence

1. Add documentation dependencies and Sphinx configuration.
2. Create the landing page, User Guide index, Examples index, and API index.
3. Write the highest-value User Guide pages from existing README/report content.
4. Write the initial example pages from simulator scripts and scenario tests.
5. Add focused docstrings for public API reference pages.
6. Update `README.md` with documentation build instructions.
7. Run Sphinx build and local tests; fix docs warnings or regressions.
