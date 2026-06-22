# Documentation Site Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a skfolio-style Sphinx documentation site for the Calais execution algorithm project, focused first on assignment evaluators and second on future developers.

**Architecture:** Use Sphinx with PyData Sphinx Theme and MyST Markdown for the public documentation tree under `docs/source/`. Keep public docs separate from `docs/superpowers/`, split conceptual guide pages from proof-oriented example pages, and generate API reference pages from focused public modules after adding concise docstrings to high-value classes and functions.

**Tech Stack:** Python 3.11+, Sphinx, PyData Sphinx Theme, MyST Parser, sphinx-autodoc-typehints, pytest, uv.

---

## File Structure

Create the public documentation tree:

- `docs/source/conf.py`: Sphinx configuration, source path setup, extensions, theme, and warning behavior.
- `docs/source/index.md`: landing page with evaluator quickstart and top-level navigation.
- `docs/source/user_guide/index.md`: guide landing page and reading path.
- `docs/source/user_guide/assignment_requirements.md`: requirement-to-implementation matrix.
- `docs/source/user_guide/architecture.md`: layered system explanation.
- `docs/source/user_guide/execution_lifecycle.md`: execution and child order lifecycle.
- `docs/source/user_guide/safety_invariants.md`: exposure invariant and reserved buckets.
- `docs/source/user_guide/chase.md`: Chase behavior and cancel/replace safety.
- `docs/source/user_guide/twap.md`: TWAP schedule, deficit, and rounding.
- `docs/source/user_guide/binance_testnet.md`: Testnet adapter, credentials, streams, and mainnet guardrails.
- `docs/source/user_guide/observability.md`: artifacts, sanitized logs, summaries, and evidence bundles.
- `docs/source/user_guide/limitations.md`: scoped limitations and evaluator caveats.
- `docs/source/examples/index.md`: examples landing page grouped by assignment risk.
- `docs/source/examples/normal_chase.md`: normal Chase simulator proof.
- `docs/source/examples/normal_twap.md`: normal TWAP simulator proof.
- `docs/source/examples/cancel_fill_race.md`: fill-during-cancel proof.
- `docs/source/examples/create_timeout_reconciliation.md`: UNKNOWN create reconciliation proof.
- `docs/source/examples/price_outside_range.md`: price bounds proof.
- `docs/source/examples/duplicate_fill_events.md`: duplicate/stale fill proof.
- `docs/source/examples/cross_zero_position.md`: target-position proof.
- `docs/source/api/index.rst`: API reference landing page.
- `docs/source/api/algorithms.rst`: algorithm helpers API docs.
- `docs/source/api/api_runtime.rst`: FastAPI/runtime/schema API docs.
- `docs/source/api/exchanges.rst`: exchange adapter API docs.
- `docs/source/api/execution.rst`: execution engine/model/state API docs.
- `docs/source/api/observability.rst`: artifact/logging/summary API docs.
- `docs/source/api/risk.rst`: Decimal and validation API docs.

Modify existing project files:

- `pyproject.toml`: add documentation dependencies to the existing `dev` dependency group.
- `uv.lock`: update lockfile after dependency changes.
- `README.md`: add a short documentation section with build command and source path.
- `src/algorithms/chase.py`: add module/function/class docstrings.
- `src/algorithms/twap.py`: add module/function docstrings.
- `src/api/app.py`: add docstring for `create_app`.
- `src/api/runtime.py`: add docstrings to runtime classes.
- `src/api/schemas.py`: add module and conversion helper docstrings.
- `src/exchanges/base.py`: add adapter contract docstrings.
- `src/exchanges/simulator.py`: add simulator class docstrings.
- `src/exchanges/binance_usdm.py`: add Binance adapter helper/class docstrings.
- `src/execution/engine.py`: add docstrings to `ExposureTracker`, `ExecutionRecord`, and `ExecutionEngine`.
- `src/execution/models.py`: add module and key model/helper docstrings.
- `src/execution/service.py`: add service facade docstring.
- `src/execution/state_machine.py`: add transition helper docstrings.
- `src/execution/ids.py`: add ID helper docstrings.
- `src/execution/clock.py`: add clock abstraction docstrings.
- `src/observability/artifacts.py`: add artifact writer docstring.
- `src/observability/logging.py`: add logging/sanitization docstrings.
- `src/observability/summary.py`: add summary metric docstrings.
- `src/risk/decimal_math.py`: add rounding/metric helper docstrings.
- `src/risk/validation.py`: add validation helper docstrings.

Do not modify `docs/superpowers/specs/2026-06-23-documentation-site-design.md` during implementation unless the user explicitly changes the spec.

---

## Task 1: Add Sphinx Tooling And Configuration

**Files:**
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Create: `docs/source/conf.py`

- [ ] **Step 1: Capture current dependency state**

Run:

```bash
uv run python - <<'PY'
import importlib.util
for name in ["sphinx", "pydata_sphinx_theme", "myst_parser", "sphinx_autodoc_typehints"]:
    print(f"{name}={importlib.util.find_spec(name) is not None}")
PY
```

Expected before dependency work: at least one package prints `False`, unless the local environment already has docs dependencies installed.

- [ ] **Step 2: Add docs dependencies to `pyproject.toml`**

Modify the existing `[dependency-groups]` `dev` list so it contains these additional entries:

```toml
    "myst-parser>=4.0.0",
    "pydata-sphinx-theme>=0.16.0",
    "sphinx>=8.0.0",
    "sphinx-autodoc-typehints>=2.4.0",
```

Keep existing dev dependencies:

```toml
    "pytest>=8.3.0",
    "pytest-asyncio>=0.23.0",
```

- [ ] **Step 3: Update the uv lockfile**

Run:

```bash
uv lock
```

Expected: command exits 0 and updates `uv.lock` if the docs dependencies were not already locked.

If the environment blocks package resolution, rerun with the required network permissions for package metadata resolution.

- [ ] **Step 4: Create `docs/source/conf.py`**

Create the file with this content:

```python
from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

project = "Calais Execution Algorithm"
author = "Calais Execution Algorithm"
copyright = "2026, Calais Execution Algorithm"
release = "0.1.0"

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "sphinx_autodoc_typehints",
]

autosummary_generate = True
autodoc_typehints = "description"
autodoc_member_order = "bysource"
autoclass_content = "both"

myst_enable_extensions = [
    "colon_fence",
    "deflist",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "pydata_sphinx_theme"
html_static_path = []
html_title = "Calais Execution Algorithm"
html_theme_options = {
    "show_toc_level": 2,
    "navigation_with_keys": False,
    "navbar_end": ["theme-switcher", "navbar-icon-links"],
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/",
            "icon": "fa-brands fa-github",
        }
    ],
}

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", {}),
    "pydantic": ("https://docs.pydantic.dev/latest", {}),
}
```

- [ ] **Step 5: Run the docs build and record the expected navigation failure**

Run:

```bash
uv run sphinx-build -b html docs/source docs/_build/html
```

Expected at this point: command fails because `docs/source/index.md` does not exist yet. The failure proves the Sphinx command is wired to the intended source directory.

- [ ] **Step 6: Commit tooling scaffold**

Run:

```bash
git add pyproject.toml uv.lock docs/source/conf.py
git commit -m "Add Sphinx documentation tooling"
```

Expected: commit succeeds and contains only the tooling/configuration changes from this task.

---

## Task 2: Create Landing Page And Section Indexes

**Files:**
- Create: `docs/source/index.md`
- Create: `docs/source/user_guide/index.md`
- Create: `docs/source/examples/index.md`
- Create: `docs/source/api/index.rst`

- [ ] **Step 1: Create `docs/source/index.md`**

Create the file with this content:

````markdown
# Calais Execution Algorithm

Compact execution algorithm service for the Calais candidate project. The implementation focuses on one exchange family, one symbol, and two algorithms so that execution correctness, edge cases, and evidence remain inspectable.

## Quickstart

```bash
uv sync
uv run pytest -q
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py
uv run python scripts/run_sim_create_timeout.py
```

## What To Review First

- Start with {doc}`user_guide/assignment_requirements` to see how the Calais brief maps to the repository.
- Read {doc}`user_guide/safety_invariants` for the core overfill-prevention model.
- Use {doc}`examples/index` to run deterministic proof cases.
- Use {doc}`api/index` when inspecting public module boundaries.

```{toctree}
:maxdepth: 2
:caption: User Guide

user_guide/index
user_guide/assignment_requirements
user_guide/architecture
user_guide/execution_lifecycle
user_guide/safety_invariants
user_guide/chase
user_guide/twap
user_guide/binance_testnet
user_guide/observability
user_guide/limitations
```

```{toctree}
:maxdepth: 2
:caption: Examples

examples/index
examples/normal_chase
examples/normal_twap
examples/cancel_fill_race
examples/create_timeout_reconciliation
examples/price_outside_range
examples/duplicate_fill_events
examples/cross_zero_position
```

```{toctree}
:maxdepth: 2
:caption: API Reference

api/index
```
````

- [ ] **Step 2: Create `docs/source/user_guide/index.md`**

Create the file with this content:

````markdown
# User Guide

The User Guide is organized around evaluator questions from the Calais brief: what the service guarantees, where the guarantee is implemented, and which tests or scripts prove the behavior.

## Reading Path

1. {doc}`assignment_requirements` maps assignment requirements to repository coverage.
2. {doc}`architecture` explains the module boundaries.
3. {doc}`execution_lifecycle` explains parent and child order states.
4. {doc}`safety_invariants` explains why the engine cannot knowingly over-submit.
5. {doc}`chase` and {doc}`twap` explain algorithm behavior.
6. {doc}`binance_testnet`, {doc}`observability`, and {doc}`limitations` explain evidence and scope.

```{toctree}
:maxdepth: 1

assignment_requirements
architecture
execution_lifecycle
safety_invariants
chase
twap
binance_testnet
observability
limitations
```
````

- [ ] **Step 3: Create `docs/source/examples/index.md`**

Create the file with this content:

````markdown
# Examples

The examples are deterministic proof cases. They are written for reviewers who want to see how a failure mode is exercised and which artifact or response field proves the result.

## Recommended Order

1. {doc}`normal_chase`
2. {doc}`normal_twap`
3. {doc}`cancel_fill_race`
4. {doc}`create_timeout_reconciliation`
5. {doc}`price_outside_range`
6. {doc}`duplicate_fill_events`
7. {doc}`cross_zero_position`

```{toctree}
:maxdepth: 1

normal_chase
normal_twap
cancel_fill_race
create_timeout_reconciliation
price_outside_range
duplicate_fill_events
cross_zero_position
```
````

- [ ] **Step 4: Create `docs/source/api/index.rst`**

Create the file with this content:

```rst
API Reference
=============

The API reference documents public modules that define the execution service,
algorithm helpers, exchange adapters, risk checks, and observability artifacts.

.. toctree::
   :maxdepth: 1

   algorithms
   api_runtime
   execution
   exchanges
   risk
   observability
```

- [ ] **Step 5: Run Sphinx to capture missing page list**

Run:

```bash
uv run sphinx-build -b html docs/source docs/_build/html
```

Expected: command fails because referenced guide, example, and API pages have not been created yet. The missing page list should match the file list in Tasks 3 through 6.

- [ ] **Step 6: Commit section indexes**

Run:

```bash
git add docs/source/index.md docs/source/user_guide/index.md docs/source/examples/index.md docs/source/api/index.rst
git commit -m "Add documentation landing pages"
```

Expected: commit succeeds and contains only the four index files.

---

## Task 3: Write User Guide Pages For Requirements, Architecture, And Lifecycle

**Files:**
- Create: `docs/source/user_guide/assignment_requirements.md`
- Create: `docs/source/user_guide/architecture.md`
- Create: `docs/source/user_guide/execution_lifecycle.md`

- [ ] **Step 1: Create `assignment_requirements.md`**

Create a page with these sections and facts:

````markdown
# Assignment Requirements

The Calais brief asks for a Python 3.11+ execution service for Binance USD-M Futures BTCUSDT Perpetual. The service receives a final target position, an allowed execution price range, and a target duration, then uses Chase or TWAP to move the account toward the target.

## Requirement Matrix

| Brief Area | Requirement | Repository Coverage | Proof |
| --- | --- | --- | --- |
| Target position | Compute `target_position - current_position`; support buy, sell, no action, and cross-zero. | `execution.models.required_trade`, `ExecutionEngine.create_execution`, API schemas. | `tests/simulation/test_required_scenarios.py::test_t10_cross_zero_position_uses_target_minus_current_absolute_quantity` |
| Price range | Never actively buy above upper bound or sell below lower bound. | `risk.validation.validate_child_order_safety`, engine price checks. | `test_t7_price_outside_range_waits_then_expires_without_invalid_order` |
| Duration | Use monotonic time for schedule and deadlines, while logging wall-clock timestamps. | `execution.clock`, engine lifecycle timestamps, artifact writer. | `tests/unit/test_engine_lifecycle.py` deadline and start-time tests |
| API/CLI | Provide create, query, cancel, and deterministic controls. | `api.app`, `api.runtime`, simulator scripts. | `tests/unit/test_api.py` and `scripts/run_sim_*.py` |
| Chase | Passive best bid/ask with threshold repricing and minimum interval. | `algorithms.chase`, engine cancel/replace path. | `test_t1_normal_chase_submits_passive_price_and_preserves_exposure_invariant`, `test_t2_chase_reprice_requires_threshold_and_minimum_interval` |
| TWAP | Absolute schedule, carry-forward deficit, and rounding-aware final quantity. | `algorithms.twap`, TWAP engine ledger. | `test_t5_twap_carry_forward_deficit_includes_previous_unfilled_quantity`, `test_t5b_twap_does_not_submit_before_first_absolute_slice_boundary` |
| Create timeout | Preserve UNKNOWN order exposure until exact reconciliation by client order ID. | `ExecutionEngine`, `ExchangeAdapter.query_order`, simulator timeout scripts. | `test_t4a_create_timeout_reconciles_to_open_order_without_new_client_order_id` |
| Cancel/fill race | A fill after cancel request reduces replacement quantity. | Exposure tracker and cancel/reconcile path. | `test_t3_cancel_fill_race_updates_confirmed_fills_before_replacement_sizing` |
| Decimal precision | Use `Decimal` and JSON decimal strings for order parameters. | `execution.models`, `api.schemas`, `risk.decimal_math`. | `tests/unit/test_models.py`, `tests/unit/test_api.py`, `tests/unit/test_decimal_math.py` |
| Logging/results | Write structured logs and result artifacts. | `observability.artifacts`, simulator and Testnet runners. | `test_cancel_race_script_writes_required_artifacts` |

## Direct Pass/Fail Risks

- Partial fill after cancel must not cause resubmission of the original total quantity.
- Create timeout must remain `UNKNOWN` until exact lookup resolves the client order ID.
- Price and quantity inputs must not be constructed from floats.
- TWAP must use absolute elapsed time rather than `sleep` drift.
- Price-out-of-range execution must report the real unfilled result.
- Tests and artifacts must be traceable by execution ID and client order ID.
````

- [ ] **Step 2: Create `architecture.md`**

Create a page with these sections:

````markdown
# Architecture

The system is intentionally small and correctness-first. Runtime code supervises the environment, the engine owns trading state, algorithm modules compute narrow decisions, exchange adapters isolate external behavior, and observability code writes evidence.

## Layers

| Layer | Responsibility | Main Files |
| --- | --- | --- |
| API/runtime | HTTP app, runtime supervisor, background loops, stream supervision, graceful shutdown. | `src/api/app.py`, `src/api/runtime.py`, `src/api/schemas.py` |
| Service/engine | Execution records, child orders, exposure accounting, reconciliation, state transitions, terminal summaries. | `src/execution/engine.py`, `src/execution/service.py`, `src/execution/models.py`, `src/execution/state_machine.py` |
| Algorithms | Pure Chase and TWAP calculations. | `src/algorithms/chase.py`, `src/algorithms/twap.py` |
| Exchange adapters | Common adapter contract, deterministic simulator, Binance USD-M implementation. | `src/exchanges/base.py`, `src/exchanges/simulator.py`, `src/exchanges/binance_usdm.py` |
| Risk | Decimal rounding, price/quantity validation, exposure invariant checks. | `src/risk/decimal_math.py`, `src/risk/validation.py` |
| Observability | Sanitized logs, summaries, CSV/JSON/JSONL artifacts. | `src/observability/artifacts.py`, `src/observability/logging.py`, `src/observability/summary.py` |

## Boundary Rule

`ExecutionEngine` is the trading-correctness boundary. Runtime code may supervise streams and retries, but it does not own exposure math. Algorithm helpers compute decisions, but the engine decides whether a child order is safe to submit.

## Data Flow

1. API request or script creates an `ExecutionRequest`.
2. `ExecutionService` delegates to `ExecutionEngine`.
3. The engine reads position, symbol rules, and market data through `ExchangeAdapter`.
4. Chase or TWAP helpers compute desired price or scheduled quantity.
5. Risk checks and exposure accounting gate every child order submit.
6. Exchange responses, stream events, and reconciliation update child and parent state.
7. Observability writers emit the reviewable artifact bundle.
````

- [ ] **Step 3: Create `execution_lifecycle.md`**

Create a page with these sections:

````markdown
# Execution Lifecycle

An execution starts with a final target position and ends with an explicit terminal status. It does not imply guaranteed full fill, because price bounds, deadline policy, exchange failures, and remaining exposure are part of the result.

## Parent Execution States

| State | Meaning |
| --- | --- |
| `CREATED` | Request accepted and execution record built. |
| `VALIDATING` | Position, symbol rules, range, duration, and parameters are checked. |
| `RUNNING` | Engine may submit, cancel, reprice, or reconcile child orders. |
| `CANCELLING` | Manual cancel or deadline cancel is draining active exposure. |
| `COMPLETED` | Required normalized quantity is filled. |
| `PARTIALLY_COMPLETED` | Some quantity filled and no more safe work remains. |
| `EXPIRED` | Deadline reached with unfilled quantity. |
| `CANCELLED` | User cancellation completed without full target fill. |
| `FAILED` | Terminal validation or exchange failure prevents safe continuation. |

## Child Order States

```text
PENDING_SUBMIT -> OPEN -> PARTIALLY_FILLED -> PENDING_CANCEL -> CANCELLED | FILLED | REJECTED | UNKNOWN
```

`UNKNOWN` means the create outcome is ambiguous. The engine reserves the unknown quantity until exact reconciliation proves whether the exchange accepted the order.

## Serialization

Each execution is serialized through its own event actor. `create`, `run_once`, `cancel`, and `reconcile` calls for one execution cannot interleave inside the engine in a way that corrupts child state or exposure accounting.

## Terminal Rule

Terminal execution states do not return to `RUNNING`. Terminal child states are not resurrected by stale reconciliation snapshots.
````

- [ ] **Step 4: Run focused docs build for these pages**

Run:

```bash
uv run sphinx-build -b html docs/source docs/_build/html
```

Expected: command still fails because later guide, example, and API pages are missing, but it should not report syntax errors in the three pages created in this task.

- [ ] **Step 5: Commit guide foundation**

Run:

```bash
git add docs/source/user_guide/assignment_requirements.md docs/source/user_guide/architecture.md docs/source/user_guide/execution_lifecycle.md
git commit -m "Document requirements architecture and lifecycle"
```

Expected: commit succeeds and contains only the three guide pages from this task.

---

## Task 4: Write User Guide Pages For Invariants, Chase, And TWAP

**Files:**
- Create: `docs/source/user_guide/safety_invariants.md`
- Create: `docs/source/user_guide/chase.md`
- Create: `docs/source/user_guide/twap.md`

- [ ] **Step 1: Create `safety_invariants.md`**

Create the file with this content structure:

````markdown
# Safety Invariants

The engine enforces exposure safety before every child submit. The core rule is:

```text
confirmed_filled
+ live_open
+ pending_submit
+ pending_cancel
+ unknown_order
+ new_child_quantity
<= normalized_target_trade_quantity
```

## Reserved Buckets

| Bucket | Why It Counts |
| --- | --- |
| `confirmed_filled` | Quantity already filled toward the parent target. |
| `live_open` | Quantity still executable on the exchange. |
| `pending_submit` | Local intent sent to the exchange before the create response is known. |
| `pending_cancel` | Quantity that can still fill after a cancel request is sent. |
| `unknown_order` | Ambiguous create outcome until exact reconciliation resolves it. |
| `new_child_quantity` | Quantity proposed for the next child order. |

## Guarantees

- Replacement quantity is based on parent cumulative filled quantity and reserved exposure, not only the active order's remaining quantity.
- Duplicate or stale cumulative fill snapshots cannot reduce or double-count parent fills.
- A create timeout cannot be retried with a new client order ID while the original outcome is unknown.
- Price-out-of-range tasks may expire unfilled, but they do not fake completion.

## Implementation And Proof

- Implementation: `src/execution/engine.py`, `src/risk/validation.py`
- Unit proof: `tests/unit/test_engine_exposure.py`
- Scenario proof: `tests/simulation/test_required_scenarios.py`
````

- [ ] **Step 2: Create `chase.md`**

Create the file with this content structure:

````markdown
# Chase

Chase places a passive limit order at the current near touch and reprices when the market has moved enough to justify cancel-and-replace.

## Passive Price

- Buy desired price: best bid.
- Sell desired price: best ask.
- Default order shape: post-only limit when supported.

## Repricing

`reprice_threshold_bps` controls how far the desired price must move from the active order price. `minimum_reprice_interval_ms` prevents a cancel storm.

The default repricing mode is `ADVERSE_ONLY`:

- Buy reprices upward when best bid moves up enough.
- Sell reprices downward when best ask moves down enough.

`TWO_SIDED` is available when favorable movement should also trigger repricing.

## Partial Fill Safety

Canceling a child does not prove it stopped filling. The replacement order is sized after accounting for confirmed parent fills plus live, pending, cancelling, and unknown exposure. The deterministic cancel/fill race example proves this behavior.

## Implementation And Proof

- Decision helpers: `src/algorithms/chase.py`
- Engine path: `src/execution/engine.py`
- Tests: `tests/unit/test_chase.py`
- Scenario proof: `tests/simulation/test_required_scenarios.py::test_t3_cancel_fill_race_updates_confirmed_fills_before_replacement_sizing`
````

- [ ] **Step 3: Create `twap.md`**

Create the file with this content structure:

````markdown
# TWAP

TWAP uses absolute elapsed time and cumulative schedule deficit. It is not implemented as `slice_qty / sleep`.

## Schedule

```text
scheduled_cumulative_quantity(t)
= total_trade_quantity * elapsed_time / total_duration

quantity_deficit(t)
= scheduled_cumulative_quantity(t) - confirmed_cumulative_filled_quantity(t)
```

The engine subtracts reserved exposure before submitting another child order.

## Carry-Forward Deficit

If an earlier slice remains unfilled or partially filled, later slices inherit the cumulative deficit. This prevents the schedule from pretending a previous child filled just because time advanced.

## Rounding

Quantities are normalized to exchange step size using Decimal arithmetic. The system records dust rather than rounding upward into an overfill.

## Implementation And Proof

- Schedule helpers: `src/algorithms/twap.py`
- Engine ledger: `src/execution/engine.py`
- Tests: `tests/unit/test_twap.py`
- Scenario proof: `tests/simulation/test_required_scenarios.py::test_t5_twap_carry_forward_deficit_includes_previous_unfilled_quantity`
````

- [ ] **Step 4: Run focused docs build for guide syntax**

Run:

```bash
uv run sphinx-build -b html docs/source docs/_build/html
```

Expected: command still fails only because later guide, example, and API pages are missing. There should be no MyST syntax errors for the three pages created in this task.

- [ ] **Step 5: Commit algorithm guide pages**

Run:

```bash
git add docs/source/user_guide/safety_invariants.md docs/source/user_guide/chase.md docs/source/user_guide/twap.md
git commit -m "Document execution safety and algorithms"
```

Expected: commit succeeds and contains only the three guide pages from this task.

---

## Task 5: Write User Guide Pages For Testnet, Observability, And Limitations

**Files:**
- Create: `docs/source/user_guide/binance_testnet.md`
- Create: `docs/source/user_guide/observability.md`
- Create: `docs/source/user_guide/limitations.md`

- [ ] **Step 1: Create `binance_testnet.md`**

Create the file with this content structure:

````markdown
# Binance Testnet

The Binance USD-M adapter maps the same `ExchangeAdapter` contract used by the simulator onto Testnet REST and WebSocket behavior.

## Credentials And Consent

Testnet scripts require:

```bash
export BINANCE_USDM_API_KEY=<testnet-api-key>
export BINANCE_USDM_API_SECRET=<testnet-api-secret>
```

They also require `--confirm-send-orders`. Without credentials or explicit confirmation, the scripts exit before sending orders and do not fall back to the simulator.

## Endpoints And Evidence

- Testnet REST base: `https://demo-fapi.binance.com`
- Public/user stream root: `wss://fstream.binancefuture.com`
- Order mutation endpoints: `POST /fapi/v1/order`, `DELETE /fapi/v1/order`
- Reconciliation endpoints: `GET /fapi/v1/order`, `GET /fapi/v1/openOrders`, `GET /fapi/v1/allOrders`, `GET /fapi/v1/userTrades`

The Testnet runner writes `symbol_rules.json`, `reconciliation_orders.csv`, `execution_summary.json`, `execution_log.jsonl`, and `evidence_manifest.json`.

## Mainnet Guardrail

Mainnet is configuration-compatible but hard-disabled by default. Mutating mainnet requests require explicit configuration and should not be used for the assignment demo.

## Implementation And Proof

- Adapter: `src/exchanges/binance_usdm.py`
- Runtime supervision: `src/api/runtime.py`
- Scripts: `scripts/run_testnet_chase.py`, `scripts/run_testnet_twap.py`
- Contract tests: `tests/integration/test_binance_testnet_contract.py`
````

- [ ] **Step 2: Create `observability.md`**

Create the file with this content structure:

````markdown
# Observability

The project writes structured artifacts so a reviewer can reconstruct request parameters, child orders, fills, timeline events, summaries, and TWAP slice behavior.

## Standard Artifact Bundle

Simulator and Testnet runs write:

- `request_snapshot.json`
- `execution_log.jsonl`
- `execution_summary.json`
- `child_orders.csv`
- `fills.csv`
- `timeline.csv`
- `twap_slice_ledger.csv`

Testnet runs also write exchange evidence such as `symbol_rules.json`, `reconciliation_orders.csv`, and `evidence_manifest.json`.

## Sanitization

Logging helpers remove secrets, signatures, authorization headers, listen keys, and raw signed payload aliases before artifacts are written.

## Summary Metrics

Terminal summaries report filled quantity, unfilled quantity, overfill quantity, VWAP, completion rate, slippage, reprices, duplicate events ignored, and reconciliation counters where relevant.

## Implementation And Proof

- Artifact writer: `src/observability/artifacts.py`
- Sanitization: `src/observability/logging.py`
- Metrics: `src/observability/summary.py`
- Tests: `tests/unit/test_observability.py`
````

- [ ] **Step 3: Create `limitations.md`**

Create the file with this content structure:

````markdown
# Limitations

The project is intentionally scoped to be small and inspectable for the assignment.

## Scope

- Persistence is in-memory; process restart loses execution state.
- The target exchange family is Binance USD-M Futures.
- The target symbol is BTCUSDT perpetual.
- The account mode is one-way mode.
- Runtime supervision is compact and Testnet-focused, not a production operations platform.
- Deterministic simulator tests prove races that Testnet may not reproduce reliably.
- Accepted Testnet order evidence depends on account funding, permissions, and Binance risk checks.
- Mainnet mutations are hard-disabled by default.

## Submission Interpretation

Simulator evidence should not be presented as a replacement for accepted Testnet order evidence. If Testnet order acceptance is blocked, keep the raw rejected/error artifact as connectivity evidence and label accepted-order evidence as pending account configuration.
````

- [ ] **Step 4: Run docs build for all guide pages**

Run:

```bash
uv run sphinx-build -b html docs/source docs/_build/html
```

Expected: command still fails because example and API pages are missing, but the User Guide toctree should be complete.

- [ ] **Step 5: Commit remaining guide pages**

Run:

```bash
git add docs/source/user_guide/binance_testnet.md docs/source/user_guide/observability.md docs/source/user_guide/limitations.md
git commit -m "Document evidence testnet and limitations"
```

Expected: commit succeeds and contains only the three guide pages from this task.

---

## Task 6: Write Proof-Oriented Example Pages

**Files:**
- Create: `docs/source/examples/normal_chase.md`
- Create: `docs/source/examples/normal_twap.md`
- Create: `docs/source/examples/cancel_fill_race.md`
- Create: `docs/source/examples/create_timeout_reconciliation.md`
- Create: `docs/source/examples/price_outside_range.md`
- Create: `docs/source/examples/duplicate_fill_events.md`
- Create: `docs/source/examples/cross_zero_position.md`

- [ ] **Step 1: Create `normal_chase.md`**

Create this page:

````markdown
# Normal Chase

This example proves that Chase submits one passive child order at the best bid for a buy execution and reserves exposure for the normalized required quantity.

## Run

```bash
uv run python scripts/run_sim_chase.py
```

## Expected Evidence

Important output fields:

```text
SIMULATOR DEMO: Chase
execution_id=<generated-execution-id>
status=ExecutionStatus.RUNNING
client_order_ids=[<generated-client-order-id>]
child_order id=<generated-child-order-id> clientOrderId=<generated-client-order-id> status=ChildOrderStatus.OPEN qty=0.010 price=50000.00
```

The generated execution ID and client order ID change per run. The stable behavior is one open passive child order and live exposure equal to the required quantity.

## Related Test

`tests/simulation/test_required_scenarios.py::test_t1_normal_chase_submits_passive_price_and_preserves_exposure_invariant`
````

- [ ] **Step 2: Create `normal_twap.md`**

Create this page:

````markdown
# Normal TWAP

This example proves that TWAP uses absolute elapsed time. With a 100 second duration and 10 slices, the first child appears at the 10 second boundary.

## Run

```bash
uv run python scripts/run_sim_twap.py
```

## Expected Evidence

Important output fields:

```text
SIMULATOR DEMO: TWAP
schedule=absolute-time TWAP target over 100 seconds
status=ExecutionStatus.RUNNING
twap_order id=<generated-child-order-id> clientOrderId=<generated-client-order-id> status=ChildOrderStatus.OPEN qty=0.001 price=50000.00
schedule_summary=elapsed_seconds=10 required_quantity=0.010 confirmed=0 reserved=0.001
```

The stable behavior is that the first submitted quantity is the scheduled deficit at the first absolute slice boundary.

## Related Tests

- `tests/simulation/test_required_scenarios.py::test_t5_twap_carry_forward_deficit_includes_previous_unfilled_quantity`
- `tests/simulation/test_required_scenarios.py::test_t5b_twap_does_not_submit_before_first_absolute_slice_boundary`
````

- [ ] **Step 3: Create `cancel_fill_race.md`**

Create this page:

````markdown
# Cancel/Fill Race

This example proves that a fill arriving during cancel reduces the replacement quantity. It targets the assignment risk where an implementation resubmits the original total quantity after cancel and creates a predictable overfill.

## Run

```bash
uv run python scripts/run_sim_cancel_race.py
```

To write artifacts to a chosen directory:

```bash
uv run python scripts/run_sim_cancel_race.py --output-dir /tmp/calais-sim-cancel-race-docs
```

## Expected Evidence

Important output fields:

```text
SIMULATOR DEMO: Cancel/Fill Race
confirmed_filled=0.004
reserved_exposure=0.006
artifact_dir=<generated-artifact-directory>
```

Inspect `child_orders.csv`. The first child is cancelled with `filled_quantity` of `0.004`; the replacement child has `submitted_quantity` of `0.006`.

## Related Test

`tests/simulation/test_required_scenarios.py::test_t3_cancel_fill_race_updates_confirmed_fills_before_replacement_sizing`
````

- [ ] **Step 4: Create `create_timeout_reconciliation.md`**

Create this page:

````markdown
# Create Timeout Reconciliation

This example proves that an ambiguous create timeout becomes `UNKNOWN`, reserves exposure, and blocks fresh client order IDs until exact reconciliation resolves the original order.

## Run

```bash
uv run python scripts/run_sim_create_timeout.py
```

## Expected Evidence

Important output fields:

```text
SIMULATOR DEMO: Create Timeout
unknown_before_reconcile=0.010
unknown_after_reconcile=0
live_open_after_reconcile=0.010
artifact_dir=<generated-artifact-directory>
```

Inspect `execution_log.jsonl`. It should include `create_timeout_unknown`, `run_before_reconcile_no_new_client_order_id`, and `reconciled_original_open`.

Inspect `execution_summary.json`. The final reason should be `CREATE_TIMEOUT_RECONCILED`.

## Related Tests

- `tests/simulation/test_required_scenarios.py::test_t4a_create_timeout_reconciles_to_open_order_without_new_client_order_id`
- `tests/simulation/test_required_scenarios.py::test_t4b_create_timeout_not_found_releases_unknown_exposure_before_safe_retry`
````

- [ ] **Step 5: Create `price_outside_range.md`**

Create this page:

````markdown
# Price Outside Range

This scenario proves that the engine waits when the quote is outside the allowed execution range and expires without submitting an invalid order if the market never becomes executable.

## Run The Scenario Test

```bash
uv run pytest tests/simulation/test_required_scenarios.py::test_t7_price_outside_range_waits_then_expires_without_invalid_order -q
```

## Expected Evidence

The test asserts:

```text
child_orders == []
status == ExecutionStatus.EXPIRED
final_reason == PRICE_OUTSIDE_RANGE
summary.metrics["price_bound_violations"] == 1
summary.metrics["unfilled_quantity"] == "0.01"
```

This is the correct result for an unexecutable price path. The system reports unfilled quantity rather than claiming completion.
````

- [ ] **Step 6: Create `duplicate_fill_events.md`**

Create this page:

````markdown
# Duplicate Fill Events

This scenario proves that duplicate and stale cumulative fill events do not double-count parent filled quantity.

## Run The Scenario Test

```bash
uv run pytest tests/simulation/test_required_scenarios.py::test_t9_duplicate_fill_event_does_not_double_count_cumulative_fill -q
```

## Expected Evidence

The test injects a valid cumulative fill of `0.003`, a duplicate event with the same trade ID, and a stale cumulative fill of `0.002`.

The stable assertions are:

```text
confirmed_filled_quantity == 0.003
duplicate_events_ignored == 2
filled_quantity == "0.01"
overfill_quantity == "0"
```

The engine applies cumulative fills monotonically and does not let stale snapshots reduce or duplicate parent fill accounting.
````

- [ ] **Step 7: Create `cross_zero_position.md`**

Create this page:

````markdown
# Cross-Zero Target Position

This scenario proves that the input is a final account position, not an order quantity.

## Run The Scenario Test

```bash
uv run pytest tests/simulation/test_required_scenarios.py::test_t10_cross_zero_position_uses_target_minus_current_absolute_quantity -q
```

## Expected Evidence

The test covers both directions:

```text
current_position = -0.003, target_position = 0.002 -> BUY 0.005
current_position = 0.004, target_position = -0.002 -> SELL 0.006
```

This prevents the common implementation error of treating `target_position` as the child order quantity.
````

- [ ] **Step 8: Run docs build for guide and example pages**

Run:

```bash
uv run sphinx-build -b html docs/source docs/_build/html
```

Expected: command still fails because API pages are missing, but User Guide and Examples toctrees should be complete.

- [ ] **Step 9: Commit examples**

Run:

```bash
git add docs/source/examples/normal_chase.md docs/source/examples/normal_twap.md docs/source/examples/cancel_fill_race.md docs/source/examples/create_timeout_reconciliation.md docs/source/examples/price_outside_range.md docs/source/examples/duplicate_fill_events.md docs/source/examples/cross_zero_position.md
git commit -m "Add execution scenario examples"
```

Expected: commit succeeds and contains only the seven example pages.

---

## Task 7: Add API Reference Pages And Focused Docstrings

**Files:**
- Create: `docs/source/api/algorithms.rst`
- Create: `docs/source/api/api_runtime.rst`
- Create: `docs/source/api/exchanges.rst`
- Create: `docs/source/api/execution.rst`
- Create: `docs/source/api/observability.rst`
- Create: `docs/source/api/risk.rst`
- Modify: public docstrings in the `src/` files listed in the File Structure section.

- [ ] **Step 1: Create `docs/source/api/algorithms.rst`**

Create the file:

```rst
Algorithms
==========

Chase
-----

.. automodule:: algorithms.chase
   :members:
   :undoc-members:
   :show-inheritance:

TWAP
----

.. automodule:: algorithms.twap
   :members:
   :undoc-members:
   :show-inheritance:
```

- [ ] **Step 2: Create `docs/source/api/api_runtime.rst`**

Create the file:

```rst
API And Runtime
===============

App
---

.. automodule:: api.app
   :members:
   :undoc-members:

Runtime
-------

.. automodule:: api.runtime
   :members:
   :undoc-members:

Schemas
-------

.. automodule:: api.schemas
   :members:
   :undoc-members:
```

- [ ] **Step 3: Create `docs/source/api/execution.rst`**

Create the file:

```rst
Execution
=========

Models
------

.. automodule:: execution.models
   :members:
   :undoc-members:

Engine
------

.. automodule:: execution.engine
   :members:
   :undoc-members:

Service
-------

.. automodule:: execution.service
   :members:
   :undoc-members:

State Machine
-------------

.. automodule:: execution.state_machine
   :members:
   :undoc-members:

IDs
---

.. automodule:: execution.ids
   :members:
   :undoc-members:

Clock
-----

.. automodule:: execution.clock
   :members:
   :undoc-members:
```

- [ ] **Step 4: Create `docs/source/api/exchanges.rst`**

Create the file:

```rst
Exchanges
=========

Adapter Contract
----------------

.. automodule:: exchanges.base
   :members:
   :undoc-members:

Deterministic Simulator
-----------------------

.. automodule:: exchanges.simulator
   :members:
   :undoc-members:

Binance USD-M
-------------

.. automodule:: exchanges.binance_usdm
   :members:
   :undoc-members:
```

- [ ] **Step 5: Create `docs/source/api/risk.rst`**

Create the file:

```rst
Risk
====

Decimal Math
------------

.. automodule:: risk.decimal_math
   :members:
   :undoc-members:

Validation
----------

.. automodule:: risk.validation
   :members:
   :undoc-members:
```

- [ ] **Step 6: Create `docs/source/api/observability.rst`**

Create the file:

```rst
Observability
=============

Artifacts
---------

.. automodule:: observability.artifacts
   :members:
   :undoc-members:

Logging
-------

.. automodule:: observability.logging
   :members:
   :undoc-members:

Summary
-------

.. automodule:: observability.summary
   :members:
   :undoc-members:
```

- [ ] **Step 7: Add algorithm docstrings**

Modify `src/algorithms/chase.py` so it starts with:

```python
"""Pure Chase order decision helpers."""
```

Add these docstrings:

```python
class ChaseDecision:
    """Decision returned by Chase repricing logic."""

def chase_desired_price(side: Side, best_bid: Decimal, best_ask: Decimal, passive: bool) -> Decimal:
    """Return the near-touch price Chase should use for the requested side."""

def reprice_difference_bps(desired_price: Decimal, active_order_price: Decimal) -> Decimal:
    """Return absolute price movement from active price to desired price in basis points."""

def should_reprice(
    side: Side,
    active_order_price: Decimal,
    desired_price: Decimal,
    threshold_bps: Decimal,
    min_interval_ms: int,
    elapsed_since_last_reprice_ms: int,
    repricing_mode: RepricingMode,
) -> ChaseDecision:
    """Decide whether an active Chase order should be cancelled and replaced."""
```

Modify `src/algorithms/twap.py` so it starts with:

```python
"""Pure TWAP schedule and safe-sizing helpers."""
```

Add these docstrings:

```python
def scheduled_cumulative_quantity(
    total_trade_quantity: Decimal,
    elapsed_time: Decimal,
    total_duration: Decimal,
) -> Decimal:
    """Return cumulative target quantity implied by absolute elapsed time."""

def effective_slice_elapsed(
    elapsed_time: Decimal,
    total_duration: Decimal,
    number_of_slices: int,
) -> Decimal:
    """Return elapsed schedule time snapped to completed TWAP slice boundaries."""

def scheduled_deficit(
    scheduled_cumulative: Decimal,
    confirmed_cumulative_filled: Decimal,
) -> Decimal:
    """Return non-negative scheduled quantity not yet confirmed filled."""

def safe_child_quantity(deficit: Decimal, exposure: Exposure) -> Decimal:
    """Return the scheduled deficit left after subtracting reserved exposure."""
```

Use the real function signatures already present in each file; do not change behavior.

- [ ] **Step 8: Add execution and exchange docstrings**

Add concise docstrings without changing behavior:

```python
class ExchangeAdapter:
    """Abstract exchange contract shared by the simulator and Binance adapter."""

class DeterministicSimulator:
    """In-memory exchange simulator used for deterministic lifecycle and race tests."""

class BinanceUsdmAdapter:
    """Binance USD-M adapter implementing the exchange contract with REST and stream helpers."""

class ExposureTracker:
    """Tracks filled and reserved exposure buckets for one parent execution."""

class ExecutionRecord:
    """Mutable engine-owned state for one execution."""

class ExecutionEngine:
    """Owns execution lifecycle, child orders, exposure accounting, and reconciliation."""

class ExecutionService:
    """Application facade over the execution engine."""
```

Also add docstrings to these functions:

```python
def required_trade(target_position: Decimal, current_position: Decimal) -> tuple[Side, Decimal]:
    """Return side and absolute quantity required to move from current to target position."""

def transition_execution(current: ExecutionStatus, target: ExecutionStatus) -> ExecutionStatus:
    """Validate and return a legal parent execution state transition."""

def transition_child(current: ChildOrderStatus, target: ChildOrderStatus) -> ChildOrderStatus:
    """Validate and return a legal child order state transition."""
```

- [ ] **Step 9: Add API, risk, observability, and clock docstrings**

Add concise docstrings without changing behavior:

```python
def create_app(
    simulator_position: str = "0",
    *,
    background_tick_interval_seconds: float = 0.25,
) -> FastAPI:
    """Create the FastAPI application and wire the execution runtime."""

class ExecutionRuntime:
    """Supervises execution services, background loops, streams, and shutdown."""

def floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    """Round a positive Decimal quantity down to the nearest exchange step."""

def round_price(price: Decimal, tick_size: Decimal, side: Side, passive: bool) -> Decimal:
    """Round a Decimal price according to side and passive/aggressive intent."""

def validate_child_order_safety(
    quantity: Decimal,
    price: Decimal,
    side: Side,
    rules: SymbolRules,
    best_bid: Decimal,
    best_ask: Decimal,
    post_only: bool,
    lower: Decimal,
    upper: Decimal,
) -> None:
    """Reject child orders that would violate price bounds or exposure safety."""

def write_execution_artifacts(
    root: Path,
    execution_id: str,
    request_snapshot: Mapping[str, Any],
    log_events: Iterable[Mapping[str, Any]],
    summary: Mapping[str, Any],
    child_orders: Iterable[Mapping[str, Any]],
    fills: Iterable[Mapping[str, Any]],
    timeline: Iterable[Mapping[str, Any]],
    twap_slice_ledger: Iterable[Mapping[str, Any]] = (),
    *,
    extra_json_artifacts: Mapping[str, Any] | None = None,
    extra_csv_artifacts: Mapping[str, Iterable[Mapping[str, Any]]] | None = None,
) -> Path:
    """Write sanitized execution artifacts for reviewer evidence."""

def sanitize_log_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return a JSON-safe payload with secrets and signed request data removed."""

def summary_metrics(
    final_status: ExecutionStatus,
    side: Side,
    raw_required_quantity: Decimal,
    required_quantity: Decimal,
    target_dust_quantity: Decimal,
    filled_quantity: Decimal,
    arrival_bid: Decimal,
    arrival_ask: Decimal,
    vwap: Decimal,
    requested_duration_seconds: int,
    actual_duration_seconds: Decimal,
    price_bound_violations: int,
    duplicate_events_ignored: int,
    unknown_orders_reconciled: int,
    max_reserved_exposure: Decimal,
) -> dict[str, str | int]:
    """Build terminal execution metrics for summaries and artifacts."""

class Clock:
    """Clock interface used to separate monotonic scheduling from wall-clock logging."""

class ManualClock:
    """Controllable clock for deterministic tests and simulator examples."""
```

Use exact existing return types and parameters. Only insert docstrings.

- [ ] **Step 10: Build API docs**

Run:

```bash
uv run sphinx-build -b html docs/source docs/_build/html
```

Expected: command exits 0 or reports only warnings that are directly fixable in this task. Fix import, reference, or docstring syntax warnings before continuing.

- [ ] **Step 11: Commit API reference and docstrings**

Run:

```bash
git add docs/source/api/algorithms.rst docs/source/api/api_runtime.rst docs/source/api/exchanges.rst docs/source/api/execution.rst docs/source/api/observability.rst docs/source/api/risk.rst src/algorithms/chase.py src/algorithms/twap.py src/api/app.py src/api/runtime.py src/api/schemas.py src/exchanges/base.py src/exchanges/simulator.py src/exchanges/binance_usdm.py src/execution/engine.py src/execution/models.py src/execution/service.py src/execution/state_machine.py src/execution/ids.py src/execution/clock.py src/observability/artifacts.py src/observability/logging.py src/observability/summary.py src/risk/decimal_math.py src/risk/validation.py
git commit -m "Add API reference documentation"
```

Expected: commit succeeds and contains only API reference pages plus docstring-only source edits.

---

## Task 8: README Integration And Final Verification

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add README documentation section**

Insert this section after the existing Quickstart block in `README.md`:

````markdown
## Documentation

The repository includes a Sphinx documentation site under `docs/source/`, organized like a compact Python package manual:

- User Guide: assignment requirements, architecture, lifecycle, safety invariants, Chase, TWAP, Testnet evidence, observability, and limitations.
- Examples: deterministic simulator and scenario-test proof cases.
- API Reference: public modules for algorithms, execution, exchanges, risk, API/runtime, and observability.

Build the HTML docs locally with:

```bash
uv run sphinx-build -b html docs/source docs/_build/html
```

Open `docs/_build/html/index.html` in a browser after the build completes.
````

- [ ] **Step 2: Run Sphinx with warnings as errors**

Run:

```bash
uv run sphinx-build -W --keep-going -b html docs/source docs/_build/html
```

Expected: command exits 0 and includes `build succeeded`.

If warnings occur, fix the referenced docs or docstrings in the same task and rerun the command until it exits 0.

- [ ] **Step 3: Run simulator example commands**

Run:

```bash
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py --output-dir /tmp/calais-docs-cancel-race
uv run python scripts/run_sim_create_timeout.py --output-dir /tmp/calais-docs-create-timeout
```

Expected:

- Chase prints `SIMULATOR DEMO: Chase`, `execution_id=`, and one open child order.
- TWAP prints `SIMULATOR DEMO: TWAP`, `schedule=absolute-time`, and a `twap_order`.
- Cancel/fill race prints `confirmed_filled=0.004` and `reserved_exposure=0.006`.
- Create timeout prints `unknown_before_reconcile=0.010`, `unknown_after_reconcile=0`, and `live_open_after_reconcile=0.010`.

- [ ] **Step 4: Run full tests**

Run:

```bash
uv run pytest -q
```

Expected: all local tests pass. Optional Binance Testnet tests may skip when credentials are absent.

- [ ] **Step 5: Inspect git status**

Run:

```bash
git status --short
```

Expected: only intended documentation/source changes from this task are unstaged. Ignore unrelated pre-existing untracked directories such as `reports/latex/` or `tmp/` unless the user explicitly asks to include them.

- [ ] **Step 6: Commit README and verification polish**

Run:

```bash
git add README.md
git commit -m "Document Sphinx site usage"
```

Expected: commit succeeds and contains the README documentation section. If warning fixes from Step 2 modified docs or docstrings, include those exact files in this commit and mention them in the commit body:

```bash
git commit -m "Document Sphinx site usage" -m "Fixes final documentation build warnings."
```

---

## Final Acceptance Checklist

- [ ] `uv run sphinx-build -W --keep-going -b html docs/source docs/_build/html` exits 0.
- [ ] `uv run pytest -q` exits 0.
- [ ] `README.md` documents the docs source path and build command.
- [ ] The docs landing page links to User Guide, Examples, and API Reference.
- [ ] User Guide pages cover assignment requirements, architecture, lifecycle, invariants, Chase, TWAP, Testnet, observability, and limitations.
- [ ] Examples cover normal Chase, normal TWAP, cancel/fill race, create-timeout reconciliation, price outside range, duplicate fills, and cross-zero target position.
- [ ] API Reference pages build and import public modules.
- [ ] Key public classes and functions have concise docstrings.
- [ ] No unrelated untracked directories are committed.

## Self-Review Against Spec

Spec coverage:

- Skfolio-style structure: Tasks 1, 2, and 7.
- Evaluator-first User Guide: Tasks 3, 4, and 5.
- Proof-oriented Examples: Task 6.
- API Reference with focused docstrings: Task 7.
- README link and build command: Task 8.
- Verification commands: Task 8.

Placeholder scan:

- The plan contains no unresolved marker strings or incomplete file slots.
- Each page creation task lists exact file paths and the content structure to write.
- Each verification task includes exact commands and expected results.

Type and name consistency:

- Sphinx extension names match installed package names: `myst_parser`, `pydata_sphinx_theme`, and `sphinx_autodoc_typehints`.
- API module names match the current source layout under `src/`.
- Scenario names match `tests/simulation/test_required_scenarios.py`.
