# Code Correctness Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix code and pipeline correctness issues found in review before producing final evidence or PDF deliverables.

**Architecture:** Keep the existing layered design: API/runtime supervise execution, engine owns state/exposure, adapters normalize exchange behavior, observability exports artifacts. The changes should be narrowly scoped and covered by behavior tests.

**Tech Stack:** Python 3.11+, asyncio, FastAPI, Decimal, pytest/pytest-asyncio, deterministic simulator, Binance USD-M adapter.

---

### Task 1: Testnet Runner Stream Supervision

**Files:**
- Modify: `scripts/testnet_runner.py`
- Test: `tests/unit/test_binance_order_mutations.py`

- [x] **Step 1: Write failing tests**

Add tests proving the standalone Testnet runner starts both market and user streams and closes both tasks on exit. Also prove `run()` can progress past `health_check_streams()` with a fake Binance adapter whose health requires both streams.

- [x] **Step 2: Verify tests fail**

Run targeted tests and confirm failure is caused by missing user-stream runner support.

- [x] **Step 3: Implement stream supervision**

Update `scripts/testnet_runner.py` so `run()` starts the market stream and user stream before calling the engine. Keep the first market snapshot wait. Keep both tasks alive until the runner exits, then cancel both. Do not fall back to simulation.

- [x] **Step 4: Verify**

Run the targeted tests and then the full suite.

### Task 2: RUNNING Start Time and Pre-Run Validation

**Files:**
- Modify: `src/execution/engine.py`
- Test: `tests/unit/test_engine_lifecycle.py`

- [x] **Step 1: Write failing tests**

Add a test with an adapter that advances the manual clock during `get_position()` / `get_symbol_rules()` and assert `started_monotonic` is set only when the execution enters `RUNNING`. Add tests proving below-min-quantity and below-min-notional targets complete/expire with explicit non-tradeable reason instead of entering `RUNNING`.

- [x] **Step 2: Verify tests fail**

Run targeted tests and confirm failures show early `started_monotonic` and delayed min validation.

- [x] **Step 3: Implement**

Set `started_monotonic` at the `VALIDATING -> RUNNING` transition. During validation, after step-flooring the raw quantity, reject/complete explicit non-tradeable targets when the normalized quantity is below min quantity or cannot satisfy min notional at any legal price bound. Preserve current dust handling and summaries.

- [x] **Step 4: Verify**

Run targeted tests and then the full suite.

### Task 3: Bounded Post-Only Retry

**Files:**
- Modify: `src/execution/models.py`
- Modify: `src/execution/engine.py`
- Modify: `src/api/schemas.py`
- Test: `tests/unit/test_engine_lifecycle.py`
- Test: `tests/unit/test_api.py`

- [x] **Step 1: Write failing tests**

Add tests for repeated retryable post-only rejection: retries must be bounded by a configurable limit and must not submit once the limit is reached. Add schema tests for the new parameter default and validation.

- [x] **Step 2: Verify tests fail**

Run targeted tests and confirm failure is due to unbounded retry or missing schema field.

- [x] **Step 3: Implement**

Add `max_post_only_reject_retries` to execution parameters with a conservative default. Count retryable post-only rejections per execution, require fresh market data before each retry via the existing `run_once` flow, and terminalize/pause with a clear final reason once the retry limit is exceeded. Do not treat terminal Binance rejects as retryable.

- [x] **Step 4: Verify**

Run targeted tests and then the full suite.

### Task 4: TWAP Schedule and Quantity Reporting

**Files:**
- Modify: `src/execution/models.py`
- Modify: `src/execution/engine.py`
- Modify: `src/api/schemas.py`
- Modify: `src/observability/summary.py`
- Modify: `scripts/_sim_demo_common.py`
- Test: `tests/unit/test_twap.py`
- Test: `tests/unit/test_api.py`
- Test: `tests/simulation/test_required_scenarios.py`

- [x] **Step 1: Write failing tests**

Add tests proving terminal TWAP output contains planned, submitted, open, filled, cancelled, unfilled quantities and latest schedule deficit. Add API/artifact tests proving these fields are exported.

- [x] **Step 2: Verify tests fail**

Run targeted tests and confirm missing TWAP ledger/reporting fields.

- [x] **Step 3: Implement**

Track the latest TWAP schedule snapshot in the execution record when TWAP demand is calculated. Extend summary/API/artifact output with the required planned/submitted/open/filled/cancelled/unfilled quantities and schedule deficit. Keep non-TWAP behavior unchanged.

- [x] **Step 4: Verify**

Run targeted tests and then the full suite.

### Task 5: Final Review and Verification

**Files:**
- Review all touched files.

- [x] **Step 1: Run full test suite**

Run `uv run pytest -q` and record the result.

- [x] **Step 2: Final code review**

Review the full diff against this plan. Confirm code-only issues are handled and list any remaining non-code deliverables separately.
