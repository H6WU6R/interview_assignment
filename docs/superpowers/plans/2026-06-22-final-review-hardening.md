# Final Review Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the external review blockers so the execution engine, API runtime, simulator, Binance adapter, tests, and submission materials are credible for final Calais review.

**Architecture:** Keep the design compact but correct. The engine remains the owner of trading correctness, all submit paths go through one exposure gate, adapter uncertainty maps to conservative child states, and FastAPI becomes a real runtime facade instead of a simulator-only manual shell.

**Tech Stack:** Python 3.12, FastAPI, httpx, websockets, pytest, Decimal arithmetic.

---

### Task 1: Engine Fill Ledger And Controlled Runtime States

**Files:**
- Modify: `src/execution/engine.py`
- Modify: `src/execution/models.py`
- Test: `tests/unit/test_engine_lifecycle.py`
- Test: `tests/simulation/test_required_scenarios.py`

Acceptance criteria:
- A `PARTIALLY_FILLED` create response immediately increases parent confirmed fills.
- A lower REST snapshot can never reduce a child cumulative fill.
- `confirmed + live_open + pending_submit + pending_cancel + unknown + new_child <= required_quantity` is enforced after submit, cancel, reconciliation, and unknown recovery.
- Stale or missing market data causes a controlled pause/final reason and no new submit; it does not raise out of `run_once`.
- Aggressive deadline demand marks the child as IOC-capable through `OrderRequest.time_in_force`.

### Task 2: Binance Adapter And Simulator Mutation Semantics

**Files:**
- Modify: `src/exchanges/binance_usdm.py`
- Modify: `src/exchanges/simulator.py`
- Modify: `src/exchanges/base.py`
- Modify: `src/execution/models.py`
- Test: `tests/unit/test_binance_order_mutations.py`
- Test: `tests/simulation/test_simulator_orders.py`

Acceptance criteria:
- Passive children use `GTX`; aggressive deadline children use `IOC`.
- `ConnectError`, `ReadError`, `RemoteProtocolError`, timeout, invalid JSON, and message-aware 503 create/cancel outcomes become conservative `UNKNOWN` or `PENDING_CANCEL` states.
- Simulator fills update account position by side.
- Simulator can script delayed, duplicate, and out-of-order user events without direct private-list mutation.
- Reconciliation is execution-prefix scoped and supports time-window/paginated reads where Binance endpoints allow it.

### Task 3: FastAPI Runtime And Background Execution

**Files:**
- Modify: `src/api/app.py`
- Modify: `src/api/schemas.py`
- Modify: `src/execution/service.py`
- Create or modify: `src/api/runtime.py`
- Test: `tests/unit/test_api.py`

Acceptance criteria:
- `environment=simulation` uses deterministic simulator and manual/test controls.
- `environment=testnet` constructs `BinanceUsdmAdapter` with `SystemClock` and credentials.
- Nonterminal executions are advanced by a background loop; TWAP progresses over real time without external `run-once`.
- Startup/shutdown tasks start and stop cleanly.
- A second active execution for the same account+symbol returns conflict instead of sharing exposure blindly.
- Request validation rejects unsupported symbol, nonpositive bounds, nonpositive timeouts/slices, negative reprice settings, nonfinite decimals, and irrelevant invalid parameter combinations.

### Task 4: Stream Health And Automatic Reconciliation

**Files:**
- Modify: `src/exchanges/binance_usdm.py`
- Modify: `src/api/runtime.py`
- Modify: `src/execution/service.py`
- Test: `tests/unit/test_binance_order_mutations.py`
- Test: `tests/unit/test_api.py`

Acceptance criteria:
- Market stream stays live after the first snapshot.
- Private user stream has listenKey create/renew lifecycle and marks health degraded on disconnect.
- User-stream disconnect pauses new submits, reconciles execution-scoped orders/fills, then resumes safely after fresh market/private health.
- `UNKNOWN` children are automatically reconciled during normal execution ticks, not only through a manual API call.

### Task 5: Verification, Evidence, And Submission Docs

**Files:**
- Modify: `README.md`
- Modify: `reports/report_draft.md`
- Modify: `reports/failure_case_log.md`
- Modify: `reports/submission_manifest.md`
- Test: full suite

Acceptance criteria:
- Add regression tests named for each external review blocker.
- Full local suite passes.
- Report describes the real bugs found and fixed: partial-create fill ledger, stale REST child downgrade, stale market exception, simulator position, and Testnet insufficient-margin evidence.
- Testnet evidence section clearly distinguishes verified connectivity from accepted-order evidence blocked by account margin until funded.
