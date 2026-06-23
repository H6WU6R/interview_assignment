# Critical Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the current P0/P1 execution-correctness, live Binance, simulator, evidence, and packaging gaps before final submission.

**Architecture:** Keep the existing engine/service/adapter boundaries. Start by adding failing regression tests for the reviewed failures, then change the smallest possible engine and adapter code to make those tests pass. Treat final submission evidence as code-owned deliverables: artifact validation, Testnet runner behavior, report truthfulness, and package importability all need automated gates.

**Tech Stack:** Python 3.11+, asyncio, Decimal, FastAPI, httpx, websockets, pytest, pytest-asyncio, Binance USD-M Futures Testnet REST/WebSocket APIs.

---

## Scope Check

This plan spans multiple subsystems. Execute it in phases and commit after each task:

1. Engine safety and fill accounting.
2. Binance adapter/runtime discipline.
3. Simulator/API fidelity.
4. Evidence, packaging, and final-submission gates.

Do not start documentation polish until the engine and adapter regression tests pass. The final PDF/report should describe what the code actually does, not what we wish it did.

## File Structure

### Engine and state-machine files

- Modify `src/execution/engine.py`
  - Reorder `run_once` and `cancel_execution` so unknown orders are reconciled before early returns.
  - Terminalize expired executions even when streams are unhealthy or market data is stale.
  - Replace aggregate-cumulative VWAP logic with per-child delta accounting.
  - Capture arrival bid/ask at `RUNNING` transition.
  - Treat temporary min-notional failures as waiting states until deadline, not immediate terminal failures.

- Modify `src/execution/state_machine.py`
  - Keep unchanged during Tasks 1-8.
  - In Task 9, allow the existing `OPEN -> CANCELLED` transition for IOC no-fill if the simulator test proves the current transition table rejects it.

- Modify `src/execution/models.py`
  - Add `seen_fill_trade_ids` and `trade_fill_vwap_inputs` on `ExecutionRecord` during Task 4 if the current record does not already expose equivalent fields.
  - Add `raw_required_quantity` only in a separate follow-up if completion metrics must distinguish raw target delta from tradable rounded delta.

- Modify `src/observability/summary.py`
  - Report completion against both normalized tradable quantity and raw target-position delta if raw delta is retained.

### Binance live adapter/runtime files

- Modify `src/exchanges/binance_usdm.py`
  - Add exact order lookup based reconciliation for active/unknown execution-scoped children.
  - Sort and page trade/order reconciliation data where broad scans are still used.
  - Make 429, 418, 503, and Binance coded overload outcomes explicit.
  - Add server-time synchronization and listen-key recovery behavior.
  - Remove incorrect listen-key keepalive params.

- Modify `src/exchanges/base.py`
  - Add typed exceptions only when the engine/runtime needs to distinguish rate limit, ban, and stream recovery.

- Modify `src/api/runtime.py`
  - Gate live ticking on private-stream health.
  - Recreate listen keys on expiry.
  - Add lifecycle ownership for any persistent HTTP client if one is introduced.

### Simulator/API files

- Modify `src/exchanges/simulator.py`
  - Make `inject_reconciliation_fill` update simulator order state and position.
  - Model IOC deadline-order terminal behavior.
  - Add direct hooks for ordered, duplicate, and delayed user-stream event testing if engine tests need stream-level coverage.

- Modify `src/api/app.py`
  - Add simulation-only control endpoints for market data, clock advance, fills, and stream health.

- Modify `src/api/schemas.py`
  - Add request/response schemas for the simulation control endpoints.

### Evidence, packaging, and verification files

- Modify `scripts/testnet_runner.py`
  - Make default max runtime at least execution duration plus reconciliation cushion.
  - Fail evidence manifests when accepted exchange order IDs are missing for mandatory runs.
  - Record private stream evidence only for execution-matching order/trade events.

- Create `scripts/verify_submission.py`
  - One command to verify tests, package import, generated report, required raw artifacts, and no stale pending evidence claims.

- Modify `pyproject.toml`
  - Fix package discovery for `src/config.py`.
  - Add quality/dev tools used by the documented verification gate.

- Modify `README.md`, `reports/report_draft.md`, `reports/latex/sections/08-testing-evidence.tex`, `reports/submission_manifest.md`
  - Remove claims that are not backed by artifacts.
  - Point to committed or bundled evidence paths, not transient `/tmp` paths.

## Non-Negotiable Verification Commands

Run these after the relevant task and before each commit:

```bash
uv run pytest -q
uv run pytest tests/unit/test_engine_review_regressions.py -q
uv run pytest tests/unit/test_binance_order_mutations.py -q
uv run pytest tests/unit/test_api.py -q
uv run python scripts/verify_submission.py --allow-missing-testnet-evidence
```

For final submission, the last command must run without `--allow-missing-testnet-evidence`.

---

### Task 1: Add Engine Regression Tests From The Harsh Review

**Files:**
- Create: `tests/unit/test_engine_review_regressions.py`
- Modify: none
- Test: `tests/unit/test_engine_review_regressions.py`

- [ ] **Step 1: Create the regression test file**

Create `tests/unit/test_engine_review_regressions.py` with this content:

```python
from __future__ import annotations

from decimal import Decimal

import pytest

from execution.ids import make_client_order_prefix
from execution.models import (
    Algorithm,
    ChildOrder,
    ChildOrderStatus,
    DeadlinePolicy,
    Fill,
    ReconciliationResult,
    Side,
)
from tests.unit.test_engine_lifecycle import SYMBOL, execution_request, fresh_service


async def test_unknown_create_cancel_reconciles_then_cancels_open_child() -> None:
    service, simulator, _ = await fresh_service()
    execution = await service.create_execution(execution_request())
    prefix = make_client_order_prefix(execution.execution_id)
    simulator.script_create_timeout(prefix)

    after_timeout = await service.run_once(execution.execution_id)
    assert after_timeout.status.value == "RUNNING"
    assert after_timeout.child_orders[0].status is ChildOrderStatus.UNKNOWN
    assert after_timeout.exposure.unknown_order_quantity == Decimal("0.010")

    after_cancel = await service.cancel_execution(execution.execution_id)

    assert after_cancel.status.value == "CANCELLED"
    assert after_cancel.exposure.reserved_exposure == Decimal("0")
    assert after_cancel.child_orders[0].status is ChildOrderStatus.CANCELLED
    assert after_cancel.child_orders[0].exchange_order_id is not None


async def test_unhealthy_stream_does_not_block_expiry() -> None:
    service, simulator, clock = await fresh_service()
    execution = await service.create_execution(execution_request(duration=1))
    opened = await service.run_once(execution.execution_id)
    assert opened.status.value == "RUNNING"
    assert opened.child_orders

    simulator.set_stream_health(user_stream_healthy=False)
    clock.advance(5)
    expired = await service.run_once(execution.execution_id)

    assert expired.status.value in {"EXPIRED", "PARTIALLY_COMPLETED", "CANCELLED"}
    assert expired.status.value != "RUNNING"
    assert expired.completed_monotonic is not None
    assert expired.final_reason is not None


async def test_aggressive_deadline_with_stale_market_terminalizes() -> None:
    service, _simulator, clock = await fresh_service()
    execution = await service.create_execution(
        execution_request(
            duration=1,
            deadline_policy=DeadlinePolicy.AGGRESSIVE_WITHIN_RANGE,
        )
    )
    clock.advance(120)

    terminal = await service.run_once(execution.execution_id)

    assert terminal.status.value == "EXPIRED"
    assert terminal.completed_monotonic is not None
    assert terminal.final_reason in {"DEADLINE_AGGRESSIVE_ATTEMPTED", "MARKET_DATA_STALE_RECONCILED"}
    assert terminal.summary is not None


async def test_late_actual_trade_price_replaces_snapshot_limit_price_for_vwap() -> None:
    service, _simulator, _clock = await fresh_service()
    execution = await service.create_execution(execution_request(target_position=Decimal("0.005")))
    opened = await service.run_once(execution.execution_id)
    child = opened.child_orders[0]

    snapshot_order = ChildOrder(
        child_order_id=child.child_order_id,
        client_order_id=child.client_order_id,
        symbol=SYMBOL,
        side=Side.BUY,
        submitted_quantity=child.submitted_quantity,
        price=child.price,
        status=ChildOrderStatus.FILLED,
        confirmed_filled_quantity=Decimal("0.005"),
        exchange_order_id="order-1",
        raw_status="FILLED",
    )
    after_snapshot = await service.apply_reconciliation_result(
        execution.execution_id,
        ReconciliationResult(orders=[snapshot_order], fills=[]),
    )
    assert after_snapshot.summary is not None
    assert after_snapshot.summary.metrics["execution_vwap"] == "95000"

    actual_fill = Fill(
        client_order_id=child.client_order_id,
        trade_id="actual-trade-1",
        cumulative_filled_quantity=Decimal("0.005"),
        last_filled_quantity=Decimal("0.005"),
        last_fill_price=Decimal("49999.5"),
        event_time_ms=1,
        transaction_time_ms=1,
        is_maker=True,
    )
    after_fill = await service.apply_reconciliation_result(
        execution.execution_id,
        ReconciliationResult(orders=[], fills=[actual_fill]),
    )

    assert after_fill.summary is not None
    assert after_fill.summary.metrics["execution_vwap"] == "49999.5"
    assert after_fill.metric_counts.get("duplicate_events_ignored", 0) == 0


async def test_out_of_order_fills_compute_vwap_from_each_trade_delta() -> None:
    service, _simulator, _clock = await fresh_service()
    execution = await service.create_execution(execution_request(target_position=Decimal("0.006")))
    opened = await service.run_once(execution.execution_id)
    child = opened.child_orders[0]

    second_fill_first = Fill(
        client_order_id=child.client_order_id,
        trade_id="trade-2",
        cumulative_filled_quantity=Decimal("0.006"),
        last_filled_quantity=Decimal("0.002"),
        last_fill_price=Decimal("95010"),
        event_time_ms=20,
        transaction_time_ms=20,
        is_maker=False,
    )
    first_fill_late = Fill(
        client_order_id=child.client_order_id,
        trade_id="trade-1",
        cumulative_filled_quantity=Decimal("0.004"),
        last_filled_quantity=Decimal("0.004"),
        last_fill_price=Decimal("95005"),
        event_time_ms=10,
        transaction_time_ms=10,
        is_maker=True,
    )

    after = await service.apply_reconciliation_result(
        execution.execution_id,
        ReconciliationResult(orders=[], fills=[second_fill_first, first_fill_late]),
    )

    assert after.summary is not None
    assert after.fill_vwap_inputs == [
        (Decimal("95005"), Decimal("0.004")),
        (Decimal("95010"), Decimal("0.002")),
    ]
    assert after.summary.metrics["execution_vwap"] == "95006.66666666666666666666667"


async def test_temporary_sell_min_notional_waits_until_price_becomes_valid() -> None:
    service, simulator, _clock = await fresh_service(
        bid=Decimal("4000"),
        ask=Decimal("4001"),
        position=Decimal("0"),
    )
    execution = await service.create_execution(
        execution_request(
            target_position=Decimal("-0.001"),
            lower=Decimal("1"),
            upper=Decimal("100000"),
            duration=100,
        )
    )

    waiting = await service.run_once(execution.execution_id)
    assert waiting.status.value == "RUNNING"
    assert waiting.child_orders == []
    assert waiting.final_reason == "ORDER_SHAPE_TEMPORARILY_UNTRADEABLE"

    await simulator.push_market_data(SYMBOL, Decimal("5000"), Decimal("5001"), exchange_event_time=20)
    active = await service.run_once(execution.execution_id)

    assert active.status.value == "RUNNING"
    assert len(active.child_orders) == 1
    assert active.child_orders[0].side is Side.SELL
```

- [ ] **Step 2: Run the new tests and verify they fail**

Run:

```bash
uv run pytest tests/unit/test_engine_review_regressions.py -q
```

Expected: multiple failures matching the audit:

```text
FAILED ... test_unknown_create_cancel_reconciles_then_cancels_open_child
FAILED ... test_unhealthy_stream_does_not_block_expiry
FAILED ... test_late_actual_trade_price_replaces_snapshot_limit_price_for_vwap
FAILED ... test_out_of_order_fills_compute_vwap_from_each_trade_delta
FAILED ... test_temporary_sell_min_notional_waits_until_price_becomes_valid
```

- [ ] **Step 3: Commit the failing tests**

Run:

```bash
git add tests/unit/test_engine_review_regressions.py
git commit -m "test: capture critical engine review regressions"
```

Expected: one commit containing only `tests/unit/test_engine_review_regressions.py`.

---

### Task 2: Fix Unknown Create Timeout, Cancel, And Expiry Lifecycle

**Files:**
- Modify: `src/execution/engine.py`
- Test: `tests/unit/test_engine_review_regressions.py`

- [ ] **Step 1: Change `cancel_execution` to reconcile unknowns before canceling**

In `src/execution/engine.py`, replace the body of the inner `cancel()` function with this structure:

```python
        async def cancel() -> ExecutionRecord:
            if record.status.is_terminal:
                return self._snapshot(record)

            if record.status is not ExecutionStatus.CANCELLING:
                record.status = transition_execution(record.status, ExecutionStatus.CANCELLING)
                record.final_reason = CANCEL_REQUESTED

            await self._reconcile_locked(record, exact_unknown_lookup=True)
            await self._cancel_active_children_locked(record)
            await self._reconcile_locked(record, exact_unknown_lookup=True)
            if self._target_filled(record):
                self._complete_locked(record, TARGET_QUANTITY_FILLED)
            else:
                self._terminalize_manual_cancel_if_clear_locked(record)
            return self._snapshot(record)
```

This makes a create-timeout order that later resolves to `OPEN` cancellable in the same user cancel request.

- [ ] **Step 2: Change unknown exposure handling in `run_once`**

In `run_once`, replace:

```python
            if record.exposure.unknown_order_quantity > Decimal("0"):
                return self._snapshot(record)
```

with:

```python
            if record.exposure.unknown_order_quantity > Decimal("0"):
                await self._reconcile_locked(record, exact_unknown_lookup=True)
                if record.exposure.unknown_order_quantity > Decimal("0"):
                    if self._deadline_reached(record):
                        record.final_reason = CREATE_TIMEOUT_PENDING_RECONCILIATION
                    return self._snapshot(record)
```

- [ ] **Step 3: Run the focused lifecycle tests**

Run:

```bash
uv run pytest tests/unit/test_engine_review_regressions.py::test_unknown_create_cancel_reconciles_then_cancels_open_child -q
```

Expected:

```text
1 passed
```

- [ ] **Step 4: Run broader engine lifecycle tests**

Run:

```bash
uv run pytest tests/unit/test_engine_lifecycle.py tests/unit/test_engine_review_regressions.py -q
```

Expected: the unknown-create regression passes and no existing lifecycle test regresses.

- [ ] **Step 5: Commit**

Run:

```bash
git add src/execution/engine.py tests/unit/test_engine_review_regressions.py
git commit -m "fix: reconcile unknown orders before cancel and run exits"
```

---

### Task 3: Fix Deadline Terminalization Under Unhealthy Streams And Stale Market Data

**Files:**
- Modify: `src/execution/engine.py`
- Test: `tests/unit/test_engine_review_regressions.py`

- [ ] **Step 1: Move deadline checks before stream-health early return**

In `src/execution/engine.py`, in `run_once`, keep the initial terminal and cancelling branches, then order the next branches like this:

```python
            if record.exposure.unknown_order_quantity > Decimal("0"):
                await self._reconcile_locked(record, exact_unknown_lookup=True)
                if record.exposure.unknown_order_quantity > Decimal("0"):
                    if self._deadline_reached(record):
                        record.final_reason = CREATE_TIMEOUT_PENDING_RECONCILIATION
                    return self._snapshot(record)

            await self._reconcile_locked(record)

            if self._target_filled(record):
                self._complete_locked(record, TARGET_QUANTITY_FILLED)
                return self._snapshot(record)

            if self._deadline_reached(record) and not await self._adapter.health_check_streams():
                await self._cancel_active_children_locked(record)
                await self._reconcile_locked(record, exact_unknown_lookup=True)
                self._terminalize_deadline_locked(record, STREAM_HEALTH_DEGRADED_RECONCILED)
                return self._snapshot(record)

            if not await self._adapter.health_check_streams():
                record.final_reason = STREAM_HEALTH_DEGRADED_RECONCILED
                return self._snapshot(record)
```

Keep the existing later deadline-policy checks below this block.

- [ ] **Step 2: Make stale market data terminalize after deadline**

Find the `except NoFreshMarketData` branch around the call to `_build_child_demand_locked`. Replace it with this behavior:

```python
            except NoFreshMarketData:
                record.final_reason = MARKET_DATA_STALE_RECONCILED
                await self._reconcile_locked(record, exact_unknown_lookup=True)
                if self._deadline_reached(record):
                    await self._cancel_active_children_locked(record)
                    await self._reconcile_locked(record, exact_unknown_lookup=True)
                    self._terminalize_deadline_locked(record, MARKET_DATA_STALE_RECONCILED)
                return self._snapshot(record)
```

If the current code catches `NoFreshMarketData` in more than one place, apply this only to the parent `run_once` decision path. Do not change adapter-level freshness validation.

- [ ] **Step 3: Allow deadline terminalization after cancellation clears exposure**

Review `_terminalize_deadline_locked`. Keep the reserved-exposure guard, but ensure the caller cancels and reconciles before calling it. Do not remove the guard unless a test proves the engine can safely terminalize with no live exposure left.

- [ ] **Step 4: Run focused tests**

Run:

```bash
uv run pytest tests/unit/test_engine_review_regressions.py::test_unhealthy_stream_does_not_block_expiry tests/unit/test_engine_review_regressions.py::test_aggressive_deadline_with_stale_market_terminalizes -q
```

Expected:

```text
2 passed
```

- [ ] **Step 5: Commit**

Run:

```bash
git add src/execution/engine.py tests/unit/test_engine_review_regressions.py
git commit -m "fix: terminalize expired executions despite stale streams"
```

---

### Task 4: Replace Aggregate Fill Accounting With Per-Trade VWAP Inputs

**Files:**
- Modify: `src/execution/engine.py`
- Test: `tests/unit/test_engine_review_regressions.py`

- [ ] **Step 1: Sort reconciliation fills before applying them**

In `_reconcile_locked`, replace:

```python
        for fill in result.fills:
```

with:

```python
        sorted_fills = sorted(
            result.fills,
            key=lambda fill: (
                fill.event_time_ms if fill.event_time_ms is not None else 0,
                fill.transaction_time_ms if fill.transaction_time_ms is not None else 0,
                fill.trade_id or "",
            ),
        )
        for fill in sorted_fills:
```

- [ ] **Step 2: Change `_update_child_cumulative_fill_locked` to compute child-level delta**

Replace `_update_child_cumulative_fill_locked` and `_apply_order_fill_locked` with this implementation:

```python
    def _update_child_cumulative_fill_locked(
        self,
        record: ExecutionRecord,
        child: ChildOrder,
        incoming_cumulative: Decimal,
        *,
        trade_id: str | None,
        fill_price: Decimal,
        is_maker: bool | None = None,
        authoritative_trade: bool = True,
    ) -> None:
        tracker = self._require_exposure_tracker(record)

        if trade_id is not None and trade_id in record.seen_fill_trade_ids:
            self._increment_metric(record, "duplicate_events_ignored")
            record.ignored_fill_trade_ids.add(trade_id)
            return

        previous_child_cumulative = child.confirmed_filled_quantity
        if incoming_cumulative > previous_child_cumulative:
            child_delta = incoming_cumulative - previous_child_cumulative
            child.confirmed_filled_quantity = incoming_cumulative
            aggregate_cumulative = tracker.exposure.confirmed_filled_quantity + child_delta
            tracker.apply_fill(trade_id, aggregate_cumulative)
            if authoritative_trade:
                record.fill_vwap_inputs.append((fill_price, child_delta))
                if is_maker is True:
                    record.maker_filled_quantity += child_delta
                    self._increment_metric(record, "maker_fills")
                elif is_maker is False:
                    record.taker_filled_quantity += child_delta
                    self._increment_metric(record, "taker_fills")
            return

        if trade_id is not None:
            record.seen_fill_trade_ids.add(trade_id)
            record.ignored_fill_trade_ids.add(trade_id)
            self._increment_metric(record, "duplicate_events_ignored")
```

If `ExecutionRecord` does not currently have `seen_fill_trade_ids`, add this field:

```python
    seen_fill_trade_ids: set[str] = field(default_factory=set)
```

If the current record already uses a differently named set for trade IDs, use that existing set and do not add another field.

- [ ] **Step 3: Treat order snapshots as non-authoritative for VWAP**

For every call where fill quantity comes from an order snapshot or cancel/create response instead of a trade fill, pass `authoritative_trade=False`. The affected call sites are:

```python
                self._update_child_cumulative_fill_locked(
                    record,
                    child,
                    child.confirmed_filled_quantity,
                    trade_id=None,
                    fill_price=child.price,
                    authoritative_trade=False,
                )
```

```python
            self._update_child_cumulative_fill_locked(
                record,
                child,
                exchange_order.confirmed_filled_quantity,
                trade_id=None,
                fill_price=child.price,
                authoritative_trade=False,
            )
```

```python
                self._update_child_cumulative_fill_locked(
                    record,
                    child,
                    child.confirmed_filled_quantity,
                    trade_id=None,
                    fill_price=child.price,
                    authoritative_trade=False,
                )
```

- [ ] **Step 4: Let late authoritative trades repair provisional VWAP**

Add an execution-record field for trade-level fills:

```python
    trade_fill_vwap_inputs: dict[str, tuple[Decimal, Decimal]] = field(default_factory=dict)
```

Then, for authoritative fills with `trade_id is not None`, update both `trade_fill_vwap_inputs` and `fill_vwap_inputs` from the sorted values:

```python
            if authoritative_trade and trade_id is not None:
                record.trade_fill_vwap_inputs[trade_id] = (fill_price, child_delta)
                record.fill_vwap_inputs = [
                    record.trade_fill_vwap_inputs[key]
                    for key in sorted(record.trade_fill_vwap_inputs)
                ]
```

If this conflicts with event-time ordering, store `(event_time_ms, transaction_time_ms, trade_id, price, quantity)` in a small dataclass instead. Keep the public `fill_vwap_inputs` as `list[tuple[Decimal, Decimal]]`.

- [ ] **Step 5: Run focused tests**

Run:

```bash
uv run pytest tests/unit/test_engine_review_regressions.py::test_late_actual_trade_price_replaces_snapshot_limit_price_for_vwap tests/unit/test_engine_review_regressions.py::test_out_of_order_fills_compute_vwap_from_each_trade_delta -q
```

Expected:

```text
2 passed
```

- [ ] **Step 6: Run full engine tests**

Run:

```bash
uv run pytest tests/unit/test_engine_lifecycle.py tests/simulation/test_required_scenarios.py tests/unit/test_engine_review_regressions.py -q
```

Expected: all selected tests pass.

- [ ] **Step 7: Commit**

Run:

```bash
git add src/execution/engine.py src/execution/models.py tests/unit/test_engine_review_regressions.py
git commit -m "fix: compute execution vwap from trade-level fills"
```

---

### Task 5: Fix Arrival Benchmark And Temporary Min-Notional Waiting

**Files:**
- Modify: `src/execution/engine.py`
- Modify: `src/execution/models.py` if raw target delta is stored
- Modify: `src/observability/summary.py` if completion metrics change
- Test: `tests/unit/test_engine_review_regressions.py`

- [ ] **Step 1: Capture arrival market at `RUNNING` transition**

In `create_execution`, immediately before:

```python
            record.exposure_tracker = ExposureTracker(required_quantity)
            record.started_monotonic = self._now_decimal()
            record.status = transition_execution(record.status, ExecutionStatus.RUNNING)
```

fetch and store the market snapshot:

```python
            arrival_market = await self._adapter.get_best_bid_ask(record.request.symbol)
            record.arrival_bid = arrival_market.bid
            record.arrival_ask = arrival_market.ask
            record.exposure_tracker = ExposureTracker(required_quantity)
            record.started_monotonic = self._now_decimal()
            record.status = transition_execution(record.status, ExecutionStatus.RUNNING)
```

If market data is missing at create time, fail request creation with the existing stale-market exception rather than creating a parent that cannot benchmark slippage.

- [ ] **Step 2: Add a regression test for arrival benchmark**

Append this test to `tests/unit/test_engine_review_regressions.py`:

```python
async def test_arrival_price_is_captured_when_execution_starts() -> None:
    service, simulator, _clock = await fresh_service(bid=Decimal("50000"), ask=Decimal("50001"))
    execution = await service.create_execution(execution_request())
    await simulator.push_market_data(SYMBOL, Decimal("51000"), Decimal("51001"), exchange_event_time=20)

    opened = await service.run_once(execution.execution_id)

    assert opened.arrival_bid == Decimal("50000")
    assert opened.arrival_ask == Decimal("50001")
    assert opened.child_orders[0].price == Decimal("51000")
```

- [ ] **Step 3: Add a named temporary-shape reason**

In `src/execution/engine.py`, define a constant next to the existing final-reason constants:

```python
ORDER_SHAPE_TEMPORARILY_UNTRADEABLE = "ORDER_SHAPE_TEMPORARILY_UNTRADEABLE"
```

- [ ] **Step 4: Wait on temporary min-notional until deadline**

In `_build_child_demand_locked`, replace this branch:

```python
            self._expire_for_validation_locked(record, exc)
            return None
```

with:

```python
            if self._is_temporary_order_shape_error(exc):
                if self._deadline_reached(record):
                    self._expire_for_validation_locked(record, exc)
                else:
                    record.final_reason = ORDER_SHAPE_TEMPORARILY_UNTRADEABLE
                return None

            self._expire_for_validation_locked(record, exc)
            return None
```

Add this helper near `_is_price_outside_range_error`:

```python
    def _is_temporary_order_shape_error(self, exc: ValidationError) -> bool:
        message = str(exc)
        return "minimum notional" in message.lower()
```

If the actual validation message differs, run `uv run pytest tests/unit/test_validation.py -q` and match the exact string emitted by `validate_child_order_safety`.

- [ ] **Step 5: Run focused tests**

Run:

```bash
uv run pytest tests/unit/test_engine_review_regressions.py::test_arrival_price_is_captured_when_execution_starts tests/unit/test_engine_review_regressions.py::test_temporary_sell_min_notional_waits_until_price_becomes_valid -q
```

Expected:

```text
2 passed
```

- [ ] **Step 6: Commit**

Run:

```bash
git add src/execution/engine.py tests/unit/test_engine_review_regressions.py
git commit -m "fix: capture arrival benchmark and wait on temporary notional"
```

---

### Task 6: Reduce Binance Reconciliation Weight And Make It Complete

**Files:**
- Modify: `src/exchanges/binance_usdm.py`
- Test: `tests/unit/test_binance_order_mutations.py`

- [ ] **Step 1: Add tests for exact order lookup preference**

Append this test to `tests/unit/test_binance_order_mutations.py`:

```python
async def test_reconcile_execution_orders_prefers_exact_client_order_lookup() -> None:
    client = RecordingClient(
        FakeResponse(
            200,
            {
                "symbol": "BTCUSDT",
                "clientOrderId": "ce_abcdef123456_1",
                "side": "BUY",
                "origQty": "0.010",
                "price": "95000",
                "status": "NEW",
                "executedQty": "0",
                "orderId": 123,
                "timeInForce": "GTX",
            },
        )
    )
    adapter = authed_adapter(client)

    order = await adapter.get_order_by_client_order_id("BTCUSDT", "ce_abcdef123456_1")

    assert order is not None
    assert order.client_order_id == "ce_abcdef123456_1"
    assert len(client.calls) == 1
    assert client.calls[0]["url"].endswith("/fapi/v1/order")
    assert client.calls[0]["params"]["origClientOrderId"] == "ce_abcdef123456_1"
```

- [ ] **Step 2: Add tests for trade sorting**

Append this test:

```python
async def test_reconcile_orders_and_fills_sorts_user_trades_before_cumulative() -> None:
    responses = [
        FakeResponse(200, []),
        FakeResponse(
            200,
            [
                {
                    "symbol": "BTCUSDT",
                    "clientOrderId": "ce_abcdef123456_1",
                    "side": "BUY",
                    "origQty": "0.006",
                    "price": "95000",
                    "status": "FILLED",
                    "executedQty": "0.006",
                    "orderId": 123,
                    "timeInForce": "GTX",
                }
            ],
        ),
        FakeResponse(
            200,
            [
                {"id": 2, "orderId": 123, "qty": "0.002", "price": "95010", "time": 20, "maker": False},
                {"id": 1, "orderId": 123, "qty": "0.004", "price": "95005", "time": 10, "maker": True},
            ],
        ),
    ]
    client = SequenceClient(responses)
    adapter = authed_adapter(client)

    result = await adapter.reconcile_orders_and_fills("BTCUSDT", client_order_prefix="ce_abcdef123456_")

    assert [fill.trade_id for fill in result.fills] == ["1", "2"]
    assert [fill.cumulative_filled_quantity for fill in result.fills] == [
        Decimal("0.004"),
        Decimal("0.006"),
    ]
```

Add this helper if `SequenceClient` does not exist:

```python
class SequenceClient:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[dict[str, Any]] = []

    async def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        return self.responses.pop(0)
```

- [ ] **Step 3: Sort trades in `reconcile_orders_and_fills`**

In `src/exchanges/binance_usdm.py`, replace:

```python
        for raw_fill in trades_raw:
```

with:

```python
        sorted_trades_raw = sorted(
            trades_raw,
            key=lambda raw_fill: (
                int(raw_fill.get("time", 0)),
                int(raw_fill.get("id", 0)),
            ),
        )
        for raw_fill in sorted_trades_raw:
```

- [ ] **Step 4: Add page helpers for broad fallback**

Add this private helper to `BinanceUsdmAdapter`:

```python
    async def _signed_request_pages(
        self,
        path: str,
        base_params: Mapping[str, Any],
        *,
        id_field: str,
        max_pages: int = 10,
    ) -> list[Any]:
        collected: list[Any] = []
        next_id = base_params.get(id_field)
        for _ in range(max_pages):
            params = dict(base_params)
            if next_id is not None:
                params[id_field] = next_id
            params["limit"] = 1000
            page = await self._signed_request("GET", path, params)
            if not isinstance(page, list):
                return collected
            collected.extend(page)
            if len(page) < 1000:
                return collected
            last = page[-1]
            raw_next_id = last.get("id") if id_field == "fromId" else last.get("orderId")
            if raw_next_id is None:
                return collected
            next_id = int(raw_next_id) + 1
        return collected
```

Use it for `ALL_ORDERS_PATH` with `id_field="orderId"` and `USER_TRADES_PATH` with `id_field="fromId"` only in final reconciliation or explicit reconcile calls. For ordinary active ticks, prefer exact known `clientOrderId` lookups from the engine.

- [ ] **Step 5: Run Binance adapter tests**

Run:

```bash
uv run pytest tests/unit/test_binance_order_mutations.py -q
```

Expected: all Binance unit tests pass.

- [ ] **Step 6: Commit**

Run:

```bash
git add src/exchanges/binance_usdm.py tests/unit/test_binance_order_mutations.py
git commit -m "fix: make binance reconciliation ordered and bounded"
```

---

### Task 7: Harden Binance Error Handling, Time Sync, And Listen-Key Recovery

**Files:**
- Modify: `src/exchanges/base.py`
- Modify: `src/exchanges/binance_usdm.py`
- Modify: `src/api/runtime.py`
- Test: `tests/unit/test_binance_order_mutations.py`

- [ ] **Step 1: Add typed exchange exceptions**

In `src/exchanges/base.py`, add:

```python
class ExchangeRateLimited(RuntimeError):
    """Raised when the venue asks the client to back off before retrying."""

    pass


class ExchangeBanned(RuntimeError):
    """Raised when the venue returns a hard ban response."""

    pass


class ListenKeyExpired(RuntimeError):
    """Raised when a user-data listen key is expired or missing."""

    pass
```

- [ ] **Step 2: Add tests for 503/-1008 and 429/418 handling**

Append:

```python
async def test_binance_503_system_overload_is_retryable_not_terminal() -> None:
    adapter = authed_adapter(FakeResponse(503, {"code": -1008, "msg": "Request throttled by system-level protection."}))

    with pytest.raises(RetryableReadFailure, match="SYSTEM_OVERLOAD"):
        await adapter._signed_request("POST", ORDER_REST_PATH, {}, mutation_kind=MutationKind.CREATE)


async def test_binance_429_and_418_have_typed_failures() -> None:
    rate_limited = authed_adapter(FakeResponse(429, {"code": -1003, "msg": "Too many requests"}))
    with pytest.raises(ExchangeRateLimited):
        await rate_limited._signed_request("GET", ORDER_QUERY_PATH, {})

    banned = authed_adapter(FakeResponse(418, {"code": -1003, "msg": "IP banned"}))
    with pytest.raises(ExchangeBanned):
        await banned._signed_request("GET", ORDER_QUERY_PATH, {})
```

Import the new exceptions at the top of the test file.

- [ ] **Step 3: Implement typed handling in `_signed_request`**

In `src/exchanges/binance_usdm.py`, import the new exceptions and replace the 429/418 branches:

```python
        if response.status_code == 429:
            raise ExchangeRateLimited("RATE_LIMIT_BACKOFF")
        if response.status_code == 418:
            raise ExchangeBanned("VENUE_BAN_HARD_STOP")
```

Change `_is_specific_terminal_5xx_reject` so `-1008` is retryable:

```python
def _is_specific_terminal_5xx_reject(reason: str) -> bool:
    retryable_5xx_reasons = {
        "BINANCE_-1000",
        "BINANCE_-1001",
        "BINANCE_-1006",
        "BINANCE_-1007",
        "BINANCE_-1008",
        "HTTP_503",
    }
    return reason.startswith("BINANCE_") and reason not in retryable_5xx_reasons
```

Before raising `UnknownCreateOutcome` for a 503 create response, if `reason == "BINANCE_-1008"`, raise `RetryableReadFailure("SYSTEM_OVERLOAD")`.

- [ ] **Step 4: Remove incorrect keepalive params**

Change `renew_listen_key` from:

```python
        await self._api_key_request("PUT", LISTEN_KEY_PATH, params={"listenKey": listen_key})
```

to:

```python
        await self._api_key_request("PUT", LISTEN_KEY_PATH, params={})
```

When `_api_key_request` sees Binance code `-1125`, raise `ListenKeyExpired("LISTEN_KEY_EXPIRED")`.

- [ ] **Step 5: Add server-time synchronization**

Add:

```python
TIME_PATH = "/fapi/v1/time"
```

Add a method to `BinanceUsdmAdapter`:

```python
    async def synchronize_server_time(self) -> int:
        timeout = httpx.Timeout(5.0)
        url = f"{self.base_url}{TIME_PATH}"
        if self.client is None:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.request("GET", url)
        else:
            response = await self.client.request("GET", url, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
        server_time_ms = int(payload["serverTime"])
        local_time_ms = self._clock_wall_ms()
        self.server_time_offset_ms = server_time_ms - local_time_ms
        return self.server_time_offset_ms
```

Call `await adapter.synchronize_server_time()` in runtime Testnet adapter construction before starting signed calls.

- [ ] **Step 6: Recreate listen key on expiry**

In `src/api/runtime.py`, in the keepalive loop, catch `ListenKeyExpired` and call `adapter.start_user_stream()` immediately. Store the returned key in the same runtime state used by the stream task, cancel the stale private stream task, and start a new private stream task with the new key.

Use this structure:

```python
            except ListenKeyExpired:
                new_listen_key = await adapter.start_user_stream()
                latest_listen_key = new_listen_key
                await self._restart_user_stream(environment, adapter, new_listen_key)
                continue
```

If `_restart_user_stream` does not exist, create it in `ExecutionRuntime` and make it cancel the old task for `(environment, "user")` before scheduling the new stream.

- [ ] **Step 7: Run focused tests**

Run:

```bash
uv run pytest tests/unit/test_binance_order_mutations.py -q
```

Expected: all tests pass, including the new error-handling tests.

- [ ] **Step 8: Commit**

Run:

```bash
git add src/exchanges/base.py src/exchanges/binance_usdm.py src/api/runtime.py tests/unit/test_binance_order_mutations.py
git commit -m "fix: harden binance error handling and stream recovery"
```

---

### Task 8: Make The HTTP Simulator Actually Scriptable

**Files:**
- Modify: `src/api/schemas.py`
- Modify: `src/api/app.py`
- Test: `tests/unit/test_api.py`

- [ ] **Step 1: Add simulation control schemas**

In `src/api/schemas.py`, add:

```python
class SimulationMarketDataRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    bid: Decimal
    ask: Decimal
    exchange_event_time: int | None = None

    @field_validator("bid", "ask", mode="before")
    @classmethod
    def validate_decimal_string(cls, value: object) -> Decimal:
        return parse_decimal_string(value)


class SimulationClockAdvanceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    seconds: Decimal

    @field_validator("seconds", mode="before")
    @classmethod
    def validate_seconds(cls, value: object) -> Decimal:
        parsed = parse_decimal_string(value)
        if parsed <= Decimal("0"):
            raise ValueError("seconds must be greater than 0")
        return parsed


class SimulationFillRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    client_order_id: str
    quantity: Decimal
    price: Decimal

    @field_validator("quantity", "price", mode="before")
    @classmethod
    def validate_decimal_string(cls, value: object) -> Decimal:
        return parse_decimal_string(value)


class SimulationStreamHealthRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_stream_healthy: bool
```

- [ ] **Step 2: Add simulation-only endpoints**

In `src/api/app.py`, import the schemas and add:

```python
    @app.post("/simulation/market-data")
    async def push_simulation_market_data(request: SimulationMarketDataRequest) -> dict[str, str]:
        await runtime.simulation_adapter.push_market_data(
            request.symbol,
            request.bid,
            request.ask,
            exchange_event_time=request.exchange_event_time,
        )
        return {"status": "ok"}

    @app.post("/simulation/clock/advance")
    async def advance_simulation_clock(request: SimulationClockAdvanceRequest) -> dict[str, str]:
        runtime.simulation_clock.advance(float(request.seconds))
        return {"status": "ok", "monotonic": str(runtime.simulation_clock.monotonic())}

    @app.post("/simulation/fills")
    async def push_simulation_fill(request: SimulationFillRequest) -> dict[str, str]:
        fill = await runtime.simulation_adapter.push_fill(
            request.client_order_id,
            request.quantity,
            request.price,
        )
        return {"status": "ok", "trade_id": fill.trade_id}

    @app.post("/simulation/stream-health")
    async def set_simulation_stream_health(request: SimulationStreamHealthRequest) -> dict[str, str]:
        runtime.simulation_adapter.set_stream_health(user_stream_healthy=request.user_stream_healthy)
        return {"status": "ok"}
```

These endpoints operate only on the built-in simulation adapter. They must not accept an `environment` parameter.

- [ ] **Step 3: Add API tests**

Append to `tests/unit/test_api.py`:

```python
async def test_simulation_http_controls_drive_execution_to_order() -> None:
    app = create_app(background_tick_interval_seconds=0.05)

    market_response = await post_json(
        app,
        "/simulation/market-data",
        {"symbol": SYMBOL, "bid": "95000", "ask": "95001", "exchange_event_time": 10},
    )
    assert market_response.status_code == 200

    create_response = await post_json(app, "/executions", execution_payload(target_duration_seconds=10))
    assert create_response.status_code == 200
    execution_id = create_response.json()["execution_id"]

    run_response = await post_json(app, f"/executions/{execution_id}/run-once")
    assert run_response.status_code == 200
    body = run_response.json()
    assert body["status"] == "RUNNING"
    assert len(body["child_orders"]) == 1


async def test_simulation_clock_advance_expires_execution() -> None:
    app = create_app(background_tick_interval_seconds=0.05)
    await post_json(app, "/simulation/market-data", {"symbol": SYMBOL, "bid": "95000", "ask": "95001"})
    create_response = await post_json(app, "/executions", execution_payload(target_duration_seconds=1))
    execution_id = create_response.json()["execution_id"]
    await post_json(app, f"/executions/{execution_id}/run-once")

    advance_response = await post_json(app, "/simulation/clock/advance", {"seconds": "5"})
    assert advance_response.status_code == 200

    terminal_response = await post_json(app, f"/executions/{execution_id}/run-once")
    assert terminal_response.status_code == 200
    assert terminal_response.json()["status"] in {"EXPIRED", "PARTIALLY_COMPLETED", "CANCELLED"}
```

- [ ] **Step 4: Run API tests**

Run:

```bash
uv run pytest tests/unit/test_api.py -q
```

Expected: all API tests pass.

- [ ] **Step 5: Commit**

Run:

```bash
git add src/api/schemas.py src/api/app.py tests/unit/test_api.py
git commit -m "feat: add deterministic simulation api controls"
```

---

### Task 9: Fix Simulator Fill And IOC Fidelity

**Files:**
- Modify: `src/exchanges/simulator.py`
- Test: `tests/simulation/test_simulator_orders.py`
- Test: `tests/unit/test_engine_review_regressions.py`

- [ ] **Step 1: Add simulator regression tests**

Append to `tests/simulation/test_simulator_orders.py`:

```python
async def test_injected_reconciliation_fill_updates_order_and_position() -> None:
    clock = ManualClock()
    simulator = DeterministicSimulator(clock=clock, position=Decimal("0"))
    await simulator.push_market_data("BTCUSDT", Decimal("95000"), Decimal("95001"))
    order = await simulator.submit_limit_order(
        OrderRequest(
            child_order_id="child_1",
            client_order_id="ce_abcdef123456_1",
            symbol="BTCUSDT",
            side=Side.BUY,
            quantity=Decimal("0.005"),
            price=Decimal("95000"),
            post_only=True,
        )
    )

    simulator.inject_reconciliation_fill(
        Fill(
            client_order_id=order.client_order_id,
            trade_id="injected-1",
            cumulative_filled_quantity=Decimal("0.005"),
            last_filled_quantity=Decimal("0.005"),
            last_fill_price=Decimal("95000"),
            event_time_ms=1,
            transaction_time_ms=1,
        )
    )

    stored = simulator._order_by_client_order_id(order.client_order_id)
    assert stored.confirmed_filled_quantity == Decimal("0.005")
    assert stored.status is ChildOrderStatus.FILLED
    assert simulator.position == Decimal("0.005")
```

- [ ] **Step 2: Implement injected fill state update**

Replace `inject_reconciliation_fill` in `src/exchanges/simulator.py` with:

```python
    def inject_reconciliation_fill(self, fill: Fill) -> Fill:
        if fill.last_filled_quantity <= Decimal("0"):
            raise ValueError("fill quantity must be positive")
        order = self._order_by_client_order_id(fill.client_order_id)
        if fill.cumulative_filled_quantity < order.confirmed_filled_quantity:
            raise SimulatorOrderRejected(f"fill cumulative regresses for {fill.client_order_id}")
        delta = fill.cumulative_filled_quantity - order.confirmed_filled_quantity
        if delta > order.remaining_quantity:
            raise SimulatorOrderRejected(f"fill exceeds remaining quantity for {fill.client_order_id}")
        order.confirmed_filled_quantity = fill.cumulative_filled_quantity
        target = self._open_status_for(order)
        if order.status != target:
            order.status = transition_child(order.status, target)
        if order.side is Side.BUY:
            self.position += delta
        elif order.side is Side.SELL:
            self.position -= delta
        self._fills.append(fill)
        return fill
```

- [ ] **Step 3: Model IOC orders in simulator**

In `submit_limit_order`, after `_validate_order_time_in_force` and before creating an open order, add:

```python
        if order_request.time_in_force is TimeInForce.IOC:
            order = self._create_open_order(order_request)
            snapshot = await self.get_best_bid_ask(order_request.symbol)
            marketable = (
                order_request.side is Side.BUY and order_request.price >= snapshot.ask
            ) or (
                order_request.side is Side.SELL and order_request.price <= snapshot.bid
            )
            if marketable:
                fill_price = snapshot.ask if order_request.side is Side.BUY else snapshot.bid
                fill = await self.push_fill(order.client_order_id, order.remaining_quantity, fill_price)
                order.status = transition_child(order.status, ChildOrderStatus.FILLED)
                return order
            order.status = transition_child(order.status, ChildOrderStatus.CANCELLED)
            await self._user_event_queue.put(
                SimulatorOrderEvent(
                    kind="order_cancelled",
                    client_order_id=order.client_order_id,
                    order=order,
                )
            )
            return order
```

If the transition from `OPEN` to `CANCELLED` fails for IOC no-fill, update the child state machine in the same task and add a dedicated state-machine test.

- [ ] **Step 4: Run simulator tests**

Run:

```bash
uv run pytest tests/simulation/test_simulator_orders.py tests/unit/test_engine_review_regressions.py -q
```

Expected: all selected tests pass.

- [ ] **Step 5: Commit**

Run:

```bash
git add src/exchanges/simulator.py tests/simulation/test_simulator_orders.py tests/unit/test_engine_review_regressions.py
git commit -m "fix: align simulator fills and ioc behavior with engine"
```

---

### Task 10: Make Testnet Evidence Honest And Hard To Overstate

**Files:**
- Modify: `scripts/testnet_runner.py`
- Create: `tests/unit/test_testnet_runner_evidence.py`
- Modify: `README.md`
- Modify: `reports/submission_manifest.md`

- [ ] **Step 1: Add tests for runner defaults and manifest strictness**

Create `tests/unit/test_testnet_runner_evidence.py`:

```python
from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from execution.models import ChildOrder, ChildOrderStatus, ExecutionStatus, Side
from scripts import testnet_runner


def test_default_max_runtime_is_not_shorter_than_duration(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        ["run_testnet_chase.py", "--confirm-send-orders"],
    )

    args = testnet_runner.parse_args(testnet_runner.Algorithm.CHASE)

    assert args.max_runtime_seconds >= args.duration_seconds + 10


def test_evidence_manifest_requires_exchange_order_ids_for_success() -> None:
    child = ChildOrder(
        child_order_id="child_1",
        client_order_id="ce_abcdef123456_1",
        symbol="BTCUSDT",
        side=Side.BUY,
        submitted_quantity=Decimal("0.001"),
        price=Decimal("95000"),
        status=ChildOrderStatus.OPEN,
        confirmed_filled_quantity=Decimal("0"),
        exchange_order_id=None,
    )
    record = SimpleNamespace(
        execution_id="exec_1",
        status=ExecutionStatus.CANCELLED,
        child_orders=[child],
        metric_counts={},
    )
    manifest = testnet_runner._evidence_manifest(
        record,
        reconciliation_orders=[],
        reconciliation_fills=[],
        user_stream_events=[],
        warnings=[],
        rate_limits=[],
    )

    assert manifest["accepted_exchange_order_evidence"] is False
    assert "missing_exchange_order_id" in manifest["warnings"]
```

- [ ] **Step 2: Fix runner default runtime**

In `scripts/testnet_runner.py`, change:

```python
    parser.add_argument("--max-runtime-seconds", type=float, default=30.0, help="Maximum runner runtime.")
```

to:

```python
    parser.add_argument("--max-runtime-seconds", type=float, default=None, help="Maximum runner runtime.")
```

After `args = parser.parse_args()`, add:

```python
    if args.max_runtime_seconds is None:
        args.max_runtime_seconds = float(args.duration_seconds + 10)
    if args.max_runtime_seconds < args.duration_seconds:
        parser.error("--max-runtime-seconds must be greater than or equal to --duration-seconds")
```

- [ ] **Step 3: Make manifest accepted-order evidence strict**

In `_evidence_manifest`, compute accepted order evidence from child orders:

```python
    child_orders = list(record.child_orders)
    accepted_exchange_order_evidence = any(
        child.exchange_order_id
        and child.status
        in {
            ChildOrderStatus.OPEN,
            ChildOrderStatus.PARTIALLY_FILLED,
            ChildOrderStatus.FILLED,
            ChildOrderStatus.CANCELLED,
        }
        for child in child_orders
    )
    if not accepted_exchange_order_evidence:
        warnings.append("missing_exchange_order_id")
```

For private stream evidence, require an execution-matching order/trade event:

```python
    private_stream_order_evidence = any(
        event.get("event_type") == "ORDER_TRADE_UPDATE"
        and str(event.get("raw", {}).get("o", {}).get("c", "")).startswith(
            make_client_order_prefix(record.execution_id)
        )
        for event in user_stream_events
    )
```

- [ ] **Step 4: Run runner evidence tests**

Run:

```bash
uv run pytest tests/unit/test_testnet_runner_evidence.py -q
```

Expected:

```text
2 passed
```

- [ ] **Step 5: Update README and manifest wording**

In `README.md` and `reports/submission_manifest.md`, state:

```markdown
Accepted Testnet evidence means at least one Chase run and one TWAP run whose `evidence_manifest.json` has `accepted_exchange_order_evidence: true` and contains at least one child order with a non-empty `exchange_order_id`. Read-only Testnet contract tests do not satisfy this requirement.
```

- [ ] **Step 6: Commit**

Run:

```bash
git add scripts/testnet_runner.py tests/unit/test_testnet_runner_evidence.py README.md reports/submission_manifest.md
git commit -m "fix: make testnet evidence requirements enforceable"
```

---

### Task 11: Fix Packaging And Add Submission Verification Gate

**Files:**
- Modify: `pyproject.toml`
- Create: `scripts/verify_submission.py`
- Test: `tests/unit/test_packaging.py`

- [ ] **Step 1: Fix top-level module packaging**

In `pyproject.toml`, add:

```toml
[tool.setuptools.py-modules]
py-modules = ["config"]
```

If setuptools rejects that table, use the setuptools-supported form:

```toml
[tool.setuptools]
package-dir = {"" = "src"}
py-modules = ["config"]
```

Keep `[tool.setuptools.packages.find] where = ["src"]`.

- [ ] **Step 2: Add dev quality tools**

In `pyproject.toml`, extend the `dev` dependency group:

```toml
    "build>=1.2.2",
    "ruff>=0.8.0",
```

Do not add `mypy` until type annotations are made strict enough for it to be a real gate.

- [ ] **Step 3: Add packaging test**

Create `tests/unit/test_packaging.py`:

```python
from __future__ import annotations

import importlib


def test_runtime_imports_config_module() -> None:
    runtime = importlib.import_module("api.runtime")

    assert hasattr(runtime, "ExecutionRuntime")
```

- [ ] **Step 4: Create submission verifier**

Create `scripts/verify_submission.py`:

```python
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def run(command: list[str]) -> None:
    completed = subprocess.run(command, cwd=ROOT, text=True)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


def require_path(path: Path, message: str) -> None:
    if not path.exists():
        raise SystemExit(message)


def verify_testnet_evidence(allow_missing: bool) -> None:
    evidence_root = ROOT / "reports" / "evidence" / "testnet"
    manifests = sorted(evidence_root.glob("**/evidence_manifest.json"))
    accepted = []
    for manifest_path in manifests:
        payload = json.loads(manifest_path.read_text())
        if payload.get("accepted_exchange_order_evidence") is True:
            accepted.append(manifest_path)
    if len(accepted) >= 2:
        return
    if allow_missing:
        print("WARNING: accepted Testnet evidence is missing or incomplete", file=sys.stderr)
        return
    raise SystemExit("accepted Chase and TWAP Testnet evidence manifests are required")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--allow-missing-testnet-evidence", action="store_true")
    args = parser.parse_args()

    run(["uv", "run", "pytest", "-q"])
    run(["uv", "run", "ruff", "check", "."])
    run(["uv", "build", "--out-dir", "/private/tmp/calais-submission-dist"])
    require_path(ROOT / "reports" / "latex" / "report.pdf", "reports/latex/report.pdf is required")
    verify_testnet_evidence(args.allow_missing_testnet_evidence)


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run packaging and verifier tests**

Run:

```bash
uv run pytest tests/unit/test_packaging.py -q
uv run python scripts/verify_submission.py --allow-missing-testnet-evidence
```

Expected:

```text
1 passed
WARNING: accepted Testnet evidence is missing or incomplete
```

The verifier may fail at `uv build` if build dependencies are not locally cached and network is unavailable. In that case, run it in a network-enabled environment and do not mark this task complete until `uv build` succeeds.

- [ ] **Step 6: Commit**

Run:

```bash
git add pyproject.toml scripts/verify_submission.py tests/unit/test_packaging.py
git commit -m "fix: add packaging and submission verification gate"
```

---

### Task 12: Produce Final Evidence Bundle And Make Report Claims Match Reality

**Files:**
- Modify: `reports/report_draft.md`
- Modify: `reports/latex/sections/08-testing-evidence.tex`
- Modify: `reports/submission_manifest.md`
- Add generated evidence under: `reports/evidence/`
- Add final PDF: `reports/latex/report.pdf`

- [ ] **Step 1: Generate simulator evidence after all code fixes**

Run:

```bash
uv run python scripts/run_sim_chase.py --output-dir reports/evidence/simulation/chase
uv run python scripts/run_sim_twap.py --output-dir reports/evidence/simulation/twap
uv run python scripts/run_sim_cancel_race.py --output-dir reports/evidence/simulation/cancel-race
uv run python scripts/run_sim_create_timeout.py --output-dir reports/evidence/simulation/create-timeout
```

Expected: each directory contains `execution_summary.json` and raw event/log files.

- [ ] **Step 2: Run accepted Testnet Chase and TWAP**

Run only with funded/configured Testnet credentials:

```bash
uv run python scripts/run_testnet_chase.py \
  --confirm-send-orders \
  --target-position 0.001 \
  --duration-seconds 60 \
  --max-runtime-seconds 75 \
  --output-dir reports/evidence/testnet/chase
```

```bash
uv run python scripts/run_testnet_twap.py \
  --confirm-send-orders \
  --target-position 0.001 \
  --duration-seconds 60 \
  --number-of-slices 5 \
  --max-runtime-seconds 75 \
  --output-dir reports/evidence/testnet/twap
```

Expected: each `evidence_manifest.json` has:

```json
{
  "accepted_exchange_order_evidence": true
}
```

and at least one child order has a non-empty `exchange_order_id`.

- [ ] **Step 3: Update report evidence table from generated artifacts**

In `reports/latex/sections/08-testing-evidence.tex`, replace pending or `/tmp` paths with committed paths:

```tex
\begin{itemize}
  \item Simulation Chase: \texttt{reports/evidence/simulation/chase}
  \item Simulation TWAP: \texttt{reports/evidence/simulation/twap}
  \item Simulation cancel/fill race: \texttt{reports/evidence/simulation/cancel-race}
  \item Simulation create-timeout: \texttt{reports/evidence/simulation/create-timeout}
  \item Binance Testnet Chase: \texttt{reports/evidence/testnet/chase}
  \item Binance Testnet TWAP: \texttt{reports/evidence/testnet/twap}
\end{itemize}
```

Remove any sentence saying accepted Testnet evidence is pending once both accepted manifests exist. If accepted manifests do not exist, keep the pending sentence and do not claim the submission is complete.

- [ ] **Step 4: Build final PDF**

Run:

```bash
cd reports/latex
latexmk -pdf -interaction=nonstopmode report.tex
```

Expected: `reports/latex/report.pdf` exists and opens.

- [ ] **Step 5: Run final verifier without bypass**

Run:

```bash
uv run python scripts/verify_submission.py
```

Expected: verifier exits 0 with no missing Testnet evidence warning.

- [ ] **Step 6: Commit final evidence and report**

Run:

```bash
git add reports/evidence reports/report_draft.md reports/latex/sections/08-testing-evidence.tex reports/latex/report.pdf reports/submission_manifest.md
git commit -m "docs: attach final verified evidence bundle"
```

---

## Self-Review

### Spec Coverage

- Execution lifecycle safety: Tasks 1, 2, and 3 cover unknown create, cancel/fill race, deadline expiry, stale market, and stream-health expiry.
- VWAP and slippage metrics: Task 4 makes VWAP trade-price based and repairs out-of-order/late-fill behavior.
- Price bounds and temporary untradeable states: Task 5 covers arrival benchmark and min-notional waiting.
- Binance Testnet/live behavior: Tasks 6 and 7 cover request weight, reconciliation ordering, rate limits, overload, server time, and listen-key recovery.
- Deterministic simulator and HTTP API: Tasks 8 and 9 make the simulator scriptable and internally consistent.
- Mandatory evidence and final report: Tasks 10 and 12 define strict accepted-order evidence and final artifact generation.
- Packaging and reproducibility: Task 11 adds package import/build verification and a single submission gate.

### Placeholder Scan

The plan contains no empty placeholder marker, no unspecified generic error-handling instruction, and no task that says only "write tests" without test code. Where exact implementation may conflict with existing state-machine constraints, the plan gives the precise expected fallback action and test target.

### Type Consistency

The snippets use existing types from the repo: `ExecutionRecord`, `ExecutionStatus`, `ChildOrderStatus`, `Fill`, `ReconciliationResult`, `OrderRequest`, `Side`, `Decimal`, `ManualClock`, `DeterministicSimulator`, `BinanceUsdmAdapter`, and `Settings`. Newly introduced names are defined in the same task before use: `ExchangeRateLimited`, `ExchangeBanned`, `ListenKeyExpired`, `SimulationMarketDataRequest`, `SimulationClockAdvanceRequest`, `SimulationFillRequest`, `SimulationStreamHealthRequest`, `SequenceClient`, and `scripts/verify_submission.py`.

## Execution Notes

- Start execution from a clean branch or isolated worktree.
- Keep each task as its own commit.
- After every task, run the focused test command and `uv run pytest -q`.
- Do not weaken tests to match the current behavior. If a test expectation is wrong, update the plan and explain the evidence before changing the test.
- Do not update the final report before the artifacts exist.
