# Calais Execution Algorithm Revised Current Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reproduce the compact-but-correct Calais execution algorithm implementation that currently exists in this repository: deterministic simulator proof, shared execution engine, Chase/TWAP logic, FastAPI create/query/cancel surface, Binance USD-M Testnet adapter hooks, and report-ready artifacts.

**Architecture:** The implementation is simulator-first and engine-centered. `ExecutionEngine` owns parent state, child-order lifecycle, exposure invariants, timeout/cancel/reconciliation behavior, and final summaries; Chase and TWAP only compute child-order demand. The FastAPI app exposes a manual `run_once` execution step for deterministic demos, while Binance Testnet support is credential-gated and uses the same `ExchangeAdapter` contract.

**Tech Stack:** Python 3.11, asyncio, dataclasses, Decimal, Pydantic v2, FastAPI, httpx, websockets, pytest, pytest-asyncio, PyYAML, uv.

---

## Scope Check

This revised plan matches the current fixed implementation. It is intentionally different from the original broad implementation plan:

1. It describes the code that was actually built, not the larger idealized architecture.
2. It keeps manual execution advancement through `ExecutionService.run_once()` as an explicit design choice for deterministic demo control.
3. It does not claim background execution workers, active-per-symbol conflict prevention, IOC final orders, database persistence, portfolio-level risk, production WebSocket supervisors, or real Testnet evidence artifacts.
4. It treats Binance as a Testnet integration adapter with strong mutation-safety semantics, while deterministic simulator tests provide the main proof for race conditions.
5. It preserves the assignment framing: small but correct, with correctness demonstrated through repeatable simulator scenarios and clear final documentation.

Do not submit this internal plan file directly to Calais unless the `For agentic workers` header and internal workflow wording are removed. The external-facing materials should be `README.md`, `reports/report_draft.md`, `reports/failure_case_log.md`, `AI_USAGE.md`, scenario artifacts, and Testnet evidence when credentials are available.

## File Structure

The implementation is organized directly under `src/`, matching the project PDF preference.

```text
pyproject.toml
  Project metadata, Python version, dependencies, pytest configuration.

uv.lock
  Locked dependency graph.

.env.example
  Environment variable names only; no secrets.

configs/example.yaml
  Safe example runtime settings.

src/config.py
  Runtime settings, stale-market threshold, recvWindow, mainnet guard config, YAML loader.

src/api/app.py
  FastAPI app factory. Uses deterministic simulator and ManualClock for API demos.

src/api/schemas.py
  Pydantic request/response models, Decimal string parsing, response serialization.

src/algorithms/chase.py
  Chase price decision and reprice decision. No adapter calls.

src/algorithms/twap.py
  Absolute scheduled cumulative quantity, schedule deficit, safe child quantity.

src/exchanges/base.py
  ExchangeAdapter contract and exchange exception types.

src/exchanges/simulator.py
  Deterministic exchange adapter for scripted market data, fills, cancels, timeouts, stream health, and reconciliation.

src/exchanges/binance_usdm.py
  Binance USD-M Testnet adapter: REST signing, GTX post-only mapping, mutation uncertainty classification, streams, exact order lookup, and reconciliation.

src/execution/clock.py
  SystemClock and ManualClock.

src/execution/events.py
  Per-execution actor lock for serializing local state mutation.

src/execution/ids.py
  Execution ID, child order ID, and compact Binance-safe clientOrderId generation.

src/execution/models.py
  Domain enums and dataclasses: requests, parameters, positions, snapshots, orders, fills, reconciliation, exposure, summaries.

src/execution/state_machine.py
  Parent and child state transition tables.

src/execution/engine.py
  Central execution correctness owner: lifecycle, exposure, submit gate, reconciliation, cancellation, timeout, Chase/TWAP child demand, summary.

src/execution/service.py
  API-facing service wrapper around ExecutionEngine.

src/observability/artifacts.py
  Request snapshot, JSONL log, CSV, and summary artifact writer.

src/observability/summary.py
  Completion-rate and side-aware slippage helpers.

src/risk/decimal_math.py
  Decimal rounding, completion, and slippage math.

src/risk/validation.py
  Symbol status, quantity, notional, tick, post-only, price-bound, and exposure-invariant validation.

scripts/run_sim_chase.py
scripts/run_sim_twap.py
scripts/run_sim_cancel_race.py
scripts/run_sim_create_timeout.py
  Deterministic simulator demos and artifact generation.

scripts/testnet_runner.py
scripts/run_testnet_chase.py
scripts/run_testnet_twap.py
  Credential-gated Binance Testnet scripts with explicit confirmation.

tests/unit/
  Focused tests for models, risk, state machine, engine lifecycle, API, Binance adapter, artifact serialization.

tests/simulation/
  Required deterministic scenario tests and simulator adapter tests.

tests/integration/
  Credential-gated Binance Testnet contract tests.

README.md
  User-facing runbook, scope, safety notes, simulator/Testnet commands, known limitations.

reports/report_draft.md
  Draft report for later conversion to PDF/LaTeX.

reports/failure_case_log.md
  Real development failure case and fix.

AI_USAGE.md
  AI assistance disclosure.
```

## Current Implementation Boundaries

These boundaries are intentional and should be explained honestly:

- One active child order is effectively used per execution path; replacements go through cancel/reconcile before new submit.
- The API app is simulator-backed and manual-step driven with `/run-once`.
- Binance Testnet scripts are separate from the FastAPI simulator app.
- `AGGRESSIVE_WITHIN_RANGE` submits a bounded non-post-only marketable limit, not IOC.
- `CANCEL_REMAINDER` is safe under manual advancement but not an autonomous background deadline job.
- Price outside target range currently expires the execution immediately without submitting an invalid order.
- Final metrics are compact; raw artifacts and scenario tests carry most audit evidence.

## Task 1: Project Skeleton, Config, And Domain Models

**Files:**
- Create: `pyproject.toml`
- Create: `.env.example`
- Create: `configs/example.yaml`
- Create: `src/__init__.py`
- Create: `src/config.py`
- Create: `src/execution/models.py`
- Create: `src/execution/clock.py`
- Create: `tests/unit/test_models.py`
- Create: `tests/unit/test_config.py`

- [ ] **Step 1: Write domain and config tests**

Add tests that prove the implementation uses `target_position - current_position`, stores absolute trade quantity, preserves side separately, parses mainnet config safely, and rejects invalid boolean strings.

```python
from decimal import Decimal

import pytest

from config import Settings
from execution.models import Environment, Side, required_trade


def test_required_trade_uses_target_minus_current_position_for_buy_and_sell() -> None:
    buy_side, buy_quantity = required_trade(
        target_position=Decimal("0.002"),
        current_position=Decimal("-0.003"),
    )
    sell_side, sell_quantity = required_trade(
        target_position=Decimal("-0.002"),
        current_position=Decimal("0.004"),
    )

    assert buy_side is Side.BUY
    assert buy_quantity == Decimal("0.005")
    assert sell_side is Side.SELL
    assert sell_quantity == Decimal("0.006")


def test_required_trade_returns_no_action_for_equal_target() -> None:
    side, quantity = required_trade(
        target_position=Decimal("0.001"),
        current_position=Decimal("0.001"),
    )

    assert side is Side.NO_ACTION
    assert quantity == Decimal("0")


def test_settings_mainnet_requires_explicit_boolean_guard() -> None:
    assert Settings(environment=Environment.MAINNET).can_trade_mainnet is False
    assert Settings(environment=Environment.MAINNET, allow_mainnet_trading=True).can_trade_mainnet is True

    with pytest.raises(ValueError, match="allow_mainnet_trading"):
        Settings(allow_mainnet_trading="maybe")
```

- [ ] **Step 2: Run tests and verify they fail before implementation**

Run:

```bash
uv run pytest tests/unit/test_models.py tests/unit/test_config.py -q
```

Expected before implementation: imports or assertions fail because domain/config code is missing.

- [ ] **Step 3: Implement the minimal domain and config layer**

Implement:

```python
def required_trade(target_position: Decimal, current_position: Decimal) -> tuple[Side, Decimal]:
    delta = target_position - current_position
    if delta == Decimal("0"):
        return Side.NO_ACTION, Decimal("0")
    if delta > Decimal("0"):
        return Side.BUY, abs(delta)
    return Side.SELL, abs(delta)
```

Implement `Settings` with:

```python
@dataclass(frozen=True)
class Settings:
    environment: Environment = Environment.SIMULATION
    allow_mainnet_trading: bool = False
    stale_market_data_ms: int = 1500
    recv_window_ms: int = 5000
    binance_api_key: str | None = None
    binance_api_secret: str | None = None

    @property
    def can_trade_mainnet(self) -> bool:
        return self.environment == Environment.MAINNET and self.allow_mainnet_trading is True
```

Implement the domain dataclasses needed by later tasks:

```python
@dataclass
class Exposure:
    confirmed_filled_quantity: Decimal = Decimal("0")
    live_open_quantity: Decimal = Decimal("0")
    pending_submit_quantity: Decimal = Decimal("0")
    pending_cancel_quantity: Decimal = Decimal("0")
    unknown_order_quantity: Decimal = Decimal("0")

    @property
    def reserved_exposure(self) -> Decimal:
        return (
            self.live_open_quantity
            + self.pending_submit_quantity
            + self.pending_cancel_quantity
            + self.unknown_order_quantity
        )
```

- [ ] **Step 4: Run domain/config tests**

Run:

```bash
uv run pytest tests/unit/test_models.py tests/unit/test_config.py -q
```

Expected: tests pass.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml .env.example configs/example.yaml src/__init__.py src/config.py src/execution/models.py src/execution/clock.py tests/unit/test_models.py tests/unit/test_config.py
git commit -m "feat: add execution domain model and config"
```

## Task 2: IDs, State Machine, Decimal Math, And Risk Validation

**Files:**
- Create: `src/execution/ids.py`
- Create: `src/execution/state_machine.py`
- Create: `src/risk/decimal_math.py`
- Create: `src/risk/validation.py`
- Create: `tests/unit/test_state_machine.py`
- Create: `tests/unit/test_risk_validation.py`
- Create: `tests/unit/test_engine_exposure.py`

- [ ] **Step 1: Write tests for IDs, state transitions, and validation**

Add tests covering compact Binance-safe client IDs, no `PENDING_CANCEL -> REJECTED`, TRADING symbol status, GTX post-only support, price bounds, post-only crossing, and exposure invariant.

```python
from decimal import Decimal

import pytest

from execution.ids import make_client_order_id, make_client_order_prefix
from execution.models import ChildOrderStatus, Exposure, Side, SymbolRules
from execution.state_machine import InvalidStateTransition, transition_child
from risk.validation import ValidationError, check_exposure_invariant, validate_child_order_safety


def test_client_order_id_is_compact_and_binance_safe() -> None:
    execution_id = "exec_abcdef1234567890"

    assert make_client_order_prefix(execution_id) == "ce_abcdef123456_"
    assert make_client_order_id(execution_id, 1) == "ce_abcdef123456_1"
    assert len(make_client_order_id(execution_id, 9999)) <= 36


def test_pending_cancel_cannot_transition_to_rejected() -> None:
    with pytest.raises(InvalidStateTransition):
        transition_child(ChildOrderStatus.PENDING_CANCEL, ChildOrderStatus.REJECTED)


def test_validate_child_order_safety_checks_status_bounds_tick_step_and_post_only() -> None:
    rules = SymbolRules(
        symbol="BTCUSDT",
        tick_size=Decimal("0.10"),
        quantity_step=Decimal("0.001"),
        min_quantity=Decimal("0.001"),
        min_notional=Decimal("5"),
        status="BREAK",
        supported_time_in_force=frozenset({"GTC", "GTX"}),
    )

    with pytest.raises(ValidationError, match="not trading"):
        validate_child_order_safety(
            quantity=Decimal("0.001"),
            price=Decimal("50000.00"),
            side=Side.BUY,
            rules=rules,
            best_bid=Decimal("49999.90"),
            best_ask=Decimal("50000.10"),
            post_only=True,
            lower=Decimal("49000"),
            upper=Decimal("51000"),
        )


def test_exposure_invariant_counts_all_reserved_buckets() -> None:
    exposure = Exposure(
        confirmed_filled_quantity=Decimal("0.004"),
        live_open_quantity=Decimal("0.002"),
        pending_submit_quantity=Decimal("0.001"),
        pending_cancel_quantity=Decimal("0.001"),
        unknown_order_quantity=Decimal("0.001"),
    )

    check_exposure_invariant(exposure, Decimal("0.001"), Decimal("0.010"))

    with pytest.raises(ValidationError, match="exceeds target"):
        check_exposure_invariant(exposure, Decimal("0.002"), Decimal("0.010"))
```

- [ ] **Step 2: Run tests and verify they fail before implementation**

Run:

```bash
uv run pytest tests/unit/test_state_machine.py tests/unit/test_risk_validation.py tests/unit/test_engine_exposure.py -q
```

Expected before implementation: imports or assertions fail.

- [ ] **Step 3: Implement ID generation and state transitions**

Implement compact client IDs:

```python
CLIENT_ORDER_ID_RE = re.compile(r"^[\.A-Z\:/a-z0-9_-]{1,36}$")


def make_client_order_prefix(execution_id_value: str) -> str:
    short_exec = execution_id_value.replace("exec_", "")[:12]
    return f"ce_{short_exec}_"


def make_client_order_id(execution_id_value: str, child_sequence: int) -> str:
    value = f"{make_client_order_prefix(execution_id_value)}{child_sequence}"
    if len(value) > 36 or not CLIENT_ORDER_ID_RE.fullmatch(value):
        raise ValueError(f"invalid Binance client order id: {value}")
    return value
```

Implement child transitions with no `PENDING_CANCEL -> REJECTED` edge:

```python
CHILD_TRANSITIONS = {
    ChildOrderStatus.PENDING_SUBMIT: {
        ChildOrderStatus.OPEN,
        ChildOrderStatus.REJECTED,
        ChildOrderStatus.UNKNOWN,
    },
    ChildOrderStatus.OPEN: {
        ChildOrderStatus.PARTIALLY_FILLED,
        ChildOrderStatus.FILLED,
        ChildOrderStatus.PENDING_CANCEL,
    },
    ChildOrderStatus.PARTIALLY_FILLED: {
        ChildOrderStatus.FILLED,
        ChildOrderStatus.PENDING_CANCEL,
    },
    ChildOrderStatus.PENDING_CANCEL: {
        ChildOrderStatus.OPEN,
        ChildOrderStatus.PARTIALLY_FILLED,
        ChildOrderStatus.CANCELLED,
        ChildOrderStatus.FILLED,
    },
}
```

- [ ] **Step 4: Implement risk validation**

Implement one combined child-order safety gate:

```python
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
    validate_price_bounds(side, price, lower, upper)
    validate_order_shape(quantity, price, side, rules, best_bid, best_ask, post_only)
```

Implement exposure invariant:

```python
def check_exposure_invariant(
    exposure: Exposure,
    new_child_quantity: Decimal,
    normalized_target_trade_quantity: Decimal,
) -> None:
    total = exposure.confirmed_filled_quantity + exposure.reserved_exposure + new_child_quantity
    if total > normalized_target_trade_quantity:
        raise ValidationError(
            "confirmed fills plus reserved exposure plus new child quantity "
            f"{total} exceeds target {normalized_target_trade_quantity}"
        )
```

- [ ] **Step 5: Run tests**

Run:

```bash
uv run pytest tests/unit/test_state_machine.py tests/unit/test_risk_validation.py tests/unit/test_engine_exposure.py -q
```

Expected: tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/execution/ids.py src/execution/state_machine.py src/risk/decimal_math.py src/risk/validation.py tests/unit/test_state_machine.py tests/unit/test_risk_validation.py tests/unit/test_engine_exposure.py
git commit -m "feat: add execution state and risk guards"
```

## Task 3: ExchangeAdapter Contract And Deterministic Simulator

**Files:**
- Create: `src/exchanges/base.py`
- Create: `src/exchanges/simulator.py`
- Create: `tests/unit/test_exchange_contract.py`
- Create: `tests/simulation/test_simulator_orders.py`

- [ ] **Step 1: Write adapter contract and simulator tests**

Test that the adapter exposes the exact interface used by the engine and that simulator reconciliation is execution-scoped.

```python
from decimal import Decimal

import pytest

from exchanges.base import ExchangeAdapter
from exchanges.simulator import DeterministicSimulator
from execution.models import OrderRequest, Side


def test_exchange_adapter_contract_names_are_stable() -> None:
    expected = {
        "get_symbol_rules",
        "get_position",
        "get_best_bid_ask",
        "stream_market_data",
        "submit_limit_order",
        "cancel_order",
        "get_order_by_client_order_id",
        "stream_user_events",
        "reconcile_orders_and_fills",
        "health_check_streams",
    }

    assert expected <= set(dir(ExchangeAdapter))


async def test_simulator_rejects_broad_reconciliation_prefix() -> None:
    simulator = DeterministicSimulator()

    with pytest.raises(ValueError, match="execution-scoped"):
        await simulator.reconcile_orders_and_fills("BTCUSDT", client_order_prefix="ce_")


async def test_simulator_create_timeout_not_found_is_execution_specific() -> None:
    simulator = DeterministicSimulator()
    await simulator.push_market_data("BTCUSDT", Decimal("50000.00"), Decimal("50001.00"))
    simulator.script_create_timeout_not_found("ce_abcdef123456_")

    request = OrderRequest(
        execution_id="exec_abcdef1234567890",
        child_order_id="child_0001",
        client_order_id="ce_abcdef123456_1",
        symbol="BTCUSDT",
        side=Side.BUY,
        quantity=Decimal("0.010"),
        price=Decimal("50000.00"),
        post_only=True,
    )

    with pytest.raises(Exception, match="timed out"):
        await simulator.submit_limit_order(request)

    result = await simulator.reconcile_orders_and_fills(
        "BTCUSDT",
        client_order_prefix="ce_abcdef123456_",
    )

    assert result.orders == []
    assert result.warnings == ["CREATE_TIMEOUT_ORDER_NOT_FOUND"]
```

- [ ] **Step 2: Run tests and verify they fail before implementation**

Run:

```bash
uv run pytest tests/unit/test_exchange_contract.py tests/simulation/test_simulator_orders.py -q
```

Expected before implementation: adapter and simulator imports fail or methods are missing.

- [ ] **Step 3: Implement adapter exceptions and abstract methods**

Implement `ExchangeAdapter` with these methods:

```python
class ExchangeAdapter(ABC):
    @abstractmethod
    async def get_symbol_rules(self, symbol: str) -> SymbolRules:
        raise NotImplementedError

    @abstractmethod
    async def get_position(self, symbol: str) -> PositionSnapshot:
        raise NotImplementedError

    @abstractmethod
    async def get_best_bid_ask(self, symbol: str) -> MarketSnapshot:
        raise NotImplementedError

    @abstractmethod
    def stream_market_data(self) -> AsyncIterator[MarketSnapshot]:
        raise NotImplementedError

    @abstractmethod
    async def submit_limit_order(self, order_request: OrderRequest) -> object:
        raise NotImplementedError

    @abstractmethod
    async def cancel_order(self, symbol: str, client_order_id: str) -> object:
        raise NotImplementedError

    @abstractmethod
    async def get_order_by_client_order_id(self, symbol: str, client_order_id: str) -> object:
        raise NotImplementedError

    @abstractmethod
    def stream_user_events(self) -> AsyncIterator[object]:
        raise NotImplementedError

    @abstractmethod
    async def reconcile_orders_and_fills(
        self,
        symbol: str,
        client_order_prefix: str | None = None,
    ) -> ReconciliationResult:
        raise NotImplementedError

    @abstractmethod
    async def health_check_streams(self) -> bool:
        raise NotImplementedError
```

- [ ] **Step 4: Implement deterministic simulator**

Implement simulator behavior:

```python
async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
    snapshot = await self.get_best_bid_ask(order_request.symbol)
    await self._validate_post_only(order_request, snapshot)

    if prefix := self._pop_matching_prefix(self._create_timeout_not_found_prefixes, order_request.client_order_id):
        self._create_timeout_not_found_warnings.add((order_request.symbol, prefix))
        raise SimulatorOrderTimeout(f"create timed out for {order_request.client_order_id}")

    order = self._create_open_order(order_request)

    if self._pop_matching_prefix(self._create_timeout_prefixes, order_request.client_order_id):
        raise SimulatorOrderTimeout(f"create timed out for {order_request.client_order_id}")

    return order
```

Implement `reconcile_orders_and_fills()` so it rejects broad prefixes and only returns records matching `ce_<12hex>_`.

- [ ] **Step 5: Run simulator tests**

Run:

```bash
uv run pytest tests/unit/test_exchange_contract.py tests/simulation/test_simulator_orders.py -q
```

Expected: tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/exchanges/base.py src/exchanges/simulator.py tests/unit/test_exchange_contract.py tests/simulation/test_simulator_orders.py
git commit -m "feat: add simulator exchange adapter"
```

## Task 4: Execution Engine Core And Single Submit Gate

**Files:**
- Create: `src/execution/events.py`
- Create: `src/execution/engine.py`
- Create: `src/execution/service.py`
- Create: `tests/unit/test_engine_lifecycle.py`

- [ ] **Step 1: Write engine lifecycle tests**

Write tests that prove there is exactly one engine submit gate, unknown create blocks new client IDs until reconciliation, cancel timeout keeps pending-cancel exposure, and fill during cancel reduces replacement size.

```python
from decimal import Decimal
from pathlib import Path

from exchanges.simulator import DeterministicSimulator
from execution.clock import ManualClock
from execution.ids import make_client_order_prefix
from execution.models import ChildOrderStatus, ExecutionParameters
from execution.service import ExecutionService


def test_static_single_submit_gate() -> None:
    source = Path("src/execution/engine.py").read_text()

    assert "def _submit_child_locked(" in source
    assert source.count(".submit_limit_order(") == 1


async def test_create_timeout_keeps_unknown_exposure_until_exact_reconciliation() -> None:
    clock = ManualClock()
    simulator = DeterministicSimulator(clock=clock)
    await simulator.push_market_data("BTCUSDT", Decimal("95000.00"), Decimal("95001.00"))
    service = ExecutionService(simulator, clock=clock)
    execution = await service.create_execution(execution_request())
    prefix = make_client_order_prefix(execution.execution_id)
    simulator.script_create_timeout(prefix)

    timed_out = await service.run_once(execution.execution_id)
    blocked = await service.run_once(execution.execution_id)

    assert timed_out.child_orders[0].status is ChildOrderStatus.UNKNOWN
    assert timed_out.exposure.unknown_order_quantity == Decimal("0.010")
    assert [child.client_order_id for child in blocked.child_orders] == [
        timed_out.child_orders[0].client_order_id
    ]
```

- [ ] **Step 2: Run tests and verify they fail before implementation**

Run:

```bash
uv run pytest tests/unit/test_engine_lifecycle.py -q
```

Expected before implementation: lifecycle tests fail because engine methods are missing or incomplete.

- [ ] **Step 3: Implement per-execution actor serialization**

Implement:

```python
class ExecutionEventActor:
    def __init__(self, execution_id: str) -> None:
        self.execution_id = execution_id
        self._lock = asyncio.Lock()

    async def apply(self, operation: Callable[[], Awaitable[T]]) -> T:
        async with self._lock:
            return await operation()
```

Document in the comment that this serializes local mutation only and does not replace exchange event-time ordering diagnostics.

- [ ] **Step 4: Implement ExposureTracker and submit gate**

Implement `ExposureTracker.check_can_submit()` by delegating to `check_exposure_invariant()`.

Implement `_submit_child_locked()` so every submit path performs:

```python
tracker = self._require_exposure_tracker(record)
tracker.check_can_submit(quantity)
record.last_child_sequence += 1
child = ChildOrder(
    child_order_id=ids.child_order_id(record.last_child_sequence),
    client_order_id=ids.make_client_order_id(record.execution_id, record.last_child_sequence),
    symbol=record.request.symbol,
    side=record.side,
    submitted_quantity=quantity,
    price=price,
)
record.child_orders.append(child)
tracker.reserve_pending_submit(quantity)
```

Then handle mutation outcomes conservatively:

```python
try:
    submitted = await self._adapter.submit_limit_order(order_request)
except OrderCreateTimeout:
    tracker.release_pending_submit(quantity)
    self._set_child_status(child, ChildOrderStatus.UNKNOWN)
    tracker.reserve_unknown_create(quantity)
    record.final_reason = CREATE_TIMEOUT_PENDING_RECONCILIATION
    return child
except OrderRejected as exc:
    tracker.release_pending_submit(quantity)
    self._set_child_status(child, ChildOrderStatus.REJECTED)
    child.terminal_reason = str(exc)
    return child
```

- [ ] **Step 5: Implement reconciliation and fill aggregation**

Implement reconciliation so:

1. Broad reconciliation uses execution-scoped `client_order_prefix`.
2. Unknown children receive exact `get_order_by_client_order_id()` lookup.
3. Child cumulative fill only increases.
4. Parent confirmed fill is the aggregate of child confirmed fills.
5. Reserved exposure is refreshed from child statuses after reconciliation.

Use this aggregation rule:

```python
aggregate_cumulative = sum(
    (stored_child.confirmed_filled_quantity for stored_child in record.child_orders),
    Decimal("0"),
)
tracker.apply_fill(trade_id, aggregate_cumulative)
```

- [ ] **Step 6: Run engine lifecycle tests**

Run:

```bash
uv run pytest tests/unit/test_engine_lifecycle.py -q
```

Expected: tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/execution/events.py src/execution/engine.py src/execution/service.py tests/unit/test_engine_lifecycle.py
git commit -m "feat: add safe execution engine lifecycle"
```

## Task 5: Chase And TWAP Algorithm Modules

**Files:**
- Create: `src/algorithms/chase.py`
- Create: `src/algorithms/twap.py`
- Create: `tests/unit/test_chase.py`
- Create: `tests/unit/test_twap.py`
- Modify: `src/execution/engine.py`

- [ ] **Step 1: Write algorithm tests**

Test passive/aggressive price decisions, ADVERSE_ONLY/TWO_SIDED repricing, TWAP absolute schedule, positive absolute quantities, and safe child quantity subtracting exposure.

```python
from decimal import Decimal

from algorithms.chase import ChaseDecision, chase_desired_price, should_reprice
from algorithms.twap import safe_child_quantity, scheduled_cumulative_quantity, scheduled_deficit
from execution.models import Exposure, RepricingMode, Side


def test_chase_passive_and_aggressive_prices_are_side_aware() -> None:
    assert chase_desired_price(Side.BUY, Decimal("100"), Decimal("101"), passive=True) == Decimal("100")
    assert chase_desired_price(Side.BUY, Decimal("100"), Decimal("101"), passive=False) == Decimal("101")
    assert chase_desired_price(Side.SELL, Decimal("100"), Decimal("101"), passive=True) == Decimal("101")
    assert chase_desired_price(Side.SELL, Decimal("100"), Decimal("101"), passive=False) == Decimal("100")


def test_chase_reprice_respects_threshold_interval_and_mode() -> None:
    assert should_reprice(
        Side.BUY,
        active_order_price=Decimal("100"),
        desired_price=Decimal("101"),
        threshold_bps=Decimal("50"),
        min_interval_ms=500,
        elapsed_since_last_reprice_ms=499,
        repricing_mode=RepricingMode.ADVERSE_ONLY,
    ) is ChaseDecision.KEEP


def test_twap_schedule_and_safe_quantity_are_absolute_positive_quantities() -> None:
    scheduled = scheduled_cumulative_quantity(
        total_trade_quantity=Decimal("1.000"),
        elapsed_time=Decimal("30"),
        total_duration=Decimal("100"),
    )
    exposure = Exposure(live_open_quantity=Decimal("0.100"))

    assert scheduled == Decimal("0.300")
    assert scheduled_deficit(scheduled, Decimal("0.050")) == Decimal("0.250")
    assert safe_child_quantity(Decimal("0.250"), exposure) == Decimal("0.150")
```

- [ ] **Step 2: Run algorithm tests and verify they fail before implementation**

Run:

```bash
uv run pytest tests/unit/test_chase.py tests/unit/test_twap.py -q
```

Expected before implementation: imports fail or functions are missing.

- [ ] **Step 3: Implement Chase module**

Implement:

```python
def chase_desired_price(side: Side, best_bid: Decimal, best_ask: Decimal, *, passive: bool) -> Decimal:
    if passive:
        return best_bid if side is Side.BUY else best_ask
    return best_ask if side is Side.BUY else best_bid
```

Implement `should_reprice()` so `ADVERSE_ONLY` only reprices when the new price is worse for the trader and the configured threshold and minimum interval both pass. Keep `TWO_SIDED` supported by the enum and function.

- [ ] **Step 4: Implement TWAP module**

Implement the exact assignment schedule formula:

```python
def scheduled_cumulative_quantity(
    total_trade_quantity: Decimal,
    elapsed_time: Decimal,
    total_duration: Decimal,
) -> Decimal:
    if elapsed_time <= Decimal("0"):
        return Decimal("0")
    if elapsed_time >= total_duration:
        return total_trade_quantity
    return total_trade_quantity * elapsed_time / total_duration
```

Implement safe quantity separately:

```python
def safe_child_quantity(schedule_deficit: Decimal, exposure: Exposure) -> Decimal:
    quantity = schedule_deficit - exposure.reserved_exposure
    return quantity if quantity > Decimal("0") else Decimal("0")
```

- [ ] **Step 5: Connect algorithms to engine child-demand calculation**

In `ExecutionEngine._build_child_demand_locked()`, keep algorithms narrow:

1. Engine fetches market and rules.
2. Engine computes Chase or TWAP quantity.
3. Engine floors quantity to step size.
4. Engine rounds price side-aware.
5. Engine calls `validate_child_order_safety()`.
6. Engine submits only through `_submit_child_locked()`.

- [ ] **Step 6: Run algorithm and engine tests**

Run:

```bash
uv run pytest tests/unit/test_chase.py tests/unit/test_twap.py tests/unit/test_engine_lifecycle.py -q
```

Expected: tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/algorithms/chase.py src/algorithms/twap.py src/execution/engine.py tests/unit/test_chase.py tests/unit/test_twap.py tests/unit/test_engine_lifecycle.py
git commit -m "feat: add chase and twap demand logic"
```

## Task 6: FastAPI Manual-Step Service

**Files:**
- Create: `src/api/app.py`
- Create: `src/api/schemas.py`
- Create: `tests/unit/test_api.py`

- [ ] **Step 1: Write API tests**

Test JSON Decimal strings, create/query/cancel/run-once/reconcile endpoints, response exposure fields, and final reason fields.

```python
from fastapi.testclient import TestClient

from api.app import create_app


def test_api_create_run_once_and_query_execution() -> None:
    client = TestClient(create_app())
    payload = {
        "environment": "SIMULATION",
        "symbol": "BTCUSDT",
        "algorithm": "CHASE",
        "target_position": "0.010",
        "target_price_lower": "49000",
        "target_price_upper": "51000",
        "target_duration_seconds": 60,
        "deadline_policy": "CANCEL_REMAINDER",
        "parameters": {
            "reprice_threshold_bps": "2.0",
            "minimum_reprice_interval_ms": 500,
            "number_of_slices": 10,
            "child_order_timeout_seconds": 20,
            "repricing_mode": "ADVERSE_ONLY",
        },
    }

    created = client.post("/executions", json=payload)
    assert created.status_code == 200
    execution_id = created.json()["execution_id"]

    stepped = client.post(f"/executions/{execution_id}/run-once")
    assert stepped.status_code == 200
    assert stepped.json()["child_orders"][0]["status"] == "OPEN"

    queried = client.get(f"/executions/{execution_id}")
    assert queried.status_code == 200
    assert queried.json()["execution_id"] == execution_id
```

- [ ] **Step 2: Run API tests and verify they fail before implementation**

Run:

```bash
uv run pytest tests/unit/test_api.py -q
```

Expected before implementation: app or schemas missing.

- [ ] **Step 3: Implement schemas**

Implement Decimal string parsing:

```python
DECIMAL_STRING_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$")


def parse_decimal_string(value: object) -> Decimal:
    if not isinstance(value, str):
        raise ValueError("decimal fields must be JSON strings")
    if not DECIMAL_STRING_RE.fullmatch(value):
        raise ValueError("decimal fields must be plain decimal strings")
    return Decimal(value)
```

Expose `final_reason`, `summary_final_reason`, all exposure buckets, child order status, and request parameters in `ExecutionResponse`.

- [ ] **Step 4: Implement simulator-backed app**

Implement:

```python
def create_app(simulator_position: str = "0") -> FastAPI:
    clock = ManualClock()
    adapter = DeterministicSimulator(clock=clock, position=Decimal(simulator_position))
    service = ExecutionService(adapter, clock=clock)

    app = FastAPI(title="Calais Execution API")
    app.state.clock = clock
    app.state.adapter = adapter
    app.state.service = service
```

Add endpoints:

```text
POST /executions
GET /executions/{execution_id}
POST /executions/{execution_id}/cancel
POST /executions/{execution_id}/run-once
POST /executions/{execution_id}/reconcile
```

Keep this API manual-step and simulator-backed. Do not claim it is an autonomous Binance production service.

- [ ] **Step 5: Run API tests**

Run:

```bash
uv run pytest tests/unit/test_api.py -q
```

Expected: tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/api/app.py src/api/schemas.py tests/unit/test_api.py
git commit -m "feat: add simulator-backed execution api"
```

## Task 7: Binance USD-M Testnet Adapter

**Files:**
- Create: `src/exchanges/binance_usdm.py`
- Create: `tests/unit/test_binance_market_data.py`
- Create: `tests/unit/test_binance_order_mutations.py`
- Create: `tests/integration/test_binance_testnet_contract.py`

- [ ] **Step 1: Write Binance adapter tests**

Test Decimal REST serialization, GTX post-only mapping, invalid client IDs, mutation timeout classification, mainnet hard stop, exact order lookup endpoint, and orderId-to-clientOrderId fill reconciliation.

```python
from decimal import Decimal

import pytest

from config import Settings
from exchanges.binance_usdm import (
    ORDER_QUERY_PATH,
    ORDER_REST_PATH,
    ExchangeTerminalReject,
    MutationKind,
    build_new_order_params,
    classify_mutation_timeout,
    decimal_to_api,
)
from execution.models import Environment


def test_new_order_payload_serializes_decimals_and_uses_gtx_for_post_only() -> None:
    params = build_new_order_params(order_request(post_only=True), rules())

    assert params["type"] == "LIMIT"
    assert params["timeInForce"] == "GTX"
    assert params["quantity"] == "0.010"
    assert params["price"] == "95000.10"
    assert params["newClientOrderId"] == "ce_abcdef123456_1"
    assert decimal_to_api(Decimal("1.2300")) == "1.2300"


def test_timeout_classification_distinguishes_create_and_cancel() -> None:
    assert ORDER_REST_PATH == "/fapi/v1/order"
    assert ORDER_QUERY_PATH == "/fapi/v1/order"
    assert classify_mutation_timeout(MutationKind.CREATE) == "UNKNOWN_CREATE_OUTCOME"
    assert classify_mutation_timeout(MutationKind.CANCEL) == "PENDING_CANCEL_OUTCOME"
```

- [ ] **Step 2: Run Binance unit tests and verify they fail before implementation**

Run:

```bash
uv run pytest tests/unit/test_binance_market_data.py tests/unit/test_binance_order_mutations.py -q
```

Expected before implementation: imports or adapter behavior missing.

- [ ] **Step 3: Implement request signing and mutation classification**

Implement:

```python
def decimal_to_api(value: Decimal) -> str:
    return format(value, "f")


def classify_mutation_timeout(kind: MutationKind) -> str:
    if kind is MutationKind.CREATE:
        return "UNKNOWN_CREATE_OUTCOME"
    if kind is MutationKind.CANCEL:
        return "PENDING_CANCEL_OUTCOME"
    raise ValueError(f"unsupported mutation kind: {kind}")
```

Implement `_signed_request()` so:

1. Signed params include `timestamp` plus `server_time_offset_ms`.
2. Signed params include configured `recvWindow`.
3. API key is sent in `X-MBX-APIKEY`.
4. HTTP timeout is bounded.
5. Mainnet mutations raise `MAINNET_TRADING_NOT_ALLOWED` unless explicitly enabled.
6. Create timeout raises `UnknownCreateOutcome`.
7. Cancel timeout raises `PendingCancelOutcome`.
8. Read timeout raises `RetryableReadFailure`.

- [ ] **Step 4: Implement order payload and exact lookup**

Implement `build_new_order_params()` so:

```python
if order_request.post_only:
    if "GTX" not in rules.supported_time_in_force:
        raise ExchangeTerminalReject("POST_ONLY_GTX_UNSUPPORTED")
    time_in_force = "GTX"
else:
    time_in_force = "GTC"
```

Implement `get_order_by_client_order_id()` using `GET /fapi/v1/order` and `origClientOrderId`, not the current-open-order endpoint.

- [ ] **Step 5: Implement market stream, user stream, and reconciliation hooks**

Implement:

1. Testnet REST base: `https://demo-fapi.binance.com`.
2. Testnet WebSocket root: `wss://fstream.binancefuture.com`.
3. Market stream URL: `/public/ws/{symbol}@bookTicker`.
4. User stream URL: `/private/ws/{listenKey}`.
5. `create_listen_key()` and `renew_listen_key()`.
6. `reconcile_orders_and_fills()` combining `openOrders`, `allOrders`, and `userTrades`.
7. User trades joined to clientOrderId through `orderId` from order rows.
8. Reconciliation requires execution-scoped `ce_<12hex>_` prefix.

- [ ] **Step 6: Run Binance unit tests**

Run:

```bash
uv run pytest tests/unit/test_binance_market_data.py tests/unit/test_binance_order_mutations.py -q
```

Expected: tests pass.

- [ ] **Step 7: Add credential-gated integration tests**

Integration tests should skip unless credentials are present. They should check:

1. `exchangeInfo` parsing.
2. position query.
3. fresh market snapshot when stream is running.
4. submit/cancel/order lookup only with explicit confirmation.

Run without credentials:

```bash
uv run pytest tests/integration/test_binance_testnet_contract.py -q
```

Expected without credentials: tests skip.

- [ ] **Step 8: Commit**

```bash
git add src/exchanges/binance_usdm.py tests/unit/test_binance_market_data.py tests/unit/test_binance_order_mutations.py tests/integration/test_binance_testnet_contract.py
git commit -m "feat: add binance usdm testnet adapter"
```

## Task 8: Artifacts, Simulator Demo Scripts, And Required Scenarios

**Files:**
- Create: `src/observability/artifacts.py`
- Create: `src/observability/summary.py`
- Create: `scripts/_sim_demo_common.py`
- Create: `scripts/run_sim_chase.py`
- Create: `scripts/run_sim_twap.py`
- Create: `scripts/run_sim_cancel_race.py`
- Create: `scripts/run_sim_create_timeout.py`
- Create: `tests/simulation/test_required_scenarios.py`

- [ ] **Step 1: Write required scenario tests**

Write behavioral tests for T1-T10. The key tests must assert actual safety behavior, not just final status.

```python
async def test_t3_cancel_fill_race_updates_confirmed_fills_before_replacement_sizing() -> None:
    clock, simulator, service = await simulator_service()
    execution = await service.create_execution(request())
    prefix = make_client_order_prefix(execution.execution_id)
    simulator.script_fill_during_cancel(prefix, Decimal("0.004"))
    execution = await service.run_once(execution.execution_id)

    clock.advance(0.6)
    await push_fresh_market(clock, simulator, bid=Decimal("50030.00"), ask=Decimal("50031.00"))
    execution = await service.run_once(execution.execution_id)

    assert execution.exposure.confirmed_filled_quantity == Decimal("0.004")
    assert execution.child_orders[1].submitted_quantity == Decimal("0.006")
    assert execution.exposure.live_open_quantity == Decimal("0.006")


async def test_t4a_create_timeout_reconciles_to_open_order_without_new_client_order_id() -> None:
    _, simulator, service = await simulator_service()
    execution = await service.create_execution(request())
    prefix = make_client_order_prefix(execution.execution_id)
    simulator.script_create_timeout(prefix)

    execution = await service.run_once(execution.execution_id)
    first_client_order_id = execution.child_orders[0].client_order_id
    before_reconcile = await service.run_once(execution.execution_id)

    assert [child.client_order_id for child in before_reconcile.child_orders] == [first_client_order_id]

    reconciled = await service.reconcile_execution(execution.execution_id)

    assert [child.client_order_id for child in reconciled.child_orders] == [first_client_order_id]
    assert reconciled.exposure.unknown_order_quantity == Decimal("0")
    assert reconciled.exposure.live_open_quantity == reconciled.required_quantity
```

- [ ] **Step 2: Run required scenarios and verify they fail before full implementation**

Run:

```bash
uv run pytest tests/simulation/test_required_scenarios.py -q
```

Expected before full implementation: lifecycle/scenario assertions fail.

- [ ] **Step 3: Implement artifact writer**

Artifact output must include:

```text
request_snapshot.json
execution_log.jsonl
execution_summary.json
child_orders.csv
fills.csv
timeline.csv
```

`timeline.csv` should use a stable union field set so heterogeneous events do not fail CSV serialization.

- [ ] **Step 4: Implement simulator scripts**

Implement scripts so they can be run directly:

```bash
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py
uv run python scripts/run_sim_create_timeout.py
```

The cancel-race and create-timeout scripts must print `artifact_dir=<path>` so tests and the report can reference artifacts.

- [ ] **Step 5: Run required scenarios and scripts**

Run:

```bash
uv run pytest tests/simulation/test_required_scenarios.py -q
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py
uv run python scripts/run_sim_create_timeout.py
```

Expected: tests pass, scripts print execution IDs and client order IDs, artifact directories contain required files.

- [ ] **Step 6: Commit**

```bash
git add src/observability/artifacts.py src/observability/summary.py scripts/_sim_demo_common.py scripts/run_sim_chase.py scripts/run_sim_twap.py scripts/run_sim_cancel_race.py scripts/run_sim_create_timeout.py tests/simulation/test_required_scenarios.py
git commit -m "feat: add simulator evidence artifacts"
```

## Task 9: Testnet Scripts And Credential-Gated Safety

**Files:**
- Create: `scripts/testnet_runner.py`
- Create: `scripts/run_testnet_chase.py`
- Create: `scripts/run_testnet_twap.py`
- Modify: `README.md`

- [ ] **Step 1: Write script contract tests or review checks**

The script behavior must require:

1. API key and secret from environment or config.
2. Testnet by default.
3. Explicit `--confirm-send-orders` for any order mutation.
4. Printed account position, required side/quantity, price bounds, symbol rules, environment, clientOrderId.
5. Cleanup/reconciliation restricted to the execution clientOrderId prefix.

- [ ] **Step 2: Implement Testnet runner**

Implement a shared runner that:

1. Creates `BinanceUsdmAdapter(Settings(environment=Environment.TESTNET, binance_api_key=api_key, binance_api_secret=api_secret))`.
2. Sets market stream symbol.
3. Waits for a fresh market snapshot.
4. Creates an `ExecutionService` using `SystemClock`.
5. Runs a bounded manual execution loop.
6. Reconciles by execution prefix.
7. Writes artifacts.

Do not send orders unless the confirmation flag is present.

- [ ] **Step 3: Run Testnet scripts without credentials**

Run:

```bash
uv run python scripts/run_testnet_chase.py
uv run python scripts/run_testnet_twap.py
```

Expected without credentials: scripts fail safely with a clear credential/confirmation message or skip order submission without mutating exchange state.

- [ ] **Step 4: Document credentialed run commands**

Add README commands:

```bash
BINANCE_API_KEY=<testnet-api-key> BINANCE_API_SECRET=<testnet-api-secret> uv run python scripts/run_testnet_chase.py --confirm-send-orders
BINANCE_API_KEY=<testnet-api-key> BINANCE_API_SECRET=<testnet-api-secret> uv run python scripts/run_testnet_twap.py --confirm-send-orders
```

Document that simulator tests are the deterministic proof path and Testnet is the integration proof path.

- [ ] **Step 5: Commit**

```bash
git add scripts/testnet_runner.py scripts/run_testnet_chase.py scripts/run_testnet_twap.py README.md
git commit -m "feat: add credential-gated testnet scripts"
```

## Task 10: Documentation, Failure Case, And Final Verification

**Files:**
- Create: `README.md`
- Create: `reports/report_draft.md`
- Create: `reports/failure_case_log.md`
- Create: `AI_USAGE.md`
- Modify: `docs/superpowers/plans/2026-06-21-execution-algorithm-revised-current-implementation.md`

- [ ] **Step 1: Write README runbook**

README must clearly state:

1. Scope: compact execution service for BTCUSDT USD-M style execution logic.
2. Mainnet disabled by default.
3. Simulator-first correctness proof.
4. API uses manual `/run-once`.
5. Binance Testnet scripts require credentials and explicit confirmation.
6. Required simulator commands.
7. Known limitations.

- [ ] **Step 2: Write report draft**

Report draft must include:

1. Architecture.
2. Parent/child state machines.
3. Exposure invariant.
4. Chase behavior.
5. TWAP schedule formula.
6. Create-timeout reconciliation.
7. Cancel/fill race behavior.
8. Simulator scenario matrix.
9. Binance Testnet integration design.
10. Known limitations and future improvements.

- [ ] **Step 3: Write real failure case log**

Failure case log must explain a real implementation issue, for example:

```text
Failure:
Parent fill accounting originally risked undercounting when multiple child orders each reported cumulative fills.

Test that exposed it:
A cancel/fill race followed by replacement sizing expected confirmed fills of 0.004 and replacement quantity of 0.006, not a full 0.010 replacement.

Fix:
Store cumulative fill on each child order and update parent confirmed fills from aggregate child confirmed quantity.

Regression:
tests/simulation/test_required_scenarios.py::test_t3_cancel_fill_race_updates_confirmed_fills_before_replacement_sizing
tests/simulation/test_required_scenarios.py::test_t9_duplicate_fill_event_does_not_double_count_cumulative_fill
```

- [ ] **Step 4: Run complete test suite**

Run:

```bash
uv run pytest -q
```

Expected without Testnet credentials:

```text
227 passed, 2 skipped
```

The exact pass count may change if tests are added, but no non-credential-gated test should fail.

- [ ] **Step 5: Run demo scripts**

Run:

```bash
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py
uv run python scripts/run_sim_create_timeout.py
```

Expected:

1. Each script prints an execution ID.
2. Each script prints clientOrderId values.
3. Artifact-producing scripts print `artifact_dir=<absolute-output-path>`.
4. Artifact directories contain request snapshot, JSONL log, summary, child order CSV, fills CSV, and timeline CSV.

- [ ] **Step 6: Check final submission hygiene**

Run:

```bash
git status --short
find . -name ".DS_Store" -print
```

Expected:

1. Only intentional files are modified.
2. `.DS_Store` files are not submitted.
3. Internal `docs/superpowers` files are excluded from the interviewer-facing package unless cleaned.

- [ ] **Step 7: Commit documentation**

```bash
git add README.md reports/report_draft.md reports/failure_case_log.md AI_USAGE.md docs/superpowers/plans/2026-06-21-execution-algorithm-revised-current-implementation.md
git commit -m "docs: add revised implementation plan and report materials"
```

## Final Acceptance Checklist

- [ ] `uv run pytest -q` passes with only credential-gated skips.
- [ ] T1-T10 simulator scenarios assert concrete safety behavior.
- [ ] Engine has exactly one adapter submit call.
- [ ] Unknown create reserves exposure and blocks duplicate clientOrderId generation until reconciliation.
- [ ] Ambiguous cancel keeps pending-cancel exposure until reconciliation.
- [ ] Fill during cancel reduces replacement quantity.
- [ ] Duplicate fill events do not double count parent cumulative fills.
- [ ] TWAP uses absolute scheduled cumulative quantity and carry-forward deficit.
- [ ] Price-bound violations submit no invalid child order and report `PRICE_OUTSIDE_RANGE`.
- [ ] Binance order query uses `/fapi/v1/order` with `origClientOrderId`.
- [ ] Testnet scripts are credential-gated and require explicit send confirmation.
- [ ] README and report draft disclose manual `run_once` execution and other compact-scope limitations.
- [ ] Internal agent workflow files are not included in the final interviewer package unless edited for external audience.

## Self-Review

Spec coverage:

- Target-position semantics are covered in Task 1 and T10 scenario tests.
- Decimal arithmetic and API Decimal strings are covered in Tasks 1, 2, 6, and 7.
- Parent/child state transitions are covered in Task 2 and Task 4.
- Open, pending submit, pending cancel, and unknown exposure are covered in Tasks 2, 4, and 8.
- Create-timeout and cancel ambiguity are covered in Tasks 4, 7, and 8.
- Chase repricing is covered in Task 5 and T2.
- TWAP schedule and carry-forward are covered in Task 5 and T5.
- Deterministic simulator proof is covered in Tasks 3 and 8.
- Binance Testnet adapter is covered in Tasks 7 and 9.
- Report, artifacts, failure case, and AI disclosure are covered in Task 10.

Known gaps intentionally not implemented in current code:

- No active-per-symbol conflict guard.
- No background execution worker in FastAPI.
- No IOC aggressive final order.
- No production-grade WebSocket supervisor.
- No real Testnet artifact package until credentials are available.
- Metrics are compact rather than full venue-quality execution analytics.

Placeholder scan:

- This plan contains no banned placeholder tokens, no empty implementation placeholders, and no references to undefined future subsystems.
- Known limitations are explicit rather than described as future work hidden inside tasks.

Type consistency:

- `ExecutionService`, `ExecutionEngine`, `ExecutionEventActor`, `ExchangeAdapter`, `ReconciliationResult`, `OrderRequest`, `ChildOrder`, `Exposure`, and `SymbolRules` names match the current repository.
- `run_once`, `reconcile_execution`, `reconcile_orders_and_fills`, `get_order_by_client_order_id`, `submit_limit_order`, and `cancel_order` match the current method names.
