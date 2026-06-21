# Calais Execution Algorithm Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a compact but correct Binance USD-M BTCUSDT execution service with Chase/TWAP algorithms, deterministic simulator proof, credential-gated Binance Testnet adapter, FastAPI create/query/cancel API, structured artifacts, and report-ready documentation.

**Architecture:** Implement a shared async execution engine that owns parent/child state, exposure invariants, event serialization, reconciliation, and summaries. Algorithms only create safe child-order demand; simulator and Binance adapters implement the same `ExchangeAdapter` contract so simulator tests exercise the real engine path.

**Tech Stack:** Python 3.11+, asyncio, dataclasses, Decimal, Pydantic v2, FastAPI, uvicorn, httpx, websockets, pytest, pytest-asyncio, PyYAML.

---

## Scope Check

The design spec is broad, but the work is one coherent executable product rather than independent products. This plan keeps a single implementation path and breaks the work into independently testable tasks:

1. Core project skeleton and domain model.
2. State machine, risk math, simulator, and observability.
3. Execution engine and algorithms.
4. API and scripts.
5. Binance adapter and integration hooks.
6. Required simulator scenarios and documentation.

Do not add multi-symbol, multi-exchange, database persistence, authentication, dashboard, Hedge Mode, portfolio risk, or live mainnet trading beyond the config guard described in the spec.

## File Structure

Create or modify these files. Keep responsibilities narrow.

```text
pyproject.toml
  Project metadata and dependencies.

configs/example.yaml
  Safe example configuration for simulation and Testnet.

.env.example
  Environment variable names only, no secrets.

src/__init__.py
  Marks src as importable package root for direct PDF-specified folder layout.

src/config.py
  Runtime settings, config loading, mainnet guard, stale thresholds, recvWindow.

src/api/__init__.py
src/api/schemas.py
src/api/app.py
  FastAPI schemas and create/query/cancel endpoints.

src/algorithms/__init__.py
src/algorithms/chase.py
src/algorithms/twap.py
  Algorithm-specific child-order demand calculation. No direct adapter calls.

src/exchanges/__init__.py
src/exchanges/base.py
src/exchanges/simulator.py
src/exchanges/binance_usdm.py
  Adapter contract, deterministic simulator, Binance USD-M implementation.

src/execution/__init__.py
src/execution/clock.py
src/execution/events.py
src/execution/ids.py
src/execution/models.py
src/execution/state_machine.py
src/execution/engine.py
src/execution/service.py
  Execution domain model, state transitions, serialized event processing, service facade.

src/risk/__init__.py
src/risk/decimal_math.py
src/risk/validation.py
  Decimal parsing, side-aware rounding, quantity/price validation, exposure checks.

src/observability/__init__.py
src/observability/logging.py
src/observability/summary.py
src/observability/artifacts.py
  Sanitized JSONL logs, CSV/JSON summaries, metrics.

scripts/run_api.py
scripts/run_sim_chase.py
scripts/run_sim_twap.py
scripts/run_sim_cancel_race.py
scripts/run_sim_create_timeout.py
scripts/run_testnet_chase.py
scripts/run_testnet_twap.py
scripts/testnet_runner.py
  Demo and integration scripts with clear simulator/Testnet labels.

tests/unit/
tests/simulation/
tests/integration/
tests/conftest.py
  Unit, deterministic simulator, and credential-gated Testnet tests.

README.md
AI_USAGE.md
reports/report_draft.md
reports/failure_case_log.md
  Deliverables and report-ready material.
```

Reference spec:

```text
docs/superpowers/specs/2026-06-21-execution-algorithm-design.md
```

Relevant Binance docs to check while implementing:

```text
https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info
https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/New-Order
https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Position-Information-V3
https://developers.binance.com/docs/derivatives/usds-margined-futures/common-definition
```

---

### Task 1: Project Dependencies And Package Skeleton

**Files:**
- Modify: `pyproject.toml`
- Create: `configs/example.yaml`
- Create: `.env.example`
- Create: `src/__init__.py`
- Create: `src/api/__init__.py`
- Create: `src/algorithms/__init__.py`
- Create: `src/exchanges/__init__.py`
- Create: `src/execution/__init__.py`
- Create: `src/risk/__init__.py`
- Create: `src/observability/__init__.py`
- Create: `tests/conftest.py`
- Test: `tests/unit/test_project_imports.py`

- [ ] **Step 1: Write the failing import smoke test**

Create `tests/unit/test_project_imports.py`:

```python
def test_project_packages_importable() -> None:
    import api
    import algorithms
    import exchanges
    import execution
    import risk
    import observability

    assert api is not None
    assert algorithms is not None
    assert exchanges is not None
    assert execution is not None
    assert risk is not None
    assert observability is not None
```

- [ ] **Step 2: Run the smoke test to verify it fails**

Run:

```bash
uv run pytest tests/unit/test_project_imports.py -v
```

Expected: FAIL with `ModuleNotFoundError` for at least one of `api`, `algorithms`, `exchanges`, `execution`, `risk`, or `observability`.

- [ ] **Step 3: Update dependencies in `pyproject.toml`**

Replace the dependency section with:

```toml
[build-system]
requires = ["setuptools>=70"]
build-backend = "setuptools.build_meta"

[project]
name = "calais-execution-algorithm"
version = "0.1.0"
description = "Small but correct execution algorithm service for Binance USD-M BTCUSDT."
readme = "README.md"
requires-python = ">=3.11"

dependencies = [
    "fastapi>=0.115.0",
    "httpx>=0.27.0",
    "pydantic>=2.8.0",
    "python-dotenv>=1.0.1",
    "pyyaml>=6.0.2",
    "uvicorn[standard]>=0.30.0",
    "websockets>=12.0",
]

[dependency-groups]
dev = [
    "pytest>=8.3.0",
    "pytest-asyncio>=0.23.0",
]

[tool.setuptools]
package-dir = {"" = "src"}

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
pythonpath = ["src"]
testpaths = ["tests"]
```

The `src` package-dir configuration is required so direct commands such as `uv run python scripts/run_sim_chase.py` and `uv run python scripts/run_testnet_chase.py` import the same packages that pytest imports.

- [ ] **Step 4: Create package marker files**

Create each `__init__.py` listed above with exactly:

```python
"""Calais execution algorithm package."""
```

- [ ] **Step 5: Add test configuration**

Create `tests/conftest.py`:

```python
from decimal import getcontext


def pytest_configure() -> None:
    getcontext().prec = 28
```

- [ ] **Step 6: Add safe example config**

Create `configs/example.yaml`:

```yaml
environment: simulation
symbol: BTCUSDT
allow_mainnet_trading: false
stale_market_data_ms: 1500
recv_window_ms: 5000
default_parameters:
  repricing_mode: ADVERSE_ONLY
  reprice_threshold_bps: "2.0"
  minimum_reprice_interval_ms: 500
  number_of_slices: 10
  child_order_timeout_seconds: 20
```

- [ ] **Step 7: Add environment variable template**

Create `.env.example`:

```text
BINANCE_USDM_API_KEY=
BINANCE_USDM_API_SECRET=
BINANCE_USDM_TESTNET=true
ALLOW_MAINNET_TRADING=false
```

- [ ] **Step 8: Run smoke test to verify it passes**

Run:

```bash
uv run pytest tests/unit/test_project_imports.py -v
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add pyproject.toml configs/example.yaml .env.example src tests/conftest.py tests/unit/test_project_imports.py
git commit -m "chore: scaffold execution project"
```

---

### Task 2: Core Domain Models And Enums

**Files:**
- Create: `src/execution/models.py`
- Test: `tests/unit/test_models.py`

- [ ] **Step 1: Write failing tests for request parsing, NO_ACTION, and exposure fields**

Create `tests/unit/test_models.py`:

```python
from decimal import Decimal

from execution.models import (
    Algorithm,
    ChildOrderStatus,
    DeadlinePolicy,
    Environment,
    ExecutionParameters,
    ExecutionRequest,
    ExecutionStatus,
    RepricingMode,
    Side,
    required_trade,
)


def test_execution_request_parses_decimal_strings() -> None:
    request = ExecutionRequest(
        environment=Environment.SIMULATION,
        symbol="BTCUSDT",
        algorithm=Algorithm.CHASE,
        target_position=Decimal("0.010"),
        target_price_lower=Decimal("94000"),
        target_price_upper=Decimal("97000"),
        target_duration_seconds=300,
        deadline_policy=DeadlinePolicy.AGGRESSIVE_WITHIN_RANGE,
        parameters=ExecutionParameters(
            reprice_threshold_bps=Decimal("2.0"),
            minimum_reprice_interval_ms=500,
            number_of_slices=10,
            child_order_timeout_seconds=20,
            repricing_mode=RepricingMode.ADVERSE_ONLY,
        ),
    )

    assert request.target_position == Decimal("0.010")
    assert request.parameters.repricing_mode is RepricingMode.ADVERSE_ONLY


def test_required_trade_uses_target_position_not_order_quantity() -> None:
    side, quantity = required_trade(
        target_position=Decimal("0.005"),
        current_position=Decimal("-0.003"),
    )

    assert side is Side.BUY
    assert quantity == Decimal("0.008")


def test_required_trade_no_action_when_target_reached() -> None:
    side, quantity = required_trade(
        target_position=Decimal("0.005"),
        current_position=Decimal("0.005"),
    )

    assert side is Side.NO_ACTION
    assert quantity == Decimal("0")


def test_terminal_statuses_are_terminal() -> None:
    assert ExecutionStatus.COMPLETED.is_terminal
    assert ExecutionStatus.PARTIALLY_COMPLETED.is_terminal
    assert ExecutionStatus.EXPIRED.is_terminal
    assert ExecutionStatus.CANCELLED.is_terminal
    assert ExecutionStatus.FAILED.is_terminal
    assert not ExecutionStatus.RUNNING.is_terminal
    assert not ChildOrderStatus.UNKNOWN.is_terminal
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/unit/test_models.py -v
```

Expected: FAIL with `ModuleNotFoundError` or missing model definitions.

- [ ] **Step 3: Implement `src/execution/models.py`**

Create `src/execution/models.py` with these definitions:

```python
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Any


class Environment(StrEnum):
    SIMULATION = "simulation"
    TESTNET = "testnet"
    MAINNET = "mainnet"


class Algorithm(StrEnum):
    CHASE = "CHASE"
    TWAP = "TWAP"


class DeadlinePolicy(StrEnum):
    CANCEL_REMAINDER = "CANCEL_REMAINDER"
    AGGRESSIVE_WITHIN_RANGE = "AGGRESSIVE_WITHIN_RANGE"


class RepricingMode(StrEnum):
    ADVERSE_ONLY = "ADVERSE_ONLY"
    TWO_SIDED = "TWO_SIDED"


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"
    NO_ACTION = "NO_ACTION"


class ExecutionStatus(StrEnum):
    CREATED = "CREATED"
    VALIDATING = "VALIDATING"
    RUNNING = "RUNNING"
    CANCELLING = "CANCELLING"
    COMPLETED = "COMPLETED"
    PARTIALLY_COMPLETED = "PARTIALLY_COMPLETED"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"

    @property
    def is_terminal(self) -> bool:
        return self in {
            ExecutionStatus.COMPLETED,
            ExecutionStatus.PARTIALLY_COMPLETED,
            ExecutionStatus.EXPIRED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.FAILED,
        }


class ChildOrderStatus(StrEnum):
    PENDING_SUBMIT = "PENDING_SUBMIT"
    OPEN = "OPEN"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    PENDING_CANCEL = "PENDING_CANCEL"
    CANCELLED = "CANCELLED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"

    @property
    def is_terminal(self) -> bool:
        return self in {
            ChildOrderStatus.CANCELLED,
            ChildOrderStatus.FILLED,
            ChildOrderStatus.REJECTED,
        }


@dataclass(frozen=True)
class ExecutionParameters:
    reprice_threshold_bps: Decimal = Decimal("2.0")
    minimum_reprice_interval_ms: int = 500
    number_of_slices: int = 10
    child_order_timeout_seconds: int = 20
    repricing_mode: RepricingMode = RepricingMode.ADVERSE_ONLY


@dataclass(frozen=True)
class ExecutionRequest:
    environment: Environment
    symbol: str
    algorithm: Algorithm
    target_position: Decimal
    target_price_lower: Decimal
    target_price_upper: Decimal
    target_duration_seconds: int
    deadline_policy: DeadlinePolicy
    parameters: ExecutionParameters = field(default_factory=ExecutionParameters)


@dataclass(frozen=True)
class SymbolRules:
    symbol: str
    tick_size: Decimal
    quantity_step: Decimal
    min_quantity: Decimal
    min_notional: Decimal
    status: str
    supported_time_in_force: frozenset[str] = frozenset({"GTC", "GTX"})


@dataclass(frozen=True)
class MarketSnapshot:
    symbol: str
    bid: Decimal
    ask: Decimal
    last_market_event_time_exchange: int | None
    last_market_event_time_local_monotonic: float

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / Decimal("2")

    @property
    def is_crossed(self) -> bool:
        return self.bid >= self.ask


@dataclass(frozen=True)
class PositionSnapshot:
    symbol: str
    position: Decimal
    update_time_ms: int | None = None


@dataclass(frozen=True)
class OrderRequest:
    execution_id: str
    child_order_id: str
    client_order_id: str
    symbol: str
    side: Side
    quantity: Decimal
    price: Decimal
    post_only: bool
    reduce_only: bool = False


@dataclass
class ChildOrder:
    child_order_id: str
    client_order_id: str
    symbol: str
    side: Side
    submitted_quantity: Decimal
    price: Decimal
    status: ChildOrderStatus = ChildOrderStatus.PENDING_SUBMIT
    confirmed_filled_quantity: Decimal = Decimal("0")
    exchange_order_id: str | None = None
    raw_status: str | None = None
    terminal_reason: str | None = None

    @property
    def remaining_quantity(self) -> Decimal:
        remaining = self.submitted_quantity - self.confirmed_filled_quantity
        return remaining if remaining > Decimal("0") else Decimal("0")


@dataclass(frozen=True)
class Fill:
    client_order_id: str
    trade_id: str | None
    cumulative_filled_quantity: Decimal
    last_filled_quantity: Decimal
    last_fill_price: Decimal
    event_time_ms: int | None
    transaction_time_ms: int | None


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


@dataclass
class ExecutionSummary:
    execution_id: str
    final_status: ExecutionStatus
    final_reason: str
    metrics: dict[str, Any]


def required_trade(target_position: Decimal, current_position: Decimal) -> tuple[Side, Decimal]:
    quantity = target_position - current_position
    if quantity > Decimal("0"):
        return Side.BUY, quantity
    if quantity < Decimal("0"):
        return Side.SELL, abs(quantity)
    return Side.NO_ACTION, Decimal("0")
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/unit/test_models.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/execution/models.py tests/unit/test_models.py
git commit -m "feat: add execution domain models"
```

---

### Task 3: State Machine, Clock, IDs, And Event Serialization

**Files:**
- Create: `src/execution/state_machine.py`
- Create: `src/execution/clock.py`
- Create: `src/execution/ids.py`
- Create: `src/execution/events.py`
- Test: `tests/unit/test_state_machine.py`
- Test: `tests/unit/test_ids.py`
- Test: `tests/unit/test_event_serialization.py`

- [ ] **Step 1: Write failing state transition tests**

Create `tests/unit/test_state_machine.py`:

```python
import pytest

from execution.models import ChildOrderStatus, ExecutionStatus
from execution.state_machine import InvalidStateTransition, transition_child, transition_execution


def test_execution_terminal_state_cannot_return_to_running() -> None:
    with pytest.raises(InvalidStateTransition):
        transition_execution(ExecutionStatus.COMPLETED, ExecutionStatus.RUNNING)


def test_execution_running_can_move_to_cancelling() -> None:
    assert transition_execution(ExecutionStatus.RUNNING, ExecutionStatus.CANCELLING) is ExecutionStatus.CANCELLING


def test_child_pending_cancel_can_fill_or_cancel() -> None:
    assert transition_child(ChildOrderStatus.PENDING_CANCEL, ChildOrderStatus.FILLED) is ChildOrderStatus.FILLED
    assert transition_child(ChildOrderStatus.PENDING_CANCEL, ChildOrderStatus.CANCELLED) is ChildOrderStatus.CANCELLED


def test_child_pending_cancel_can_reconcile_to_still_live_order() -> None:
    assert transition_child(ChildOrderStatus.PENDING_CANCEL, ChildOrderStatus.OPEN) is ChildOrderStatus.OPEN
    assert transition_child(ChildOrderStatus.PENDING_CANCEL, ChildOrderStatus.PARTIALLY_FILLED) is ChildOrderStatus.PARTIALLY_FILLED


def test_child_open_cannot_move_to_unknown() -> None:
    with pytest.raises(InvalidStateTransition):
        transition_child(ChildOrderStatus.OPEN, ChildOrderStatus.UNKNOWN)


def test_child_unknown_can_be_reconciled_to_exchange_truth() -> None:
    assert transition_child(ChildOrderStatus.UNKNOWN, ChildOrderStatus.OPEN) is ChildOrderStatus.OPEN
    assert transition_child(ChildOrderStatus.UNKNOWN, ChildOrderStatus.PARTIALLY_FILLED) is ChildOrderStatus.PARTIALLY_FILLED
    assert transition_child(ChildOrderStatus.UNKNOWN, ChildOrderStatus.FILLED) is ChildOrderStatus.FILLED
    assert transition_child(ChildOrderStatus.UNKNOWN, ChildOrderStatus.CANCELLED) is ChildOrderStatus.CANCELLED
    assert transition_child(ChildOrderStatus.UNKNOWN, ChildOrderStatus.REJECTED) is ChildOrderStatus.REJECTED
```

- [ ] **Step 2: Write failing client order ID tests**

Create `tests/unit/test_ids.py`:

```python
import re

from execution.ids import child_order_id, client_order_id, execution_id, make_client_order_id, make_client_order_prefix


def test_client_order_id_is_binance_compatible() -> None:
    execution = execution_id()
    child = child_order_id(1)
    client = client_order_id(execution, child)

    assert len(client) <= 36
    assert re.fullmatch(r"^[\.A-Z\:/a-z0-9_-]{1,36}$", client)
    assert client.startswith("ce_")


def test_client_order_id_changes_by_child_sequence() -> None:
    execution = "exec_0123456789abcdef"
    assert client_order_id(execution, child_order_id(1)) != client_order_id(execution, child_order_id(2))


def test_client_order_prefix_matches_derived_order_ids() -> None:
    execution = "exec_0123456789abcdef"
    client = make_client_order_id(execution, 1)

    assert client.startswith(make_client_order_prefix(execution))
```

- [ ] **Step 3: Write failing event serialization test**

Create `tests/unit/test_event_serialization.py`:

```python
import asyncio

from execution.events import ExecutionEventActor


async def test_events_for_one_execution_are_serialized() -> None:
    actor = ExecutionEventActor(execution_id="exec_test")
    seen: list[int] = []

    async def handler(value: int) -> None:
        await asyncio.sleep(0)
        seen.append(value)

    await asyncio.gather(
        actor.apply(lambda: handler(1)),
        actor.apply(lambda: handler(2)),
        actor.apply(lambda: handler(3)),
    )

    assert seen == [1, 2, 3]
```

- [ ] **Step 4: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/unit/test_state_machine.py tests/unit/test_ids.py tests/unit/test_event_serialization.py -v
```

Expected: FAIL with missing modules or missing functions.

- [ ] **Step 5: Implement state machine**

Create `src/execution/state_machine.py`:

```python
from __future__ import annotations

from execution.models import ChildOrderStatus, ExecutionStatus


class InvalidStateTransition(ValueError):
    pass


EXECUTION_TRANSITIONS: dict[ExecutionStatus, set[ExecutionStatus]] = {
    ExecutionStatus.CREATED: {ExecutionStatus.VALIDATING, ExecutionStatus.FAILED},
    ExecutionStatus.VALIDATING: {ExecutionStatus.RUNNING, ExecutionStatus.COMPLETED, ExecutionStatus.FAILED},
    ExecutionStatus.RUNNING: {
        ExecutionStatus.CANCELLING,
        ExecutionStatus.COMPLETED,
        ExecutionStatus.PARTIALLY_COMPLETED,
        ExecutionStatus.EXPIRED,
        ExecutionStatus.FAILED,
    },
    ExecutionStatus.CANCELLING: {
        ExecutionStatus.CANCELLED,
        ExecutionStatus.PARTIALLY_COMPLETED,
        ExecutionStatus.FAILED,
    },
    ExecutionStatus.COMPLETED: set(),
    ExecutionStatus.PARTIALLY_COMPLETED: set(),
    ExecutionStatus.EXPIRED: set(),
    ExecutionStatus.CANCELLED: set(),
    ExecutionStatus.FAILED: set(),
}


CHILD_TRANSITIONS: dict[ChildOrderStatus, set[ChildOrderStatus]] = {
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
        ChildOrderStatus.REJECTED,
    },
    ChildOrderStatus.CANCELLED: set(),
    ChildOrderStatus.FILLED: set(),
    ChildOrderStatus.REJECTED: set(),
    ChildOrderStatus.UNKNOWN: {
        ChildOrderStatus.OPEN,
        ChildOrderStatus.PARTIALLY_FILLED,
        ChildOrderStatus.FILLED,
        ChildOrderStatus.CANCELLED,
        ChildOrderStatus.REJECTED,
    },
}


def transition_execution(current: ExecutionStatus, target: ExecutionStatus) -> ExecutionStatus:
    if target not in EXECUTION_TRANSITIONS[current]:
        raise InvalidStateTransition(f"execution transition {current} -> {target} is not allowed")
    return target


def transition_child(current: ChildOrderStatus, target: ChildOrderStatus) -> ChildOrderStatus:
    if target not in CHILD_TRANSITIONS[current]:
        raise InvalidStateTransition(f"child order transition {current} -> {target} is not allowed")
    return target
```

- [ ] **Step 6: Implement clock and IDs**

Create `src/execution/clock.py`:

```python
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime


class Clock:
    def monotonic(self) -> float:
        raise NotImplementedError

    def utc_now(self) -> datetime:
        raise NotImplementedError


class SystemClock(Clock):
    def monotonic(self) -> float:
        return time.monotonic()

    def utc_now(self) -> datetime:
        return datetime.now(tz=UTC)


@dataclass
class ManualClock(Clock):
    current: float = 0.0

    def monotonic(self) -> float:
        return self.current

    def utc_now(self) -> datetime:
        return datetime.fromtimestamp(self.current, tz=UTC)

    def advance(self, seconds: float) -> None:
        self.current += seconds
```

Create `src/execution/ids.py`:

```python
from __future__ import annotations

import re
import uuid

CLIENT_ORDER_ID_RE = re.compile(r"^[\.A-Z\:/a-z0-9_-]{1,36}$")


def execution_id() -> str:
    return f"exec_{uuid.uuid4().hex[:16]}"


def child_order_id(sequence: int) -> str:
    return f"child_{sequence:04d}"


def make_client_order_prefix(execution_id_value: str) -> str:
    short_exec = execution_id_value.replace("exec_", "")[:12]
    return f"ce_{short_exec}_"


def make_client_order_id(execution_id_value: str, child_sequence: int) -> str:
    value = f"{make_client_order_prefix(execution_id_value)}{child_sequence}"
    if len(value) > 36 or not CLIENT_ORDER_ID_RE.fullmatch(value):
        raise ValueError(f"invalid Binance client order id: {value}")
    return value


def client_order_id(execution_id_value: str, child_order_id_value: str) -> str:
    child_seq = child_order_id_value.replace("child_", "").lstrip("0") or "0"
    return make_client_order_id(execution_id_value, int(child_seq))
```

- [ ] **Step 7: Implement per-execution event actor**

Create `src/execution/events.py`:

```python
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")


class ExecutionEventActor:
    def __init__(self, execution_id: str) -> None:
        self.execution_id = execution_id
        self._lock = asyncio.Lock()

    async def apply(self, operation: Callable[[], Awaitable[T]]) -> T:
        async with self._lock:
            return await operation()
```

- [ ] **Step 8: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/unit/test_state_machine.py tests/unit/test_ids.py tests/unit/test_event_serialization.py -v
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add src/execution/state_machine.py src/execution/clock.py src/execution/ids.py src/execution/events.py tests/unit/test_state_machine.py tests/unit/test_ids.py tests/unit/test_event_serialization.py
git commit -m "feat: add execution state and event primitives"
```

---

### Task 4: Decimal Math, Request Validation, And Exposure Invariants

**Files:**
- Create: `src/risk/decimal_math.py`
- Create: `src/risk/validation.py`
- Test: `tests/unit/test_decimal_math.py`
- Test: `tests/unit/test_validation.py`

- [ ] **Step 1: Write failing Decimal math tests**

Create `tests/unit/test_decimal_math.py`:

```python
from decimal import Decimal

from execution.models import Side
from risk.decimal_math import completion_rate, floor_to_step, round_price, slippage_bps


def test_floor_to_step_rounds_toward_zero() -> None:
    assert floor_to_step(Decimal("0.0089"), Decimal("0.001")) == Decimal("0.008")


def test_passive_buy_rounds_down_to_tick() -> None:
    assert round_price(Decimal("94000.09"), Decimal("0.10"), Side.BUY, passive=True) == Decimal("94000.0")


def test_passive_sell_rounds_up_to_tick() -> None:
    assert round_price(Decimal("94000.01"), Decimal("0.10"), Side.SELL, passive=True) == Decimal("94000.1")


def test_completion_rate_uses_absolute_quantity() -> None:
    assert completion_rate(Decimal("0.004"), Decimal("0.008")) == Decimal("0.5")


def test_slippage_bps_is_side_aware() -> None:
    assert slippage_bps(Side.BUY, Decimal("100"), Decimal("101")) == Decimal("100")
    assert slippage_bps(Side.SELL, Decimal("100"), Decimal("99")) == Decimal("100")
```

- [ ] **Step 2: Write failing validation tests**

Create `tests/unit/test_validation.py`:

```python
from decimal import Decimal

import pytest

from execution.models import Exposure, Side, SymbolRules
from risk.validation import (
    ValidationError,
    check_exposure_invariant,
    validate_order_shape,
    validate_price_bounds,
    validate_quantity,
)


RULES = SymbolRules(
    symbol="BTCUSDT",
    tick_size=Decimal("0.10"),
    quantity_step=Decimal("0.001"),
    min_quantity=Decimal("0.001"),
    min_notional=Decimal("5"),
    status="TRADING",
)


def test_validate_quantity_rejects_below_min_notional() -> None:
    with pytest.raises(ValidationError):
        validate_quantity(Decimal("0.001"), Decimal("1000"), RULES)


def test_validate_price_bounds_rejects_aggressive_buy_above_upper() -> None:
    with pytest.raises(ValidationError):
        validate_price_bounds(Side.BUY, Decimal("101"), Decimal("90"), Decimal("100"))


def test_validate_order_shape_rejects_non_step_quantity_and_non_tick_price() -> None:
    with pytest.raises(ValidationError, match="quantity step"):
        validate_order_shape(
            quantity=Decimal("0.0015"),
            price=Decimal("100.10"),
            side=Side.BUY,
            rules=RULES,
            best_bid=Decimal("100"),
            best_ask=Decimal("101"),
            post_only=True,
        )
    with pytest.raises(ValidationError, match="tick size"):
        validate_order_shape(
            quantity=Decimal("0.002"),
            price=Decimal("100.15"),
            side=Side.BUY,
            rules=RULES,
            best_bid=Decimal("100"),
            best_ask=Decimal("101"),
            post_only=True,
        )


def test_validate_order_shape_rejects_post_only_crossing_prices() -> None:
    with pytest.raises(ValidationError, match="post-only buy"):
        validate_order_shape(
            quantity=Decimal("0.002"),
            price=Decimal("101"),
            side=Side.BUY,
            rules=RULES,
            best_bid=Decimal("100"),
            best_ask=Decimal("101"),
            post_only=True,
        )
    with pytest.raises(ValidationError, match="post-only sell"):
        validate_order_shape(
            quantity=Decimal("0.002"),
            price=Decimal("100"),
            side=Side.SELL,
            rules=RULES,
            best_bid=Decimal("100"),
            best_ask=Decimal("101"),
            post_only=True,
        )


def test_exposure_invariant_rejects_over_reserved_quantity() -> None:
    exposure = Exposure(
        confirmed_filled_quantity=Decimal("0.005"),
        live_open_quantity=Decimal("0.003"),
    )
    with pytest.raises(ValidationError):
        check_exposure_invariant(exposure, Decimal("0.001"), Decimal("0.008"))
```

- [ ] **Step 3: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/unit/test_decimal_math.py tests/unit/test_validation.py -v
```

Expected: FAIL with missing risk modules.

- [ ] **Step 4: Implement Decimal helpers**

Create `src/risk/decimal_math.py`:

```python
from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

from execution.models import Side


def floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if value < Decimal("0"):
        raise ValueError("value must be non-negative")
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
    if value < Decimal("0"):
        raise ValueError("value must be non-negative")
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


def round_price(price: Decimal, tick_size: Decimal, side: Side, passive: bool) -> Decimal:
    units = price / tick_size
    if passive:
        rounding = ROUND_FLOOR if side is Side.BUY else ROUND_CEILING
    else:
        rounding = ROUND_CEILING if side is Side.BUY else ROUND_FLOOR
    return units.to_integral_value(rounding=rounding) * tick_size


def completion_rate(filled_quantity: Decimal, required_quantity: Decimal) -> Decimal:
    if required_quantity == Decimal("0"):
        return Decimal("1")
    return filled_quantity / required_quantity


def slippage_bps(side: Side, arrival_mid: Decimal, execution_vwap: Decimal) -> Decimal:
    if arrival_mid == Decimal("0"):
        return Decimal("0")
    if side is Side.BUY:
        return (execution_vwap - arrival_mid) / arrival_mid * Decimal("10000")
    if side is Side.SELL:
        return (arrival_mid - execution_vwap) / arrival_mid * Decimal("10000")
    return Decimal("0")
```

- [ ] **Step 5: Implement validation helpers**

Create `src/risk/validation.py`:

```python
from __future__ import annotations

from decimal import Decimal

from execution.models import Exposure, Side, SymbolRules


class ValidationError(ValueError):
    pass


def validate_quantity(quantity: Decimal, price: Decimal, rules: SymbolRules) -> None:
    if quantity < rules.min_quantity:
        raise ValidationError(f"quantity {quantity} below min quantity {rules.min_quantity}")
    notional = quantity * price
    if notional < rules.min_notional:
        raise ValidationError(f"notional {notional} below min notional {rules.min_notional}")


def validate_price_bounds(side: Side, price: Decimal, lower: Decimal, upper: Decimal) -> None:
    if lower > upper:
        raise ValidationError("lower price bound cannot exceed upper price bound")
    if side is Side.BUY and price > upper:
        raise ValidationError(f"buy price {price} exceeds upper bound {upper}")
    if side is Side.SELL and price < lower:
        raise ValidationError(f"sell price {price} below lower bound {lower}")


def _is_multiple(value: Decimal, step: Decimal) -> bool:
    return value % step == Decimal("0")


def validate_order_shape(
    quantity: Decimal,
    price: Decimal,
    side: Side,
    rules: SymbolRules,
    best_bid: Decimal,
    best_ask: Decimal,
    post_only: bool,
) -> None:
    validate_quantity(quantity, price, rules)
    if not _is_multiple(quantity, rules.quantity_step):
        raise ValidationError(f"quantity step violation: {quantity} not multiple of {rules.quantity_step}")
    if not _is_multiple(price, rules.tick_size):
        raise ValidationError(f"tick size violation: {price} not multiple of {rules.tick_size}")
    if post_only and side is Side.BUY and price >= best_ask:
        raise ValidationError(f"post-only buy would cross ask: price={price} ask={best_ask}")
    if post_only and side is Side.SELL and price <= best_bid:
        raise ValidationError(f"post-only sell would cross bid: price={price} bid={best_bid}")


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

- [ ] **Step 6: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/unit/test_decimal_math.py tests/unit/test_validation.py -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/risk/decimal_math.py src/risk/validation.py tests/unit/test_decimal_math.py tests/unit/test_validation.py
git commit -m "feat: add decimal risk validation"
```

---

### Task 5: Exchange Adapter Contract And Deterministic Simulator Foundation

**Files:**
- Create: `src/exchanges/base.py`
- Create: `src/exchanges/simulator.py`
- Test: `tests/unit/test_exchange_contract.py`
- Test: `tests/simulation/test_simulator_market_data.py`

- [ ] **Step 1: Write failing exchange contract tests**

Create `tests/unit/test_exchange_contract.py`:

```python
from exchanges.base import ExchangeAdapter


def test_exchange_adapter_defines_required_methods() -> None:
    required = {
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
    for name in required:
        assert hasattr(ExchangeAdapter, name)
```

- [ ] **Step 2: Write failing simulator market-data tests**

Create `tests/simulation/test_simulator_market_data.py`:

```python
from decimal import Decimal

import pytest

from execution.clock import ManualClock
from exchanges.simulator import DeterministicSimulator, NoFreshMarketData


async def test_simulator_has_no_actionable_quote_before_first_snapshot() -> None:
    simulator = DeterministicSimulator(clock=ManualClock())

    with pytest.raises(NoFreshMarketData):
        await simulator.get_best_bid_ask("BTCUSDT")


async def test_simulator_returns_fresh_snapshot_after_market_event() -> None:
    clock = ManualClock()
    simulator = DeterministicSimulator(clock=clock)

    await simulator.push_market_data("BTCUSDT", Decimal("100"), Decimal("101"), exchange_event_time=10)
    snapshot = await simulator.get_best_bid_ask("BTCUSDT")

    assert snapshot.bid == Decimal("100")
    assert snapshot.ask == Decimal("101")
    assert snapshot.last_market_event_time_local_monotonic == 0
```

- [ ] **Step 3: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/unit/test_exchange_contract.py tests/simulation/test_simulator_market_data.py -v
```

Expected: FAIL with missing exchange modules.

- [ ] **Step 4: Implement adapter contract**

Create `src/exchanges/base.py`:

```python
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from execution.models import MarketSnapshot, OrderRequest, PositionSnapshot, SymbolRules


class NoFreshMarketData(RuntimeError):
    pass


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
    async def stream_market_data(self) -> AsyncIterator[MarketSnapshot]:
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
    async def stream_user_events(self) -> AsyncIterator[object]:
        raise NotImplementedError

    @abstractmethod
    async def reconcile_orders_and_fills(self, symbol: str, client_order_prefix: str | None = None) -> object:
        raise NotImplementedError

    @abstractmethod
    async def health_check_streams(self) -> bool:
        raise NotImplementedError
```

- [ ] **Step 5: Implement simulator market data and rules**

Create `src/exchanges/simulator.py` with this foundation:

```python
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from decimal import Decimal

from exchanges.base import ExchangeAdapter, NoFreshMarketData
from execution.clock import Clock, ManualClock
from execution.models import MarketSnapshot, OrderRequest, PositionSnapshot, SymbolRules


@dataclass
class DeterministicSimulator(ExchangeAdapter):
    clock: Clock = field(default_factory=ManualClock)
    position: Decimal = Decimal("0")
    stale_market_data_seconds: float = 1.5
    _market: dict[str, MarketSnapshot] = field(default_factory=dict)
    _market_queue: asyncio.Queue[MarketSnapshot] = field(default_factory=asyncio.Queue)

    async def get_symbol_rules(self, symbol: str) -> SymbolRules:
        return SymbolRules(
            symbol=symbol,
            tick_size=Decimal("0.10"),
            quantity_step=Decimal("0.001"),
            min_quantity=Decimal("0.001"),
            min_notional=Decimal("5"),
            status="TRADING",
            supported_time_in_force=frozenset({"GTC", "GTX"}),
        )

    async def get_position(self, symbol: str) -> PositionSnapshot:
        return PositionSnapshot(symbol=symbol, position=self.position)

    async def push_market_data(
        self,
        symbol: str,
        bid: Decimal,
        ask: Decimal,
        exchange_event_time: int | None = None,
    ) -> None:
        snapshot = MarketSnapshot(
            symbol=symbol,
            bid=bid,
            ask=ask,
            last_market_event_time_exchange=exchange_event_time,
            last_market_event_time_local_monotonic=self.clock.monotonic(),
        )
        self._market[symbol] = snapshot
        await self._market_queue.put(snapshot)

    async def get_best_bid_ask(self, symbol: str) -> MarketSnapshot:
        snapshot = self._market.get(symbol)
        if snapshot is None:
            raise NoFreshMarketData(f"no fresh market data for {symbol}")
        if snapshot.is_crossed:
            raise NoFreshMarketData(f"crossed market data for {symbol}")
        age = self.clock.monotonic() - snapshot.last_market_event_time_local_monotonic
        if age > self.stale_market_data_seconds:
            raise NoFreshMarketData(f"stale market data for {symbol}: age={age}")
        return snapshot

    async def stream_market_data(self) -> AsyncIterator[MarketSnapshot]:
        while True:
            yield await self._market_queue.get()

    async def submit_limit_order(self, order_request: OrderRequest) -> object:
        raise NotImplementedError("order lifecycle is added in the simulator order task")

    async def cancel_order(self, symbol: str, client_order_id: str) -> object:
        raise NotImplementedError("order lifecycle is added in the simulator order task")

    async def get_order_by_client_order_id(self, symbol: str, client_order_id: str) -> object:
        raise NotImplementedError("order lifecycle is added in the simulator order task")

    async def stream_user_events(self) -> AsyncIterator[object]:
        if False:
            yield None
        return

    async def reconcile_orders_and_fills(self, symbol: str, client_order_prefix: str | None = None) -> object:
        return []

    async def health_check_streams(self) -> bool:
        return True
```

- [ ] **Step 6: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/unit/test_exchange_contract.py tests/simulation/test_simulator_market_data.py -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/exchanges/base.py src/exchanges/simulator.py tests/unit/test_exchange_contract.py tests/simulation/test_simulator_market_data.py
git commit -m "feat: add exchange adapter and simulator market data"
```

---

### Task 6: Observability, Sanitized Logs, And Execution Summaries

**Files:**
- Create: `src/observability/logging.py`
- Create: `src/observability/summary.py`
- Create: `src/observability/artifacts.py`
- Test: `tests/unit/test_observability.py`

- [ ] **Step 1: Write failing observability tests**

Create `tests/unit/test_observability.py`:

```python
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from execution.models import ExecutionStatus, Side
from observability.logging import sanitize_log_payload, to_jsonable
from observability.summary import execution_vwap, summary_metrics
from observability.artifacts import write_execution_artifacts


def test_sanitize_log_payload_removes_sensitive_fields() -> None:
    payload = {
        "api_key": "abc",
        "secret_key": "def",
        "signature": "sig",
        "listenKey": "listen",
        "clientOrderId": "ce_abc_1",
        "orderId": 123,
        "price": "100",
    }

    sanitized = sanitize_log_payload(payload)

    assert "api_key" not in sanitized
    assert "secret_key" not in sanitized
    assert "signature" not in sanitized
    assert "listenKey" not in sanitized
    assert sanitized["clientOrderId"] == "ce_abc_1"
    assert sanitized["orderId"] == 123


def test_to_jsonable_converts_decimal_enum_datetime_and_path() -> None:
    payload = {
        "quantity": Decimal("0.010"),
        "status": ExecutionStatus.COMPLETED,
        "timestamp": datetime(2026, 6, 21, tzinfo=UTC),
        "path": Path("outputs/exec_test"),
        "items": [Decimal("1.5")],
    }

    converted = to_jsonable(payload)

    assert converted == {
        "quantity": "0.010",
        "status": "COMPLETED",
        "timestamp": "2026-06-21T00:00:00+00:00",
        "path": "outputs/exec_test",
        "items": ["1.5"],
    }


def test_execution_vwap_uses_decimal_weighted_average() -> None:
    fills = [(Decimal("100"), Decimal("0.01")), (Decimal("110"), Decimal("0.03"))]
    assert execution_vwap(fills) == Decimal("107.5")


def test_summary_metrics_include_side_aware_slippage() -> None:
    metrics = summary_metrics(
        final_status=ExecutionStatus.COMPLETED,
        side=Side.BUY,
        required_quantity=Decimal("0.010"),
        filled_quantity=Decimal("0.005"),
        arrival_mid=Decimal("100"),
        vwap=Decimal("101"),
    )

    assert metrics["completion_rate"] == "0.5"
    assert metrics["slippage_bps"] == "100"
    assert metrics["final_status"] == "COMPLETED"


def test_write_execution_artifacts_creates_required_files(tmp_path) -> None:
    output_dir = write_execution_artifacts(
        root=tmp_path,
        execution_id="exec_test",
        request_snapshot={"symbol": "BTCUSDT", "target_position": Decimal("0.010")},
        log_events=[{"execution_id": "exec_test", "client_order_id": "ce_test_1", "quantity": Decimal("0.004")}],
        summary={"execution_id": "exec_test", "final_status": ExecutionStatus.PARTIALLY_COMPLETED},
        child_orders=[{"client_order_id": "ce_test_1", "status": "CANCELLED", "quantity": Decimal("0.010")}],
        fills=[{"client_order_id": "ce_test_1", "trade_id": "t1", "price": Decimal("100")}],
        timeline=[{"event": "cancel_fill_race", "timestamp": datetime(2026, 6, 21, tzinfo=UTC)}],
    )

    assert (output_dir / "request_snapshot.json").exists()
    assert (output_dir / "execution_log.jsonl").exists()
    assert (output_dir / "execution_summary.json").exists()
    assert (output_dir / "child_orders.csv").exists()
    assert (output_dir / "fills.csv").exists()
    assert (output_dir / "timeline.csv").exists()
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/unit/test_observability.py -v
```

Expected: FAIL with missing observability modules.

- [ ] **Step 3: Implement sanitized logging helpers**

Create `src/observability/logging.py`:

```python
from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

SENSITIVE_KEYS = {
    "api_key",
    "apiKey",
    "secret_key",
    "secretKey",
    "signature",
    "listenKey",
    "raw_authenticated_request",
    "rawAuthenticatedRequest",
}


def to_jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {key: to_jsonable(inner) for key, inner in value.items()}
    if isinstance(value, list | tuple):
        return [to_jsonable(item) for item in value]
    return value


def sanitize_log_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for key, value in payload.items():
        if key in SENSITIVE_KEYS:
            continue
        sanitized[key] = to_jsonable(value)
    return sanitized


def append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(sanitize_log_payload(payload), sort_keys=True) + "\n")
```

- [ ] **Step 4: Implement summary helpers**

Create `src/observability/summary.py`:

```python
from __future__ import annotations

from decimal import Decimal

from execution.models import ExecutionStatus, Side
from risk.decimal_math import completion_rate, slippage_bps


def decimal_string(value: Decimal) -> str:
    return format(value.normalize(), "f")


def execution_vwap(fills: list[tuple[Decimal, Decimal]]) -> Decimal:
    filled_quantity = sum(quantity for _, quantity in fills)
    if filled_quantity == Decimal("0"):
        return Decimal("0")
    notional = sum(price * quantity for price, quantity in fills)
    return notional / filled_quantity


def summary_metrics(
    final_status: ExecutionStatus,
    side: Side,
    required_quantity: Decimal,
    filled_quantity: Decimal,
    arrival_mid: Decimal,
    vwap: Decimal,
) -> dict[str, str]:
    return {
        "final_status": final_status.value,
        "required_quantity": decimal_string(required_quantity),
        "filled_quantity": decimal_string(filled_quantity),
        "completion_rate": decimal_string(completion_rate(filled_quantity, required_quantity)),
        "slippage_bps": decimal_string(slippage_bps(side, arrival_mid, vwap)),
    }
```

- [ ] **Step 5: Implement artifact writer**

Create `src/observability/artifacts.py`:

```python
from __future__ import annotations

import csv
import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from observability.logging import append_jsonl, sanitize_log_payload


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(sanitize_log_payload(payload), indent=2, sort_keys=True), encoding="utf-8")


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    rows = [sanitize_log_payload(row) for row in rows]
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_execution_artifacts(
    root: Path,
    execution_id: str,
    request_snapshot: Mapping[str, Any],
    log_events: Iterable[Mapping[str, Any]],
    summary: Mapping[str, Any],
    child_orders: Iterable[Mapping[str, Any]],
    fills: Iterable[Mapping[str, Any]],
    timeline: Iterable[Mapping[str, Any]],
) -> Path:
    output_dir = root / execution_id
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(output_dir / "request_snapshot.json", request_snapshot)
    for event in log_events:
        append_jsonl(output_dir / "execution_log.jsonl", event)
    _write_json(output_dir / "execution_summary.json", summary)
    _write_csv(output_dir / "child_orders.csv", child_orders)
    _write_csv(output_dir / "fills.csv", fills)
    _write_csv(output_dir / "timeline.csv", timeline)
    return output_dir
```

- [ ] **Step 6: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/unit/test_observability.py -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/observability/logging.py src/observability/summary.py src/observability/artifacts.py tests/unit/test_observability.py
git commit -m "feat: add sanitized execution observability"
```

---

### Task 7: Simulator Order Lifecycle, Events, And Reconciliation

**Files:**
- Modify: `src/exchanges/simulator.py`
- Test: `tests/simulation/test_simulator_orders.py`

- [ ] **Step 1: Write failing simulator order tests**

Create `tests/simulation/test_simulator_orders.py`:

```python
from decimal import Decimal

from execution.clock import ManualClock
from execution.models import ChildOrderStatus, OrderRequest, Side
from exchanges.simulator import DeterministicSimulator, SimulatorOrderEvent


def make_order(client_order_id: str = "ce_exec_1") -> OrderRequest:
    return OrderRequest(
        execution_id="exec_abc",
        child_order_id="child_0001",
        client_order_id=client_order_id,
        symbol="BTCUSDT",
        side=Side.BUY,
        quantity=Decimal("0.010"),
        price=Decimal("100"),
        post_only=True,
    )


async def test_submit_order_opens_order_and_emits_event() -> None:
    simulator = DeterministicSimulator(clock=ManualClock())
    await simulator.submit_limit_order(make_order())

    order = await simulator.get_order_by_client_order_id("BTCUSDT", "ce_exec_1")
    event = await simulator.next_user_event()

    assert order.status is ChildOrderStatus.OPEN
    assert isinstance(event, SimulatorOrderEvent)
    assert event.client_order_id == "ce_exec_1"


async def test_fill_after_cancel_request_maps_to_filled() -> None:
    simulator = DeterministicSimulator(clock=ManualClock())
    await simulator.submit_limit_order(make_order())
    await simulator.cancel_order("BTCUSDT", "ce_exec_1")
    await simulator.push_fill("ce_exec_1", trade_id="t1", cumulative=Decimal("0.010"), price=Decimal("100"))

    order = await simulator.get_order_by_client_order_id("BTCUSDT", "ce_exec_1")

    assert order.status is ChildOrderStatus.FILLED
    assert order.confirmed_filled_quantity == Decimal("0.010")


async def test_reconcile_is_scoped_by_client_order_prefix() -> None:
    simulator = DeterministicSimulator(clock=ManualClock())
    await simulator.submit_limit_order(make_order("ce_exec_1"))
    await simulator.submit_limit_order(make_order("manual_order"))

    scoped = await simulator.reconcile_orders_and_fills("BTCUSDT", client_order_prefix="ce_")

    assert [order.client_order_id for order in scoped] == ["ce_exec_1"]


async def test_scripted_fill_during_cancel_keeps_partial_fill_exposure() -> None:
    simulator = DeterministicSimulator(clock=ManualClock())
    simulator.script_fill_during_cancel(client_order_prefix="ce_", fill_quantity=Decimal("0.004"))
    await simulator.submit_limit_order(make_order("ce_exec_1"))

    child = await simulator.cancel_order("BTCUSDT", "ce_exec_1")

    assert child.status is ChildOrderStatus.PENDING_CANCEL
    assert child.confirmed_filled_quantity == Decimal("0.004")


async def test_scripted_create_timeout_creates_unknown_order_for_reconciliation() -> None:
    simulator = DeterministicSimulator(clock=ManualClock())
    simulator.script_create_timeout(client_order_prefix="ce_")

    child = await simulator.submit_limit_order(make_order("ce_exec_1"))
    reconciled = await simulator.reconcile_orders_and_fills("BTCUSDT", client_order_prefix="ce_")

    assert child.status is ChildOrderStatus.UNKNOWN
    assert reconciled[0].client_order_id == "ce_exec_1"


async def test_scripted_ambiguous_cancel_reconciles_to_still_open() -> None:
    simulator = DeterministicSimulator(clock=ManualClock())
    simulator.script_cancel_reconcile_open(client_order_prefix="ce_")
    await simulator.submit_limit_order(make_order("ce_exec_1"))

    child = await simulator.cancel_order("BTCUSDT", "ce_exec_1")
    reconciled = await simulator.reconcile_orders_and_fills("BTCUSDT", client_order_prefix="ce_")

    assert child.status is ChildOrderStatus.PENDING_CANCEL
    assert reconciled[0].status is ChildOrderStatus.OPEN
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/simulation/test_simulator_orders.py -v
```

Expected: FAIL because simulator order lifecycle is not implemented.

- [ ] **Step 3: Extend simulator with order events and lifecycle**

Modify `src/exchanges/simulator.py` by adding these definitions near the top:

```python
from execution.models import ChildOrder, ChildOrderStatus, Fill


@dataclass(frozen=True)
class SimulatorOrderEvent:
    client_order_id: str
    status: ChildOrderStatus
    fill: Fill | None = None
    raw_status: str | None = None
```

Add these fields to `DeterministicSimulator`:

```python
    _orders: dict[str, ChildOrder] = field(default_factory=dict)
    _user_event_queue: asyncio.Queue[SimulatorOrderEvent] = field(default_factory=asyncio.Queue)
    _fill_during_cancel: dict[str, Decimal] = field(default_factory=dict)
    _create_timeout_prefixes: set[str] = field(default_factory=set)
    _cancel_reconcile_open_prefixes: set[str] = field(default_factory=set)
```

Replace the order methods with:

```python
    def script_fill_during_cancel(self, client_order_prefix: str, fill_quantity: Decimal) -> None:
        self._fill_during_cancel[client_order_prefix] = fill_quantity

    def script_create_timeout(self, client_order_prefix: str) -> None:
        self._create_timeout_prefixes.add(client_order_prefix)

    def script_cancel_reconcile_open(self, client_order_prefix: str) -> None:
        self._cancel_reconcile_open_prefixes.add(client_order_prefix)

    def _matching_prefix(self, client_order_id: str, prefixes: set[str] | dict[str, Decimal]) -> str | None:
        return next((prefix for prefix in prefixes if client_order_id.startswith(prefix)), None)

    async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
        if self._matching_prefix(order_request.client_order_id, self._create_timeout_prefixes):
            child = ChildOrder(
                child_order_id=order_request.child_order_id,
                client_order_id=order_request.client_order_id,
                symbol=order_request.symbol,
                side=order_request.side,
                submitted_quantity=order_request.quantity,
                price=order_request.price,
                status=ChildOrderStatus.UNKNOWN,
                terminal_reason="SCRIPTED_CREATE_TIMEOUT_PENDING_RECONCILIATION",
            )
            self._orders[order_request.client_order_id] = child
            return child

        if order_request.post_only:
            rules = await self.get_symbol_rules(order_request.symbol)
            if "GTX" not in rules.supported_time_in_force:
                child = ChildOrder(
                    child_order_id=order_request.child_order_id,
                    client_order_id=order_request.client_order_id,
                    symbol=order_request.symbol,
                    side=order_request.side,
                    submitted_quantity=order_request.quantity,
                    price=order_request.price,
                    status=ChildOrderStatus.REJECTED,
                    terminal_reason="POST_ONLY_GTX_UNSUPPORTED",
                )
                self._orders[order_request.client_order_id] = child
                await self._user_event_queue.put(
                    SimulatorOrderEvent(order_request.client_order_id, ChildOrderStatus.REJECTED)
                )
                return child

        child = ChildOrder(
            child_order_id=order_request.child_order_id,
            client_order_id=order_request.client_order_id,
            symbol=order_request.symbol,
            side=order_request.side,
            submitted_quantity=order_request.quantity,
            price=order_request.price,
            status=ChildOrderStatus.OPEN,
        )
        self._orders[order_request.client_order_id] = child
        await self._user_event_queue.put(SimulatorOrderEvent(child.client_order_id, ChildOrderStatus.OPEN))
        return child

    async def cancel_order(self, symbol: str, client_order_id: str) -> ChildOrder:
        child = self._orders[client_order_id]
        if child.status is ChildOrderStatus.FILLED:
            return child
        fill_prefix = self._matching_prefix(client_order_id, self._fill_during_cancel)
        if fill_prefix is not None:
            cumulative = self._fill_during_cancel[fill_prefix]
            child.confirmed_filled_quantity = cumulative
            child.status = ChildOrderStatus.PENDING_CANCEL
            fill = Fill(
                client_order_id=client_order_id,
                trade_id=f"race-{client_order_id}",
                cumulative_filled_quantity=cumulative,
                last_filled_quantity=cumulative,
                last_fill_price=child.price,
                event_time_ms=None,
                transaction_time_ms=None,
            )
            await self._user_event_queue.put(SimulatorOrderEvent(client_order_id, ChildOrderStatus.PENDING_CANCEL, fill=fill))
            return child
        if self._matching_prefix(client_order_id, self._cancel_reconcile_open_prefixes):
            child.status = ChildOrderStatus.PENDING_CANCEL
            await self._user_event_queue.put(SimulatorOrderEvent(client_order_id, ChildOrderStatus.PENDING_CANCEL))
            return child
        child.status = ChildOrderStatus.CANCELLED
        child.terminal_reason = "CANCELLED_BY_ENGINE"
        await self._user_event_queue.put(SimulatorOrderEvent(client_order_id, ChildOrderStatus.CANCELLED))
        return child

    async def push_fill(
        self,
        client_order_id: str,
        trade_id: str,
        cumulative: Decimal,
        price: Decimal,
    ) -> None:
        child = self._orders[client_order_id]
        child.confirmed_filled_quantity = cumulative
        if cumulative >= child.submitted_quantity:
            child.status = ChildOrderStatus.FILLED
            status = ChildOrderStatus.FILLED
        else:
            child.status = ChildOrderStatus.PARTIALLY_FILLED
            status = ChildOrderStatus.PARTIALLY_FILLED
        fill = Fill(
            client_order_id=client_order_id,
            trade_id=trade_id,
            cumulative_filled_quantity=cumulative,
            last_filled_quantity=cumulative,
            last_fill_price=price,
            event_time_ms=None,
            transaction_time_ms=None,
        )
        await self._user_event_queue.put(SimulatorOrderEvent(client_order_id, status, fill=fill))

    async def get_order_by_client_order_id(self, symbol: str, client_order_id: str) -> ChildOrder:
        return self._orders[client_order_id]

    async def next_user_event(self) -> SimulatorOrderEvent:
        return await self._user_event_queue.get()

    async def stream_user_events(self) -> AsyncIterator[SimulatorOrderEvent]:
        while True:
            yield await self._user_event_queue.get()

    async def reconcile_orders_and_fills(
        self,
        symbol: str,
        client_order_prefix: str | None = None,
    ) -> list[ChildOrder]:
        orders = [order for order in self._orders.values() if order.symbol == symbol]
        if client_order_prefix is not None:
            orders = [order for order in orders if order.client_order_id.startswith(client_order_prefix)]
        for order in orders:
            if (
                self._matching_prefix(order.client_order_id, self._cancel_reconcile_open_prefixes)
                and order.status is ChildOrderStatus.PENDING_CANCEL
            ):
                order.status = ChildOrderStatus.OPEN
        return orders
```

- [ ] **Step 4: Run simulator order tests**

Run:

```bash
uv run pytest tests/simulation/test_simulator_orders.py -v
```

Expected: PASS.

- [ ] **Step 5: Run simulator foundation tests too**

Run:

```bash
uv run pytest tests/simulation/test_simulator_market_data.py tests/simulation/test_simulator_orders.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/exchanges/simulator.py tests/simulation/test_simulator_orders.py
git commit -m "feat: add deterministic simulator order lifecycle"
```

---

### Task 8: Execution Engine And Service Basics

**Files:**
- Create: `src/execution/engine.py`
- Create: `src/execution/service.py`
- Test: `tests/unit/test_engine_basics.py`

- [ ] **Step 1: Write failing engine tests for NO_ACTION, cancel idempotency, and event serialization**

Create `tests/unit/test_engine_basics.py`:

```python
from decimal import Decimal

from execution.clock import ManualClock
from execution.models import (
    Algorithm,
    DeadlinePolicy,
    Environment,
    ExecutionParameters,
    ExecutionRequest,
    ExecutionStatus,
)
from execution.service import ExecutionService
from exchanges.simulator import DeterministicSimulator


def request(target: Decimal) -> ExecutionRequest:
    return ExecutionRequest(
        environment=Environment.SIMULATION,
        symbol="BTCUSDT",
        algorithm=Algorithm.CHASE,
        target_position=target,
        target_price_lower=Decimal("90"),
        target_price_upper=Decimal("110"),
        target_duration_seconds=30,
        deadline_policy=DeadlinePolicy.CANCEL_REMAINDER,
        parameters=ExecutionParameters(),
    )


async def test_no_action_target_already_reached() -> None:
    simulator = DeterministicSimulator(clock=ManualClock(), position=Decimal("0.010"))
    service = ExecutionService(adapter=simulator, clock=simulator.clock)

    execution = await service.create_execution(request(Decimal("0.010")))

    assert execution.status is ExecutionStatus.COMPLETED
    assert execution.final_reason == "NO_ACTION_TARGET_ALREADY_REACHED"
    assert execution.child_orders == []


async def test_cancel_is_idempotent_for_completed_execution() -> None:
    simulator = DeterministicSimulator(clock=ManualClock(), position=Decimal("0.010"))
    service = ExecutionService(adapter=simulator, clock=simulator.clock)

    execution = await service.create_execution(request(Decimal("0.010")))
    cancelled = await service.cancel_execution(execution.execution_id)

    assert cancelled.status is ExecutionStatus.COMPLETED
    assert cancelled.final_reason == "NO_ACTION_TARGET_ALREADY_REACHED"
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/unit/test_engine_basics.py -v
```

Expected: FAIL with missing `ExecutionService`.

- [ ] **Step 3: Implement minimal engine/service state**

Create `src/execution/engine.py`:

```python
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from execution.clock import Clock
from execution.events import ExecutionEventActor
from execution.ids import execution_id
from execution.models import (
    ChildOrder,
    ExecutionRequest,
    ExecutionStatus,
    ExecutionSummary,
    Side,
    required_trade,
)
from exchanges.base import ExchangeAdapter


@dataclass
class ExecutionRecord:
    execution_id: str
    request: ExecutionRequest
    status: ExecutionStatus
    side: Side
    required_quantity: Decimal
    initial_position: Decimal
    final_reason: str | None = None
    child_orders: list[ChildOrder] = field(default_factory=list)
    summary: ExecutionSummary | None = None


class ExecutionEngine:
    def __init__(self, adapter: ExchangeAdapter, clock: Clock) -> None:
        self.adapter = adapter
        self.clock = clock
        self._records: dict[str, ExecutionRecord] = {}
        self._actors: dict[str, ExecutionEventActor] = {}

    async def create(self, request: ExecutionRequest) -> ExecutionRecord:
        position = await self.adapter.get_position(request.symbol)
        side, required_quantity = required_trade(request.target_position, position.position)
        record = ExecutionRecord(
            execution_id=execution_id(),
            request=request,
            status=ExecutionStatus.CREATED,
            side=side,
            required_quantity=required_quantity,
            initial_position=position.position,
        )
        self._records[record.execution_id] = record
        self._actors[record.execution_id] = ExecutionEventActor(record.execution_id)

        async def apply_create() -> ExecutionRecord:
            if side is Side.NO_ACTION:
                record.status = ExecutionStatus.COMPLETED
                record.final_reason = "NO_ACTION_TARGET_ALREADY_REACHED"
                record.summary = ExecutionSummary(
                    execution_id=record.execution_id,
                    final_status=record.status,
                    final_reason=record.final_reason,
                    metrics={"child_orders": 0},
                )
                return record
            record.status = ExecutionStatus.RUNNING
            return record

        return await self._actors[record.execution_id].apply(apply_create)

    async def get(self, execution_id_value: str) -> ExecutionRecord:
        return self._records[execution_id_value]

    async def cancel(self, execution_id_value: str) -> ExecutionRecord:
        record = self._records[execution_id_value]
        actor = self._actors[execution_id_value]

        async def apply_cancel() -> ExecutionRecord:
            if record.status is ExecutionStatus.CANCELLING or record.status.is_terminal:
                return record
            record.status = ExecutionStatus.CANCELLING
            record.final_reason = "CANCEL_REQUESTED"
            return record

        return await actor.apply(apply_cancel)
```

Create `src/execution/service.py`:

```python
from __future__ import annotations

from execution.clock import Clock
from execution.engine import ExecutionEngine, ExecutionRecord
from execution.models import ExecutionRequest
from exchanges.base import ExchangeAdapter


class ExecutionService:
    def __init__(self, adapter: ExchangeAdapter, clock: Clock) -> None:
        self.engine = ExecutionEngine(adapter=adapter, clock=clock)

    async def create_execution(self, request: ExecutionRequest) -> ExecutionRecord:
        return await self.engine.create(request)

    async def get_execution(self, execution_id: str) -> ExecutionRecord:
        return await self.engine.get(execution_id)

    async def cancel_execution(self, execution_id: str) -> ExecutionRecord:
        return await self.engine.cancel(execution_id)
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/unit/test_engine_basics.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/execution/engine.py src/execution/service.py tests/unit/test_engine_basics.py
git commit -m "feat: add execution engine service basics"
```

---

### Task 9: Chase Algorithm Demand Calculation

**Files:**
- Create: `src/algorithms/chase.py`
- Test: `tests/unit/test_chase.py`

- [ ] **Step 1: Write failing Chase tests**

Create `tests/unit/test_chase.py`:

```python
from decimal import Decimal

from algorithms.chase import ChaseDecision, chase_desired_price, should_reprice
from execution.models import RepricingMode, Side


def test_chase_buy_defaults_to_best_bid() -> None:
    assert chase_desired_price(Side.BUY, Decimal("100"), Decimal("101"), passive=True) == Decimal("100")


def test_chase_sell_defaults_to_best_ask() -> None:
    assert chase_desired_price(Side.SELL, Decimal("100"), Decimal("101"), passive=True) == Decimal("101")


def test_adverse_only_buy_reprices_only_when_bid_moves_up() -> None:
    decision = should_reprice(
        side=Side.BUY,
        active_order_price=Decimal("100"),
        desired_price=Decimal("100.03"),
        threshold_bps=Decimal("2.0"),
        min_interval_ms=500,
        elapsed_since_last_reprice_ms=600,
        repricing_mode=RepricingMode.ADVERSE_ONLY,
    )

    assert decision is ChaseDecision.REPRICE


def test_adverse_only_buy_does_not_reprice_when_bid_moves_down() -> None:
    decision = should_reprice(
        side=Side.BUY,
        active_order_price=Decimal("100"),
        desired_price=Decimal("99.90"),
        threshold_bps=Decimal("2.0"),
        min_interval_ms=500,
        elapsed_since_last_reprice_ms=600,
        repricing_mode=RepricingMode.ADVERSE_ONLY,
    )

    assert decision is ChaseDecision.WAIT
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/unit/test_chase.py -v
```

Expected: FAIL with missing `algorithms.chase`.

- [ ] **Step 3: Implement Chase helpers**

Create `src/algorithms/chase.py`:

```python
from __future__ import annotations

from decimal import Decimal
from enum import StrEnum

from execution.models import RepricingMode, Side


class ChaseDecision(StrEnum):
    WAIT = "WAIT"
    REPRICE = "REPRICE"


def chase_desired_price(side: Side, best_bid: Decimal, best_ask: Decimal, passive: bool) -> Decimal:
    if not passive:
        return best_ask if side is Side.BUY else best_bid
    return best_bid if side is Side.BUY else best_ask


def reprice_difference_bps(desired_price: Decimal, active_order_price: Decimal) -> Decimal:
    if active_order_price == Decimal("0"):
        return Decimal("0")
    return abs(desired_price / active_order_price - Decimal("1")) * Decimal("10000")


def _is_adverse(side: Side, desired_price: Decimal, active_order_price: Decimal) -> bool:
    if side is Side.BUY:
        return desired_price > active_order_price
    if side is Side.SELL:
        return desired_price < active_order_price
    return False


def should_reprice(
    side: Side,
    active_order_price: Decimal,
    desired_price: Decimal,
    threshold_bps: Decimal,
    min_interval_ms: int,
    elapsed_since_last_reprice_ms: int,
    repricing_mode: RepricingMode,
) -> ChaseDecision:
    if elapsed_since_last_reprice_ms < min_interval_ms:
        return ChaseDecision.WAIT
    if reprice_difference_bps(desired_price, active_order_price) < threshold_bps:
        return ChaseDecision.WAIT
    if repricing_mode is RepricingMode.ADVERSE_ONLY and not _is_adverse(side, desired_price, active_order_price):
        return ChaseDecision.WAIT
    return ChaseDecision.REPRICE
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/unit/test_chase.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/algorithms/chase.py tests/unit/test_chase.py
git commit -m "feat: add chase repricing logic"
```

---

### Task 10: TWAP Scheduling And Safe Child Quantity

**Files:**
- Create: `src/algorithms/twap.py`
- Test: `tests/unit/test_twap.py`

- [ ] **Step 1: Write failing TWAP tests**

Create `tests/unit/test_twap.py`:

```python
from decimal import Decimal

from algorithms.twap import safe_child_quantity, scheduled_cumulative_quantity, scheduled_deficit
from execution.models import Exposure


def test_scheduled_cumulative_quantity_uses_absolute_time_progress() -> None:
    assert scheduled_cumulative_quantity(
        total_trade_quantity=Decimal("1.0"),
        elapsed_time=Decimal("30"),
        total_duration=Decimal("120"),
    ) == Decimal("0.25")


def test_scheduled_deficit_subtracts_confirmed_fills() -> None:
    assert scheduled_deficit(Decimal("0.25"), Decimal("0.10")) == Decimal("0.15")


def test_safe_child_quantity_subtracts_reserved_exposure() -> None:
    exposure = Exposure(
        live_open_quantity=Decimal("0.02"),
        pending_cancel_quantity=Decimal("0.03"),
        unknown_order_quantity=Decimal("0.01"),
    )

    assert safe_child_quantity(Decimal("0.15"), exposure) == Decimal("0.09")


def test_safe_child_quantity_never_negative() -> None:
    exposure = Exposure(live_open_quantity=Decimal("0.20"))

    assert safe_child_quantity(Decimal("0.15"), exposure) == Decimal("0")
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/unit/test_twap.py -v
```

Expected: FAIL with missing `algorithms.twap`.

- [ ] **Step 3: Implement TWAP helpers**

Create `src/algorithms/twap.py`:

```python
from __future__ import annotations

from decimal import Decimal

from execution.models import Exposure


def scheduled_cumulative_quantity(
    total_trade_quantity: Decimal,
    elapsed_time: Decimal,
    total_duration: Decimal,
) -> Decimal:
    if total_duration <= Decimal("0"):
        raise ValueError("total_duration must be positive")
    capped_elapsed = min(max(elapsed_time, Decimal("0")), total_duration)
    return total_trade_quantity * capped_elapsed / total_duration


def scheduled_deficit(
    scheduled_cumulative: Decimal,
    confirmed_cumulative_filled: Decimal,
) -> Decimal:
    deficit = scheduled_cumulative - confirmed_cumulative_filled
    return deficit if deficit > Decimal("0") else Decimal("0")


def safe_child_quantity(deficit: Decimal, exposure: Exposure) -> Decimal:
    quantity = deficit - exposure.reserved_exposure
    return quantity if quantity > Decimal("0") else Decimal("0")
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/unit/test_twap.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/algorithms/twap.py tests/unit/test_twap.py
git commit -m "feat: add twap schedule math"
```

---

### Task 11: Engine Order Submission, Exposure Buckets, And Fill Deduplication

**Files:**
- Modify: `src/execution/engine.py`
- Test: `tests/unit/test_engine_exposure.py`

- [ ] **Step 1: Write failing exposure tests**

Create `tests/unit/test_engine_exposure.py`:

```python
from decimal import Decimal

import pytest

from execution.engine import ExposureTracker
from risk.validation import ValidationError


def test_unknown_create_exposure_is_reserved_until_reconciled() -> None:
    tracker = ExposureTracker(target_quantity=Decimal("0.010"))

    tracker.reserve_unknown_create(Decimal("0.004"))

    assert tracker.exposure.unknown_order_quantity == Decimal("0.004")
    assert tracker.available_to_submit() == Decimal("0.006")


def test_ambiguous_cancel_keeps_pending_cancel_reserved() -> None:
    tracker = ExposureTracker(target_quantity=Decimal("0.010"))

    tracker.reserve_live_open(Decimal("0.004"))
    tracker.mark_pending_cancel(Decimal("0.004"))

    assert tracker.exposure.live_open_quantity == Decimal("0")
    assert tracker.exposure.pending_cancel_quantity == Decimal("0.004")


def test_overfill_invariant_rejects_new_child_quantity() -> None:
    tracker = ExposureTracker(target_quantity=Decimal("0.010"))
    tracker.exposure.confirmed_filled_quantity = Decimal("0.008")
    tracker.reserve_live_open(Decimal("0.002"))

    with pytest.raises(ValidationError):
        tracker.check_can_submit(Decimal("0.001"))


def test_duplicate_trade_id_not_counted_twice() -> None:
    tracker = ExposureTracker(target_quantity=Decimal("0.010"))

    tracker.apply_fill(trade_id="t1", cumulative=Decimal("0.003"))
    tracker.apply_fill(trade_id="t1", cumulative=Decimal("0.003"))

    assert tracker.exposure.confirmed_filled_quantity == Decimal("0.003")
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/unit/test_engine_exposure.py -v
```

Expected: FAIL with missing `ExposureTracker`.

- [ ] **Step 3: Add exposure tracker to `src/execution/engine.py`**

Append this class above `ExecutionEngine`:

```python
from risk.validation import check_exposure_invariant


@dataclass
class ExposureTracker:
    target_quantity: Decimal
    exposure: Exposure = field(default_factory=Exposure)
    seen_trade_ids: set[str] = field(default_factory=set)

    def available_to_submit(self) -> Decimal:
        available = self.target_quantity - self.exposure.confirmed_filled_quantity - self.exposure.reserved_exposure
        return available if available > Decimal("0") else Decimal("0")

    def check_can_submit(self, new_child_quantity: Decimal) -> None:
        check_exposure_invariant(self.exposure, new_child_quantity, self.target_quantity)

    def reserve_live_open(self, quantity: Decimal) -> None:
        self.check_can_submit(quantity)
        self.exposure.live_open_quantity += quantity

    def reserve_unknown_create(self, quantity: Decimal) -> None:
        self.check_can_submit(quantity)
        self.exposure.unknown_order_quantity += quantity

    def mark_pending_cancel(self, quantity: Decimal) -> None:
        self.exposure.live_open_quantity -= quantity
        if self.exposure.live_open_quantity < Decimal("0"):
            self.exposure.live_open_quantity = Decimal("0")
        self.exposure.pending_cancel_quantity += quantity

    def apply_fill(self, trade_id: str | None, cumulative: Decimal) -> None:
        if trade_id is not None:
            if trade_id in self.seen_trade_ids:
                return
            self.seen_trade_ids.add(trade_id)
        if cumulative > self.exposure.confirmed_filled_quantity:
            self.exposure.confirmed_filled_quantity = cumulative
```

Update the import list in `src/execution/engine.py` to include `Exposure` from `execution.models`.

- [ ] **Step 4: Run exposure tests**

Run:

```bash
uv run pytest tests/unit/test_engine_exposure.py -v
```

Expected: PASS.

- [ ] **Step 5: Run existing engine/model tests**

Run:

```bash
uv run pytest tests/unit/test_engine_basics.py tests/unit/test_models.py tests/unit/test_validation.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/execution/engine.py tests/unit/test_engine_exposure.py
git commit -m "feat: add exposure reservation tracking"
```

---

### Task 12: Engine Child-Order Lifecycle Loop

**Files:**
- Modify: `src/execution/engine.py`
- Modify: `src/execution/service.py`
- Test: `tests/unit/test_engine_lifecycle.py`

- [ ] **Step 1: Write failing lifecycle tests for algorithm dispatch, submit safety, and reconciliation**

Create `tests/unit/test_engine_lifecycle.py`:

```python
from decimal import Decimal

from execution.clock import ManualClock
from execution.models import Algorithm, DeadlinePolicy, Environment, ExecutionRequest, ExecutionStatus
from execution.service import ExecutionService
from exchanges.simulator import DeterministicSimulator


def request(algorithm: Algorithm, target: Decimal = Decimal("0.010")) -> ExecutionRequest:
    return ExecutionRequest(
        environment=Environment.SIMULATION,
        symbol="BTCUSDT",
        algorithm=algorithm,
        target_position=target,
        target_price_lower=Decimal("90"),
        target_price_upper=Decimal("110"),
        target_duration_seconds=30,
        deadline_policy=DeadlinePolicy.CANCEL_REMAINDER,
    )


async def test_chase_run_once_submits_one_safe_child_order() -> None:
    clock = ManualClock()
    simulator = DeterministicSimulator(clock=clock, position=Decimal("0"))
    await simulator.push_market_data("BTCUSDT", Decimal("100"), Decimal("101"))
    service = ExecutionService(adapter=simulator, clock=clock)
    execution = await service.create_execution(request(Algorithm.CHASE))

    execution = await service.run_once(execution.execution_id)

    assert execution.status is ExecutionStatus.RUNNING
    assert len(execution.child_orders) == 1
    assert execution.exposure.live_open_quantity <= execution.required_quantity


async def test_create_timeout_keeps_unknown_exposure_until_reconciled() -> None:
    clock = ManualClock()
    simulator = DeterministicSimulator(clock=clock, position=Decimal("0"))
    simulator.script_create_timeout(client_order_prefix="ce_")
    await simulator.push_market_data("BTCUSDT", Decimal("100"), Decimal("101"))
    service = ExecutionService(adapter=simulator, clock=clock)
    execution = await service.create_execution(request(Algorithm.CHASE))

    execution = await service.run_once(execution.execution_id)

    assert execution.exposure.unknown_order_quantity > Decimal("0")
    assert execution.exposure.live_open_quantity == Decimal("0")


async def test_cancel_requests_active_order_cancel_without_dropping_exposure() -> None:
    clock = ManualClock()
    simulator = DeterministicSimulator(clock=clock, position=Decimal("0"))
    simulator.script_fill_during_cancel(client_order_prefix="ce_", fill_quantity=Decimal("0.004"))
    await simulator.push_market_data("BTCUSDT", Decimal("100"), Decimal("101"))
    service = ExecutionService(adapter=simulator, clock=clock)
    execution = await service.create_execution(request(Algorithm.CHASE))
    await service.run_once(execution.execution_id)

    cancelled = await service.cancel_execution(execution.execution_id)

    assert cancelled.status in {ExecutionStatus.CANCELLING, ExecutionStatus.PARTIALLY_COMPLETED}
    assert cancelled.exposure.confirmed_filled_quantity <= cancelled.required_quantity


async def test_ambiguous_cancel_reconciled_open_keeps_live_exposure_and_no_replacement() -> None:
    clock = ManualClock()
    simulator = DeterministicSimulator(clock=clock, position=Decimal("0"))
    simulator.script_cancel_reconcile_open(client_order_prefix="ce_")
    await simulator.push_market_data("BTCUSDT", Decimal("100"), Decimal("101"))
    service = ExecutionService(adapter=simulator, clock=clock)
    execution = await service.create_execution(request(Algorithm.CHASE))
    execution = await service.run_once(execution.execution_id)
    original_client_order_id = execution.child_orders[0].client_order_id

    await service.cancel_execution(execution.execution_id)
    reconciled = await service.reconcile_execution(execution.execution_id)
    after_run = await service.run_once(execution.execution_id)

    assert reconciled.exposure.live_open_quantity > Decimal("0")
    assert reconciled.exposure.pending_cancel_quantity == Decimal("0")
    assert len(after_run.child_orders) == 1
    assert after_run.child_orders[0].client_order_id == original_client_order_id


async def test_twap_run_once_uses_schedule_deficit_not_equal_sleep_slices() -> None:
    clock = ManualClock()
    simulator = DeterministicSimulator(clock=clock, position=Decimal("0"))
    await simulator.push_market_data("BTCUSDT", Decimal("100"), Decimal("101"))
    service = ExecutionService(adapter=simulator, clock=clock)
    execution = await service.create_execution(request(Algorithm.TWAP))

    clock.advance(15)
    execution = await service.run_once(execution.execution_id)

    assert execution.child_orders
    assert execution.child_orders[0].submitted_quantity <= Decimal("0.005")
```

- [ ] **Step 2: Run lifecycle tests to verify they fail**

Run:

```bash
uv run pytest tests/unit/test_engine_lifecycle.py -v
```

Expected: FAIL with missing `run_once`, `ExecutionRecord.exposure`, or lifecycle wiring.

- [ ] **Step 3: Extend execution records with lifecycle fields**

Modify `ExecutionRecord` in `src/execution/engine.py`:

```python
@dataclass
class ExecutionRecord:
    execution_id: str
    request: ExecutionRequest
    status: ExecutionStatus
    side: Side
    required_quantity: Decimal
    initial_position: Decimal
    final_reason: str | None = None
    child_orders: list[ChildOrder] = field(default_factory=list)
    exposure_tracker: ExposureTracker | None = None
    last_child_sequence: int = 0
    started_monotonic: Decimal = Decimal("0")
    last_reprice_monotonic: Decimal | None = None
    summary: ExecutionSummary | None = None

    @property
    def exposure(self) -> Exposure:
        if self.exposure_tracker is None:
            return Exposure()
        return self.exposure_tracker.exposure
```

When creating a non-`NO_ACTION` record, initialize:

```python
record.exposure_tracker = ExposureTracker(target_quantity=required_quantity)
record.started_monotonic = Decimal(str(self.clock.monotonic()))
record.status = ExecutionStatus.RUNNING
```

- [ ] **Step 4: Add service facade methods**

Modify `src/execution/service.py`:

```python
    async def run_once(self, execution_id: str) -> ExecutionRecord:
        return await self.engine.run_once(execution_id)

    async def reconcile_execution(self, execution_id: str) -> ExecutionRecord:
        return await self.engine.reconcile(execution_id)
```

- [ ] **Step 5: Implement `ExecutionEngine.run_once` with one active child order by default**

Add this outline to `ExecutionEngine`. Keep all state mutation inside the per-execution actor:

Update imports in `src/execution/engine.py`:

```python
from algorithms.chase import chase_desired_price
from algorithms.twap import safe_child_quantity, scheduled_cumulative_quantity, scheduled_deficit
from execution.ids import make_client_order_id, make_client_order_prefix
from risk.decimal_math import floor_to_step, round_price
from risk.validation import validate_order_shape, validate_price_bounds
```

```python
    async def run_once(self, execution_id_value: str) -> ExecutionRecord:
        record = self._records[execution_id_value]
        actor = self._actors[execution_id_value]

        async def apply_run() -> ExecutionRecord:
            if record.status.is_terminal or record.status is ExecutionStatus.CANCELLING:
                return record
            await self._reconcile_locked(record)
            if record.exposure_tracker is None:
                return record
            if record.exposure.confirmed_filled_quantity >= record.required_quantity:
                return self._complete_locked(record, "TARGET_QUANTITY_FILLED")
            if record.exposure.reserved_exposure > Decimal("0"):
                return record

            quantity, price, post_only = await self._build_child_demand_locked(record)
            if quantity <= Decimal("0"):
                return record
            await self._submit_child_locked(record, quantity=quantity, price=price, post_only=post_only)
            return record

        return await actor.apply(apply_run)
```

Implement `_build_child_demand_locked` so both algorithms share the same submit path:

```python
    async def _build_child_demand_locked(self, record: ExecutionRecord) -> tuple[Decimal, Decimal, bool]:
        market = await self.adapter.get_best_bid_ask(record.request.symbol)
        rules = await self.adapter.get_symbol_rules(record.request.symbol)
        available = record.exposure_tracker.available_to_submit()

        if record.request.algorithm is Algorithm.CHASE:
            price = chase_desired_price(record.side, market.bid, market.ask, passive=True)
            quantity = available
            post_only = True
        else:
            elapsed = Decimal(str(self.clock.monotonic())) - record.started_monotonic
            scheduled = scheduled_cumulative_quantity(
                total_trade_quantity=record.required_quantity,
                elapsed_time=elapsed,
                total_duration=Decimal(str(record.request.target_duration_seconds)),
            )
            deficit = scheduled_deficit(scheduled, record.exposure.confirmed_filled_quantity)
            quantity = safe_child_quantity(deficit, record.exposure)
            price = market.bid if record.side is Side.BUY else market.ask
            post_only = True

        quantity = floor_to_step(quantity, rules.quantity_step)
        price = round_price(price, rules.tick_size, record.side, passive=post_only)
        if quantity <= Decimal("0"):
            return Decimal("0"), price, post_only
        validate_price_bounds(record.side, price, record.request.target_price_lower, record.request.target_price_upper)
        validate_order_shape(
            quantity=quantity,
            price=price,
            side=record.side,
            rules=rules,
            best_bid=market.bid,
            best_ask=market.ask,
            post_only=post_only,
        )
        return quantity, price, post_only
```

Important behavior:

```text
Initial implementation uses a single active child order per execution.
TWAP can still reserve open exposure, so safe_child_quantity subtracts live, pending, and unknown exposure.
Trading validation runs again before every child order because market data, rules, and exposure can change.
```

- [ ] **Step 6: Implement submit, cancel, and reconciliation transitions**

Add `_submit_child_locked`:

```python
    async def _submit_child_locked(
        self,
        record: ExecutionRecord,
        quantity: Decimal,
        price: Decimal,
        post_only: bool,
    ) -> None:
        record.exposure_tracker.check_can_submit(quantity)
        record.last_child_sequence += 1
        child_order_id = f"child_{record.last_child_sequence:04d}"
        client_order_id = make_client_order_id(record.execution_id, record.last_child_sequence)
        order_request = OrderRequest(
            execution_id=record.execution_id,
            child_order_id=child_order_id,
            client_order_id=client_order_id,
            symbol=record.request.symbol,
            side=record.side,
            quantity=quantity,
            price=price,
            post_only=post_only,
        )
        child = ChildOrder(
            child_order_id=child_order_id,
            client_order_id=client_order_id,
            symbol=record.request.symbol,
            side=record.side,
            submitted_quantity=quantity,
            price=price,
            status=ChildOrderStatus.PENDING_SUBMIT,
        )
        record.child_orders.append(child)
        record.exposure.pending_submit_quantity += quantity

        submitted = await self.adapter.submit_limit_order(order_request)
        record.exposure.pending_submit_quantity -= quantity
        child.status = submitted.status
        if submitted.status is ChildOrderStatus.UNKNOWN:
            record.exposure_tracker.reserve_unknown_create(quantity)
        elif submitted.status in {ChildOrderStatus.OPEN, ChildOrderStatus.PARTIALLY_FILLED}:
            record.exposure_tracker.reserve_live_open(quantity - submitted.confirmed_filled_quantity)
        elif submitted.status is ChildOrderStatus.FILLED:
            record.exposure_tracker.apply_fill(None, submitted.confirmed_filled_quantity)
```

Update `cancel()` to request cancellation for active execution-scoped orders. Repeated cancel requests are idempotent:

```python
    async def _cancel_active_children_locked(self, record: ExecutionRecord) -> None:
        for child in record.child_orders:
            if child.status not in {ChildOrderStatus.OPEN, ChildOrderStatus.PARTIALLY_FILLED}:
                continue
            open_quantity = child.submitted_quantity - child.confirmed_filled_quantity
            record.exposure_tracker.mark_pending_cancel(open_quantity)
            result = await self.adapter.cancel_order(record.request.symbol, child.client_order_id)
            child.status = result.status
            child.confirmed_filled_quantity = result.confirmed_filled_quantity
            if result.status is ChildOrderStatus.FILLED:
                record.exposure_tracker.apply_fill(None, result.confirmed_filled_quantity)
            elif result.status is ChildOrderStatus.CANCELLED:
                record.exposure.pending_cancel_quantity -= open_quantity
```

Add `_reconcile_locked`:

```python
    async def _reconcile_locked(self, record: ExecutionRecord) -> None:
        prefix = make_client_order_prefix(record.execution_id)
        reconciled = await self.adapter.reconcile_orders_and_fills(record.request.symbol, client_order_prefix=prefix)
        for exchange_child in reconciled:
            local = next((child for child in record.child_orders if child.client_order_id == exchange_child.client_order_id), None)
            if local is None:
                continue
            previous_status = local.status
            local.status = exchange_child.status
            local.confirmed_filled_quantity = exchange_child.confirmed_filled_quantity
            if exchange_child.status is ChildOrderStatus.FILLED:
                record.exposure_tracker.apply_fill(None, exchange_child.confirmed_filled_quantity)
            elif exchange_child.status in {ChildOrderStatus.OPEN, ChildOrderStatus.PARTIALLY_FILLED}:
                open_quantity = exchange_child.submitted_quantity - exchange_child.confirmed_filled_quantity
                if previous_status is ChildOrderStatus.PENDING_CANCEL or record.exposure.pending_cancel_quantity > Decimal("0"):
                    record.exposure.pending_cancel_quantity = Decimal("0")
                    record.exposure.live_open_quantity = open_quantity
```

- [ ] **Step 7: Run lifecycle and exposure tests**

Run:

```bash
uv run pytest tests/unit/test_engine_lifecycle.py tests/unit/test_engine_exposure.py tests/unit/test_engine_basics.py -v
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/execution/engine.py src/execution/service.py tests/unit/test_engine_lifecycle.py
git commit -m "feat: wire engine child order lifecycle"
```

---

### Task 13: FastAPI Schemas And Service Endpoints

**Files:**
- Create: `src/api/schemas.py`
- Create: `src/api/app.py`
- Test: `tests/unit/test_api.py`

- [ ] **Step 1: Write failing API tests**

Create `tests/unit/test_api.py`:

```python
import pytest
from fastapi.testclient import TestClient

from api.app import create_app


def test_create_execution_no_action() -> None:
    app = create_app(simulator_position="0.010")
    client = TestClient(app)

    response = client.post(
        "/executions",
        json={
            "environment": "simulation",
            "symbol": "BTCUSDT",
            "algorithm": "CHASE",
            "target_position": "0.010",
            "target_price_lower": "94000",
            "target_price_upper": "97000",
            "target_duration_seconds": 300,
            "deadline_policy": "CANCEL_REMAINDER",
            "parameters": {
                "reprice_threshold_bps": "2.0",
                "minimum_reprice_interval_ms": 500,
                "number_of_slices": 10,
                "child_order_timeout_seconds": 20,
                "repricing_mode": "ADVERSE_ONLY",
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "COMPLETED"
    assert body["final_reason"] == "NO_ACTION_TARGET_ALREADY_REACHED"
    assert body["child_orders"] == []


def test_cancel_terminal_execution_is_idempotent() -> None:
    app = create_app(simulator_position="0.010")
    client = TestClient(app)

    created = client.post(
        "/executions",
        json={
            "environment": "simulation",
            "symbol": "BTCUSDT",
            "algorithm": "CHASE",
            "target_position": "0.010",
            "target_price_lower": "94000",
            "target_price_upper": "97000",
            "target_duration_seconds": 300,
            "deadline_policy": "CANCEL_REMAINDER",
            "parameters": {},
        },
    ).json()

    response = client.post(f"/executions/{created['execution_id']}/cancel")

    assert response.status_code == 200
    assert response.json()["status"] == "COMPLETED"


def valid_payload() -> dict:
    return {
        "environment": "simulation",
        "symbol": "BTCUSDT",
        "algorithm": "CHASE",
        "target_position": "0.010",
        "target_price_lower": "94000",
        "target_price_upper": "97000",
        "target_duration_seconds": 300,
        "deadline_policy": "CANCEL_REMAINDER",
        "parameters": {"reprice_threshold_bps": "2.0"},
    }


@pytest.mark.parametrize(
    ("field_path", "numeric_value"),
    [
        (("target_position",), 0.010),
        (("target_price_lower",), 94000.0),
        (("target_price_upper",), 97000.0),
        (("parameters", "reprice_threshold_bps"), 2.0),
    ],
)
def test_api_rejects_json_float_decimal_fields(field_path: tuple[str, ...], numeric_value: float) -> None:
    app = create_app(simulator_position="0")
    client = TestClient(app)
    payload = valid_payload()
    target = payload
    for key in field_path[:-1]:
        target = target[key]
    target[field_path[-1]] = numeric_value

    response = client.post(
        "/executions",
        json=payload,
    )

    assert response.status_code == 422
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/unit/test_api.py -v
```

Expected: FAIL with missing API app.

- [ ] **Step 3: Implement API schemas**

Create `src/api/schemas.py`:

```python
from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field, field_validator

from execution.models import Algorithm, DeadlinePolicy, Environment, ExecutionParameters, ExecutionRequest, RepricingMode


class ParametersIn(BaseModel):
    reprice_threshold_bps: str = "2.0"
    minimum_reprice_interval_ms: int = 500
    number_of_slices: int = 10
    child_order_timeout_seconds: int = 20
    repricing_mode: RepricingMode = RepricingMode.ADVERSE_ONLY

    @field_validator("reprice_threshold_bps", mode="before")
    @classmethod
    def decimal_parameter_must_be_string(cls, value: object) -> str:
        if not isinstance(value, str):
            raise ValueError("decimal parameters must be JSON strings")
        Decimal(value)
        return value

    def to_domain(self) -> ExecutionParameters:
        return ExecutionParameters(
            reprice_threshold_bps=Decimal(self.reprice_threshold_bps),
            minimum_reprice_interval_ms=self.minimum_reprice_interval_ms,
            number_of_slices=self.number_of_slices,
            child_order_timeout_seconds=self.child_order_timeout_seconds,
            repricing_mode=self.repricing_mode,
        )


class ExecutionRequestIn(BaseModel):
    environment: Environment
    symbol: str
    algorithm: Algorithm
    target_position: str
    target_price_lower: str
    target_price_upper: str
    target_duration_seconds: int = Field(gt=0)
    deadline_policy: DeadlinePolicy
    parameters: ParametersIn = Field(default_factory=ParametersIn)

    @field_validator("target_position", "target_price_lower", "target_price_upper", mode="before")
    @classmethod
    def decimal_string(cls, value: object) -> str:
        if not isinstance(value, str):
            raise ValueError("decimal fields must be JSON strings")
        Decimal(value)
        return value

    def to_domain(self) -> ExecutionRequest:
        lower = Decimal(self.target_price_lower)
        upper = Decimal(self.target_price_upper)
        if lower > upper:
            raise ValueError("target_price_lower cannot exceed target_price_upper")
        return ExecutionRequest(
            environment=self.environment,
            symbol=self.symbol,
            algorithm=self.algorithm,
            target_position=Decimal(self.target_position),
            target_price_lower=lower,
            target_price_upper=upper,
            target_duration_seconds=self.target_duration_seconds,
            deadline_policy=self.deadline_policy,
            parameters=self.parameters.to_domain(),
        )


class ExecutionOut(BaseModel):
    execution_id: str
    status: str
    final_reason: str | None
    child_orders: list[dict[str, str]]
```

- [ ] **Step 4: Implement FastAPI app**

Create `src/api/app.py`:

```python
from __future__ import annotations

from decimal import Decimal

from fastapi import FastAPI, HTTPException

from api.schemas import ExecutionOut, ExecutionRequestIn
from execution.clock import ManualClock
from execution.engine import ExecutionRecord
from execution.service import ExecutionService
from exchanges.simulator import DeterministicSimulator


def _to_out(record: ExecutionRecord) -> ExecutionOut:
    return ExecutionOut(
        execution_id=record.execution_id,
        status=record.status.value,
        final_reason=record.final_reason,
        child_orders=[
            {
                "child_order_id": child.child_order_id,
                "client_order_id": child.client_order_id,
                "status": child.status.value,
            }
            for child in record.child_orders
        ],
    )


def create_app(simulator_position: str = "0") -> FastAPI:
    clock = ManualClock()
    adapter = DeterministicSimulator(clock=clock, position=Decimal(simulator_position))
    service = ExecutionService(adapter=adapter, clock=clock)
    app = FastAPI(title="Calais Execution Algorithm")

    @app.post("/executions", response_model=ExecutionOut)
    async def create_execution(payload: ExecutionRequestIn) -> ExecutionOut:
        record = await service.create_execution(payload.to_domain())
        return _to_out(record)

    @app.get("/executions/{execution_id}", response_model=ExecutionOut)
    async def get_execution(execution_id: str) -> ExecutionOut:
        try:
            return _to_out(await service.get_execution(execution_id))
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="execution not found") from exc

    @app.post("/executions/{execution_id}/cancel", response_model=ExecutionOut)
    async def cancel_execution(execution_id: str) -> ExecutionOut:
        try:
            return _to_out(await service.cancel_execution(execution_id))
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="execution not found") from exc

    return app


app = create_app()
```

- [ ] **Step 5: Run API tests**

Run:

```bash
uv run pytest tests/unit/test_api.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/api/schemas.py src/api/app.py tests/unit/test_api.py
git commit -m "feat: add execution api endpoints"
```

---

### Task 14: Simulator Scenario Coverage And Demo Scripts

**Files:**
- Create: `tests/simulation/test_required_scenarios.py`
- Create: `scripts/run_sim_chase.py`
- Create: `scripts/run_sim_twap.py`
- Create: `scripts/run_sim_cancel_race.py`
- Create: `scripts/run_sim_create_timeout.py`

- [ ] **Step 1: Write scenario tests that initially fail against the minimal engine**

Create `tests/simulation/test_required_scenarios.py`:

```python
from decimal import Decimal
import subprocess
import sys

import pytest

from execution.clock import ManualClock
from execution.models import (
    Algorithm,
    DeadlinePolicy,
    Environment,
    ExecutionParameters,
    ExecutionRequest,
    ExecutionStatus,
)
from execution.service import ExecutionService
from exchanges.simulator import DeterministicSimulator


def make_request(algorithm: Algorithm, target: Decimal = Decimal("0.010")) -> ExecutionRequest:
    return ExecutionRequest(
        environment=Environment.SIMULATION,
        symbol="BTCUSDT",
        algorithm=algorithm,
        target_position=target,
        target_price_lower=Decimal("90"),
        target_price_upper=Decimal("110"),
        target_duration_seconds=30,
        deadline_policy=DeadlinePolicy.CANCEL_REMAINDER,
        parameters=ExecutionParameters(
            reprice_threshold_bps=Decimal("2.0"),
            minimum_reprice_interval_ms=500,
            number_of_slices=3,
            child_order_timeout_seconds=2,
        ),
    )


async def test_t1_normal_chase_produces_terminal_or_running_state_with_no_overfill() -> None:
    clock = ManualClock()
    simulator = DeterministicSimulator(clock=clock, position=Decimal("0"))
    await simulator.push_market_data("BTCUSDT", Decimal("100"), Decimal("101"))
    service = ExecutionService(adapter=simulator, clock=clock)

    execution = await service.create_execution(make_request(Algorithm.CHASE))
    execution = await service.run_once(execution.execution_id)

    assert execution.status is ExecutionStatus.RUNNING
    assert execution.required_quantity == Decimal("0.010")
    assert len(execution.child_orders) == 1
    assert execution.child_orders[0].price == Decimal("100")
    assert execution.exposure.reserved_exposure <= execution.required_quantity


async def test_t7_price_outside_range_does_not_complete_falsely() -> None:
    clock = ManualClock()
    simulator = DeterministicSimulator(clock=clock, position=Decimal("0"))
    await simulator.push_market_data("BTCUSDT", Decimal("120"), Decimal("121"))
    service = ExecutionService(adapter=simulator, clock=clock)

    execution = await service.create_execution(make_request(Algorithm.CHASE))

    assert execution.status is ExecutionStatus.EXPIRED
    assert execution.final_reason == "PRICE_OUTSIDE_RANGE"
    assert execution.child_orders == []


async def test_t10_cross_zero_position_computes_required_quantity() -> None:
    clock = ManualClock()
    simulator = DeterministicSimulator(clock=clock, position=Decimal("-0.003"))
    await simulator.push_market_data("BTCUSDT", Decimal("100"), Decimal("101"))
    service = ExecutionService(adapter=simulator, clock=clock)

    execution = await service.create_execution(make_request(Algorithm.CHASE, target=Decimal("0.005")))

    assert execution.required_quantity == Decimal("0.008")


def test_cancel_race_demo_writes_required_artifacts(tmp_path) -> None:
    output_root = tmp_path / "outputs"

    result = subprocess.run(
        [sys.executable, "scripts/run_sim_cancel_race.py", "--output-dir", str(output_root)],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    execution_dirs = list(output_root.iterdir())
    assert len(execution_dirs) == 1
    output_dir = execution_dirs[0]
    assert (output_dir / "request_snapshot.json").exists()
    assert (output_dir / "execution_log.jsonl").exists()
    assert (output_dir / "execution_summary.json").exists()
    assert (output_dir / "child_orders.csv").exists()
    assert (output_dir / "fills.csv").exists()
    assert (output_dir / "timeline.csv").exists()
```

- [ ] **Step 2: Run scenario tests and record current failures**

Run:

```bash
uv run pytest tests/simulation/test_required_scenarios.py -v
```

Expected: At least T1/T7 may fail until engine submit logic is connected to Chase/TWAP. Keep the failure output for `reports/failure_case_log.md` if it becomes the real development failure case.

- [ ] **Step 3: Extend engine just enough to create one safe child order for running executions**

Modify `ExecutionEngine.create` so non-`NO_ACTION` executions:

```python
await self.adapter.get_best_bid_ask(request.symbol)
record.status = ExecutionStatus.RUNNING
```

Do not submit a child order yet if price bounds fail or no fresh market data exists. If the market is outside range, set:

```python
record.status = ExecutionStatus.EXPIRED
record.final_reason = "PRICE_OUTSIDE_RANGE"
```

This makes the price-range behavior explicit before full algorithm execution is added.

- [ ] **Step 4: Run scenario tests again**

Run:

```bash
uv run pytest tests/simulation/test_required_scenarios.py -v
```

Expected: PASS for the three smoke scenarios.

- [ ] **Step 5: Add clearly labeled simulator scripts**

Create `scripts/run_sim_chase.py`:

```python
import asyncio
from decimal import Decimal

from execution.clock import ManualClock
from execution.models import Algorithm, DeadlinePolicy, Environment, ExecutionRequest
from execution.service import ExecutionService
from exchanges.simulator import DeterministicSimulator


async def main() -> None:
    print("SIMULATOR DEMO: Chase")
    clock = ManualClock()
    simulator = DeterministicSimulator(clock=clock, position=Decimal("0"))
    await simulator.push_market_data("BTCUSDT", Decimal("100"), Decimal("101"))
    service = ExecutionService(adapter=simulator, clock=clock)
    execution = await service.create_execution(
        ExecutionRequest(
            environment=Environment.SIMULATION,
            symbol="BTCUSDT",
            algorithm=Algorithm.CHASE,
            target_position=Decimal("0.010"),
            target_price_lower=Decimal("90"),
            target_price_upper=Decimal("110"),
            target_duration_seconds=30,
            deadline_policy=DeadlinePolicy.CANCEL_REMAINDER,
        )
    )
    print(execution)


if __name__ == "__main__":
    asyncio.run(main())
```

Create `scripts/run_sim_twap.py`:

```python
import asyncio
from decimal import Decimal

from execution.clock import ManualClock
from execution.models import Algorithm, DeadlinePolicy, Environment, ExecutionParameters, ExecutionRequest
from execution.service import ExecutionService
from exchanges.simulator import DeterministicSimulator


async def main() -> None:
    print("SIMULATOR DEMO: TWAP")
    clock = ManualClock()
    simulator = DeterministicSimulator(clock=clock, position=Decimal("0"))
    await simulator.push_market_data("BTCUSDT", Decimal("100"), Decimal("101"))
    service = ExecutionService(adapter=simulator, clock=clock)
    execution = await service.create_execution(
        ExecutionRequest(
            environment=Environment.SIMULATION,
            symbol="BTCUSDT",
            algorithm=Algorithm.TWAP,
            target_position=Decimal("0.010"),
            target_price_lower=Decimal("90"),
            target_price_upper=Decimal("110"),
            target_duration_seconds=30,
            deadline_policy=DeadlinePolicy.CANCEL_REMAINDER,
            parameters=ExecutionParameters(number_of_slices=3),
        )
    )
    print(execution)


if __name__ == "__main__":
    asyncio.run(main())
```

Create `scripts/run_sim_cancel_race.py`:

```python
import argparse
import asyncio
from decimal import Decimal
from pathlib import Path

from execution.clock import ManualClock
from execution.models import Algorithm, DeadlinePolicy, Environment, ExecutionRequest
from execution.service import ExecutionService
from exchanges.simulator import DeterministicSimulator
from observability.artifacts import write_execution_artifacts


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=Path("outputs"))
    args = parser.parse_args()

    print("SIMULATOR DEMO: Cancel/fill race")
    clock = ManualClock()
    simulator = DeterministicSimulator(clock=clock, position=Decimal("0"))
    simulator.script_fill_during_cancel(client_order_prefix="ce_", fill_quantity=Decimal("0.004"))
    await simulator.push_market_data("BTCUSDT", Decimal("100"), Decimal("101"))
    service = ExecutionService(adapter=simulator, clock=clock)
    request = ExecutionRequest(
        environment=Environment.SIMULATION,
        symbol="BTCUSDT",
        algorithm=Algorithm.CHASE,
        target_position=Decimal("0.010"),
        target_price_lower=Decimal("90"),
        target_price_upper=Decimal("110"),
        target_duration_seconds=30,
        deadline_policy=DeadlinePolicy.CANCEL_REMAINDER,
    )
    execution = await service.create_execution(request)
    execution = await service.run_once(execution.execution_id)
    execution = await service.cancel_execution(execution.execution_id)
    write_execution_artifacts(
        root=args.output_dir,
        execution_id=execution.execution_id,
        request_snapshot={
            "symbol": request.symbol,
            "algorithm": request.algorithm.value,
            "target_position": str(request.target_position),
        },
        log_events=[
            {
                "execution_id": execution.execution_id,
                "event": "cancel_fill_race",
                "client_order_id": child.client_order_id,
                "status": child.status.value,
            }
            for child in execution.child_orders
        ],
        summary={
            "execution_id": execution.execution_id,
            "final_status": execution.status.value,
            "final_reason": execution.final_reason,
        },
        child_orders=[
            {
                "child_order_id": child.child_order_id,
                "client_order_id": child.client_order_id,
                "status": child.status.value,
                "submitted_quantity": str(child.submitted_quantity),
                "filled_quantity": str(child.confirmed_filled_quantity),
            }
            for child in execution.child_orders
        ],
        fills=[
            {
                "client_order_id": child.client_order_id,
                "filled_quantity": str(child.confirmed_filled_quantity),
            }
            for child in execution.child_orders
            if child.confirmed_filled_quantity > Decimal("0")
        ],
        timeline=[{"event": "submit_then_cancel", "monotonic_time": str(clock.monotonic())}],
    )
    print(execution)


if __name__ == "__main__":
    asyncio.run(main())
```

Create `scripts/run_sim_create_timeout.py`:

```python
import asyncio
from decimal import Decimal

from execution.clock import ManualClock
from execution.models import Algorithm, DeadlinePolicy, Environment, ExecutionParameters, ExecutionRequest
from execution.service import ExecutionService
from exchanges.simulator import DeterministicSimulator


async def main() -> None:
    print("SIMULATOR DEMO: Create timeout")
    clock = ManualClock()
    simulator = DeterministicSimulator(clock=clock, position=Decimal("0"))
    simulator.script_create_timeout(client_order_prefix="ce_")
    await simulator.push_market_data("BTCUSDT", Decimal("100"), Decimal("101"))
    service = ExecutionService(adapter=simulator, clock=clock)
    execution = await service.create_execution(
        ExecutionRequest(
            environment=Environment.SIMULATION,
            symbol="BTCUSDT",
            algorithm=Algorithm.CHASE,
            target_position=Decimal("0.010"),
            target_price_lower=Decimal("90"),
            target_price_upper=Decimal("110"),
            target_duration_seconds=30,
            deadline_policy=DeadlinePolicy.CANCEL_REMAINDER,
            parameters=ExecutionParameters(child_order_timeout_seconds=1),
        )
    )
    print(execution)


if __name__ == "__main__":
    asyncio.run(main())
```

Each script must instantiate `DeterministicSimulator`; none of these scripts may read Binance environment variables.

- [ ] **Step 6: Run simulator script smoke test**

Run:

```bash
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_cancel_race.py --output-dir outputs
```

Expected:

```text
run_sim_chase.py stdout begins with SIMULATOR DEMO: Chase.
run_sim_cancel_race.py stdout begins with SIMULATOR DEMO: Cancel/fill race.
outputs/<execution_id>/ contains request_snapshot.json, execution_log.jsonl, execution_summary.json, child_orders.csv, fills.csv, and timeline.csv.
```

- [ ] **Step 7: Confirm final required scenario assertions before commit**

Before committing Task 14, ensure `tests/simulation/test_required_scenarios.py` or follow-up simulation tests cover these final assertions:

```text
T1 Normal Chase:
  child order is submitted at best bid for buy or best ask for sell;
  no price-bound violation is recorded;
  reserved exposure never exceeds required quantity.

T2 Chase Reprice:
  exactly one cancel-and-replace occurs when threshold and minimum interval are both satisfied;
  no replacement occurs before minimum_reprice_interval_ms.

T3 Partial Fill + Cancel Race:
  fill during pending cancel increases parent confirmed fills;
  replacement quantity is no larger than safe remaining quantity;
  no overfill occurs.

T4 Create Timeout:
  timed-out create becomes UNKNOWN exposure;
  no second clientOrderId is created before reconciliation.

T5 TWAP Carry-forward:
  later scheduled deficit includes prior unfilled quantity;
  safe_child_quantity subtracts live, pending-cancel, and unknown exposure.

T6 TWAP Tail Quantity:
  final small executable remainder is attempted if it passes min quantity and min notional;
  non-executable dust is reported rather than rounded upward.

T7 Price Outside Range:
  no child order is submitted outside bounds;
  final status is EXPIRED, PARTIALLY_COMPLETED, or unfilled with PRICE_OUTSIDE_RANGE, never fake COMPLETED.

T8 Stream Disconnect:
  new submits pause while stream health is failed;
  execution-scoped reconciliation runs before resuming.

T9 Duplicate Fill Event:
  duplicate trade ID or non-increasing cumulative fill does not increase parent cumulative fills twice.

T10 Cross-zero Position:
  required_trade_quantity = target_position - current_position using absolute quantity and side separately.
```

- [ ] **Step 8: Commit**

```bash
git add src/execution/engine.py tests/simulation/test_required_scenarios.py scripts/run_sim_chase.py scripts/run_sim_twap.py scripts/run_sim_cancel_race.py scripts/run_sim_create_timeout.py
git commit -m "feat: add simulator scenario smoke coverage"
```

---

### Task 15: Binance Adapter REST Foundation

**Files:**
- Create: `src/config.py`
- Create: `src/exchanges/binance_usdm.py`
- Test: `tests/unit/test_binance_adapter.py`

- [ ] **Step 1: Write failing Binance adapter unit tests**

Create `tests/unit/test_binance_adapter.py`:

```python
import re
from decimal import Decimal

import pytest

from config import Settings
from exchanges.base import NoFreshMarketData
from exchanges.binance_usdm import (
    BinanceUsdmAdapter,
    classify_http_status,
    normalize_order_status,
    parse_exchange_info_rate_limits,
    parse_symbol_rules_from_exchange_info,
    sign_params,
)
from execution.clock import ManualClock
from execution.models import ChildOrderStatus, MarketSnapshot, SymbolRules


def test_mainnet_requires_explicit_allow_flag() -> None:
    settings = Settings(environment="mainnet", allow_mainnet_trading=False)

    assert not settings.can_trade_mainnet


def test_sign_params_adds_signature_without_exposing_secret() -> None:
    signed = sign_params({"symbol": "BTCUSDT", "timestamp": "1"}, "secret")

    assert "signature" in signed
    assert signed["signature"] != "secret"


def test_http_429_and_418_classification() -> None:
    assert classify_http_status(429) == "RATE_LIMIT_BACKOFF"
    assert classify_http_status(418) == "VENUE_BAN_HARD_STOP"


def test_normalize_binance_statuses() -> None:
    assert normalize_order_status("FILLED") is ChildOrderStatus.FILLED
    assert normalize_order_status("CANCELED") is ChildOrderStatus.CANCELLED
    assert normalize_order_status("REJECTED") is ChildOrderStatus.REJECTED
    assert normalize_order_status("EXPIRED_IN_MATCH") is ChildOrderStatus.CANCELLED


def test_post_only_requires_gtx_support() -> None:
    rules = SymbolRules(
        symbol="BTCUSDT",
        tick_size=Decimal("0.10"),
        quantity_step=Decimal("0.001"),
        min_quantity=Decimal("0.001"),
        min_notional=Decimal("5"),
        status="TRADING",
        supported_time_in_force=frozenset({"GTC"}),
    )
    adapter = BinanceUsdmAdapter(settings=Settings(environment="testnet"))

    assert adapter.supports_post_only(rules) is False


def test_exchange_info_parsing_uses_filters_not_precision_fields() -> None:
    payload = {
        "rateLimits": [
            {"rateLimitType": "REQUEST_WEIGHT", "interval": "MINUTE", "intervalNum": 1, "limit": 2400},
            {"rateLimitType": "ORDERS", "interval": "MINUTE", "intervalNum": 1, "limit": 1200},
        ],
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "pricePrecision": 8,
                "quantityPrecision": 8,
                "timeInForce": ["GTC", "IOC", "FOK", "GTX"],
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                    {"filterType": "LOT_SIZE", "minQty": "0.001", "stepSize": "0.001"},
                    {"filterType": "MIN_NOTIONAL", "notional": "100"},
                ],
            }
        ],
    }

    rules = parse_symbol_rules_from_exchange_info(payload, "BTCUSDT")
    rate_limits = parse_exchange_info_rate_limits(payload)

    assert rules.tick_size == Decimal("0.10")
    assert rules.min_quantity == Decimal("0.001")
    assert rules.quantity_step == Decimal("0.001")
    assert rules.tick_size != Decimal("0.00000001")
    assert rules.quantity_step != Decimal("0.00000001")
    assert rules.min_notional == Decimal("100")
    assert rules.status == "TRADING"
    assert "GTX" in rules.supported_time_in_force
    assert rate_limits["REQUEST_WEIGHT"] == 2400
    assert rate_limits["ORDERS"] == 1200


async def test_binance_best_bid_ask_rejects_missing_crossed_and_stale_snapshot() -> None:
    clock = ManualClock()
    adapter = BinanceUsdmAdapter(settings=Settings(environment="testnet", stale_market_data_ms=1000), clock=clock)

    with pytest.raises(NoFreshMarketData):
        await adapter.get_best_bid_ask("BTCUSDT")

    adapter._latest_market["BTCUSDT"] = MarketSnapshot(
        symbol="BTCUSDT",
        bid=Decimal("101"),
        ask=Decimal("100"),
        last_market_event_time_exchange=1,
        last_market_event_time_local_monotonic=clock.monotonic(),
    )
    with pytest.raises(NoFreshMarketData):
        await adapter.get_best_bid_ask("BTCUSDT")

    adapter._latest_market["BTCUSDT"] = MarketSnapshot(
        symbol="BTCUSDT",
        bid=Decimal("100"),
        ask=Decimal("101"),
        last_market_event_time_exchange=2,
        last_market_event_time_local_monotonic=clock.monotonic(),
    )
    clock.advance(2)
    with pytest.raises(NoFreshMarketData):
        await adapter.get_best_bid_ask("BTCUSDT")
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/unit/test_binance_adapter.py -v
```

Expected: FAIL with missing config or Binance adapter.

- [ ] **Step 3: Implement settings**

Create `src/config.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class Settings:
    environment: str = "simulation"
    allow_mainnet_trading: bool = False
    stale_market_data_ms: int = 1500
    recv_window_ms: int = 5000
    binance_api_key: str | None = None
    binance_api_secret: str | None = None

    @property
    def can_trade_mainnet(self) -> bool:
        return self.environment == "mainnet" and self.allow_mainnet_trading


def load_settings(path: Path) -> Settings:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return Settings(**data)
```

- [ ] **Step 4: Implement Binance REST foundation**

Create `src/exchanges/binance_usdm.py`:

```python
from __future__ import annotations

import hashlib
import hmac
from collections.abc import AsyncIterator
from decimal import Decimal
from urllib.parse import urlencode

import httpx

from config import Settings
from exchanges.base import ExchangeAdapter, NoFreshMarketData
from execution.clock import Clock, SystemClock
from execution.models import ChildOrderStatus, MarketSnapshot, OrderRequest, PositionSnapshot, SymbolRules


def sign_params(params: dict[str, str], secret: str) -> dict[str, str]:
    query = urlencode(params)
    signature = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    return {**params, "signature": signature}


def classify_http_status(status_code: int) -> str:
    if status_code == 429:
        return "RATE_LIMIT_BACKOFF"
    if status_code == 418:
        return "VENUE_BAN_HARD_STOP"
    if 500 <= status_code <= 599:
        return "RETRYABLE_READ_OR_UNKNOWN_MUTATION"
    if 400 <= status_code <= 499:
        return "TERMINAL_REJECT"
    return "OK"


def normalize_order_status(raw_status: str) -> ChildOrderStatus:
    mapping = {
        "NEW": ChildOrderStatus.OPEN,
        "PARTIALLY_FILLED": ChildOrderStatus.PARTIALLY_FILLED,
        "FILLED": ChildOrderStatus.FILLED,
        "CANCELED": ChildOrderStatus.CANCELLED,
        "REJECTED": ChildOrderStatus.REJECTED,
        "EXPIRED": ChildOrderStatus.CANCELLED,
        "EXPIRED_IN_MATCH": ChildOrderStatus.CANCELLED,
    }
    return mapping[raw_status]


def parse_symbol_rules_from_exchange_info(payload: dict, symbol: str) -> SymbolRules:
    symbol_data = next(item for item in payload["symbols"] if item["symbol"] == symbol)
    filters = {item["filterType"]: item for item in symbol_data["filters"]}
    price_filter = filters["PRICE_FILTER"]
    lot_size = filters["LOT_SIZE"]
    min_notional = filters["MIN_NOTIONAL"]
    return SymbolRules(
        symbol=symbol,
        tick_size=Decimal(price_filter["tickSize"]),
        quantity_step=Decimal(lot_size["stepSize"]),
        min_quantity=Decimal(lot_size["minQty"]),
        min_notional=Decimal(min_notional.get("notional", min_notional.get("minNotional", "0"))),
        status=symbol_data["status"],
        supported_time_in_force=frozenset(symbol_data.get("timeInForce", [])),
    )


def parse_exchange_info_rate_limits(payload: dict) -> dict[str, int]:
    return {
        item["rateLimitType"]: int(item["limit"])
        for item in payload.get("rateLimits", [])
        if item.get("rateLimitType") in {"REQUEST_WEIGHT", "ORDERS"}
    }


class BinanceUsdmAdapter(ExchangeAdapter):
    def __init__(
        self,
        settings: Settings,
        client: httpx.AsyncClient | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.settings = settings
        self.client = client or httpx.AsyncClient()
        self.clock = clock or SystemClock()
        self.server_time_offset_ms = 0
        self._latest_market: dict[str, MarketSnapshot] = {}

    def supports_post_only(self, rules: SymbolRules) -> bool:
        return "GTX" in rules.supported_time_in_force

    def signed_params(self, params: dict[str, str], now_ms: int) -> dict[str, str]:
        adjusted = str(now_ms + self.server_time_offset_ms)
        base = {**params, "timestamp": adjusted, "recvWindow": str(self.settings.recv_window_ms)}
        if self.settings.binance_api_secret is None:
            raise RuntimeError("missing Binance API secret")
        return sign_params(base, self.settings.binance_api_secret)

    async def get_symbol_rules(self, symbol: str) -> SymbolRules:
        raise NotImplementedError("exchangeInfo parsing is added in the Binance integration task")

    async def get_position(self, symbol: str) -> PositionSnapshot:
        raise NotImplementedError("position query is added in the Binance integration task")

    async def get_best_bid_ask(self, symbol: str) -> MarketSnapshot:
        snapshot = self._latest_market.get(symbol)
        if snapshot is None:
            raise NoFreshMarketData(f"no fresh market data for {symbol}")
        if snapshot.is_crossed:
            raise NoFreshMarketData(f"crossed market data for {symbol}")
        age_seconds = self.clock.monotonic() - snapshot.last_market_event_time_local_monotonic
        if age_seconds * 1000 > self.settings.stale_market_data_ms:
            raise NoFreshMarketData(f"stale market data for {symbol}: age_seconds={age_seconds}")
        return snapshot

    async def stream_market_data(self) -> AsyncIterator[MarketSnapshot]:
        if False:
            yield None
        return

    async def submit_limit_order(self, order_request: OrderRequest) -> object:
        raise NotImplementedError("order submission is added in the Binance integration task")

    async def cancel_order(self, symbol: str, client_order_id: str) -> object:
        raise NotImplementedError("cancel is added in the Binance integration task")

    async def get_order_by_client_order_id(self, symbol: str, client_order_id: str) -> object:
        raise NotImplementedError("order lookup is added in the Binance integration task")

    async def stream_user_events(self) -> AsyncIterator[object]:
        if False:
            yield None
        return

    async def reconcile_orders_and_fills(self, symbol: str, client_order_prefix: str | None = None) -> object:
        return []

    async def health_check_streams(self) -> bool:
        return True
```

- [ ] **Step 5: Run adapter tests**

Run:

```bash
uv run pytest tests/unit/test_binance_adapter.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/config.py src/exchanges/binance_usdm.py tests/unit/test_binance_adapter.py
git commit -m "feat: add binance adapter rest foundation"
```

---

### Task 16: Binance Integration Hooks And Testnet Scripts

**Files:**
- Modify: `src/exchanges/binance_usdm.py`
- Create: `tests/integration/test_binance_testnet_contract.py`
- Create: `scripts/run_testnet_chase.py`
- Create: `scripts/run_testnet_twap.py`

- [ ] **Step 1: Write credential-gated integration tests**

Create `tests/integration/test_binance_testnet_contract.py`:

```python
import os

import pytest

from config import Settings
from exchanges.binance_usdm import BinanceUsdmAdapter


def has_testnet_credentials() -> bool:
    return bool(os.getenv("BINANCE_USDM_API_KEY") and os.getenv("BINANCE_USDM_API_SECRET"))


@pytest.mark.skipif(
    not has_testnet_credentials(),
    reason="Binance Testnet credentials are not configured",
)
async def test_testnet_exchange_info_loads_btcusdt_rules() -> None:
    adapter = BinanceUsdmAdapter(
        Settings(
            environment="testnet",
            binance_api_key=os.environ["BINANCE_USDM_API_KEY"],
            binance_api_secret=os.environ["BINANCE_USDM_API_SECRET"],
        )
    )

    rules = await adapter.get_symbol_rules("BTCUSDT")

    assert rules.symbol == "BTCUSDT"
    assert rules.tick_size > 0
    assert rules.quantity_step > 0
    assert rules.status
```

- [ ] **Step 2: Run integration test without credentials**

Run:

```bash
uv run pytest tests/integration/test_binance_testnet_contract.py -v -rs
```

Expected: SKIPPED with reason `Binance Testnet credentials are not configured`.

- [ ] **Step 3: Implement `get_symbol_rules` parsing**

In `src/exchanges/binance_usdm.py`, implement:

```python
    @property
    def base_url(self) -> str:
        if self.settings.environment == "testnet":
            return "https://demo-fapi.binance.com"
        return "https://fapi.binance.com"

    async def get_symbol_rules(self, symbol: str) -> SymbolRules:
        response = await self.client.get(f"{self.base_url}/fapi/v1/exchangeInfo", timeout=5.0)
        response.raise_for_status()
        data = response.json()
        self.rate_limits = parse_exchange_info_rate_limits(data)
        return parse_symbol_rules_from_exchange_info(data, symbol)
```

- [ ] **Step 4: Add clearly labeled Testnet scripts**

Create `scripts/run_testnet_chase.py`:

```python
import os


def main() -> None:
    print("BINANCE TESTNET DEMO: Chase")
    if not os.getenv("BINANCE_USDM_API_KEY") or not os.getenv("BINANCE_USDM_API_SECRET"):
        raise SystemExit("Missing BINANCE_USDM_API_KEY or BINANCE_USDM_API_SECRET. This script never falls back to simulation.")
    print("Credentials detected. Task 17 replaces this guard with the explicit --confirm-send-orders evidence runner.")


if __name__ == "__main__":
    main()
```

Create `scripts/run_testnet_twap.py`:

```python
import os


def main() -> None:
    print("BINANCE TESTNET DEMO: TWAP")
    if not os.getenv("BINANCE_USDM_API_KEY") or not os.getenv("BINANCE_USDM_API_SECRET"):
        raise SystemExit("Missing BINANCE_USDM_API_KEY or BINANCE_USDM_API_SECRET. This script never falls back to simulation.")
    print("Credentials detected. Task 17 replaces this guard with the explicit --confirm-send-orders evidence runner.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run integration test without credentials**

Run:

```bash
uv run pytest tests/integration/test_binance_testnet_contract.py -v -rs
```

Expected: SKIPPED when no credentials are present. If credentials are present, expected PASS loading BTCUSDT rules.

- [ ] **Step 6: Run Testnet script without credentials**

Run:

```bash
uv run python scripts/run_testnet_chase.py
```

Expected: exits with message containing `This script never falls back to simulation.`

- [ ] **Step 7: Commit**

```bash
git add src/exchanges/binance_usdm.py tests/integration/test_binance_testnet_contract.py scripts/run_testnet_chase.py scripts/run_testnet_twap.py
git commit -m "feat: add credential-gated binance testnet hooks"
```

---

### Task 17: Binance Order Mutations, Streams, And Reconciliation

**Files:**
- Modify: `src/exchanges/binance_usdm.py`
- Test: `tests/unit/test_binance_order_mutations.py`
- Modify: `tests/integration/test_binance_testnet_contract.py`
- Create: `scripts/testnet_runner.py`
- Modify: `scripts/run_testnet_chase.py`
- Modify: `scripts/run_testnet_twap.py`

- [ ] **Step 1: Write failing Binance order-mutation unit tests**

Create `tests/unit/test_binance_order_mutations.py`:

```python
from decimal import Decimal

import pytest

from config import Settings
from exchanges.binance_usdm import (
    BinanceUsdmAdapter,
    ExchangeTerminalReject,
    MutationKind,
    classify_mutation_timeout,
)
from execution.ids import make_client_order_id
from execution.models import OrderRequest, Side, SymbolRules


def rules_with_time_in_force(values: set[str]) -> SymbolRules:
    return SymbolRules(
        symbol="BTCUSDT",
        tick_size=Decimal("0.10"),
        quantity_step=Decimal("0.001"),
        min_quantity=Decimal("0.001"),
        min_notional=Decimal("5"),
        status="TRADING",
        supported_time_in_force=frozenset(values),
    )


def make_order(post_only: bool = True) -> OrderRequest:
    return OrderRequest(
        execution_id="exec_abcdef",
        child_order_id="child_0001",
        client_order_id=make_client_order_id("exec_abcdef", 1),
        symbol="BTCUSDT",
        side=Side.BUY,
        quantity=Decimal("0.010"),
        price=Decimal("100.10"),
        post_only=post_only,
    )


def test_new_order_payload_serializes_decimal_strings_and_uses_gtx() -> None:
    adapter = BinanceUsdmAdapter(settings=Settings(environment="testnet"))

    payload = adapter.build_new_order_params(make_order(), rules_with_time_in_force({"GTC", "GTX"}))

    assert payload["quantity"] == "0.010"
    assert payload["price"] == "100.10"
    assert payload["timeInForce"] == "GTX"
    assert len(payload["newClientOrderId"]) <= 36


def test_post_only_rejects_when_gtx_is_missing_or_uncertain() -> None:
    adapter = BinanceUsdmAdapter(settings=Settings(environment="testnet"))

    with pytest.raises(ExchangeTerminalReject, match="POST_ONLY_GTX_UNSUPPORTED"):
        adapter.build_new_order_params(make_order(post_only=True), rules_with_time_in_force({"GTC"}))


def test_timeout_classification_distinguishes_create_and_cancel() -> None:
    assert classify_mutation_timeout(MutationKind.CREATE) == "UNKNOWN_CREATE_OUTCOME"
    assert classify_mutation_timeout(MutationKind.CANCEL) == "PENDING_CANCEL_OUTCOME"
```

- [ ] **Step 2: Run order-mutation tests to verify they fail**

Run:

```bash
uv run pytest tests/unit/test_binance_order_mutations.py -v
```

Expected: FAIL with missing helpers and exception classes.

- [ ] **Step 3: Add exchange error classes, mutation classification, and payload builders**

Update imports in `src/exchanges/binance_usdm.py`:

```python
import json
import websockets
from execution.models import ChildOrder, ChildOrderStatus, MarketSnapshot, OrderRequest, PositionSnapshot, Side, SymbolRules
```

In `src/exchanges/binance_usdm.py`, add:

```python
from enum import Enum


class MutationKind(str, Enum):
    CREATE = "CREATE"
    CANCEL = "CANCEL"


class ExchangeTerminalReject(RuntimeError):
    pass


class UnknownCreateOutcome(RuntimeError):
    pass


class PendingCancelOutcome(RuntimeError):
    pass


class RetryableReadFailure(RuntimeError):
    pass


class StreamHealthFailure(RuntimeError):
    pass


def decimal_to_api(value: Decimal) -> str:
    return format(value, "f")


def classify_mutation_timeout(kind: MutationKind) -> str:
    if kind is MutationKind.CREATE:
        return "UNKNOWN_CREATE_OUTCOME"
    return "PENDING_CANCEL_OUTCOME"
```

Add this method to `BinanceUsdmAdapter`:

```python
    def build_new_order_params(self, order_request: OrderRequest, rules: SymbolRules) -> dict[str, str]:
        if order_request.post_only:
            if "GTX" not in rules.supported_time_in_force:
                raise ExchangeTerminalReject("POST_ONLY_GTX_UNSUPPORTED")
            time_in_force = "GTX"
        else:
            time_in_force = "GTC"

        if len(order_request.client_order_id) > 36:
            raise ExchangeTerminalReject("CLIENT_ORDER_ID_TOO_LONG")

        return {
            "symbol": order_request.symbol,
            "side": order_request.side.value,
            "type": "LIMIT",
            "timeInForce": time_in_force,
            "quantity": decimal_to_api(order_request.quantity),
            "price": decimal_to_api(order_request.price),
            "newClientOrderId": order_request.client_order_id,
        }
```

- [ ] **Step 4: Implement signed REST order, cancel, lookup, position, and reconciliation methods**

Add a single signed request helper. It must use bounded timeout, `timestamp`, `recvWindow`, server-time offset, API key header, sanitized logging, and no blind retry for mutating requests:

```python
    async def _signed_request(
        self,
        method: str,
        path: str,
        params: dict[str, str],
        mutation_kind: MutationKind | None = None,
    ) -> dict:
        signed = self.signed_params(params, now_ms=self.clock_wall_ms())
        headers = {"X-MBX-APIKEY": self.settings.binance_api_key or ""}
        try:
            response = await self.client.request(
                method,
                f"{self.base_url}{path}",
                params=signed,
                headers=headers,
                timeout=5.0,
            )
        except httpx.TimeoutException as exc:
            if mutation_kind is MutationKind.CREATE:
                raise UnknownCreateOutcome("create timed out; reconcile by clientOrderId before retry") from exc
            if mutation_kind is MutationKind.CANCEL:
                raise PendingCancelOutcome("cancel timed out; keep pending_cancel exposure until reconciliation") from exc
            raise RetryableReadFailure("read timed out") from exc

        classification = classify_http_status(response.status_code)
        if classification == "RATE_LIMIT_BACKOFF":
            raise RetryableReadFailure("429 rate limit; bounded backoff required")
        if classification == "VENUE_BAN_HARD_STOP":
            raise RuntimeError("418 venue ban; stop all exchange activity")
        if classification == "TERMINAL_REJECT":
            raise ExchangeTerminalReject(response.text)
        response.raise_for_status()
        return response.json()
```

Add parse and clock helpers:

```python
    @property
    def ws_root_url(self) -> str:
        if self.settings.environment == "testnet":
            return "wss://stream.binancefuture.com"
        return "wss://fstream.binance.com"

    @property
    def market_ws_base_url(self) -> str:
        return f"{self.ws_root_url}/market"

    @property
    def private_ws_base_url(self) -> str:
        return f"{self.ws_root_url}/private"

    def clock_wall_ms(self) -> int:
        return int(self.clock.utc_now().timestamp() * 1000)

    def parse_order(self, raw: dict, fallback: OrderRequest | None = None) -> ChildOrder:
        client_order_id_value = str(raw.get("clientOrderId") or raw.get("newClientOrderId") or fallback.client_order_id)
        side = Side(str(raw.get("side") or fallback.side.value))
        submitted_quantity = Decimal(str(raw.get("origQty") or fallback.quantity))
        filled_quantity = Decimal(str(raw.get("executedQty") or "0"))
        child_order_id_value = fallback.child_order_id if fallback is not None else client_order_id_value
        return ChildOrder(
            child_order_id=child_order_id_value,
            client_order_id=client_order_id_value,
            symbol=str(raw.get("symbol") or fallback.symbol),
            side=side,
            submitted_quantity=submitted_quantity,
            confirmed_filled_quantity=filled_quantity,
            exchange_order_id=str(raw["orderId"]) if raw.get("orderId") is not None else None,
            price=Decimal(str(raw.get("price") or fallback.price)),
            status=normalize_order_status(str(raw["status"])),
        )
```

Implement the adapter methods:

```python
    async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
        rules = await self.get_symbol_rules(order_request.symbol)
        payload = self.build_new_order_params(order_request, rules)
        raw = await self._signed_request("POST", "/fapi/v1/order", payload, mutation_kind=MutationKind.CREATE)
        return self.parse_order(raw, order_request)

    async def cancel_order(self, symbol: str, client_order_id: str) -> ChildOrder:
        raw = await self._signed_request(
            "DELETE",
            "/fapi/v1/order",
            {"symbol": symbol, "origClientOrderId": client_order_id},
            mutation_kind=MutationKind.CANCEL,
        )
        return self.parse_order(raw)

    async def get_order_by_client_order_id(self, symbol: str, client_order_id: str) -> ChildOrder:
        raw = await self._signed_request(
            "GET",
            "/fapi/v1/order",
            {"symbol": symbol, "origClientOrderId": client_order_id},
        )
        return self.parse_order(raw)

    async def get_position(self, symbol: str) -> PositionSnapshot:
        raw = await self._signed_request("GET", "/fapi/v3/positionRisk", {"symbol": symbol})
        row = raw[0] if isinstance(raw, list) else raw
        if row.get("positionSide") not in {None, "BOTH"}:
            raise ExchangeTerminalReject("HEDGE_MODE_UNSUPPORTED")
        return PositionSnapshot(symbol=symbol, position=Decimal(row["positionAmt"]))
```

Implement reconciliation so it cannot absorb unrelated manual orders:

```python
    async def reconcile_orders_and_fills(
        self,
        symbol: str,
        client_order_prefix: str | None = None,
    ) -> list[ChildOrder]:
        if client_order_prefix is None:
            raise ValueError("client_order_prefix is required for execution reconciliation")
        open_orders = await self._signed_request("GET", "/fapi/v1/openOrders", {"symbol": symbol})
        recent_orders = await self._signed_request("GET", "/fapi/v1/allOrders", {"symbol": symbol, "limit": "50"})
        rows = [*open_orders, *recent_orders]
        filtered = [
            self.parse_order(row)
            for row in rows
            if str(row.get("clientOrderId", "")).startswith(client_order_prefix)
        ]
        return filtered
```

Important implementation rules:

```text
Create timeout -> child order status UNKNOWN and unknown_order_quantity exposure until reconciliation.
Cancel timeout -> keep existing order in PENDING_CANCEL exposure until reconciliation.
Already-filled cancel response -> terminal FILLED reconciliation result, not a fatal engine failure.
Order create ACK only confirms request acceptance; final truth comes from user stream or REST reconciliation.
```

- [ ] **Step 5: Implement market-data and user-data stream hooks**

Add stream helper methods:

```python
    def parse_book_ticker(self, message: str) -> MarketSnapshot:
        data = json.loads(message)
        symbol = str(data["s"])
        return MarketSnapshot(
            symbol=symbol,
            bid=Decimal(str(data["b"])),
            ask=Decimal(str(data["a"])),
            last_market_event_time_exchange=data.get("E"),
            last_market_event_time_local_monotonic=self.clock.monotonic(),
        )

    async def create_listen_key(self) -> str:
        headers = {"X-MBX-APIKEY": self.settings.binance_api_key or ""}
        response = await self.client.post(f"{self.base_url}/fapi/v1/listenKey", headers=headers, timeout=5.0)
        response.raise_for_status()
        return str(response.json()["listenKey"])

    async def renew_listen_key(self, listen_key: str) -> None:
        headers = {"X-MBX-APIKEY": self.settings.binance_api_key or ""}
        response = await self.client.put(
            f"{self.base_url}/fapi/v1/listenKey",
            params={"listenKey": listen_key},
            headers=headers,
            timeout=5.0,
        )
        response.raise_for_status()

    def parse_user_event(self, message: str) -> dict:
        event = json.loads(message)
        return {
            "event_type": event.get("e"),
            "event_time_ms": event.get("E"),
            "transaction_time_ms": event.get("T"),
            "raw": event,
        }
```

Extend `stream_market_data()` and `stream_user_events()` in `src/exchanges/binance_usdm.py`:

```python
    async def stream_market_data(self) -> AsyncIterator[MarketSnapshot]:
        url = f"{self.market_ws_base_url}/ws/{'btcusdt'}@bookTicker"
        async with websockets.connect(url) as websocket:
            async for message in websocket:
                snapshot = self.parse_book_ticker(message)
                self._latest_market[snapshot.symbol] = snapshot
                yield snapshot

    async def stream_user_events(self) -> AsyncIterator[object]:
        listen_key = await self.create_listen_key()
        url = f"{self.private_ws_base_url}/ws/{listen_key}"
        async with websockets.connect(url) as websocket:
            async for message in websocket:
                event = self.parse_user_event(message)
                # Store both event time E and transaction time T when present for ordering diagnostics.
                yield event
```

Keep this compact: the adapter only needs enough stream code to support Testnet and reconnect/reconcile behavior. Market data uses the routed `/market` WebSocket path and private user data uses the routed `/private` path. On disconnect or 24h renewal, mark stream health degraded, reconnect, and call `reconcile_orders_and_fills(symbol, execution_prefix)` before new child orders are submitted.

- [ ] **Step 6: Upgrade Testnet scripts into explicit opt-in evidence runners**

Create `scripts/testnet_runner.py`. This helper is intentionally small: it refuses to run without credentials, refuses to send orders without `--confirm-send-orders`, waits for a fresh Testnet market-data snapshot before creating the execution, runs the real `ExecutionService` path, reconciles after each loop, and writes the same artifact set used by simulator demos.

```python
import argparse
import asyncio
import os
from decimal import Decimal
from pathlib import Path
from typing import Sequence

from config import Settings
from execution.models import Algorithm, DeadlinePolicy, Environment, ExecutionParameters, ExecutionRequest
from execution.service import ExecutionService
from exchanges.binance_usdm import BinanceUsdmAdapter
from observability.artifacts import write_execution_artifacts


def parse_args(label: str, argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=f"BINANCE TESTNET DEMO: {label}")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--target-position")
    parser.add_argument("--target-price-lower")
    parser.add_argument("--target-price-upper")
    parser.add_argument("--duration-seconds", type=int, default=60)
    parser.add_argument("--number-of-slices", type=int, default=5)
    parser.add_argument("--max-runtime-seconds", type=int, default=90)
    parser.add_argument("--market-timeout-seconds", type=int, default=10)
    parser.add_argument("--output-dir", type=Path, default=Path("outputs/testnet"))
    parser.add_argument("--confirm-send-orders", action="store_true")
    return parser.parse_args(argv)


def require_credentials() -> tuple[str, str]:
    api_key = os.getenv("BINANCE_USDM_API_KEY")
    api_secret = os.getenv("BINANCE_USDM_API_SECRET")
    if not api_key or not api_secret:
        raise SystemExit("Missing BINANCE_USDM_API_KEY or BINANCE_USDM_API_SECRET. This script never falls back to simulation.")
    return api_key, api_secret


def require_decimal_arg(args: argparse.Namespace, name: str) -> Decimal:
    value = getattr(args, name)
    if value is None:
        raise SystemExit(f"Missing --{name.replace('_', '-')}. Review order size and price bounds before sending Testnet orders.")
    return Decimal(value)


async def wait_for_fresh_market(adapter: BinanceUsdmAdapter, symbol: str, timeout_seconds: int) -> None:
    async with asyncio.timeout(timeout_seconds):
        async for snapshot in adapter.stream_market_data():
            if snapshot.symbol == symbol:
                return


async def run_testnet_evidence(algorithm: Algorithm, args: argparse.Namespace) -> None:
    api_key, api_secret = require_credentials()
    if not args.confirm_send_orders:
        raise SystemExit("Refusing to send Testnet orders without --confirm-send-orders.")

    target_position = require_decimal_arg(args, "target_position")
    lower = require_decimal_arg(args, "target_price_lower")
    upper = require_decimal_arg(args, "target_price_upper")

    settings = Settings(
        environment="testnet",
        binance_api_key=api_key,
        binance_api_secret=api_secret,
    )
    adapter = BinanceUsdmAdapter(settings=settings)
    await wait_for_fresh_market(adapter, args.symbol, args.market_timeout_seconds)

    service = ExecutionService(adapter=adapter, clock=adapter.clock)
    request = ExecutionRequest(
        environment=Environment.TESTNET,
        symbol=args.symbol,
        algorithm=algorithm,
        target_position=target_position,
        target_price_lower=lower,
        target_price_upper=upper,
        target_duration_seconds=args.duration_seconds,
        deadline_policy=DeadlinePolicy.CANCEL_REMAINDER,
        parameters=ExecutionParameters(number_of_slices=args.number_of_slices),
    )
    execution = await service.create_execution(request)

    stop_at = adapter.clock.monotonic() + args.max_runtime_seconds
    while not execution.status.is_terminal and adapter.clock.monotonic() < stop_at:
        execution = await service.run_once(execution.execution_id)
        await asyncio.sleep(1)
        execution = await service.reconcile_execution(execution.execution_id)

    if not execution.status.is_terminal:
        execution = await service.cancel_execution(execution.execution_id)
        execution = await service.reconcile_execution(execution.execution_id)

    output_dir = write_execution_artifacts(
        root=args.output_dir,
        execution_id=execution.execution_id,
        request_snapshot={
            "environment": "testnet",
            "symbol": request.symbol,
            "algorithm": request.algorithm.value,
            "target_position": request.target_position,
            "target_price_lower": request.target_price_lower,
            "target_price_upper": request.target_price_upper,
            "duration_seconds": request.target_duration_seconds,
        },
        log_events=[
            {
                "execution_id": execution.execution_id,
                "event": "testnet_child_order_final_state",
                "child_order_id": child.child_order_id,
                "client_order_id": child.client_order_id,
                "exchange_order_id": child.exchange_order_id,
                "status": child.status,
                "submitted_quantity": child.submitted_quantity,
                "filled_quantity": child.confirmed_filled_quantity,
                "price": child.price,
            }
            for child in execution.child_orders
        ],
        summary={
            "execution_id": execution.execution_id,
            "final_status": execution.status,
            "final_reason": execution.final_reason,
        },
        child_orders=[
            {
                "child_order_id": child.child_order_id,
                "client_order_id": child.client_order_id,
                "exchange_order_id": child.exchange_order_id,
                "status": child.status,
                "submitted_quantity": child.submitted_quantity,
                "filled_quantity": child.confirmed_filled_quantity,
                "price": child.price,
            }
            for child in execution.child_orders
        ],
        fills=[
            {
                "client_order_id": child.client_order_id,
                "exchange_order_id": child.exchange_order_id,
                "filled_quantity": child.confirmed_filled_quantity,
                "price": child.price,
            }
            for child in execution.child_orders
            if child.confirmed_filled_quantity > Decimal("0")
        ],
        timeline=[{"event": "testnet_evidence_run", "final_status": execution.status}],
    )
    print(f"Testnet evidence artifacts written to {output_dir}")
```

Replace `scripts/run_testnet_chase.py` with:

```python
import asyncio

from execution.models import Algorithm
from testnet_runner import parse_args, run_testnet_evidence


def main() -> None:
    print("BINANCE TESTNET DEMO: Chase")
    args = parse_args("Chase")
    asyncio.run(run_testnet_evidence(Algorithm.CHASE, args))


if __name__ == "__main__":
    main()
```

Replace `scripts/run_testnet_twap.py` with:

```python
import asyncio

from execution.models import Algorithm
from testnet_runner import parse_args, run_testnet_evidence


def main() -> None:
    print("BINANCE TESTNET DEMO: TWAP")
    args = parse_args("TWAP")
    asyncio.run(run_testnet_evidence(Algorithm.TWAP, args))


if __name__ == "__main__":
    main()
```

- [ ] **Step 7: Add Testnet contract assertions for no credentials, explicit confirmation, and safe labels**

Extend `tests/integration/test_binance_testnet_contract.py` with a script gate check. It must not be skipped when credentials are absent, and it must prove the script refuses to run rather than falling back to simulation:

```python
from pathlib import Path
import subprocess
import sys


def test_testnet_order_script_refuses_without_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BINANCE_USDM_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_USDM_API_SECRET", raising=False)

    result = subprocess.run(
        [sys.executable, "scripts/run_testnet_chase.py"],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "never falls back to simulation" in result.stderr + result.stdout
    assert "DeterministicSimulator" not in Path("scripts/run_testnet_chase.py").read_text(encoding="utf-8")
    assert "DeterministicSimulator" not in Path("scripts/testnet_runner.py").read_text(encoding="utf-8")


def test_testnet_order_script_requires_explicit_confirmation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BINANCE_USDM_API_KEY", "fake-key")
    monkeypatch.setenv("BINANCE_USDM_API_SECRET", "fake-secret")

    result = subprocess.run(
        [sys.executable, "scripts/run_testnet_chase.py"],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "Refusing to send Testnet orders without --confirm-send-orders" in result.stderr + result.stdout
    assert "--confirm-send-orders" in Path("scripts/run_testnet_chase.py").read_text(encoding="utf-8") + Path("scripts/testnet_runner.py").read_text(encoding="utf-8")
```

- [ ] **Step 8: Run Binance unit and integration tests**

Run:

```bash
uv run pytest tests/unit/test_binance_adapter.py tests/unit/test_binance_order_mutations.py -v
uv run pytest tests/integration/test_binance_testnet_contract.py -v -rs
```

Expected:

- Unit tests PASS using mocked/local helpers only.
- Integration test SKIPPED without credentials, or PASS loading rules with Testnet credentials.
- Testnet order scripts refuse to send orders unless both credentials and `--confirm-send-orders` are present.

- [ ] **Step 9: Commit**

```bash
git add src/exchanges/binance_usdm.py tests/unit/test_binance_order_mutations.py tests/integration/test_binance_testnet_contract.py scripts/testnet_runner.py scripts/run_testnet_chase.py scripts/run_testnet_twap.py
git commit -m "feat: add binance order mutation and reconciliation policies"
```

---

### Task 18: Documentation, Report Draft, AI Usage, And Final Verification

**Files:**
- Modify: `README.md`
- Create: `AI_USAGE.md`
- Create: `reports/report_draft.md`
- Create: `reports/failure_case_log.md`

- [ ] **Step 1: Write README with required sections**

Replace `README.md` with:

```markdown
# Calais Execution Algorithm

Small but correct execution algorithm service for Binance USD-M BTCUSDT Perpetual.

## Scope

- Algorithms: CHASE and TWAP
- Environments: simulation, Binance Testnet, mainnet configuration guarded off by default
- Main proof path: deterministic simulator
- Mainnet trading: disabled unless `ALLOW_MAINNET_TRADING=true`

## Architecture

FastAPI and CLI scripts call `ExecutionService`. `ExecutionService` delegates lifecycle ownership to `ExecutionEngine`. Chase and TWAP decide order demand only. The engine owns state transitions, exposure buckets, event serialization, reconciliation, and summaries. Simulator and Binance implement the same `ExchangeAdapter` contract.

## Target Position Logic

The request provides final `target_position`, not order quantity.

```text
required_trade_quantity = target_position - current_position
```

If the required quantity is zero, the execution submits no child orders and returns `COMPLETED` with `NO_ACTION_TARGET_ALREADY_REACHED`.

## Correctness Invariants

- Confirmed filled quantity is monotonic.
- Confirmed fills plus live open, pending submit, pending cancel, unknown exposure, and new child quantity never exceeds normalized target quantity.
- Duplicate fill events do not increase cumulative fills twice.
- Timed-out create requests remain unknown until reconciled by `clientOrderId`.
- Aggressive execution never violates configured price bounds.
- Terminal execution state never returns to `RUNNING`.

## Running Tests

```bash
uv run pytest
```

## Running Simulator Demos

```bash
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py
uv run python scripts/run_sim_create_timeout.py
```

## Binance Testnet

Set credentials with:

```bash
export BINANCE_USDM_API_KEY=...
export BINANCE_USDM_API_SECRET=...
```

Then run:

```bash
uv run pytest tests/integration -v -rs
```

Testnet scripts are explicitly labeled, never fall back to simulation, and refuse to send orders unless `--confirm-send-orders` is supplied. Use very small Testnet size and review price bounds before running:

```bash
uv run python scripts/run_testnet_chase.py \
  --target-position "0.001" \
  --target-price-lower "90000" \
  --target-price-upper "130000" \
  --confirm-send-orders

uv run python scripts/run_testnet_twap.py \
  --target-position "0.001" \
  --target-price-lower "90000" \
  --target-price-upper "130000" \
  --number-of-slices 3 \
  --confirm-send-orders
```

Before final submission, if Binance Testnet credentials are available, run one Chase and one TWAP Testnet execution and attach the raw execution logs, order IDs, request snapshot, and result summary. If credentials are unavailable, state that explicitly and show that Testnet scripts are credential-gated.

## Known Limitations

- Only BTCUSDT is supported.
- Only Binance One-way Mode is supported.
- The Binance adapter implements the minimal Testnet integration path required for the assignment; deterministic simulator tests provide proof for race conditions that Testnet may not reliably reproduce.
- Testnet may not reliably produce partial fills; race cases are proven in simulator.
- No database persistence is included in the compact version.
- The compact version assumes no external BTCUSDT trading during one execution and reports drift if detected.
```

- [ ] **Step 2: Write AI usage disclosure**

Create `AI_USAGE.md`:

```markdown
# AI Usage Disclosure

AI tools were used for planning, implementation assistance, code review, and documentation drafting.

The candidate remains responsible for the submitted code. Correctness was validated through:

- Manual review of target-position, exposure, and deadline logic.
- Unit tests for Decimal math, state transitions, idempotency, source-of-truth behavior, and log sanitization.
- Deterministic simulator scenarios for partial fills, cancel/fill race, create timeout, duplicate events, stale market data, price-outside-range behavior, and cross-zero target position.
- Credential-gated Binance Testnet checks for exchangeInfo and adapter contract behavior.

The implementation avoids relying on generated code without tests. Core logic is intentionally small and explainable for live interview modification.
```

- [ ] **Step 3: Write report draft**

Create `reports/report_draft.md`:

```markdown
# Calais Execution Algorithm Report Draft

## Design Summary

This project implements a small but correct execution service for BTCUSDT perpetual futures. The engine accepts final target position, price bounds, duration, algorithm, and deadline policy. It computes required trade quantity from current position and uses Chase or TWAP to submit safe child orders.

## Key Invariants

- Confirmed fills are monotonic.
- Reserved and unknown exposure are counted before any replacement order.
- Duplicate fills are deduplicated by trade ID or monotonic cumulative quantity.
- Timed-out create requests are reconciled by `clientOrderId` before retry.
- Price bounds are never violated by aggressive execution.

## Simulator Evidence

Include output from:

- Normal Chase
- Chase Reprice
- Partial Fill + Cancel Race
- Create Timeout
- TWAP Carry-forward
- Tail Quantity
- Price Outside Range
- Stream Disconnect
- Duplicate Event
- Cross-zero Position

## Testnet Evidence

After API keys are added, include raw execution log, order IDs, request snapshot, and result summary for one Chase and one TWAP run. If Testnet credentials are not available before submission, state that limitation explicitly and include the credential-gating output.

## Real Development Failure Case

Record one real failure encountered during implementation. Include failing behavior, root cause, test or log evidence, and final fix.

## Known Limitations

- Only BTCUSDT.
- Only One-way Mode.
- Binance WebSocket handling is intentionally compact and Testnet-focused, not a claim of production-grade reconnect infrastructure.
- Simulator is deterministic but not a full matching engine.
- Testnet liquidity may not reproduce all race conditions.
- Mainnet is disabled by default.
```

- [ ] **Step 4: Write failure case log template**

Create `reports/failure_case_log.md`:

```markdown
# Development Failure Case Log

## Selected Case

Use the first real bug that meaningfully demonstrates execution safety. Good candidates:

- Overfill risk during cancel/fill race.
- Stale-market-data action.
- Decimal rounding error.
- Duplicate fill counted twice.
- Create-timeout duplicate-order risk.

## Evidence To Capture

- Failing test name.
- Failing assertion or log excerpt.
- Root cause in one paragraph.
- Code change that fixed it.
- Passing test command after fix.
```

- [ ] **Step 5: Confirm submission packaging excludes internal agent workflow text**

The internal plan under `docs/superpowers/plans/` may keep the `For agentic workers` header because it is a private execution tracker. Do not copy that line into `README.md`, `reports/report_draft.md`, the final PDF, or any Calais-facing design appendix.

- [ ] **Step 6: Run full automated test suite**

Run:

```bash
uv run pytest -v -rs
```

Expected: PASS for unit and simulation tests; integration tests SKIPPED without credentials.

- [ ] **Step 7: Run smoke scripts**

Run:

```bash
uv run python scripts/run_sim_chase.py
uv run python scripts/run_testnet_chase.py
```

Expected:

- Simulator script prints `SIMULATOR DEMO: Chase`.
- Testnet script exits with missing-credentials message if credentials are absent, or refuses order placement without `--confirm-send-orders` if credentials are present.

- [ ] **Step 8: Commit**

```bash
git add README.md AI_USAGE.md reports/report_draft.md reports/failure_case_log.md
git commit -m "docs: add execution deliverables"
```

---

## Final Verification Checklist

Run these commands before declaring implementation complete:

```bash
uv run pytest -v -rs
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py
uv run python scripts/run_sim_create_timeout.py
```

Expected:

- Unit and simulation tests pass.
- Integration tests skip cleanly without Binance credentials.
- Simulator scripts clearly identify themselves as simulator demos.
- Testnet scripts clearly refuse to run without credentials, require `--confirm-send-orders` before placing Testnet orders, and never fall back to simulation.

Manual checks:

```text
README explains architecture, state machines, overfill invariant, Chase/TWAP design, Testnet setup, known limitations.
AI_USAGE.md discloses AI support and validation.
reports/report_draft.md has sections for simulator evidence, Testnet evidence, failure case, and limitations.
reports/failure_case_log.md contains at least one real implementation failure before final submission.
Simulator demo artifacts exist for request snapshot, JSONL log, summary JSON, child orders CSV, fills CSV, and timeline CSV.
If Testnet credentials are available, Chase and TWAP Testnet evidence artifacts exist with order IDs, clientOrderIds, request snapshots, and summaries.
Artifact/log serialization converts Decimal, Enum, datetime, and Path values before JSON encoding.
Calais-facing README/report content does not include the internal agentic-worker execution-plan header.
No logs or artifacts contain API keys, secret keys, signatures, listenKeys, or raw authenticated payloads.
```

## Self-Review Notes

Spec coverage:

```text
FastAPI create/query/cancel: Task 13
Target-position required quantity and NO_ACTION: Tasks 2, 8, 12
Parent/child state machine: Task 3
UNKNOWN child orders are reconcilable rather than terminal: Tasks 2, 3, 11, 12, 17
PENDING_CANCEL can reconcile back to live order state without freeing exposure unsafely: Tasks 3, 7, 12
Per-execution event serialization: Task 3 and Task 18 checklist
Decimal and side-aware rounding: Task 4
Exact tick/step and post-only crossing validation: Tasks 4, 12, 17
API decimal strings reject JSON floats: Task 13
Exposure buckets and overfill invariant: Task 11
Chase ADVERSE_ONLY/TWO_SIDED config: Task 9 plus config in Task 1
TWAP schedule and carry-forward math: Task 10
child_order_timeout_seconds: Tasks 10, 11, 12, and Task 14 scenarios
ExchangeAdapter interface: Task 5
Deterministic simulator: Tasks 5, 7, 14
Simulator artifact generation and smoke checks: Tasks 6, 14
Binance adapter, signing, recvWindow, rate limits, status mapping: Tasks 15, 16, 17
Binance exchangeInfo parsing, market-data freshness, and routed WebSocket paths: Tasks 15, 16, 17
Source-of-truth priority and REST reconciliation: Tasks 11, 12, 15, 16, 17
Log sanitization, JSON-safe artifact values, and summaries: Task 6
External position drift assumption: Task 14/18 documentation
Testnet credential gating and final evidence instructions: Tasks 16, 17, 18
README, AI_USAGE, report and failure case: Task 18
```

Known plan limits:

```text
The plan intentionally starts with a minimal engine and grows it through tests.
Mainnet remains configuration-compatible only and hard-disabled by default.
Binance Testnet order placement remains credential-gated and explicitly labeled; simulator demos never read Binance credentials.
```
