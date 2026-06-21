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


@dataclass(frozen=True)
class ReconciliationResult:
    orders: list[ChildOrder]
    fills: list[Fill]
    warnings: list[str] = field(default_factory=list)


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


def required_trade(
    target_position: Decimal,
    current_position: Decimal,
) -> tuple[Side, Decimal]:
    quantity = target_position - current_position
    if quantity > Decimal("0"):
        return Side.BUY, quantity
    if quantity < Decimal("0"):
        return Side.SELL, abs(quantity)
    return Side.NO_ACTION, Decimal("0")
