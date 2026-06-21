from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from exchanges.base import ExchangeAdapter
from execution import ids
from execution.events import ExecutionEventActor
from execution.models import (
    ChildOrder,
    ExecutionRequest,
    ExecutionStatus,
    ExecutionSummary,
    Exposure,
    PositionSnapshot,
    Side,
    required_trade,
)
from execution.state_machine import transition_execution
from risk.validation import ValidationError, check_exposure_invariant


NO_ACTION_TARGET_ALREADY_REACHED = "NO_ACTION_TARGET_ALREADY_REACHED"
CANCEL_REQUESTED = "CANCEL_REQUESTED"


@dataclass
class ExposureTracker:
    target_quantity: Decimal
    exposure: Exposure = field(default_factory=Exposure)
    seen_trade_ids: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        self._require_non_negative(self.target_quantity)

    def available_to_submit(self) -> Decimal:
        available = (
            self.target_quantity
            - self.exposure.confirmed_filled_quantity
            - self.exposure.reserved_exposure
        )
        return available if available > Decimal("0") else Decimal("0")

    def check_can_submit(self, new_child_quantity: Decimal) -> None:
        self._require_non_negative(new_child_quantity)
        check_exposure_invariant(
            self.exposure,
            new_child_quantity,
            self.target_quantity,
        )

    def reserve_live_open(self, quantity: Decimal) -> None:
        self.check_can_submit(quantity)
        self.exposure.live_open_quantity += quantity

    def reserve_pending_submit(self, quantity: Decimal) -> None:
        self.check_can_submit(quantity)
        self.exposure.pending_submit_quantity += quantity

    def release_pending_submit(self, quantity: Decimal) -> None:
        self._require_non_negative(quantity)
        self.exposure.pending_submit_quantity = self._subtract_floor_zero(
            self.exposure.pending_submit_quantity,
            quantity,
        )

    def reserve_unknown_create(self, quantity: Decimal) -> None:
        self.check_can_submit(quantity)
        self.exposure.unknown_order_quantity += quantity

    def clear_unknown_create(self, quantity: Decimal | None = None) -> None:
        if quantity is None:
            self.exposure.unknown_order_quantity = Decimal("0")
            return

        self._require_non_negative(quantity)
        self.exposure.unknown_order_quantity = self._subtract_floor_zero(
            self.exposure.unknown_order_quantity,
            quantity,
        )

    def mark_pending_cancel(self, quantity: Decimal) -> None:
        self._require_non_negative(quantity)
        moved_quantity = min(quantity, self.exposure.live_open_quantity)
        self.exposure.live_open_quantity -= moved_quantity
        self.exposure.pending_cancel_quantity += moved_quantity

    def release_pending_cancel(self, quantity: Decimal) -> None:
        self._require_non_negative(quantity)
        self.exposure.pending_cancel_quantity = self._subtract_floor_zero(
            self.exposure.pending_cancel_quantity,
            quantity,
        )

    def set_live_open(self, quantity: Decimal) -> None:
        self._require_non_negative(quantity)
        self.exposure.live_open_quantity = quantity

    def apply_fill(self, trade_id: str | None, cumulative: Decimal) -> None:
        self._require_non_negative(cumulative)
        if trade_id is not None:
            if trade_id in self.seen_trade_ids:
                return
            self.seen_trade_ids.add(trade_id)

        if cumulative <= self.exposure.confirmed_filled_quantity:
            return

        self.exposure.confirmed_filled_quantity = cumulative

    @staticmethod
    def _require_non_negative(quantity: Decimal) -> None:
        if quantity < Decimal("0"):
            raise ValidationError(f"quantity {quantity} cannot be negative")

    @staticmethod
    def _subtract_floor_zero(current: Decimal, quantity: Decimal) -> Decimal:
        remaining = current - quantity
        return remaining if remaining > Decimal("0") else Decimal("0")


@dataclass
class ExecutionRecord:
    execution_id: str
    request: ExecutionRequest
    status: ExecutionStatus
    side: Side
    required_quantity: Decimal
    initial_position: PositionSnapshot
    final_reason: str | None = None
    child_orders: list[ChildOrder] = field(default_factory=list)
    summary: ExecutionSummary | None = None


class ExecutionEngine:
    def __init__(self, adapter: ExchangeAdapter) -> None:
        self._adapter = adapter
        self._records: dict[str, ExecutionRecord] = {}
        self._actors: dict[str, ExecutionEventActor] = {}

    async def create_execution(self, request: ExecutionRequest) -> ExecutionRecord:
        position = await self._adapter.get_position(request.symbol)
        side, required_quantity = required_trade(
            target_position=request.target_position,
            current_position=position.position,
        )
        execution_id = ids.execution_id()
        record = ExecutionRecord(
            execution_id=execution_id,
            request=request,
            status=ExecutionStatus.CREATED,
            side=side,
            required_quantity=required_quantity,
            initial_position=position,
        )
        actor = ExecutionEventActor(execution_id)
        self._records[execution_id] = record
        self._actors[execution_id] = actor

        async def start() -> ExecutionRecord:
            record.status = transition_execution(record.status, ExecutionStatus.VALIDATING)
            if side is Side.NO_ACTION or required_quantity == Decimal("0"):
                record.status = transition_execution(record.status, ExecutionStatus.COMPLETED)
                record.final_reason = NO_ACTION_TARGET_ALREADY_REACHED
                record.summary = self._summary(record)
                return self._snapshot(record)

            record.status = transition_execution(record.status, ExecutionStatus.RUNNING)
            return self._snapshot(record)

        return await actor.apply(start)

    async def get_execution(self, execution_id: str) -> ExecutionRecord:
        record = self._records[execution_id]
        actor = self._actors[execution_id]

        async def read() -> ExecutionRecord:
            return self._snapshot(record)

        return await actor.apply(read)

    async def cancel_execution(self, execution_id: str) -> ExecutionRecord:
        record = self._records[execution_id]
        actor = self._actors[execution_id]

        async def cancel() -> ExecutionRecord:
            if record.status.is_terminal or record.status is ExecutionStatus.CANCELLING:
                return self._snapshot(record)

            record.status = transition_execution(record.status, ExecutionStatus.CANCELLING)
            record.final_reason = CANCEL_REQUESTED
            return self._snapshot(record)

        return await actor.apply(cancel)

    def _summary(self, record: ExecutionRecord) -> ExecutionSummary:
        return ExecutionSummary(
            execution_id=record.execution_id,
            final_status=record.status,
            final_reason=record.final_reason or "",
            metrics=self._summary_metrics(record),
        )

    def _summary_metrics(self, record: ExecutionRecord) -> dict[str, Any]:
        return {
            "initial_position": record.initial_position.position,
            "target_position": record.request.target_position,
            "required_quantity": record.required_quantity,
            "side": record.side,
            "child_order_count": len(record.child_orders),
        }

    def _snapshot(self, record: ExecutionRecord) -> ExecutionRecord:
        return deepcopy(record)
