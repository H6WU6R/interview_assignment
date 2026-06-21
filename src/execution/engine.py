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
    PositionSnapshot,
    Side,
    required_trade,
)
from execution.state_machine import transition_execution


NO_ACTION_TARGET_ALREADY_REACHED = "NO_ACTION_TARGET_ALREADY_REACHED"
CANCEL_REQUESTED = "CANCEL_REQUESTED"


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
