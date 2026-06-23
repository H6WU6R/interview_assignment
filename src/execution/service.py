"""Application service facade for execution engine operations."""

from __future__ import annotations

from exchanges.base import ExchangeAdapter
from execution.clock import Clock, SystemClock
from execution.engine import ExecutionEngine, ExecutionRecord
from execution.models import Environment, ExecutionRequest, ReconciliationResult


class ExecutionService:
    """Application facade over the execution engine."""

    def __init__(self, adapter: ExchangeAdapter, clock: Clock | None = None) -> None:
        self._engine = ExecutionEngine(
            adapter, clock=clock or getattr(adapter, "clock", None) or SystemClock()
        )

    async def create_execution(self, request: ExecutionRequest) -> ExecutionRecord:
        return await self._engine.create_execution(request)

    async def active_execution_for(
        self,
        environment: Environment,
        symbol: str,
    ) -> ExecutionRecord | None:
        for record in await self.active_executions():
            if (
                record.request.environment is environment
                and record.request.symbol == symbol
            ):
                return record
        return None

    async def active_executions(self) -> list[ExecutionRecord]:
        active = []
        for execution_id in list(self._engine._records):
            record = await self._engine.get_execution(execution_id)
            if not record.status.is_terminal:
                active.append(record)
        return active

    async def get_execution(self, execution_id: str) -> ExecutionRecord:
        return await self._engine.get_execution(execution_id)

    async def cancel_execution(self, execution_id: str) -> ExecutionRecord:
        return await self._engine.cancel_execution(execution_id)

    async def run_once(self, execution_id: str) -> ExecutionRecord:
        return await self._engine.run_once(execution_id)

    async def reconcile_execution(
        self,
        execution_id: str,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> ExecutionRecord:
        return await self._engine.reconcile_execution(
            execution_id,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )

    async def apply_reconciliation_result(
        self,
        execution_id: str,
        result: ReconciliationResult,
    ) -> ExecutionRecord:
        return await self._engine.apply_reconciliation_result(execution_id, result)
