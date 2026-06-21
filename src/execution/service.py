from __future__ import annotations

from exchanges.base import ExchangeAdapter
from execution.engine import ExecutionEngine, ExecutionRecord
from execution.models import ExecutionRequest


class ExecutionService:
    def __init__(self, adapter: ExchangeAdapter) -> None:
        self._engine = ExecutionEngine(adapter)

    async def create_execution(self, request: ExecutionRequest) -> ExecutionRecord:
        return await self._engine.create_execution(request)

    async def get_execution(self, execution_id: str) -> ExecutionRecord:
        return await self._engine.get_execution(execution_id)

    async def cancel_execution(self, execution_id: str) -> ExecutionRecord:
        return await self._engine.cancel_execution(execution_id)
