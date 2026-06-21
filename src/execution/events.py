from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")


class ExecutionEventActor:
    def __init__(self, execution_id: str) -> None:
        self.execution_id = execution_id
        # Serializes local state mutation only. Exchange event times E/T are still retained
        # for audit and reconciliation ordering diagnostics.
        self._lock = asyncio.Lock()

    async def apply(self, operation: Callable[[], Awaitable[T]]) -> T:
        async with self._lock:
            return await operation()
