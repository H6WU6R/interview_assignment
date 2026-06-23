from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from api.runtime import ExecutionRuntime
from execution.engine import ExecutionRecord
from execution.ids import make_client_order_prefix
from execution.models import (
    Algorithm,
    DeadlinePolicy,
    Environment,
    ExecutionParameters,
    ExecutionRequest,
    ExecutionStatus,
    Fill,
    PositionSnapshot,
    ReconciliationResult,
    Side,
)


SYMBOL = "BTCUSDT"


def _request() -> ExecutionRequest:
    return ExecutionRequest(
        environment=Environment.SIMULATION,
        symbol=SYMBOL,
        algorithm=Algorithm.CHASE,
        target_position=Decimal("0.010"),
        target_price_lower=Decimal("94000"),
        target_price_upper=Decimal("97000"),
        target_duration_seconds=300,
        deadline_policy=DeadlinePolicy.AGGRESSIVE_WITHIN_RANGE,
        parameters=ExecutionParameters(),
    )


def _record(execution_id: str = "exec_0123456789abcdef") -> ExecutionRecord:
    return ExecutionRecord(
        execution_id=execution_id,
        request=_request(),
        status=ExecutionStatus.RUNNING,
        side=Side.BUY,
        required_quantity=Decimal("0.010"),
        raw_required_quantity=Decimal("0.010"),
        initial_position=PositionSnapshot(symbol=SYMBOL, position=Decimal("0")),
    )


def _result_for(record: ExecutionRecord, *, event_time_ms: int) -> ReconciliationResult:
    return ReconciliationResult(
        orders=[],
        fills=[
            Fill(
                client_order_id=f"{make_client_order_prefix(record.execution_id)}1",
                trade_id="stream-trade",
                cumulative_filled_quantity=Decimal("0.004"),
                last_filled_quantity=Decimal("0.004"),
                last_fill_price=Decimal("95000"),
                event_time_ms=event_time_ms,
                transaction_time_ms=event_time_ms,
                is_maker=True,
            )
        ],
    )


class _UserEventAdapter:
    def __init__(self, result: ReconciliationResult) -> None:
        self._result = result

    def reconciliation_from_user_event(self, _event: object) -> ReconciliationResult:
        return self._result


class _UserEventService:
    def __init__(
        self,
        record: ExecutionRecord,
        *,
        fail_apply: bool = False,
        fail_active_lookup: bool = False,
    ) -> None:
        self.record = record
        self.fail_apply = fail_apply
        self.fail_active_lookup = fail_active_lookup
        self.applied_results: list[ReconciliationResult] = []

    async def get_execution(self, execution_id: str) -> ExecutionRecord:
        assert execution_id == self.record.execution_id
        return self.record

    async def active_executions(self) -> list[ExecutionRecord]:
        if self.fail_active_lookup:
            raise RuntimeError("active lookup failed")
        return [self.record]

    async def apply_reconciliation_result(
        self,
        execution_id: str,
        result: ReconciliationResult,
    ) -> ExecutionRecord:
        assert execution_id == self.record.execution_id
        self.applied_results.append(result)
        if self.fail_apply:
            raise RuntimeError("apply failed")
        return self.record


async def _install_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    event_time_ms: int,
    fail_apply: bool = False,
    fail_active_lookup: bool = False,
) -> tuple[ExecutionRuntime, _UserEventService, list[tuple[Environment, int | None, int | None]]]:
    runtime = ExecutionRuntime()
    record = _record()
    service = _UserEventService(
        record,
        fail_apply=fail_apply,
        fail_active_lookup=fail_active_lookup,
    )
    runtime._services[Environment.SIMULATION] = service  # type: ignore[assignment]
    runtime._adapters[Environment.SIMULATION] = _UserEventAdapter(
        _result_for(record, event_time_ms=event_time_ms)
    )
    runtime._remember_execution(record)

    fallback_calls: list[tuple[Environment, int | None, int | None]] = []

    async def record_fallback(
        environment: Environment,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> None:
        fallback_calls.append((environment, start_time_ms, end_time_ms))

    monkeypatch.setattr(
        runtime,
        "_reconcile_active_executions_for_environment",
        record_fallback,
    )
    return runtime, service, fallback_calls


@pytest.mark.asyncio
async def test_user_event_apply_failure_falls_back_to_bounded_rest_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, service, fallback_calls = await _install_runtime(
        monkeypatch,
        event_time_ms=222_000,
        fail_apply=True,
    )

    await runtime._reconcile_active_executions_for_user_event(
        Environment.SIMULATION,
        {"event_time_ms": 222_000},
    )

    assert len(service.applied_results) == 1
    assert fallback_calls == [(Environment.SIMULATION, 162_000, 222_000)]
    assert runtime.runtime_errors["exec_0123456789abcdef"] == ["RuntimeError: apply failed"]


@pytest.mark.asyncio
async def test_active_lookup_failure_falls_back_to_bounded_rest_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, service, fallback_calls = await _install_runtime(
        monkeypatch,
        event_time_ms=98_765,
        fail_active_lookup=True,
    )

    await runtime._reconcile_active_executions_for_user_event(
        Environment.SIMULATION,
        {"event_time_ms": 98_765},
    )

    assert len(service.applied_results) == 1
    assert fallback_calls == [(Environment.SIMULATION, 38_765, 98_765)]
    assert runtime.runtime_errors["simulation.active_executions"] == [
        "RuntimeError: active lookup failed"
    ]


@pytest.mark.asyncio
async def test_successful_user_event_application_avoids_extra_rest_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, service, fallback_calls = await _install_runtime(
        monkeypatch,
        event_time_ms=123_456,
    )

    await runtime._reconcile_active_executions_for_user_event(
        Environment.SIMULATION,
        {"event_time_ms": 123_456},
    )

    assert len(service.applied_results) == 1
    assert fallback_calls == []
