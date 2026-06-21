import asyncio
from decimal import Decimal

from exchanges.simulator import DeterministicSimulator
from execution.models import (
    Algorithm,
    DeadlinePolicy,
    Environment,
    ExecutionRequest,
    ExecutionStatus,
    Side,
)
from execution.service import ExecutionService


SYMBOL = "BTCUSDT"


def execution_request(target_position: Decimal) -> ExecutionRequest:
    return ExecutionRequest(
        environment=Environment.SIMULATION,
        symbol=SYMBOL,
        algorithm=Algorithm.CHASE,
        target_position=target_position,
        target_price_lower=Decimal("94000"),
        target_price_upper=Decimal("97000"),
        target_duration_seconds=300,
        deadline_policy=DeadlinePolicy.AGGRESSIVE_WITHIN_RANGE,
    )


async def test_no_action_target_already_reached_completes_immediately() -> None:
    simulator = DeterministicSimulator(position=Decimal("0.010"))
    service = ExecutionService(simulator)

    record = await service.create_execution(execution_request(Decimal("0.010")))

    assert record.status is ExecutionStatus.COMPLETED
    assert record.final_reason == "NO_ACTION_TARGET_ALREADY_REACHED"
    assert record.child_orders == []
    assert record.required_quantity == Decimal("0")
    assert record.side is Side.NO_ACTION
    assert record.initial_position.position == Decimal("0.010")
    assert record.summary is not None
    assert record.summary.execution_id == record.execution_id
    assert record.summary.final_status is ExecutionStatus.COMPLETED
    assert record.summary.final_reason == "NO_ACTION_TARGET_ALREADY_REACHED"


async def test_cancel_is_idempotent_for_completed_no_action_execution() -> None:
    simulator = DeterministicSimulator(position=Decimal("0.010"))
    service = ExecutionService(simulator)
    record = await service.create_execution(execution_request(Decimal("0.010")))

    cancelled = await service.cancel_execution(record.execution_id)

    assert cancelled.status is ExecutionStatus.COMPLETED
    assert cancelled.final_reason == "NO_ACTION_TARGET_ALREADY_REACHED"


async def test_create_nonzero_execution_runs_and_stores_normalized_quantity() -> None:
    simulator = DeterministicSimulator(position=Decimal("0"))
    service = ExecutionService(simulator)

    record = await service.create_execution(execution_request(Decimal("0.010")))

    assert record.status is ExecutionStatus.RUNNING
    assert record.side is Side.BUY
    assert record.required_quantity == Decimal("0.010")
    assert record.child_orders == []
    assert record.final_reason is None
    assert record.summary is None
    stored = await service.get_execution(record.execution_id)
    assert stored.execution_id == record.execution_id
    assert stored.status is ExecutionStatus.RUNNING
    reconciliation = await simulator.reconcile_orders_and_fills(SYMBOL)
    assert reconciliation.orders == []


async def test_sell_target_below_current_uses_absolute_quantity() -> None:
    simulator = DeterministicSimulator(position=Decimal("0.020"))
    service = ExecutionService(simulator)

    record = await service.create_execution(execution_request(Decimal("0.005")))

    assert record.status is ExecutionStatus.RUNNING
    assert record.side is Side.SELL
    assert record.required_quantity == Decimal("0.015")
    assert record.child_orders == []


async def test_cancel_running_execution_is_idempotent() -> None:
    simulator = DeterministicSimulator(position=Decimal("0"))
    service = ExecutionService(simulator)
    record = await service.create_execution(execution_request(Decimal("0.010")))

    first_cancel = await service.cancel_execution(record.execution_id)
    second_cancel = await service.cancel_execution(record.execution_id)

    assert first_cancel.status is ExecutionStatus.CANCELLING
    assert first_cancel.final_reason == "CANCEL_REQUESTED"
    assert second_cancel.status is ExecutionStatus.CANCELLING
    assert second_cancel.final_reason == "CANCEL_REQUESTED"


async def test_returned_snapshot_mutation_does_not_change_engine_state() -> None:
    simulator = DeterministicSimulator(position=Decimal("0"))
    service = ExecutionService(simulator)
    record = await service.create_execution(execution_request(Decimal("0.010")))

    record.status = ExecutionStatus.FAILED
    record.final_reason = "CALLER_MUTATED_SNAPSHOT"
    record.child_orders.append("not-a-child-order")  # type: ignore[arg-type]

    stored = await service.get_execution(record.execution_id)

    assert stored.status is ExecutionStatus.RUNNING
    assert stored.final_reason is None
    assert stored.child_orders == []


async def test_per_execution_actor_serializes_concurrent_cancels() -> None:
    simulator = DeterministicSimulator(position=Decimal("0"))
    service = ExecutionService(simulator)
    record = await service.create_execution(execution_request(Decimal("0.010")))

    results = await asyncio.gather(
        service.cancel_execution(record.execution_id),
        service.cancel_execution(record.execution_id),
        service.cancel_execution(record.execution_id),
        service.cancel_execution(record.execution_id),
    )

    assert all(result.status is ExecutionStatus.CANCELLING for result in results)
    assert all(result.final_reason == "CANCEL_REQUESTED" for result in results)
    stored = await service.get_execution(record.execution_id)
    assert stored.status is ExecutionStatus.CANCELLING
    assert stored.final_reason == "CANCEL_REQUESTED"
