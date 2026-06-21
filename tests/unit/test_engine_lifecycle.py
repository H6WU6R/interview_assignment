from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from exchanges.simulator import DeterministicSimulator
from execution.clock import ManualClock
from execution.ids import make_client_order_prefix
from execution.models import (
    Algorithm,
    ChildOrderStatus,
    DeadlinePolicy,
    Environment,
    ExecutionParameters,
    ExecutionRequest,
    ExecutionStatus,
    SymbolRules,
)
from execution.service import ExecutionService


SYMBOL = "BTCUSDT"


def execution_request(
    *,
    algorithm: Algorithm = Algorithm.CHASE,
    target_position: Decimal = Decimal("0.010"),
    lower: Decimal = Decimal("94000"),
    upper: Decimal = Decimal("97000"),
    duration: int = 100,
    parameters: ExecutionParameters | None = None,
) -> ExecutionRequest:
    return ExecutionRequest(
        environment=Environment.SIMULATION,
        symbol=SYMBOL,
        algorithm=algorithm,
        target_position=target_position,
        target_price_lower=lower,
        target_price_upper=upper,
        target_duration_seconds=duration,
        deadline_policy=DeadlinePolicy.AGGRESSIVE_WITHIN_RANGE,
        parameters=parameters or ExecutionParameters(),
    )


async def fresh_service(
    *,
    clock: ManualClock | None = None,
    bid: Decimal = Decimal("95000.00"),
    ask: Decimal = Decimal("95001.00"),
    position: Decimal = Decimal("0"),
) -> tuple[ExecutionService, DeterministicSimulator, ManualClock]:
    clock = clock or ManualClock()
    simulator = DeterministicSimulator(clock=clock, position=position)
    await simulator.push_market_data(SYMBOL, bid, ask, exchange_event_time=10)
    return ExecutionService(simulator, clock=clock), simulator, clock


def test_static_single_submit_gate() -> None:
    source = Path("src/execution/engine.py").read_text()

    assert "def _submit_child_locked(" in source
    assert source.count(".submit_limit_order(") == 1


async def test_chase_run_once_submits_one_safe_child() -> None:
    service, simulator, _ = await fresh_service()
    execution = await service.create_execution(execution_request())

    snapshot = await service.run_once(execution.execution_id)
    prefix = make_client_order_prefix(execution.execution_id)
    reconciliation = await simulator.reconcile_orders_and_fills(SYMBOL, client_order_prefix=prefix)

    assert snapshot.status is ExecutionStatus.RUNNING
    assert len(snapshot.child_orders) == 1
    assert snapshot.child_orders[0].status is ChildOrderStatus.OPEN
    assert snapshot.child_orders[0].submitted_quantity == Decimal("0.010")
    assert snapshot.exposure.live_open_quantity == Decimal("0.010")
    assert snapshot.exposure.reserved_exposure <= snapshot.required_quantity
    assert [order.client_order_id for order in reconciliation.orders] == [
        snapshot.child_orders[0].client_order_id
    ]


async def test_create_timeout_discoverable_reserves_unknown_until_reconcile_maps_live_open() -> None:
    service, simulator, _ = await fresh_service()
    execution = await service.create_execution(execution_request())
    prefix = make_client_order_prefix(execution.execution_id)
    simulator.script_create_timeout(prefix)

    timed_out = await service.run_once(execution.execution_id)

    assert len(timed_out.child_orders) == 1
    assert timed_out.child_orders[0].status is ChildOrderStatus.UNKNOWN
    assert timed_out.exposure.unknown_order_quantity == Decimal("0.010")
    assert timed_out.exposure.reserved_exposure == timed_out.required_quantity

    reconciled = await service.reconcile_execution(execution.execution_id)

    assert reconciled.child_orders[0].status is ChildOrderStatus.OPEN
    assert reconciled.exposure.unknown_order_quantity == Decimal("0")
    assert reconciled.exposure.live_open_quantity == Decimal("0.010")


async def test_create_timeout_not_found_clears_unknown_and_retries_with_new_client_order_id() -> None:
    service, simulator, _ = await fresh_service()
    execution = await service.create_execution(execution_request())
    prefix = make_client_order_prefix(execution.execution_id)
    simulator.script_create_timeout_not_found(prefix)

    timed_out = await service.run_once(execution.execution_id)
    reconciled = await service.reconcile_execution(execution.execution_id)
    retried = await service.run_once(execution.execution_id)

    assert timed_out.child_orders[0].status is ChildOrderStatus.UNKNOWN
    assert reconciled.exposure.unknown_order_quantity == Decimal("0")
    assert len(retried.child_orders) == 2
    assert retried.child_orders[0].status is ChildOrderStatus.REJECTED
    assert retried.child_orders[0].terminal_reason == "CREATE_TIMEOUT_ORDER_NOT_FOUND"
    assert retried.child_orders[1].status is ChildOrderStatus.OPEN
    assert retried.child_orders[1].client_order_id != retried.child_orders[0].client_order_id


async def test_fill_during_cancel_reduces_replacement_size_without_overfill() -> None:
    clock = ManualClock()
    service, simulator, _ = await fresh_service(clock=clock)
    execution = await service.create_execution(execution_request())
    first = await service.run_once(execution.execution_id)
    prefix = make_client_order_prefix(execution.execution_id)
    simulator.script_fill_during_cancel(prefix, Decimal("0.004"))
    clock.advance(0.5)
    await simulator.push_market_data(SYMBOL, Decimal("96000.00"), Decimal("96001.00"), exchange_event_time=20)

    repriced = await service.run_once(execution.execution_id)

    assert len(first.child_orders) == 1
    assert len(repriced.child_orders) == 2
    assert repriced.child_orders[0].status is ChildOrderStatus.CANCELLED
    assert repriced.child_orders[0].confirmed_filled_quantity == Decimal("0.004")
    assert repriced.child_orders[1].status is ChildOrderStatus.OPEN
    assert repriced.child_orders[1].submitted_quantity == Decimal("0.006")
    assert repriced.exposure.confirmed_filled_quantity == Decimal("0.004")
    assert repriced.exposure.live_open_quantity == Decimal("0.006")
    assert repriced.exposure.confirmed_filled_quantity + repriced.exposure.reserved_exposure <= Decimal("0.010")


async def test_twap_uses_absolute_schedule_and_carries_forward_unfilled_deficit() -> None:
    clock = ManualClock()
    service, simulator, _ = await fresh_service(
        clock=clock,
        bid=Decimal("100.00"),
        ask=Decimal("101.00"),
    )
    execution = await service.create_execution(
        execution_request(
            algorithm=Algorithm.TWAP,
            target_position=Decimal("1.000"),
            lower=Decimal("90"),
            upper=Decimal("110"),
            duration=100,
        )
    )
    clock.advance(30)
    await simulator.push_market_data(SYMBOL, Decimal("100.00"), Decimal("101.00"), exchange_event_time=20)

    first = await service.run_once(execution.execution_id)
    clock.advance(30)
    await simulator.push_market_data(SYMBOL, Decimal("100.00"), Decimal("101.00"), exchange_event_time=30)
    still_reserved = await service.run_once(execution.execution_id)
    await simulator.cancel_order(SYMBOL, first.child_orders[0].client_order_id)
    carried_forward = await service.run_once(execution.execution_id)

    assert first.child_orders[0].submitted_quantity == Decimal("0.300")
    assert len(still_reserved.child_orders) == 1
    assert carried_forward.child_orders[0].status is ChildOrderStatus.CANCELLED
    assert carried_forward.child_orders[1].submitted_quantity == Decimal("0.600")


async def test_price_outside_range_rejects_before_submit_without_fake_completion() -> None:
    service, simulator, _ = await fresh_service()
    execution = await service.create_execution(execution_request(upper=Decimal("94999")))
    prefix = make_client_order_prefix(execution.execution_id)

    snapshot = await service.run_once(execution.execution_id)
    reconciliation = await simulator.reconcile_orders_and_fills(SYMBOL, client_order_prefix=prefix)

    assert snapshot.status is ExecutionStatus.EXPIRED
    assert snapshot.final_reason == "PRICE_OUTSIDE_RANGE"
    assert snapshot.child_orders == []
    assert reconciliation.orders == []


async def test_post_only_unsupported_rejects_before_submit() -> None:
    service, simulator, _ = await fresh_service()
    simulator.set_symbol_rules(
        SymbolRules(
            symbol=SYMBOL,
            tick_size=Decimal("0.10"),
            quantity_step=Decimal("0.001"),
            min_quantity=Decimal("0.001"),
            min_notional=Decimal("5"),
            status="TRADING",
            supported_time_in_force=frozenset({"GTC"}),
        )
    )
    execution = await service.create_execution(execution_request())
    prefix = make_client_order_prefix(execution.execution_id)

    snapshot = await service.run_once(execution.execution_id)
    reconciliation = await simulator.reconcile_orders_and_fills(SYMBOL, client_order_prefix=prefix)

    assert snapshot.status is ExecutionStatus.EXPIRED
    assert snapshot.final_reason == "POST_ONLY_CROSSES"
    assert snapshot.child_orders == []
    assert reconciliation.orders == []


async def test_stream_health_failure_pauses_new_submit_and_reconciles() -> None:
    service, simulator, _ = await fresh_service()
    execution = await service.create_execution(execution_request())
    simulator.set_stream_health(user_stream_healthy=False)

    snapshot = await service.run_once(execution.execution_id)

    assert snapshot.status is ExecutionStatus.RUNNING
    assert snapshot.final_reason == "STREAM_HEALTH_DEGRADED_RECONCILED"
    assert snapshot.child_orders == []
    assert snapshot.exposure.reserved_exposure == Decimal("0")


async def test_duplicate_fill_reconciliation_does_not_double_count_confirmed_fills() -> None:
    service, simulator, _ = await fresh_service()
    execution = await service.create_execution(execution_request())
    opened = await service.run_once(execution.execution_id)
    await simulator.push_fill(opened.child_orders[0].client_order_id, Decimal("0.004"), Decimal("95000.00"))

    first_reconcile = await service.reconcile_execution(execution.execution_id)
    second_reconcile = await service.reconcile_execution(execution.execution_id)

    assert first_reconcile.exposure.confirmed_filled_quantity == Decimal("0.004")
    assert second_reconcile.exposure.confirmed_filled_quantity == Decimal("0.004")
