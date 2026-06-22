from __future__ import annotations

from decimal import Decimal

from execution.ids import make_client_order_prefix
from execution.models import (
    ChildOrder,
    ChildOrderStatus,
    DeadlinePolicy,
    ExecutionStatus,
    Fill,
    ReconciliationResult,
    Side,
)
from test_engine_lifecycle import SYMBOL, execution_request, fresh_service


async def test_unknown_create_cancel_reconciles_then_cancels_open_child() -> None:
    service, simulator, _ = await fresh_service()
    execution = await service.create_execution(execution_request())
    prefix = make_client_order_prefix(execution.execution_id)
    simulator.script_create_timeout(prefix)

    after_timeout = await service.run_once(execution.execution_id)
    assert after_timeout.status is ExecutionStatus.RUNNING
    assert after_timeout.child_orders[0].status is ChildOrderStatus.UNKNOWN
    assert after_timeout.exposure.unknown_order_quantity == Decimal("0.010")

    after_cancel = await service.cancel_execution(execution.execution_id)

    assert after_cancel.status is ExecutionStatus.CANCELLED
    assert after_cancel.exposure.reserved_exposure == Decimal("0")
    assert after_cancel.child_orders[0].status is ChildOrderStatus.CANCELLED
    assert after_cancel.child_orders[0].exchange_order_id is not None


async def test_unhealthy_stream_does_not_block_expiry() -> None:
    service, simulator, clock = await fresh_service()
    execution = await service.create_execution(execution_request(duration=1))
    opened = await service.run_once(execution.execution_id)
    assert opened.status is ExecutionStatus.RUNNING
    assert opened.child_orders

    simulator.set_stream_health(user_stream_healthy=False)
    clock.advance(5)
    expired = await service.run_once(execution.execution_id)

    assert expired.status is ExecutionStatus.EXPIRED
    assert expired.completed_monotonic is not None
    assert expired.final_reason == "STREAM_HEALTH_DEGRADED_RECONCILED"


async def test_aggressive_deadline_with_stale_market_terminalizes() -> None:
    service, _simulator, clock = await fresh_service()
    execution = await service.create_execution(
        execution_request(
            duration=1,
            deadline_policy=DeadlinePolicy.AGGRESSIVE_WITHIN_RANGE,
        )
    )
    clock.advance(120)

    terminal = await service.run_once(execution.execution_id)

    assert terminal.status is ExecutionStatus.EXPIRED
    assert terminal.completed_monotonic is not None
    assert terminal.final_reason in {"DEADLINE_AGGRESSIVE_ATTEMPTED", "MARKET_DATA_STALE_RECONCILED"}
    assert terminal.summary is not None


async def test_late_actual_trade_price_replaces_snapshot_limit_price_for_vwap() -> None:
    service, _simulator, _clock = await fresh_service()
    execution = await service.create_execution(execution_request(target_position=Decimal("0.005")))
    opened = await service.run_once(execution.execution_id)
    child = opened.child_orders[0]

    snapshot_order = ChildOrder(
        child_order_id=child.child_order_id,
        client_order_id=child.client_order_id,
        symbol=SYMBOL,
        side=Side.BUY,
        submitted_quantity=child.submitted_quantity,
        price=child.price,
        status=ChildOrderStatus.FILLED,
        confirmed_filled_quantity=Decimal("0.005"),
        exchange_order_id="order-1",
        raw_status="FILLED",
    )
    after_snapshot = await service.apply_reconciliation_result(
        execution.execution_id,
        ReconciliationResult(orders=[snapshot_order], fills=[]),
    )
    assert after_snapshot.summary is not None
    assert after_snapshot.summary.metrics["execution_vwap"] == "95000"

    actual_fill = Fill(
        client_order_id=child.client_order_id,
        trade_id="actual-trade-1",
        cumulative_filled_quantity=Decimal("0.005"),
        last_filled_quantity=Decimal("0.005"),
        last_fill_price=Decimal("49999.5"),
        event_time_ms=1,
        transaction_time_ms=1,
        is_maker=True,
    )
    after_fill = await service.apply_reconciliation_result(
        execution.execution_id,
        ReconciliationResult(orders=[], fills=[actual_fill]),
    )

    assert after_fill.summary is not None
    assert after_fill.summary.metrics["execution_vwap"] == "49999.5"
    assert after_fill.metric_counts.get("duplicate_events_ignored", 0) == 0


async def test_out_of_order_fills_compute_vwap_from_each_trade_delta() -> None:
    service, _simulator, _clock = await fresh_service()
    execution = await service.create_execution(execution_request(target_position=Decimal("0.006")))
    opened = await service.run_once(execution.execution_id)
    child = opened.child_orders[0]

    second_fill_first = Fill(
        client_order_id=child.client_order_id,
        trade_id="trade-2",
        cumulative_filled_quantity=Decimal("0.006"),
        last_filled_quantity=Decimal("0.002"),
        last_fill_price=Decimal("95010"),
        event_time_ms=20,
        transaction_time_ms=20,
        is_maker=False,
    )
    first_fill_late = Fill(
        client_order_id=child.client_order_id,
        trade_id="trade-1",
        cumulative_filled_quantity=Decimal("0.004"),
        last_filled_quantity=Decimal("0.004"),
        last_fill_price=Decimal("95005"),
        event_time_ms=10,
        transaction_time_ms=10,
        is_maker=True,
    )

    after = await service.apply_reconciliation_result(
        execution.execution_id,
        ReconciliationResult(orders=[], fills=[second_fill_first, first_fill_late]),
    )

    assert after.summary is not None
    assert after.summary.metrics["execution_vwap"] == "95006.66666666666666666666667"


async def test_split_batch_out_of_order_actual_trade_repairs_vwap_without_exposure_change() -> None:
    service, _simulator, _clock = await fresh_service()
    execution = await service.create_execution(execution_request(target_position=Decimal("0.006")))
    opened = await service.run_once(execution.execution_id)
    child = opened.child_orders[0]

    second_fill_first = Fill(
        client_order_id=child.client_order_id,
        trade_id="split-trade-2",
        cumulative_filled_quantity=Decimal("0.006"),
        last_filled_quantity=Decimal("0.002"),
        last_fill_price=Decimal("95010"),
        event_time_ms=20,
        transaction_time_ms=20,
        is_maker=False,
    )
    after_second = await service.apply_reconciliation_result(
        execution.execution_id,
        ReconciliationResult(orders=[], fills=[second_fill_first]),
    )

    assert after_second.exposure.confirmed_filled_quantity == Decimal("0.006")
    assert after_second.summary is not None
    assert after_second.summary.metrics["execution_vwap"] == "95010"

    first_fill_late = Fill(
        client_order_id=child.client_order_id,
        trade_id="split-trade-1",
        cumulative_filled_quantity=Decimal("0.004"),
        last_filled_quantity=Decimal("0.004"),
        last_fill_price=Decimal("95005"),
        event_time_ms=10,
        transaction_time_ms=10,
        is_maker=True,
    )
    after_late_first = await service.apply_reconciliation_result(
        execution.execution_id,
        ReconciliationResult(orders=[], fills=[first_fill_late]),
    )

    assert after_late_first.exposure.confirmed_filled_quantity == Decimal("0.006")
    assert after_late_first.child_orders[0].confirmed_filled_quantity == Decimal("0.006")
    assert after_late_first.summary is not None
    assert (
        after_late_first.summary.metrics["execution_vwap"]
        == "95006.66666666666666666666667"
    )
    assert after_late_first.summary.metrics["maker_filled_quantity"] == Decimal("0.004")
    assert after_late_first.summary.metrics["taker_filled_quantity"] == Decimal("0.002")
    assert after_late_first.metric_counts.get("duplicate_events_ignored", 0) == 0

    after_duplicate = await service.apply_reconciliation_result(
        execution.execution_id,
        ReconciliationResult(orders=[], fills=[first_fill_late]),
    )

    assert after_duplicate.exposure.confirmed_filled_quantity == Decimal("0.006")
    assert after_duplicate.summary is not None
    assert (
        after_duplicate.summary.metrics["execution_vwap"]
        == "95006.66666666666666666666667"
    )
    assert after_duplicate.summary.metrics["maker_filled_quantity"] == Decimal("0.004")
    assert after_duplicate.summary.metrics["taker_filled_quantity"] == Decimal("0.002")
    assert after_duplicate.metric_counts["duplicate_events_ignored"] == 1


async def test_unique_lower_cumulative_trade_does_not_overcount_authoritative_metrics() -> None:
    service, _simulator, _clock = await fresh_service()
    execution = await service.create_execution(execution_request(target_position=Decimal("0.003")))
    opened = await service.run_once(execution.execution_id)
    child = opened.child_orders[0]

    first_fill = Fill(
        client_order_id=child.client_order_id,
        trade_id="trade-real",
        cumulative_filled_quantity=Decimal("0.003"),
        last_filled_quantity=Decimal("0.003"),
        last_fill_price=Decimal("95000"),
        event_time_ms=10,
        transaction_time_ms=10,
        is_maker=True,
    )
    after_first = await service.apply_reconciliation_result(
        execution.execution_id,
        ReconciliationResult(orders=[], fills=[first_fill]),
    )

    assert after_first.exposure.confirmed_filled_quantity == Decimal("0.003")
    assert after_first.summary is not None
    assert after_first.summary.metrics["execution_vwap"] == "95000"
    assert after_first.summary.metrics["maker_filled_quantity"] == Decimal("0.003")

    stale_lower_fill = Fill(
        client_order_id=child.client_order_id,
        trade_id="trade-stale-lower",
        cumulative_filled_quantity=Decimal("0.002"),
        last_filled_quantity=Decimal("0.002"),
        last_fill_price=Decimal("96000"),
        event_time_ms=20,
        transaction_time_ms=20,
        is_maker=True,
    )
    after_stale = await service.apply_reconciliation_result(
        execution.execution_id,
        ReconciliationResult(orders=[], fills=[stale_lower_fill]),
    )

    assert after_stale.exposure.confirmed_filled_quantity == Decimal("0.003")
    assert after_stale.child_orders[0].confirmed_filled_quantity == Decimal("0.003")
    assert after_stale.summary is not None
    assert after_stale.summary.metrics["execution_vwap"] == "95000"
    assert after_stale.summary.metrics["maker_filled_quantity"] == Decimal("0.003")
    assert after_stale.metric_counts["duplicate_events_ignored"] == 1


async def test_snapshot_then_older_actual_trade_repairs_provisional_summary() -> None:
    service, _simulator, _clock = await fresh_service()
    execution = await service.create_execution(execution_request(target_position=Decimal("0.006")))
    opened = await service.run_once(execution.execution_id)
    child = opened.child_orders[0]

    snapshot_order = ChildOrder(
        child_order_id=child.child_order_id,
        client_order_id=child.client_order_id,
        symbol=SYMBOL,
        side=Side.BUY,
        submitted_quantity=child.submitted_quantity,
        price=child.price,
        status=ChildOrderStatus.PARTIALLY_FILLED,
        confirmed_filled_quantity=Decimal("0.006"),
        exchange_order_id="snapshot-order-1",
        raw_status="PARTIALLY_FILLED",
    )
    after_snapshot = await service.apply_reconciliation_result(
        execution.execution_id,
        ReconciliationResult(orders=[snapshot_order], fills=[]),
    )

    assert after_snapshot.exposure.confirmed_filled_quantity == Decimal("0.006")
    assert after_snapshot.summary is not None
    assert after_snapshot.summary.metrics["execution_vwap"] == "95000"

    older_actual = Fill(
        client_order_id=child.client_order_id,
        trade_id="snapshot-repair-trade-1",
        cumulative_filled_quantity=Decimal("0.004"),
        last_filled_quantity=Decimal("0.004"),
        last_fill_price=Decimal("94990"),
        event_time_ms=10,
        transaction_time_ms=10,
        is_maker=True,
    )
    after_actual = await service.apply_reconciliation_result(
        execution.execution_id,
        ReconciliationResult(orders=[], fills=[older_actual]),
    )

    assert after_actual.exposure.confirmed_filled_quantity == Decimal("0.006")
    assert after_actual.child_orders[0].confirmed_filled_quantity == Decimal("0.006")
    assert after_actual.summary is not None
    assert after_actual.summary.metrics["execution_vwap"] == "94990"
    assert after_actual.summary.metrics["maker_filled_quantity"] == Decimal("0.004")
    assert after_actual.summary.metrics["taker_filled_quantity"] == Decimal("0")
    assert after_actual.metric_counts.get("duplicate_events_ignored", 0) == 0


async def test_temporary_sell_min_notional_waits_until_price_becomes_valid() -> None:
    service, simulator, _clock = await fresh_service(
        bid=Decimal("4000"),
        ask=Decimal("4001"),
        position=Decimal("0"),
    )
    execution = await service.create_execution(
        execution_request(
            target_position=Decimal("-0.001"),
            lower=Decimal("1"),
            upper=Decimal("100000"),
            duration=100,
        )
    )

    waiting = await service.run_once(execution.execution_id)
    assert waiting.status is ExecutionStatus.RUNNING
    assert waiting.child_orders == []
    assert waiting.final_reason == "ORDER_SHAPE_TEMPORARILY_UNTRADEABLE"

    await simulator.push_market_data(SYMBOL, Decimal("5000"), Decimal("5001"), exchange_event_time=20)
    active = await service.run_once(execution.execution_id)

    assert active.status is ExecutionStatus.RUNNING
    assert len(active.child_orders) == 1
    assert active.child_orders[0].side is Side.SELL
