from decimal import Decimal

import pytest

from exchanges.simulator import (
    DeterministicSimulator,
    NoFreshMarketData,
    SimulatorOrderRejected,
    SimulatorOrderTimeout,
)
from execution.clock import ManualClock
from execution.ids import make_client_order_id, make_client_order_prefix
from execution.models import ChildOrderStatus, OrderRequest, Side, SymbolRules


SYMBOL = "BTCUSDT"


def order_request(
    execution_id: str = "exec_0123456789abcdef",
    sequence: int = 1,
    *,
    side: Side = Side.BUY,
    quantity: Decimal = Decimal("0.010"),
    price: Decimal = Decimal("99.00"),
    post_only: bool = True,
) -> OrderRequest:
    return OrderRequest(
        execution_id=execution_id,
        child_order_id=f"child_{sequence:04d}",
        client_order_id=make_client_order_id(execution_id, sequence),
        symbol=SYMBOL,
        side=side,
        quantity=quantity,
        price=price,
        post_only=post_only,
    )


async def fresh_simulator() -> DeterministicSimulator:
    simulator = DeterministicSimulator(clock=ManualClock())
    await simulator.push_market_data(SYMBOL, Decimal("100.00"), Decimal("101.00"), exchange_event_time=10)
    return simulator


async def test_submit_stores_open_order_and_lookup_by_client_order_id() -> None:
    simulator = await fresh_simulator()
    request = order_request()

    order = await simulator.submit_limit_order(request)

    assert order.status == ChildOrderStatus.OPEN
    assert order.client_order_id == request.client_order_id
    assert order.submitted_quantity == Decimal("0.010")
    assert order.confirmed_filled_quantity == Decimal("0")
    assert await simulator.get_order_by_client_order_id(SYMBOL, request.client_order_id) == order


async def test_submit_requires_fresh_market_snapshot() -> None:
    simulator = DeterministicSimulator(clock=ManualClock())

    with pytest.raises(NoFreshMarketData):
        await simulator.submit_limit_order(order_request())


async def test_cancel_open_order_and_already_filled_cancel_is_not_fatal() -> None:
    simulator = await fresh_simulator()
    request = order_request()
    await simulator.submit_limit_order(request)

    cancelled = await simulator.cancel_order(SYMBOL, request.client_order_id)

    assert cancelled.status == ChildOrderStatus.CANCELLED

    filled_request = order_request(sequence=2)
    await simulator.submit_limit_order(filled_request)
    await simulator.push_fill(filled_request.client_order_id, Decimal("0.010"), Decimal("99.00"))

    already_terminal = await simulator.cancel_order(SYMBOL, filled_request.client_order_id)

    assert already_terminal.status == ChildOrderStatus.FILLED


async def test_fill_during_cancel_records_fill_event_and_reconciles_state() -> None:
    execution_id = "exec_0123456789abcdef"
    prefix = make_client_order_prefix(execution_id)
    simulator = await fresh_simulator()
    request = order_request(execution_id=execution_id, quantity=Decimal("0.010"))
    await simulator.submit_limit_order(request)
    simulator.script_fill_during_cancel(prefix, Decimal("0.004"))

    order = await simulator.cancel_order(SYMBOL, request.client_order_id)
    result = await simulator.reconcile_orders_and_fills(SYMBOL, client_order_prefix=prefix)

    assert order.status == ChildOrderStatus.CANCELLED
    assert order.confirmed_filled_quantity == Decimal("0.004")
    assert [fill.last_filled_quantity for fill in result.fills] == [Decimal("0.004")]
    assert result.orders == [order]


async def test_repeated_partial_fills_accumulate_without_status_self_transition() -> None:
    simulator = await fresh_simulator()
    request = order_request(quantity=Decimal("0.010"))
    order = await simulator.submit_limit_order(request)

    first_fill = await simulator.push_fill(request.client_order_id, Decimal("0.003"), Decimal("99.00"))
    second_fill = await simulator.push_fill(request.client_order_id, Decimal("0.002"), Decimal("99.50"))

    assert order.status == ChildOrderStatus.PARTIALLY_FILLED
    assert order.confirmed_filled_quantity == Decimal("0.005")
    assert first_fill.cumulative_filled_quantity == Decimal("0.003")
    assert second_fill.cumulative_filled_quantity == Decimal("0.005")
    assert [first_fill.last_filled_quantity, second_fill.last_filled_quantity] == [
        Decimal("0.003"),
        Decimal("0.002"),
    ]


async def test_cancel_reconcile_open_script_leaves_order_open_until_reconciliation() -> None:
    execution_id = "exec_0123456789abcdef"
    prefix = make_client_order_prefix(execution_id)
    simulator = await fresh_simulator()
    request = order_request(execution_id=execution_id)
    await simulator.submit_limit_order(request)
    simulator.script_cancel_reconcile_open(prefix)

    order = await simulator.cancel_order(SYMBOL, request.client_order_id)
    result = await simulator.reconcile_orders_and_fills(SYMBOL, client_order_prefix=prefix)

    assert order.status == ChildOrderStatus.OPEN
    assert result.orders == [order]


async def test_create_timeout_reconciliation_finds_open_order_for_same_execution_prefix() -> None:
    execution_id = "exec_0123456789abcdef"
    prefix = make_client_order_prefix(execution_id)
    simulator = await fresh_simulator()
    request = order_request(execution_id=execution_id)
    simulator.script_create_timeout(prefix)

    with pytest.raises(SimulatorOrderTimeout):
        await simulator.submit_limit_order(request)

    result = await simulator.reconcile_orders_and_fills(SYMBOL, client_order_prefix=prefix)

    assert [order.client_order_id for order in result.orders] == [request.client_order_id]
    assert result.orders[0].status == ChildOrderStatus.OPEN
    assert result.warnings == []


async def test_create_timeout_not_found_warning_is_execution_specific() -> None:
    execution_id = "exec_0123456789abcdef"
    other_execution_id = "exec_fedcba9876543210"
    prefix = make_client_order_prefix(execution_id)
    other_prefix = make_client_order_prefix(other_execution_id)
    simulator = await fresh_simulator()
    simulator.script_create_timeout_not_found(prefix)

    with pytest.raises(SimulatorOrderTimeout):
        await simulator.submit_limit_order(order_request(execution_id=execution_id))

    same_result = await simulator.reconcile_orders_and_fills(SYMBOL, client_order_prefix=prefix)
    other_result = await simulator.reconcile_orders_and_fills(SYMBOL, client_order_prefix=other_prefix)

    assert same_result.orders == []
    assert same_result.fills == []
    assert same_result.warnings == ["CREATE_TIMEOUT_ORDER_NOT_FOUND"]
    assert other_result.warnings == []


async def test_reconciliation_rejects_non_execution_scoped_prefixes() -> None:
    simulator = await fresh_simulator()
    await simulator.submit_limit_order(order_request())

    for invalid_prefix in ("ce_012345", "ce_0123456789ab", "manual_order_"):
        with pytest.raises(ValueError, match="execution-scoped"):
            await simulator.reconcile_orders_and_fills(SYMBOL, client_order_prefix=invalid_prefix)


async def test_scripts_reject_non_execution_scoped_prefixes() -> None:
    simulator = await fresh_simulator()

    for invalid_prefix in ("ce_012345", "ce_0123456789ab"):
        with pytest.raises(ValueError, match="execution-scoped"):
            simulator.script_create_timeout(invalid_prefix)
        with pytest.raises(ValueError, match="execution-scoped"):
            simulator.script_create_timeout_not_found(invalid_prefix)
        with pytest.raises(ValueError, match="execution-scoped"):
            simulator.script_fill_during_cancel(invalid_prefix, Decimal("0.001"))
        with pytest.raises(ValueError, match="execution-scoped"):
            simulator.script_cancel_reconcile_open(invalid_prefix)


async def test_reconcile_filters_orders_and_fills_by_exact_execution_prefix() -> None:
    execution_id = "exec_0123456789abcdef"
    other_execution_id = "exec_fedcba9876543210"
    prefix = make_client_order_prefix(execution_id)
    other_prefix = make_client_order_prefix(other_execution_id)
    simulator = await fresh_simulator()
    request = order_request(execution_id=execution_id)
    other_request = order_request(execution_id=other_execution_id)
    await simulator.submit_limit_order(request)
    await simulator.submit_limit_order(other_request)
    await simulator.push_fill(request.client_order_id, Decimal("0.003"), Decimal("99.00"))
    await simulator.push_fill(other_request.client_order_id, Decimal("0.002"), Decimal("98.50"))

    result = await simulator.reconcile_orders_and_fills(SYMBOL, client_order_prefix=prefix)
    other_result = await simulator.reconcile_orders_and_fills(SYMBOL, client_order_prefix=other_prefix)

    assert [order.client_order_id for order in result.orders] == [request.client_order_id]
    assert [fill.client_order_id for fill in result.fills] == [request.client_order_id]
    assert [order.client_order_id for order in other_result.orders] == [other_request.client_order_id]
    assert [fill.client_order_id for fill in other_result.fills] == [other_request.client_order_id]


async def test_user_event_stream_yields_queued_order_and_fill_events() -> None:
    simulator = await fresh_simulator()
    request = order_request()
    order = await simulator.submit_limit_order(request)
    fill = await simulator.push_fill(request.client_order_id, Decimal("0.010"), Decimal("99.00"))
    events = simulator.stream_user_events()

    first = await anext(events)
    second = await anext(events)

    assert first.kind == "order_opened"
    assert first.order == order
    assert second.kind == "fill"
    assert second.fill == fill


async def test_stream_health_flag_changes_health_check_streams_output() -> None:
    simulator = await fresh_simulator()

    assert await simulator.health_check_streams() is True

    simulator.set_stream_health(user_stream_healthy=False)

    assert await simulator.health_check_streams() is False


async def test_post_only_rejects_when_crossing_or_gtx_unsupported() -> None:
    simulator = await fresh_simulator()

    with pytest.raises(SimulatorOrderRejected, match="post-only order would cross"):
        await simulator.submit_limit_order(order_request(price=Decimal("101.00")))

    unsupported = await fresh_simulator()
    unsupported.set_symbol_rules(
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

    with pytest.raises(SimulatorOrderRejected, match="does not support GTX/post-only"):
        await unsupported.submit_limit_order(order_request())
