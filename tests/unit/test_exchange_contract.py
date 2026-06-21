import inspect
from decimal import Decimal

from exchanges.base import ExchangeAdapter
from exchanges.simulator import DeterministicSimulator
from execution.clock import ManualClock
from execution.models import MarketSnapshot
from execution.models import ReconciliationResult


def test_exchange_adapter_defines_required_methods() -> None:
    required = {
        "get_symbol_rules",
        "get_position",
        "get_best_bid_ask",
        "stream_market_data",
        "submit_limit_order",
        "cancel_order",
        "get_order_by_client_order_id",
        "stream_user_events",
        "reconcile_orders_and_fills",
        "health_check_streams",
    }
    for name in required:
        assert hasattr(ExchangeAdapter, name)


def test_reconciliation_result_exposes_orders_fills_and_warnings() -> None:
    result = ReconciliationResult(orders=[], fills=[], warnings=["diagnostic"])

    assert result.orders == []
    assert result.fills == []
    assert result.warnings == ["diagnostic"]


def test_exchange_stream_contract_methods_are_not_coroutine_functions() -> None:
    assert not inspect.iscoroutinefunction(ExchangeAdapter.stream_market_data)
    assert not inspect.iscoroutinefunction(ExchangeAdapter.stream_user_events)
    assert not inspect.iscoroutinefunction(DeterministicSimulator.stream_market_data)
    assert not inspect.iscoroutinefunction(DeterministicSimulator.stream_user_events)
    assert not inspect.isasyncgenfunction(DeterministicSimulator.stream_market_data)
    assert not inspect.isasyncgenfunction(DeterministicSimulator.stream_user_events)


async def test_market_data_stream_can_be_consumed_from_adapter_typed_variable() -> None:
    simulator = DeterministicSimulator(clock=ManualClock())
    adapter: ExchangeAdapter = simulator

    await simulator.push_market_data("BTCUSDT", Decimal("100"), Decimal("101"), exchange_event_time=10)

    async for snapshot in adapter.stream_market_data():
        assert snapshot == MarketSnapshot(
            symbol="BTCUSDT",
            bid=Decimal("100"),
            ask=Decimal("101"),
            last_market_event_time_exchange=10,
            last_market_event_time_local_monotonic=0,
        )
        break


def test_market_data_stream_returns_async_iterator_from_adapter_typed_variable() -> None:
    adapter: ExchangeAdapter = DeterministicSimulator(clock=ManualClock())

    market_data = adapter.stream_market_data()

    assert hasattr(market_data, "__anext__")
    assert not inspect.isawaitable(market_data)


def test_user_events_stream_returns_async_iterator_from_adapter_typed_variable() -> None:
    adapter: ExchangeAdapter = DeterministicSimulator(clock=ManualClock())

    user_events = adapter.stream_user_events()

    assert hasattr(user_events, "__anext__")
    assert not inspect.isawaitable(user_events)
