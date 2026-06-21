from exchanges.base import ExchangeAdapter
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
