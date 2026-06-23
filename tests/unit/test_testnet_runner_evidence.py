from __future__ import annotations

import importlib.util
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from execution.ids import make_client_order_prefix
from execution.models import (
    Algorithm,
    ChildOrderStatus,
    DeadlinePolicy,
    Environment,
    ExecutionParameters,
    ExecutionRequest,
    ExecutionStatus,
    ReconciliationResult,
)


def load_runner_module() -> Any:
    spec = importlib.util.spec_from_file_location(
        "testnet_runner", Path("scripts/testnet_runner.py")
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def request() -> ExecutionRequest:
    return ExecutionRequest(
        environment=Environment.TESTNET,
        symbol="BTCUSDT",
        algorithm=Algorithm.CHASE,
        target_position=Decimal("0.001"),
        target_price_lower=Decimal("90000"),
        target_price_upper=Decimal("120000"),
        target_duration_seconds=60,
        deadline_policy=DeadlinePolicy.AGGRESSIVE_WITHIN_RANGE,
        parameters=ExecutionParameters(number_of_slices=1),
    )


def record(
    *,
    execution_id: str = "exec_abcdef1234567890",
    child_orders: list[Any] | None = None,
) -> Any:
    return SimpleNamespace(
        execution_id=execution_id,
        status=ExecutionStatus.CANCELLED,
        child_orders=child_orders or [],
    )


def order(
    *,
    client_order_id: str = "ce_abcdef123456_1",
    exchange_order_id: str | None = None,
    status: Any = ChildOrderStatus.OPEN,
) -> Any:
    return SimpleNamespace(
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        status=status,
    )


def manifest_for(
    module: Any,
    *,
    record_value: Any | None = None,
    reconciliation: ReconciliationResult | None = None,
    events: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return module._evidence_manifest(
        request=request(),
        record=record_value or record(),
        reconciliation=reconciliation or ReconciliationResult(orders=[], fills=[]),
        events=events or [],
        adapter=SimpleNamespace(rate_limits={}),
    )


@pytest.mark.parametrize("accepted_status", [ChildOrderStatus.OPEN, "FILLED"])
def test_manifest_reports_accepted_exchange_order_evidence(
    accepted_status: Any,
) -> None:
    module = load_runner_module()
    accepted_order = order(exchange_order_id="12345", status=accepted_status)

    manifest = manifest_for(
        module,
        record_value=record(child_orders=[accepted_order]),
    )

    assert manifest["accepted_exchange_order_evidence"] is True
    assert "missing_accepted_exchange_order_evidence" not in manifest["warnings"]


def test_manifest_warns_when_accepted_exchange_order_evidence_is_missing() -> None:
    module = load_runner_module()
    rejected_order = order(
        exchange_order_id="rejected-123", status=ChildOrderStatus.REJECTED
    )

    manifest = manifest_for(
        module,
        reconciliation=ReconciliationResult(
            orders=[rejected_order],
            fills=[],
            warnings=["post_run_reconciliation_rate_limited"],
        ),
    )

    assert manifest["accepted_exchange_order_evidence"] is False
    assert manifest["warnings"] == [
        "post_run_reconciliation_rate_limited",
        "missing_accepted_exchange_order_evidence",
    ]


@pytest.mark.parametrize(
    "raw_shape",
    [
        "nested_o_c",
        "flat_c",
    ],
)
def test_private_stream_evidence_requires_order_trade_update_for_execution_prefix(
    raw_shape: str,
) -> None:
    module = load_runner_module()
    execution_id = "exec_abcdef1234567890"
    prefix = make_client_order_prefix(execution_id)
    client_order_id = f"{prefix}1"
    if raw_shape == "nested_o_c":
        matching_raw = {"e": "ORDER_TRADE_UPDATE", "o": {"c": client_order_id}}
    else:
        matching_raw = {"e": "ORDER_TRADE_UPDATE", "c": client_order_id}
    unrelated_events = [
        {
            "event": "user_stream_event",
            "user_event": {
                "event_type": "ACCOUNT_UPDATE",
                "raw": {"e": "ACCOUNT_UPDATE", "c": client_order_id},
            },
        },
        {
            "event": "user_stream_event",
            "user_event": {
                "event_type": "ORDER_TRADE_UPDATE",
                "raw": {"e": "ORDER_TRADE_UPDATE", "o": {"c": "ce_otherexec_1"}},
            },
        },
        {
            "event": "market_snapshot",
            "user_event": {
                "event_type": "ORDER_TRADE_UPDATE",
                "raw": matching_raw,
            },
        },
    ]

    unrelated_manifest = manifest_for(
        module,
        record_value=record(execution_id=execution_id),
        events=unrelated_events,
    )
    matching_manifest = manifest_for(
        module,
        record_value=record(execution_id=execution_id),
        events=[
            *unrelated_events,
            {
                "event": "user_stream_event",
                "user_event": {
                    "event_type": "ORDER_TRADE_UPDATE",
                    "raw": matching_raw,
                },
            },
        ],
    )

    assert unrelated_manifest["has_private_user_stream_events"] is True
    assert unrelated_manifest["has_execution_matching_private_order_event"] is False
    assert matching_manifest["has_execution_matching_private_order_event"] is True


def test_artifact_user_event_redacts_account_update_balance_and_position_fields() -> (
    None
):
    module = load_runner_module()

    sanitized = module._artifact_user_event(
        {
            "event_type": "ACCOUNT_UPDATE",
            "event_time_ms": 1_700_000_000_001,
            "transaction_time_ms": 1_700_000_000_002,
            "raw": {
                "e": "ACCOUNT_UPDATE",
                "T": 1_700_000_000_002,
                "a": {
                    "B": [{"a": "USDT", "wb": "123.45", "cw": "120.00"}],
                    "P": [
                        {
                            "s": "BTCUSDT",
                            "pa": "0.002",
                            "ep": "65000",
                            "bep": "64900",
                            "up": "12.34",
                        }
                    ],
                },
            },
        }
    )

    assert sanitized == {
        "event_type": "ACCOUNT_UPDATE",
        "event_time_ms": 1_700_000_000_001,
        "transaction_time_ms": 1_700_000_000_002,
        "raw": {"e": "ACCOUNT_UPDATE", "redacted": True},
    }
    sanitized_text = str(sanitized)
    for sensitive_value in ("123.45", "120.00", "0.002", "65000", "64900", "12.34"):
        assert sensitive_value not in sanitized_text


def test_artifact_user_event_preserves_order_trade_update_client_order_id() -> None:
    module = load_runner_module()
    event = {
        "event_type": "ORDER_TRADE_UPDATE",
        "event_time_ms": 1_700_000_000_001,
        "raw": {
            "e": "ORDER_TRADE_UPDATE",
            "o": {"c": "ce_abcdef123456_1", "X": "NEW"},
        },
    }

    assert module._artifact_user_event(event) == event
