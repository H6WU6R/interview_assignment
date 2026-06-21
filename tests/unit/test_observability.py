from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from execution.models import ExecutionStatus, Side
from observability.artifacts import write_execution_artifacts
from observability.logging import sanitize_log_payload, to_jsonable
from observability.summary import execution_vwap, summary_metrics


def test_sanitize_log_payload_removes_sensitive_fields() -> None:
    payload = {
        "api_key": "abc",
        "secret_key": "def",
        "signature": "sig",
        "listenKey": "listen",
        "clientOrderId": "ce_abc_1",
        "orderId": 123,
        "price": "100",
    }

    sanitized = sanitize_log_payload(payload)

    assert "api_key" not in sanitized
    assert "secret_key" not in sanitized
    assert "signature" not in sanitized
    assert "listenKey" not in sanitized
    assert sanitized["clientOrderId"] == "ce_abc_1"
    assert sanitized["orderId"] == 123


def test_sanitize_log_payload_removes_nested_sensitive_fields() -> None:
    payload = {
        "request": {
            "clientOrderId": "ce_abc_1",
            "signature": "nested_sig",
            "raw_authenticated_request": "DELETE /signed",
        },
        "events": [
            {"orderId": 123, "secretKey": "nested_secret"},
            {"status": ExecutionStatus.RUNNING},
        ],
    }

    sanitized = sanitize_log_payload(payload)

    assert sanitized == {
        "request": {"clientOrderId": "ce_abc_1"},
        "events": [{"orderId": 123}, {"status": "RUNNING"}],
    }


def test_to_jsonable_converts_decimal_enum_datetime_and_path() -> None:
    payload = {
        "quantity": Decimal("0.010"),
        "status": ExecutionStatus.COMPLETED,
        "timestamp": datetime(2026, 6, 21, tzinfo=UTC),
        "path": Path("outputs/exec_test"),
        "items": [Decimal("1.5")],
    }

    converted = to_jsonable(payload)

    assert converted == {
        "quantity": "0.010",
        "status": "COMPLETED",
        "timestamp": "2026-06-21T00:00:00+00:00",
        "path": "outputs/exec_test",
        "items": ["1.5"],
    }


def test_execution_vwap_uses_decimal_weighted_average() -> None:
    fills = [(Decimal("100"), Decimal("0.01")), (Decimal("110"), Decimal("0.03"))]
    assert execution_vwap(fills) == Decimal("107.5")


def test_summary_metrics_include_side_aware_slippage() -> None:
    metrics = summary_metrics(
        final_status=ExecutionStatus.COMPLETED,
        side=Side.BUY,
        required_quantity=Decimal("0.010"),
        filled_quantity=Decimal("0.005"),
        arrival_mid=Decimal("100"),
        vwap=Decimal("101"),
    )

    assert metrics["completion_rate"] == "0.5"
    assert metrics["slippage_bps"] == "100"
    assert metrics["final_status"] == "COMPLETED"


def test_write_execution_artifacts_creates_required_files(tmp_path) -> None:
    output_dir = write_execution_artifacts(
        root=tmp_path,
        execution_id="exec_test",
        request_snapshot={"symbol": "BTCUSDT", "target_position": Decimal("0.010")},
        log_events=[
            {
                "execution_id": "exec_test",
                "client_order_id": "ce_test_1",
                "quantity": Decimal("0.004"),
            }
        ],
        summary={
            "execution_id": "exec_test",
            "final_status": ExecutionStatus.PARTIALLY_COMPLETED,
        },
        child_orders=[
            {
                "client_order_id": "ce_test_1",
                "status": "CANCELLED",
                "quantity": Decimal("0.010"),
            }
        ],
        fills=[{"client_order_id": "ce_test_1", "trade_id": "t1", "price": Decimal("100")}],
        timeline=[{"event": "cancel_fill_race", "timestamp": datetime(2026, 6, 21, tzinfo=UTC)}],
    )

    assert (output_dir / "request_snapshot.json").exists()
    assert (output_dir / "execution_log.jsonl").exists()
    assert (output_dir / "execution_summary.json").exists()
    assert (output_dir / "child_orders.csv").exists()
    assert (output_dir / "fills.csv").exists()
    assert (output_dir / "timeline.csv").exists()


def test_write_execution_artifacts_tolerates_heterogeneous_timeline_rows(tmp_path) -> None:
    output_dir = write_execution_artifacts(
        root=tmp_path,
        execution_id="exec_mixed_timeline",
        request_snapshot={"symbol": "BTCUSDT"},
        log_events=[],
        summary={"execution_id": "exec_mixed_timeline"},
        child_orders=[],
        fills=[],
        timeline=[
            {"event": "submit", "client_order_id": "ce_test_1"},
            {"event": "reconcile", "warning": "order_not_found"},
        ],
    )

    timeline = (output_dir / "timeline.csv").read_text(encoding="utf-8")
    assert "client_order_id" in timeline
    assert "warning" in timeline


def test_write_execution_artifacts_creates_empty_execution_log(tmp_path) -> None:
    output_dir = write_execution_artifacts(
        root=tmp_path,
        execution_id="exec_empty_log",
        request_snapshot={"symbol": "BTCUSDT"},
        log_events=[],
        summary={"execution_id": "exec_empty_log"},
        child_orders=[],
        fills=[],
        timeline=[],
    )

    execution_log = output_dir / "execution_log.jsonl"
    assert execution_log.exists()
    assert execution_log.read_text(encoding="utf-8") == ""
