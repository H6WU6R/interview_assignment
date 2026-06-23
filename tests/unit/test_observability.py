import csv
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from execution.models import ExecutionStatus, Side
from observability.artifacts import write_execution_artifacts
from observability.logging import sanitize_log_payload, to_jsonable
from observability.summary import execution_vwap, overfill_quantity, summary_metrics


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


def test_sanitize_log_payload_removes_sensitive_alias_keys() -> None:
    payload = {
        "binance_api_key": "binance-key",
        "api_secret": "api-secret",
        "listen_key": "listen-key",
        "X-MBX-APIKEY": "header-key",
        "clientOrderId": "ce_abc_1",
        "orderId": 123,
        "price": "100",
        "quantity": "0.01",
        "status": ExecutionStatus.RUNNING,
    }

    sanitized = sanitize_log_payload(payload)

    assert sanitized == {
        "clientOrderId": "ce_abc_1",
        "orderId": 123,
        "price": "100",
        "quantity": "0.01",
        "status": "RUNNING",
    }


def test_sanitize_log_payload_sanitizes_nested_auth_headers_and_signed_payloads() -> (
    None
):
    payload = {
        "headers": {
            "X-MBX-APIKEY": "nested-header-key",
            "Content-Type": "application/json",
        },
        "signed_request": {
            "symbol": "BTCUSDT",
            "signature": "signed-container-signature",
        },
        "authenticated request": "POST /signed",
        "params": {
            "timestamp": 1_782_009_600_000,
            "signature": "nested-signature",
            "api secret": "nested-secret",
            "listen-key": "nested-listen-key",
        },
        "client_order_id": "ce_abc_1",
        "status": "ACKED",
    }

    sanitized = sanitize_log_payload(payload)

    assert sanitized == {
        "headers": {"Content-Type": "application/json"},
        "params": {"timestamp": 1_782_009_600_000},
        "client_order_id": "ce_abc_1",
        "status": "ACKED",
    }


def test_sanitize_log_payload_removes_raw_authenticated_and_signed_artifact_aliases() -> (
    None
):
    payload = {
        "authenticated_payload": {"signature": "authenticated-payload-signature"},
        "authenticated_params": {"signature": "authenticated-params-signature"},
        "raw_payload": {"signature": "raw-payload-signature"},
        "raw_params": {"signature": "raw-params-signature"},
        "raw_authenticated_payload": {
            "signature": "raw-authenticated-payload-signature"
        },
        "raw-authenticated-params": {"signature": "raw-authenticated-params-signature"},
        "signed_request": {"signature": "signed-request-signature"},
        "signed_payload": {"signature": "signed-payload-signature"},
        "signed_params": {"signature": "signed-params-signature"},
        "client_order_id": "ce_abc_1",
        "orderId": 123,
        "timestamp": 1_782_009_600_000,
        "quantity": Decimal("0.010"),
        "status": "ACKED",
    }

    sanitized = sanitize_log_payload(payload)

    assert sanitized == {
        "client_order_id": "ce_abc_1",
        "orderId": 123,
        "timestamp": 1_782_009_600_000,
        "quantity": "0.010",
        "status": "ACKED",
    }


def test_sanitize_log_payload_stringifies_mapping_keys() -> None:
    sanitized = sanitize_log_payload({1: "one", "status": "ACKED"})

    assert sanitized == {"1": "one", "status": "ACKED"}


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


def test_overfill_quantity_reports_only_amount_above_required() -> None:
    assert overfill_quantity(Decimal("0.011"), Decimal("0.010")) == Decimal("0.001")
    assert overfill_quantity(Decimal("0.009"), Decimal("0.010")) == Decimal("0")


def test_summary_metrics_include_side_aware_slippage() -> None:
    metrics = summary_metrics(
        final_status=ExecutionStatus.COMPLETED,
        side=Side.BUY,
        raw_required_quantity=Decimal("0.010"),
        required_quantity=Decimal("0.010"),
        target_dust_quantity=Decimal("0"),
        filled_quantity=Decimal("0.005"),
        arrival_bid=Decimal("99"),
        arrival_ask=Decimal("101"),
        vwap=Decimal("101"),
        requested_duration_seconds=300,
        actual_duration_seconds=Decimal("120"),
        price_bound_violations=0,
        duplicate_events_ignored=0,
        unknown_orders_reconciled=0,
        max_reserved_exposure=Decimal("0.010"),
    )

    assert metrics["completion_rate"] == "0.5"
    assert metrics["slippage_bps"] == "100"
    assert metrics["final_status"] == "COMPLETED"


def test_summary_metrics_include_pdf_required_quantity_price_and_safety_fields() -> (
    None
):
    metrics = summary_metrics(
        final_status=ExecutionStatus.PARTIALLY_COMPLETED,
        side=Side.BUY,
        raw_required_quantity=Decimal("0.0105"),
        required_quantity=Decimal("0.010"),
        target_dust_quantity=Decimal("0.0005"),
        filled_quantity=Decimal("0.004"),
        arrival_bid=Decimal("50000"),
        arrival_ask=Decimal("50002"),
        vwap=Decimal("50001"),
        requested_duration_seconds=300,
        actual_duration_seconds=Decimal("300"),
        price_bound_violations=2,
        duplicate_events_ignored=1,
        unknown_orders_reconciled=1,
        max_reserved_exposure=Decimal("0.010"),
    )

    assert metrics["raw_required_quantity"] == "0.0105"
    assert metrics["required_quantity"] == "0.01"
    assert metrics["target_dust_quantity"] == "0.0005"
    assert metrics["filled_quantity"] == "0.004"
    assert metrics["unfilled_quantity"] == "0.006"
    assert metrics["completion_rate"] == "0.4"
    assert metrics["arrival_mid"] == "50001"
    assert metrics["price_bound_violations"] == 2
    assert metrics["duplicate_events_ignored"] == 1
    assert metrics["unknown_orders_reconciled"] == 1
    assert metrics["max_reserved_exposure"] == "0.01"
    assert metrics["overfill_quantity"] == "0"


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
        fills=[
            {"client_order_id": "ce_test_1", "trade_id": "t1", "price": Decimal("100")}
        ],
        timeline=[
            {
                "event": "cancel_fill_race",
                "timestamp": datetime(2026, 6, 21, tzinfo=UTC),
            }
        ],
    )

    assert (output_dir / "request_snapshot.json").exists()
    assert (output_dir / "execution_log.jsonl").exists()
    assert (output_dir / "execution_summary.json").exists()
    assert (output_dir / "child_orders.csv").exists()
    assert (output_dir / "fills.csv").exists()
    assert (output_dir / "timeline.csv").exists()
    assert (output_dir / "twap_slice_ledger.csv").exists()


def test_write_execution_artifacts_tolerates_heterogeneous_timeline_rows(
    tmp_path,
) -> None:
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


def test_write_execution_artifacts_uses_lf_csv_line_endings(tmp_path) -> None:
    output_dir = write_execution_artifacts(
        root=tmp_path,
        execution_id="exec_lf_csv",
        request_snapshot={"symbol": "BTCUSDT"},
        log_events=[],
        summary={"execution_id": "exec_lf_csv"},
        child_orders=[{"client_order_id": "ce_test_1", "status": "OPEN"}],
        fills=[{"client_order_id": "ce_test_1", "trade_id": "trade_1"}],
        timeline=[{"event": "submit", "client_order_id": "ce_test_1"}],
        twap_slice_ledger=[{"slice_index": 1, "submitted_quantity": Decimal("0.001")}],
        extra_csv_artifacts={
            "reconciliation_orders.csv": [
                {"client_order_id": "ce_test_1", "exchange_order_id": "12345"}
            ]
        },
    )

    for filename in (
        "child_orders.csv",
        "fills.csv",
        "timeline.csv",
        "twap_slice_ledger.csv",
        "reconciliation_orders.csv",
    ):
        payload = (output_dir / filename).read_bytes()
        assert b"\n" in payload
        assert b"\r\n" not in payload


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
    assert (output_dir / "twap_slice_ledger.csv").exists()
    assert (output_dir / "twap_slice_ledger.csv").read_text(encoding="utf-8") == ""


def test_write_execution_artifacts_writes_sanitized_twap_slice_ledger(tmp_path) -> None:
    output_dir = write_execution_artifacts(
        root=tmp_path,
        execution_id="exec_twap_ledger",
        request_snapshot={"symbol": "BTCUSDT"},
        log_events=[],
        summary={"execution_id": "exec_twap_ledger"},
        child_orders=[],
        fills=[],
        timeline=[],
        twap_slice_ledger=[
            {
                "execution_id": "exec_twap_ledger",
                "slice_index": 1,
                "planned_slice_quantity": Decimal("0.100"),
                "filled_quantity": Decimal("0.025"),
                "child_order_ids": ["child_0001"],
                "metadata": {"source": "unit"},
            }
        ],
    )

    with (output_dir / "twap_slice_ledger.csv").open(
        newline="", encoding="utf-8"
    ) as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    row = rows[0]
    assert row["planned_slice_quantity"] == "0.100"
    assert row["filled_quantity"] == "0.025"
    assert json.loads(row["child_order_ids"]) == ["child_0001"]
    assert json.loads(row["metadata"]) == {"source": "unit"}


def test_write_execution_artifacts_sanitizes_secret_aliases_in_outputs(
    tmp_path,
) -> None:
    output_dir = write_execution_artifacts(
        root=tmp_path,
        execution_id="exec_secret_aliases",
        request_snapshot={
            "symbol": "BTCUSDT",
            "binance_api_key": "request-api-key",
            "authenticated_request": {"signature": "request-signature"},
            "target_position": Decimal("0.010"),
        },
        log_events=[
            {
                "execution_id": "exec_secret_aliases",
                "headers": {"X-MBX-APIKEY": "log-header-key"},
                "client_order_id": "ce_secret_1",
                "signed_payload": {"api_secret": "log-secret"},
            }
        ],
        summary={"execution_id": "exec_secret_aliases", "status": "COMPLETED"},
        child_orders=[
            {
                "client_order_id": "ce_secret_1",
                "api_secret": "child-secret",
                "status": "CANCELLED",
            }
        ],
        fills=[
            {
                "client_order_id": "ce_secret_1",
                "signature": "fill-signature",
                "price": Decimal("100"),
            }
        ],
        timeline=[
            {
                "event": "reconcile",
                "listen_key": "timeline-listen-key",
                "signed request": "timeline-signed-request",
            }
        ],
    )

    output_text = "\n".join(
        [
            (output_dir / "request_snapshot.json").read_text(encoding="utf-8"),
            (output_dir / "execution_log.jsonl").read_text(encoding="utf-8"),
            (output_dir / "execution_summary.json").read_text(encoding="utf-8"),
            (output_dir / "child_orders.csv").read_text(encoding="utf-8"),
            (output_dir / "fills.csv").read_text(encoding="utf-8"),
            (output_dir / "timeline.csv").read_text(encoding="utf-8"),
        ]
    )

    for secret in [
        "request-api-key",
        "request-signature",
        "log-header-key",
        "log-secret",
        "child-secret",
        "fill-signature",
        "timeline-listen-key",
        "timeline-signed-request",
    ]:
        assert secret not in output_text
    assert "ce_secret_1" in output_text
    assert "BTCUSDT" in output_text


def test_write_execution_artifacts_writes_sanitized_extra_json_and_csv(
    tmp_path,
) -> None:
    output_dir = write_execution_artifacts(
        root=tmp_path,
        execution_id="exec_extra_artifacts",
        request_snapshot={"symbol": "BTCUSDT"},
        log_events=[],
        summary={"execution_id": "exec_extra_artifacts"},
        child_orders=[],
        fills=[],
        timeline=[],
        extra_json_artifacts={
            "review_evidence.json": {
                "execution_id": "exec_extra_artifacts",
                "signature": "json-secret",
                "filled_quantity": Decimal("0.010"),
            }
        },
        extra_csv_artifacts={
            "reconciliation_orders.csv": [
                {
                    "client_order_id": "ce_extra_1",
                    "api_secret": "csv-secret",
                    "quantity": Decimal("0.010"),
                    "metadata": {"source": "unit"},
                }
            ]
        },
    )

    extra_json = json.loads(
        (output_dir / "review_evidence.json").read_text(encoding="utf-8")
    )
    assert extra_json == {
        "execution_id": "exec_extra_artifacts",
        "filled_quantity": "0.010",
    }

    with (output_dir / "reconciliation_orders.csv").open(
        newline="", encoding="utf-8"
    ) as handle:
        rows = list(csv.DictReader(handle))

    assert rows == [
        {
            "client_order_id": "ce_extra_1",
            "metadata": json.dumps({"source": "unit"}, sort_keys=True),
            "quantity": "0.010",
        }
    ]
    output_text = "\n".join(
        [
            (output_dir / "review_evidence.json").read_text(encoding="utf-8"),
            (output_dir / "reconciliation_orders.csv").read_text(encoding="utf-8"),
        ]
    )
    assert "json-secret" not in output_text
    assert "csv-secret" not in output_text


@pytest.mark.parametrize(
    "extra_json_artifacts",
    [
        {"../escape.json": {"ok": True}},
        {"nested/escape.json": {"ok": True}},
        {"/tmp/escape.json": {"ok": True}},
        {"request_snapshot.json": {"ok": True}},
        {"bad\nname.json": {"ok": True}},
        {"bad name.json": {"ok": True}},
    ],
)
def test_write_execution_artifacts_rejects_unsafe_extra_json_names(
    tmp_path,
    extra_json_artifacts,
) -> None:
    with pytest.raises(ValueError, match="extra artifact filename"):
        write_execution_artifacts(
            root=tmp_path,
            execution_id="exec_unsafe_extra",
            request_snapshot={"symbol": "BTCUSDT"},
            log_events=[],
            summary={"execution_id": "exec_unsafe_extra"},
            child_orders=[],
            fills=[],
            timeline=[],
            extra_json_artifacts=extra_json_artifacts,
        )

    assert not (tmp_path / "escape.json").exists()


def test_write_execution_artifacts_rejects_unsafe_extra_csv_names(tmp_path) -> None:
    with pytest.raises(ValueError, match="extra artifact filename"):
        write_execution_artifacts(
            root=tmp_path,
            execution_id="exec_unsafe_extra_csv",
            request_snapshot={"symbol": "BTCUSDT"},
            log_events=[],
            summary={"execution_id": "exec_unsafe_extra_csv"},
            child_orders=[],
            fills=[],
            timeline=[],
            extra_csv_artifacts={"../escape.csv": [{"ok": True}]},
        )

    assert not (tmp_path / "escape.csv").exists()
