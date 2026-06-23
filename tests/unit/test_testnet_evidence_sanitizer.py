from __future__ import annotations

import csv
import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest


def load_sanitizer_module() -> Any:
    spec = importlib.util.spec_from_file_location(
        "sanitize_testnet_evidence",
        Path("scripts/sanitize_testnet_evidence.py"),
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_sanitizer_redacts_account_update_in_jsonl(tmp_path: Path) -> None:
    module = load_sanitizer_module()
    evidence_dir = tmp_path / "exec_1"
    evidence_dir.mkdir()
    log_path = evidence_dir / "execution_log.jsonl"
    log_path.write_text(
        json.dumps(
            {
                "event": "user_stream_event",
                "user_event": {
                    "event_type": "ACCOUNT_UPDATE",
                    "event_time_ms": 1_700_000_000_001,
                    "transaction_time_ms": 1_700_000_000_002,
                    "raw": {
                        "e": "ACCOUNT_UPDATE",
                        "a": {
                            "B": [{"cw": "123.45", "wb": "123.45"}],
                            "P": [{"pa": "0.001", "ep": "65000"}],
                        },
                    },
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (evidence_dir / "timeline.csv").write_text("event,user_event\n", encoding="utf-8")

    module.sanitize_evidence_dir(evidence_dir)

    sanitized = json.loads(log_path.read_text(encoding="utf-8"))
    assert sanitized["user_event"] == {
        "event_type": "ACCOUNT_UPDATE",
        "event_time_ms": 1_700_000_000_001,
        "transaction_time_ms": 1_700_000_000_002,
        "raw": {"e": "ACCOUNT_UPDATE", "redacted": True},
    }


def test_sanitizer_redacts_account_update_in_timeline_csv(tmp_path: Path) -> None:
    module = load_sanitizer_module()
    evidence_dir = tmp_path / "exec_1"
    evidence_dir.mkdir()
    (evidence_dir / "execution_log.jsonl").write_text("", encoding="utf-8")
    timeline_path = evidence_dir / "timeline.csv"
    with timeline_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["event", "user_event"])
        writer.writeheader()
        writer.writerow(
            {
                "event": "user_stream_event",
                "user_event": json.dumps(
                    {
                        "event_type": "ACCOUNT_UPDATE",
                        "event_time_ms": 1_700_000_000_001,
                        "transaction_time_ms": 1_700_000_000_002,
                        "raw": {
                            "e": "ACCOUNT_UPDATE",
                            "a": {
                                "B": [{"cw": "123.45", "wb": "123.45"}],
                                "P": [{"pa": "0.001", "ep": "65000"}],
                            },
                        },
                    }
                ),
            }
        )

    module.sanitize_evidence_dir(evidence_dir)

    with timeline_path.open(encoding="utf-8", newline="") as handle:
        row = next(csv.DictReader(handle))
    assert json.loads(row["user_event"]) == {
        "event_type": "ACCOUNT_UPDATE",
        "event_time_ms": 1_700_000_000_001,
        "transaction_time_ms": 1_700_000_000_002,
        "raw": {"e": "ACCOUNT_UPDATE", "redacted": True},
    }


def test_sanitizer_preserves_order_trade_update_payload(tmp_path: Path) -> None:
    module = load_sanitizer_module()
    evidence_dir = tmp_path / "exec_1"
    evidence_dir.mkdir()
    event = {
        "event": "user_stream_event",
        "user_event": {
            "event_type": "ORDER_TRADE_UPDATE",
            "event_time_ms": 1_700_000_000_001,
            "raw": {
                "e": "ORDER_TRADE_UPDATE",
                "o": {
                    "c": "ce_abcdef123456_1",
                    "i": 16277695886,
                    "ps": "BOTH",
                    "X": "FILLED",
                },
            },
        },
    }
    (evidence_dir / "execution_log.jsonl").write_text(
        json.dumps(event) + "\n", encoding="utf-8"
    )
    (evidence_dir / "timeline.csv").write_text("event,user_event\n", encoding="utf-8")

    module.sanitize_evidence_dir(evidence_dir)

    assert (
        json.loads((evidence_dir / "execution_log.jsonl").read_text(encoding="utf-8"))
        == event
    )


def test_sanitizer_rejects_remaining_private_account_keys(tmp_path: Path) -> None:
    module = load_sanitizer_module()
    evidence_dir = tmp_path / "exec_1"
    evidence_dir.mkdir()
    (evidence_dir / "execution_log.jsonl").write_text(
        json.dumps(
            {
                "event": "user_stream_event",
                "user_event": {
                    "event_type": "ORDER_TRADE_UPDATE",
                    "raw": {"e": "ORDER_TRADE_UPDATE", "o": {"c": "ce_abcdef123456_1"}},
                },
                "leaked_account": {"cw": "123.45"},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (evidence_dir / "timeline.csv").write_text("event,user_event\n", encoding="utf-8")

    with pytest.raises(module.PrivateAccountKeyError):
        module.sanitize_evidence_dir(evidence_dir)


def test_sanitizer_rejects_private_keys_in_order_trade_update_siblings(
    tmp_path: Path,
) -> None:
    module = load_sanitizer_module()
    evidence_dir = tmp_path / "exec_1"
    evidence_dir.mkdir()
    (evidence_dir / "execution_log.jsonl").write_text(
        json.dumps(
            {
                "event": "user_stream_event",
                "user_event": {
                    "event_type": "ORDER_TRADE_UPDATE",
                    "raw": {
                        "e": "ORDER_TRADE_UPDATE",
                        "o": {"c": "ce_abcdef123456_1", "ps": "BOTH"},
                    },
                    "leaked_account": {"cw": "123.45"},
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (evidence_dir / "timeline.csv").write_text("event,user_event\n", encoding="utf-8")

    with pytest.raises(module.PrivateAccountKeyError):
        module.sanitize_evidence_dir(evidence_dir)


def test_sanitizer_rejects_private_keys_in_order_trade_update_raw_sibling_jsonl(
    tmp_path: Path,
) -> None:
    module = load_sanitizer_module()
    evidence_dir = tmp_path / "exec_1"
    evidence_dir.mkdir()
    (evidence_dir / "execution_log.jsonl").write_text(
        json.dumps(
            {
                "event": "user_stream_event",
                "user_event": {
                    "event_type": "ORDER_TRADE_UPDATE",
                    "raw": {
                        "e": "ORDER_TRADE_UPDATE",
                        "o": {"c": "ce_abcdef123456_1", "ps": "BOTH"},
                        "a": {"B": [{"cw": "123.45"}]},
                    },
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (evidence_dir / "timeline.csv").write_text("event,user_event\n", encoding="utf-8")

    with pytest.raises(module.PrivateAccountKeyError):
        module.sanitize_evidence_dir(evidence_dir)


def test_sanitizer_rejects_private_keys_in_order_trade_update_raw_sibling_csv(
    tmp_path: Path,
) -> None:
    module = load_sanitizer_module()
    evidence_dir = tmp_path / "exec_1"
    evidence_dir.mkdir()
    (evidence_dir / "execution_log.jsonl").write_text("", encoding="utf-8")
    timeline_path = evidence_dir / "timeline.csv"
    with timeline_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["event", "user_event"])
        writer.writeheader()
        writer.writerow(
            {
                "event": "user_stream_event",
                "user_event": json.dumps(
                    {
                        "event_type": "ORDER_TRADE_UPDATE",
                        "raw": {
                            "e": "ORDER_TRADE_UPDATE",
                            "o": {"c": "ce_abcdef123456_1", "ps": "BOTH"},
                            "accidental_account": {"cw": "123.45"},
                        },
                    }
                ),
            }
        )

    with pytest.raises(module.PrivateAccountKeyError):
        module.sanitize_evidence_dir(evidence_dir)
