from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any


PRIVATE_ACCOUNT_KEYS = {"cw", "wb", "bep", "ep", "iw", "ma", "mt", "pa", "ps", "up"}


class PrivateAccountKeyError(RuntimeError):
    pass


def _is_account_update_event(value: dict[str, Any]) -> bool:
    raw = value.get("raw")
    return (
        value.get("event_type") == "ACCOUNT_UPDATE"
        or value.get("e") == "ACCOUNT_UPDATE"
        or (isinstance(raw, dict) and raw.get("e") == "ACCOUNT_UPDATE")
    )


def _is_order_trade_update_event(value: dict[str, Any]) -> bool:
    raw = value.get("raw")
    return (
        value.get("event_type") == "ORDER_TRADE_UPDATE"
        or value.get("e") == "ORDER_TRADE_UPDATE"
        or (isinstance(raw, dict) and raw.get("e") == "ORDER_TRADE_UPDATE")
    )


def _redacted_account_update(value: dict[str, Any]) -> dict[str, Any]:
    redacted: dict[str, Any] = {
        "event_type": "ACCOUNT_UPDATE",
        "raw": {"e": "ACCOUNT_UPDATE", "redacted": True},
    }
    for key in ("event_time_ms", "transaction_time_ms"):
        if key in value:
            redacted[key] = value[key]
        elif isinstance(value.get("raw"), dict):
            raw_key = "E" if key == "event_time_ms" else "T"
            if raw_key in value["raw"]:
                redacted[key] = value["raw"][raw_key]
    return redacted


def sanitize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        if _is_account_update_event(value):
            return _redacted_account_update(value)
        return {key: sanitize_json_value(child) for key, child in value.items()}
    if isinstance(value, list):
        return [sanitize_json_value(child) for child in value]
    return value


def _private_key_paths(
    value: Any,
    path: str = "$",
    *,
    in_order_trade_update_raw: bool = False,
    in_order_trade_update_order: bool = False,
) -> list[str]:
    if isinstance(value, dict):
        paths: list[str] = []
        for key, child in value.items():
            child_path = f"{path}.{key}"
            if key in PRIVATE_ACCOUNT_KEYS and not in_order_trade_update_order:
                paths.append(child_path)
            child_in_order_trade_update_raw = (
                key == "raw"
                and isinstance(child, dict)
                and child.get("e") == "ORDER_TRADE_UPDATE"
                and _is_order_trade_update_event(value)
            )
            child_in_order_trade_update_order = in_order_trade_update_order or (
                key == "o" and in_order_trade_update_raw
            )
            paths.extend(
                _private_key_paths(
                    child,
                    child_path,
                    in_order_trade_update_raw=child_in_order_trade_update_raw,
                    in_order_trade_update_order=child_in_order_trade_update_order,
                )
            )
        return paths
    if isinstance(value, list):
        paths = []
        for index, child in enumerate(value):
            paths.extend(
                _private_key_paths(
                    child,
                    f"{path}[{index}]",
                    in_order_trade_update_raw=in_order_trade_update_raw,
                    in_order_trade_update_order=in_order_trade_update_order,
                )
            )
        return paths
    return []


def _loads_json_cell(value: str) -> Any:
    stripped = value.strip()
    if not stripped or stripped[0] not in "[{":
        raise ValueError("not a JSON cell")
    return json.loads(stripped)


def _sanitize_jsonl(path: Path) -> None:
    if not path.exists():
        return
    rewritten: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            rewritten.append(line)
            continue
        rewritten.append(json.dumps(sanitize_json_value(json.loads(line)), sort_keys=True))
    path.write_text("\n".join(rewritten) + ("\n" if rewritten else ""), encoding="utf-8")


def _sanitize_timeline_csv(path: Path) -> None:
    if not path.exists():
        return
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fieldnames = reader.fieldnames
    if fieldnames is None:
        return

    for row in rows:
        for key, value in row.items():
            if value is None:
                continue
            try:
                parsed = _loads_json_cell(value)
            except (json.JSONDecodeError, ValueError):
                continue
            row[key] = json.dumps(sanitize_json_value(parsed), sort_keys=True)

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _validate_jsonl(path: Path) -> None:
    if not path.exists():
        return
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        paths = _private_key_paths(json.loads(line), f"{path}:{line_number}")
        if paths:
            raise PrivateAccountKeyError(f"private account keys remain in {', '.join(paths)}")


def _validate_timeline_csv(path: Path) -> None:
    if not path.exists():
        return
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row_number, row in enumerate(reader, start=2):
            for column, value in row.items():
                if value is None:
                    continue
                try:
                    parsed = _loads_json_cell(value)
                except (json.JSONDecodeError, ValueError):
                    continue
                paths = _private_key_paths(parsed, f"{path}:{row_number}.{column}")
                if paths:
                    raise PrivateAccountKeyError(f"private account keys remain in {', '.join(paths)}")


def sanitize_evidence_dir(evidence_dir: Path | str) -> None:
    evidence_path = Path(evidence_dir)
    _sanitize_jsonl(evidence_path / "execution_log.jsonl")
    _sanitize_timeline_csv(evidence_path / "timeline.csv")
    _validate_jsonl(evidence_path / "execution_log.jsonl")
    _validate_timeline_csv(evidence_path / "timeline.csv")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sanitize accepted Binance Testnet evidence artifacts.")
    parser.add_argument("evidence_dirs", nargs="+", type=Path)
    args = parser.parse_args(argv)

    try:
        for evidence_dir in args.evidence_dirs:
            sanitize_evidence_dir(evidence_dir)
            print(f"sanitized {evidence_dir}")
    except (OSError, json.JSONDecodeError, csv.Error, PrivateAccountKeyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
