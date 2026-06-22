from __future__ import annotations

import csv
import json
import re
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from observability.logging import append_jsonl, sanitize_log_payload


_RESERVED_ARTIFACT_FILENAMES = frozenset(
    {
        "request_snapshot.json",
        "execution_log.jsonl",
        "execution_summary.json",
        "child_orders.csv",
        "fills.csv",
        "timeline.csv",
        "twap_slice_ledger.csv",
    }
)
_EXTRA_ARTIFACT_FILENAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(_sanitize_json_payload(payload), indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _sanitize_json_payload(payload: Any) -> Any:
    if isinstance(payload, Mapping):
        return sanitize_log_payload(payload)
    return sanitize_log_payload({"payload": payload}).get("payload")


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    rows = [_csv_row(sanitize_log_payload(row)) for row in rows]
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _csv_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {key: _csv_value(value) for key, value in row.items()}


def _csv_value(value: Any) -> Any:
    if isinstance(value, Mapping) or isinstance(value, list):
        return json.dumps(value, sort_keys=True)
    return value


def _extra_artifact_path(output_dir: Path, filename: str, suffix: str) -> Path:
    path = Path(filename)
    if (
        not filename
        or path.is_absolute()
        or path.name != filename
        or "/" in filename
        or "\\" in filename
        or path.stem == ""
        or path.suffix != suffix
        or filename in _RESERVED_ARTIFACT_FILENAMES
        or _EXTRA_ARTIFACT_FILENAME_RE.fullmatch(filename) is None
    ):
        raise ValueError(f"invalid extra artifact filename: {filename!r}")
    return output_dir / filename


def write_execution_artifacts(
    root: Path,
    execution_id: str,
    request_snapshot: Mapping[str, Any],
    log_events: Iterable[Mapping[str, Any]],
    summary: Mapping[str, Any],
    child_orders: Iterable[Mapping[str, Any]],
    fills: Iterable[Mapping[str, Any]],
    timeline: Iterable[Mapping[str, Any]],
    twap_slice_ledger: Iterable[Mapping[str, Any]] = (),
    *,
    extra_json_artifacts: Mapping[str, Any] | None = None,
    extra_csv_artifacts: Mapping[str, Iterable[Mapping[str, Any]]] | None = None,
) -> Path:
    output_dir = root / execution_id
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(output_dir / "request_snapshot.json", request_snapshot)
    (output_dir / "execution_log.jsonl").write_text("", encoding="utf-8")
    for event in log_events:
        append_jsonl(output_dir / "execution_log.jsonl", event)
    _write_json(output_dir / "execution_summary.json", summary)
    _write_csv(output_dir / "child_orders.csv", child_orders)
    _write_csv(output_dir / "fills.csv", fills)
    _write_csv(output_dir / "timeline.csv", timeline)
    _write_csv(output_dir / "twap_slice_ledger.csv", twap_slice_ledger)
    for filename, payload in (extra_json_artifacts or {}).items():
        _write_json(_extra_artifact_path(output_dir, filename, ".json"), payload)
    for filename, rows in (extra_csv_artifacts or {}).items():
        _write_csv(_extra_artifact_path(output_dir, filename, ".csv"), rows)
    return output_dir
