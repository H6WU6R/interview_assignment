from __future__ import annotations

import csv
import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from observability.logging import append_jsonl, sanitize_log_payload


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(sanitize_log_payload(payload), indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    rows = [sanitize_log_payload(row) for row in rows]
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_execution_artifacts(
    root: Path,
    execution_id: str,
    request_snapshot: Mapping[str, Any],
    log_events: Iterable[Mapping[str, Any]],
    summary: Mapping[str, Any],
    child_orders: Iterable[Mapping[str, Any]],
    fills: Iterable[Mapping[str, Any]],
    timeline: Iterable[Mapping[str, Any]],
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
    return output_dir
