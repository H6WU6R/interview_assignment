"""JSON logging helpers with secret sanitization."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

SENSITIVE_KEY_CONCEPTS = (
    "apikey",
    "mbxapikey",
    "secret",
    "signature",
    "listenkey",
    "rawauthenticatedrequest",
    "authenticatedrequest",
    "signedrequest",
    "signedpayload",
    "signedparams",
)
SENSITIVE_CONTAINER_PREFIXES = ("raw", "authenticated", "signed")
SENSITIVE_CONTAINER_TARGETS = ("request", "payload", "params")

_KEY_NORMALIZATION_TABLE = str.maketrans("", "", "_- ")


def to_jsonable(value: Any) -> Any:
    """Convert common runtime values into JSON-serializable values."""

    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {key: to_jsonable(inner) for key, inner in value.items()}
    if isinstance(value, list | tuple):
        return [to_jsonable(item) for item in value]
    return value


def sanitize_log_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return a JSON-safe payload with secrets and signed request data removed."""

    sanitized: dict[str, Any] = {}
    for key, value in payload.items():
        if _is_sensitive_key(key):
            continue
        sanitized[str(key)] = _sanitize_value(value)
    return sanitized


def _is_sensitive_key(key: Any) -> bool:
    normalized = str(key).lower().translate(_KEY_NORMALIZATION_TABLE)
    return any(concept in normalized for concept in SENSITIVE_KEY_CONCEPTS) or (
        any(normalized.startswith(prefix) for prefix in SENSITIVE_CONTAINER_PREFIXES)
        and any(target in normalized for target in SENSITIVE_CONTAINER_TARGETS)
    )


def _sanitize_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return sanitize_log_payload(value)
    if isinstance(value, list | tuple):
        return [_sanitize_value(item) for item in value]
    return to_jsonable(value)


def append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    """Append one sanitized payload as a JSON Lines record."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(sanitize_log_payload(payload), sort_keys=True) + "\n")
