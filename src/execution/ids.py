from __future__ import annotations

import re
import uuid

CLIENT_ORDER_ID_RE = re.compile(r"^[\.A-Z\:/a-z0-9_-]{1,36}$")


def execution_id() -> str:
    return f"exec_{uuid.uuid4().hex[:16]}"


def child_order_id(sequence: int) -> str:
    return f"child_{sequence:04d}"


def make_client_order_prefix(execution_id_value: str) -> str:
    short_exec = execution_id_value.replace("exec_", "")[:12]
    return f"ce_{short_exec}_"


def make_client_order_id(execution_id_value: str, child_sequence: int) -> str:
    value = f"{make_client_order_prefix(execution_id_value)}{child_sequence}"
    if len(value) > 36 or not CLIENT_ORDER_ID_RE.fullmatch(value):
        raise ValueError(f"invalid Binance client order id: {value}")
    return value


def client_order_id(execution_id_value: str, child_order_id_value: str) -> str:
    child_seq = child_order_id_value.replace("child_", "").lstrip("0") or "0"
    return make_client_order_id(execution_id_value, int(child_seq))
