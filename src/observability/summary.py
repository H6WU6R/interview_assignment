from __future__ import annotations

from decimal import Decimal

from execution.models import ExecutionStatus, Side
from risk.decimal_math import completion_rate, slippage_bps


def decimal_string(value: Decimal) -> str:
    return format(value.normalize(), "f")


def execution_vwap(fills: list[tuple[Decimal, Decimal]]) -> Decimal:
    filled_quantity = sum(quantity for _, quantity in fills)
    if filled_quantity == Decimal("0"):
        return Decimal("0")
    notional = sum(price * quantity for price, quantity in fills)
    return notional / filled_quantity


def summary_metrics(
    final_status: ExecutionStatus,
    side: Side,
    required_quantity: Decimal,
    filled_quantity: Decimal,
    arrival_mid: Decimal,
    vwap: Decimal,
) -> dict[str, str]:
    return {
        "final_status": final_status.value,
        "required_quantity": decimal_string(required_quantity),
        "filled_quantity": decimal_string(filled_quantity),
        "completion_rate": decimal_string(completion_rate(filled_quantity, required_quantity)),
        "slippage_bps": decimal_string(slippage_bps(side, arrival_mid, vwap)),
    }
