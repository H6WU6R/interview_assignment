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


def overfill_quantity(filled_quantity: Decimal, required_quantity: Decimal) -> Decimal:
    excess = filled_quantity - required_quantity
    return excess if excess > Decimal("0") else Decimal("0")


def summary_metrics(
    final_status: ExecutionStatus,
    side: Side,
    raw_required_quantity: Decimal,
    required_quantity: Decimal,
    target_dust_quantity: Decimal,
    filled_quantity: Decimal,
    arrival_bid: Decimal,
    arrival_ask: Decimal,
    vwap: Decimal,
    requested_duration_seconds: int,
    actual_duration_seconds: Decimal,
    price_bound_violations: int,
    duplicate_events_ignored: int,
    unknown_orders_reconciled: int,
    max_reserved_exposure: Decimal,
) -> dict[str, str | int]:
    unfilled = required_quantity - filled_quantity
    if unfilled < Decimal("0"):
        unfilled = Decimal("0")
    arrival_mid = (arrival_bid + arrival_ask) / Decimal("2")
    slippage = (
        Decimal("0")
        if filled_quantity == Decimal("0") or vwap == Decimal("0")
        else slippage_bps(side, arrival_mid, vwap)
    )
    return {
        "final_status": final_status.value,
        "raw_required_quantity": decimal_string(raw_required_quantity),
        "required_quantity": decimal_string(required_quantity),
        "target_dust_quantity": decimal_string(target_dust_quantity),
        "filled_quantity": decimal_string(filled_quantity),
        "unfilled_quantity": decimal_string(unfilled),
        "completion_rate": decimal_string(completion_rate(filled_quantity, required_quantity)),
        "arrival_bid": decimal_string(arrival_bid),
        "arrival_ask": decimal_string(arrival_ask),
        "arrival_mid": decimal_string(arrival_mid),
        "execution_vwap": decimal_string(vwap),
        "slippage_bps": decimal_string(slippage),
        "requested_duration_seconds": requested_duration_seconds,
        "actual_duration_seconds": decimal_string(actual_duration_seconds),
        "price_bound_violations": price_bound_violations,
        "duplicate_events_ignored": duplicate_events_ignored,
        "unknown_orders_reconciled": unknown_orders_reconciled,
        "max_reserved_exposure": decimal_string(max_reserved_exposure),
        "overfill_quantity": decimal_string(overfill_quantity(filled_quantity, required_quantity)),
    }
