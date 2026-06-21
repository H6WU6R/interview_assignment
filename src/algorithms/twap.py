from __future__ import annotations

from decimal import Decimal

from execution.models import Exposure


def scheduled_cumulative_quantity(
    total_trade_quantity: Decimal,
    elapsed_time: Decimal,
    total_duration: Decimal,
) -> Decimal:
    if total_duration <= Decimal("0"):
        raise ValueError("total_duration must be positive")
    if total_trade_quantity < Decimal("0"):
        raise ValueError("total_trade_quantity must be an absolute non-negative quantity")

    clamped_elapsed_time = min(max(elapsed_time, Decimal("0")), total_duration)
    return total_trade_quantity * clamped_elapsed_time / total_duration


def scheduled_deficit(
    scheduled_cumulative: Decimal,
    confirmed_cumulative_filled: Decimal,
) -> Decimal:
    if scheduled_cumulative < Decimal("0"):
        raise ValueError("scheduled_cumulative must be non-negative")
    if confirmed_cumulative_filled < Decimal("0"):
        raise ValueError("confirmed_cumulative_filled must be non-negative")

    deficit = scheduled_cumulative - confirmed_cumulative_filled
    return deficit if deficit > Decimal("0") else Decimal("0")


def safe_child_quantity(deficit: Decimal, exposure: Exposure) -> Decimal:
    if deficit < Decimal("0"):
        raise ValueError("deficit must be non-negative")

    quantity = deficit - exposure.reserved_exposure
    return quantity if quantity > Decimal("0") else Decimal("0")
