"""Decimal rounding and execution metric helpers."""

from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

from execution.models import Side


def floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    """Round a non-negative Decimal value down to the nearest exchange step."""

    if value < Decimal("0"):
        raise ValueError("value must be non-negative")
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
    """Round a non-negative Decimal value up to the nearest exchange step."""

    if value < Decimal("0"):
        raise ValueError("value must be non-negative")
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


def round_price(price: Decimal, tick_size: Decimal, side: Side, passive: bool) -> Decimal:
    """Round a Decimal price according to side and passive/aggressive intent."""

    units = price / tick_size
    if passive:
        rounding = ROUND_FLOOR if side is Side.BUY else ROUND_CEILING
    else:
        rounding = ROUND_CEILING if side is Side.BUY else ROUND_FLOOR
    return units.to_integral_value(rounding=rounding) * tick_size


def completion_rate(filled_quantity: Decimal, required_quantity: Decimal) -> Decimal:
    """Return filled quantity as a fraction of required quantity."""

    if required_quantity == Decimal("0"):
        return Decimal("1")
    return abs(filled_quantity) / abs(required_quantity)


def slippage_bps(side: Side, arrival_mid: Decimal, execution_vwap: Decimal) -> Decimal:
    """Return side-aware execution slippage from arrival mid in basis points."""

    if arrival_mid == Decimal("0"):
        return Decimal("0")
    if side is Side.BUY:
        return (execution_vwap - arrival_mid) / arrival_mid * Decimal("10000")
    if side is Side.SELL:
        return (arrival_mid - execution_vwap) / arrival_mid * Decimal("10000")
    return Decimal("0")
