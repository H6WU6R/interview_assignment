from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

from execution.models import Side


def floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if value < Decimal("0"):
        raise ValueError("value must be non-negative")
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
    if value < Decimal("0"):
        raise ValueError("value must be non-negative")
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


def round_price(price: Decimal, tick_size: Decimal, side: Side, passive: bool) -> Decimal:
    units = price / tick_size
    if passive:
        rounding = ROUND_FLOOR if side is Side.BUY else ROUND_CEILING
    else:
        rounding = ROUND_CEILING if side is Side.BUY else ROUND_FLOOR
    return units.to_integral_value(rounding=rounding) * tick_size


def completion_rate(filled_quantity: Decimal, required_quantity: Decimal) -> Decimal:
    if required_quantity == Decimal("0"):
        return Decimal("1")
    return abs(filled_quantity) / abs(required_quantity)


def slippage_bps(side: Side, arrival_mid: Decimal, execution_vwap: Decimal) -> Decimal:
    if arrival_mid == Decimal("0"):
        return Decimal("0")
    if side is Side.BUY:
        return (execution_vwap - arrival_mid) / arrival_mid * Decimal("10000")
    if side is Side.SELL:
        return (arrival_mid - execution_vwap) / arrival_mid * Decimal("10000")
    return Decimal("0")
