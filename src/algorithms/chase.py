"""Pure Chase order decision helpers."""

from __future__ import annotations

from decimal import Decimal
from enum import StrEnum

from execution.models import RepricingMode, Side


class ChaseDecision(StrEnum):
    """Decision returned by Chase repricing logic."""

    WAIT = "WAIT"
    REPRICE = "REPRICE"


def chase_desired_price(
    side: Side, best_bid: Decimal, best_ask: Decimal, passive: bool
) -> Decimal:
    """Return the near-touch price Chase should use for the requested side."""

    if side is Side.NO_ACTION:
        raise ValueError("NO_ACTION does not have a tradable chase price")
    if side is Side.BUY:
        return best_bid if passive else best_ask
    if side is Side.SELL:
        return best_ask if passive else best_bid
    raise ValueError(f"unsupported side: {side}")


def reprice_difference_bps(
    desired_price: Decimal, active_order_price: Decimal
) -> Decimal:
    """Return absolute price movement from active price to desired price in basis points."""

    if active_order_price <= Decimal("0"):
        return Decimal("0")
    return (
        abs(desired_price - active_order_price) / active_order_price * Decimal("10000")
    )


def should_reprice(
    side: Side,
    active_order_price: Decimal,
    desired_price: Decimal,
    threshold_bps: Decimal,
    min_interval_ms: int,
    elapsed_since_last_reprice_ms: int,
    repricing_mode: RepricingMode,
) -> ChaseDecision:
    """Decide whether an active Chase order should be cancelled and replaced."""

    if side is Side.NO_ACTION or elapsed_since_last_reprice_ms < min_interval_ms:
        return ChaseDecision.WAIT

    if active_order_price <= Decimal("0"):
        return ChaseDecision.WAIT

    if reprice_difference_bps(desired_price, active_order_price) < threshold_bps:
        return ChaseDecision.WAIT

    if repricing_mode is RepricingMode.TWO_SIDED:
        return ChaseDecision.REPRICE

    if repricing_mode is RepricingMode.ADVERSE_ONLY:
        if side is Side.BUY and desired_price > active_order_price:
            return ChaseDecision.REPRICE
        if side is Side.SELL and desired_price < active_order_price:
            return ChaseDecision.REPRICE

    return ChaseDecision.WAIT
