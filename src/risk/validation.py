"""Risk validation helpers for order shape and exposure safety."""

from __future__ import annotations

from decimal import Decimal

from execution.models import Exposure, Side, SymbolRules


class ValidationError(ValueError):
    """Raised when an order or exposure safety check fails."""

    pass


def validate_quantity(quantity: Decimal, price: Decimal, rules: SymbolRules) -> None:
    """Reject quantities that violate exchange quantity or notional rules."""

    if quantity < rules.min_quantity:
        raise ValidationError(f"quantity {quantity} below min quantity {rules.min_quantity}")
    notional = quantity * price
    if notional < rules.min_notional:
        raise ValidationError(f"notional {notional} below min notional {rules.min_notional}")


def validate_price_bounds(side: Side, price: Decimal, lower: Decimal, upper: Decimal) -> None:
    """Reject prices outside the target price band for the trade side."""

    if lower > upper:
        raise ValidationError("lower price bound cannot exceed upper price bound")
    if side is Side.BUY and price > upper:
        raise ValidationError(f"buy price {price} exceeds upper bound {upper}")
    if side is Side.SELL and price < lower:
        raise ValidationError(f"sell price {price} below lower bound {lower}")


def _is_multiple(value: Decimal, step: Decimal) -> bool:
    if step <= Decimal("0"):
        raise ValidationError(f"step must be positive: {step}")
    return value % step == Decimal("0")


def validate_order_shape(
    quantity: Decimal,
    price: Decimal,
    side: Side,
    rules: SymbolRules,
    best_bid: Decimal,
    best_ask: Decimal,
    post_only: bool,
) -> None:
    """Reject orders that violate tick, post-only, or quantity rules."""

    if rules.status != "TRADING":
        raise ValidationError(f"symbol {rules.symbol} is not trading: status={rules.status}")
    if rules.quantity_step <= Decimal("0"):
        raise ValidationError(f"quantity step must be positive: {rules.quantity_step}")
    if rules.tick_size <= Decimal("0"):
        raise ValidationError(f"tick size must be positive: {rules.tick_size}")
    if post_only and "GTX" not in rules.supported_time_in_force:
        raise ValidationError(f"post-only time in force unsupported for {rules.symbol}")
    if not _is_multiple(quantity, rules.quantity_step):
        raise ValidationError(f"quantity step violation: {quantity} not multiple of {rules.quantity_step}")
    if not _is_multiple(price, rules.tick_size):
        raise ValidationError(f"tick size violation: {price} not multiple of {rules.tick_size}")
    if post_only and side is Side.BUY and price >= best_ask:
        raise ValidationError(f"post-only buy would cross ask: price={price} ask={best_ask}")
    if post_only and side is Side.SELL and price <= best_bid:
        raise ValidationError(f"post-only sell would cross bid: price={price} bid={best_bid}")
    validate_quantity(quantity, price, rules)


def validate_child_order_safety(
    quantity: Decimal,
    price: Decimal,
    side: Side,
    rules: SymbolRules,
    best_bid: Decimal,
    best_ask: Decimal,
    post_only: bool,
    lower: Decimal,
    upper: Decimal,
) -> None:
    """Reject child orders that violate price bounds or exchange order shape rules."""

    validate_price_bounds(side, price, lower, upper)
    validate_order_shape(quantity, price, side, rules, best_bid, best_ask, post_only)


def check_exposure_invariant(
    exposure: Exposure,
    new_child_quantity: Decimal,
    normalized_target_trade_quantity: Decimal,
) -> None:
    """Reject exposure that would exceed the normalized target trade quantity."""

    total = exposure.confirmed_filled_quantity + exposure.reserved_exposure + new_child_quantity
    if total > normalized_target_trade_quantity:
        raise ValidationError(
            "confirmed fills plus reserved exposure plus new child quantity "
            f"{total} exceeds target {normalized_target_trade_quantity}"
        )
