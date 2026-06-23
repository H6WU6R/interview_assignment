from decimal import Decimal

import pytest

from execution.models import Exposure, Side, SymbolRules
from risk.validation import (
    ValidationError,
    check_exposure_invariant,
    validate_child_order_safety,
    validate_order_shape,
    validate_price_bounds,
    validate_quantity,
)


RULES = SymbolRules(
    symbol="BTCUSDT",
    tick_size=Decimal("0.10"),
    quantity_step=Decimal("0.001"),
    min_quantity=Decimal("0.001"),
    min_notional=Decimal("5"),
    status="TRADING",
)


def test_validate_quantity_rejects_below_min_notional() -> None:
    with pytest.raises(ValidationError):
        validate_quantity(Decimal("0.001"), Decimal("1000"), RULES)


def test_validate_price_bounds_rejects_aggressive_buy_above_upper() -> None:
    with pytest.raises(ValidationError):
        validate_price_bounds(Side.BUY, Decimal("101"), Decimal("90"), Decimal("100"))


def test_validate_child_order_safety_rejects_buy_above_upper_bound() -> None:
    with pytest.raises(ValidationError, match="upper bound"):
        validate_child_order_safety(
            quantity=Decimal("0.1"),
            price=Decimal("102"),
            side=Side.BUY,
            rules=RULES,
            best_bid=Decimal("100"),
            best_ask=Decimal("103"),
            post_only=True,
            lower=Decimal("90"),
            upper=Decimal("101"),
        )


def test_validate_child_order_safety_rejects_sell_below_lower_bound() -> None:
    with pytest.raises(ValidationError, match="lower bound"):
        validate_child_order_safety(
            quantity=Decimal("0.1"),
            price=Decimal("98"),
            side=Side.SELL,
            rules=RULES,
            best_bid=Decimal("97"),
            best_ask=Decimal("101"),
            post_only=True,
            lower=Decimal("99"),
            upper=Decimal("110"),
        )


def test_validate_order_shape_rejects_non_step_quantity_and_non_tick_price() -> None:
    with pytest.raises(ValidationError, match="quantity step"):
        validate_order_shape(
            quantity=Decimal("0.0015"),
            price=Decimal("100.10"),
            side=Side.BUY,
            rules=RULES,
            best_bid=Decimal("100"),
            best_ask=Decimal("101"),
            post_only=True,
        )
    with pytest.raises(ValidationError, match="tick size"):
        validate_order_shape(
            quantity=Decimal("0.002"),
            price=Decimal("100.15"),
            side=Side.BUY,
            rules=RULES,
            best_bid=Decimal("100"),
            best_ask=Decimal("101"),
            post_only=True,
        )


def test_validate_order_shape_rejects_post_only_crossing_prices() -> None:
    with pytest.raises(ValidationError, match="post-only buy"):
        validate_order_shape(
            quantity=Decimal("0.002"),
            price=Decimal("101"),
            side=Side.BUY,
            rules=RULES,
            best_bid=Decimal("100"),
            best_ask=Decimal("101"),
            post_only=True,
        )
    with pytest.raises(ValidationError, match="post-only sell"):
        validate_order_shape(
            quantity=Decimal("0.002"),
            price=Decimal("100"),
            side=Side.SELL,
            rules=RULES,
            best_bid=Decimal("100"),
            best_ask=Decimal("101"),
            post_only=True,
        )


def test_validate_order_shape_rejects_non_trading_symbol() -> None:
    rules = SymbolRules(
        symbol="BTCUSDT",
        tick_size=Decimal("0.10"),
        quantity_step=Decimal("0.001"),
        min_quantity=Decimal("0.001"),
        min_notional=Decimal("5"),
        status="BREAK",
    )

    with pytest.raises(ValidationError, match="not trading"):
        validate_order_shape(
            quantity=Decimal("0.001"),
            price=Decimal("100000"),
            side=Side.BUY,
            rules=rules,
            best_bid=Decimal("99999.9"),
            best_ask=Decimal("100000.1"),
            post_only=True,
        )


def test_validate_order_shape_rejects_non_positive_tick_and_quantity_step() -> None:
    zero_step_rules = SymbolRules(
        symbol="BTCUSDT",
        tick_size=Decimal("0.10"),
        quantity_step=Decimal("0"),
        min_quantity=Decimal("0.001"),
        min_notional=Decimal("5"),
        status="TRADING",
    )
    with pytest.raises(ValidationError, match="quantity step"):
        validate_order_shape(
            quantity=Decimal("0.1"),
            price=Decimal("100.10"),
            side=Side.BUY,
            rules=zero_step_rules,
            best_bid=Decimal("100"),
            best_ask=Decimal("101"),
            post_only=True,
        )

    negative_tick_rules = SymbolRules(
        symbol="BTCUSDT",
        tick_size=Decimal("-0.10"),
        quantity_step=Decimal("0.001"),
        min_quantity=Decimal("0.001"),
        min_notional=Decimal("5"),
        status="TRADING",
    )
    with pytest.raises(ValidationError, match="tick size"):
        validate_order_shape(
            quantity=Decimal("0.1"),
            price=Decimal("100.10"),
            side=Side.BUY,
            rules=negative_tick_rules,
            best_bid=Decimal("100"),
            best_ask=Decimal("101"),
            post_only=True,
        )


def test_exposure_invariant_rejects_over_reserved_quantity() -> None:
    exposure = Exposure(
        confirmed_filled_quantity=Decimal("0.005"),
        live_open_quantity=Decimal("0.003"),
    )
    with pytest.raises(ValidationError):
        check_exposure_invariant(exposure, Decimal("0.001"), Decimal("0.008"))


def test_exposure_invariant_allows_explicit_permitted_tolerance() -> None:
    exposure = Exposure(
        confirmed_filled_quantity=Decimal("0.005"),
        live_open_quantity=Decimal("0.003"),
    )

    check_exposure_invariant(
        exposure,
        Decimal("0.001"),
        Decimal("0.008"),
        permitted_tolerance=Decimal("0.001"),
    )


def test_exposure_invariant_rejects_negative_permitted_tolerance() -> None:
    with pytest.raises(ValidationError, match="permitted tolerance"):
        check_exposure_invariant(
            Exposure(),
            Decimal("0"),
            Decimal("0.008"),
            permitted_tolerance=Decimal("-0.001"),
        )


def test_exposure_invariant_counts_all_reserved_buckets() -> None:
    exposure = Exposure(
        confirmed_filled_quantity=Decimal("0.002"),
        live_open_quantity=Decimal("0.001"),
        pending_submit_quantity=Decimal("0.001"),
        pending_cancel_quantity=Decimal("0.001"),
        unknown_order_quantity=Decimal("0.001"),
    )
    with pytest.raises(ValidationError):
        check_exposure_invariant(exposure, Decimal("0.001"), Decimal("0.006"))
