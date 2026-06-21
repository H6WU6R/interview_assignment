from decimal import Decimal

import pytest

from algorithms.chase import ChaseDecision, chase_desired_price, reprice_difference_bps, should_reprice
from execution.models import RepricingMode, Side


def test_passive_desired_price_uses_near_touch() -> None:
    assert chase_desired_price(Side.BUY, Decimal("100"), Decimal("101"), passive=True) == Decimal("100")
    assert chase_desired_price(Side.SELL, Decimal("100"), Decimal("101"), passive=True) == Decimal("101")


def test_aggressive_desired_price_crosses_to_opposite_touch() -> None:
    assert chase_desired_price(Side.BUY, Decimal("100"), Decimal("101"), passive=False) == Decimal("101")
    assert chase_desired_price(Side.SELL, Decimal("100"), Decimal("101"), passive=False) == Decimal("100")


def test_no_action_desired_price_raises() -> None:
    with pytest.raises(ValueError, match="NO_ACTION"):
        chase_desired_price(Side.NO_ACTION, Decimal("100"), Decimal("101"), passive=True)


def test_adverse_only_buy_reprices_only_when_desired_moves_up() -> None:
    assert (
        should_reprice(
            Side.BUY,
            active_order_price=Decimal("100"),
            desired_price=Decimal("100.03"),
            threshold_bps=Decimal("2"),
            min_interval_ms=500,
            elapsed_since_last_reprice_ms=500,
            repricing_mode=RepricingMode.ADVERSE_ONLY,
        )
        is ChaseDecision.REPRICE
    )


def test_adverse_only_buy_waits_when_desired_moves_down() -> None:
    assert (
        should_reprice(
            Side.BUY,
            active_order_price=Decimal("100"),
            desired_price=Decimal("99"),
            threshold_bps=Decimal("2"),
            min_interval_ms=500,
            elapsed_since_last_reprice_ms=500,
            repricing_mode=RepricingMode.ADVERSE_ONLY,
        )
        is ChaseDecision.WAIT
    )


def test_adverse_only_sell_reprices_only_when_desired_moves_down() -> None:
    assert (
        should_reprice(
            Side.SELL,
            active_order_price=Decimal("100"),
            desired_price=Decimal("99.97"),
            threshold_bps=Decimal("2"),
            min_interval_ms=500,
            elapsed_since_last_reprice_ms=500,
            repricing_mode=RepricingMode.ADVERSE_ONLY,
        )
        is ChaseDecision.REPRICE
    )


def test_two_sided_reprices_on_favorable_move() -> None:
    assert (
        should_reprice(
            Side.BUY,
            active_order_price=Decimal("100"),
            desired_price=Decimal("99.97"),
            threshold_bps=Decimal("2"),
            min_interval_ms=500,
            elapsed_since_last_reprice_ms=500,
            repricing_mode=RepricingMode.TWO_SIDED,
        )
        is ChaseDecision.REPRICE
    )


def test_min_interval_blocks_repricing() -> None:
    assert (
        should_reprice(
            Side.BUY,
            active_order_price=Decimal("100"),
            desired_price=Decimal("101"),
            threshold_bps=Decimal("2"),
            min_interval_ms=500,
            elapsed_since_last_reprice_ms=499,
            repricing_mode=RepricingMode.ADVERSE_ONLY,
        )
        is ChaseDecision.WAIT
    )


def test_threshold_equality_reprices_but_just_below_waits() -> None:
    assert reprice_difference_bps(Decimal("100.02"), Decimal("100")) == Decimal("2.0000")
    assert (
        should_reprice(
            Side.BUY,
            active_order_price=Decimal("100"),
            desired_price=Decimal("100.02"),
            threshold_bps=Decimal("2"),
            min_interval_ms=500,
            elapsed_since_last_reprice_ms=500,
            repricing_mode=RepricingMode.ADVERSE_ONLY,
        )
        is ChaseDecision.REPRICE
    )
    assert (
        should_reprice(
            Side.BUY,
            active_order_price=Decimal("100"),
            desired_price=Decimal("100.019"),
            threshold_bps=Decimal("2"),
            min_interval_ms=500,
            elapsed_since_last_reprice_ms=500,
            repricing_mode=RepricingMode.ADVERSE_ONLY,
        )
        is ChaseDecision.WAIT
    )


def test_zero_active_order_price_waits_without_division_crash() -> None:
    assert reprice_difference_bps(Decimal("100"), Decimal("0")) == Decimal("0")
    assert (
        should_reprice(
            Side.BUY,
            active_order_price=Decimal("0"),
            desired_price=Decimal("100"),
            threshold_bps=Decimal("2"),
            min_interval_ms=500,
            elapsed_since_last_reprice_ms=500,
            repricing_mode=RepricingMode.ADVERSE_ONLY,
        )
        is ChaseDecision.WAIT
    )
