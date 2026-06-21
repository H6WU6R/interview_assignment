from decimal import Decimal

from execution.models import Side
from risk.decimal_math import ceil_to_step, completion_rate, floor_to_step, round_price, slippage_bps


def test_floor_to_step_rounds_toward_zero() -> None:
    assert floor_to_step(Decimal("0.0089"), Decimal("0.001")) == Decimal("0.008")


def test_ceil_to_step_rounds_positive_values_up() -> None:
    assert ceil_to_step(Decimal("0.0081"), Decimal("0.001")) == Decimal("0.009")


def test_passive_buy_rounds_down_to_tick() -> None:
    assert round_price(Decimal("94000.09"), Decimal("0.10"), Side.BUY, passive=True) == Decimal("94000.0")


def test_passive_sell_rounds_up_to_tick() -> None:
    assert round_price(Decimal("94000.01"), Decimal("0.10"), Side.SELL, passive=True) == Decimal("94000.1")


def test_aggressive_buy_rounds_up_to_tick() -> None:
    assert round_price(Decimal("94000.01"), Decimal("0.10"), Side.BUY, passive=False) == Decimal("94000.1")


def test_aggressive_sell_rounds_down_to_tick() -> None:
    assert round_price(Decimal("94000.09"), Decimal("0.10"), Side.SELL, passive=False) == Decimal("94000.0")


def test_completion_rate_uses_absolute_quantity() -> None:
    assert completion_rate(Decimal("0.004"), Decimal("0.008")) == Decimal("0.5")
    assert completion_rate(Decimal("-0.004"), Decimal("0.008")) == Decimal("0.5")


def test_slippage_bps_is_side_aware() -> None:
    assert slippage_bps(Side.BUY, Decimal("100"), Decimal("101")) == Decimal("100")
    assert slippage_bps(Side.SELL, Decimal("100"), Decimal("99")) == Decimal("100")
