from __future__ import annotations

from decimal import Decimal

import pytest

from algorithms.twap import (
    safe_child_quantity,
    scheduled_cumulative_quantity,
    scheduled_deficit,
)
from execution.models import Exposure


def test_scheduled_cumulative_uses_absolute_time_progress_formula() -> None:
    scheduled = scheduled_cumulative_quantity(
        total_trade_quantity=Decimal("1.0"),
        elapsed_time=Decimal("30"),
        total_duration=Decimal("120"),
    )

    assert scheduled == Decimal("0.25")


def test_scheduled_cumulative_clamps_elapsed_time_below_zero_to_zero() -> None:
    scheduled = scheduled_cumulative_quantity(
        total_trade_quantity=Decimal("1.0"),
        elapsed_time=Decimal("-1"),
        total_duration=Decimal("120"),
    )

    assert scheduled == Decimal("0")


def test_scheduled_cumulative_clamps_elapsed_time_above_duration_to_total_quantity() -> None:
    scheduled = scheduled_cumulative_quantity(
        total_trade_quantity=Decimal("1.0"),
        elapsed_time=Decimal("121"),
        total_duration=Decimal("120"),
    )

    assert scheduled == Decimal("1.0")


@pytest.mark.parametrize("total_duration", [Decimal("0"), Decimal("-1")])
def test_scheduled_cumulative_rejects_non_positive_total_duration(total_duration: Decimal) -> None:
    with pytest.raises(ValueError):
        scheduled_cumulative_quantity(
            total_trade_quantity=Decimal("1.0"),
            elapsed_time=Decimal("30"),
            total_duration=total_duration,
        )


def test_scheduled_cumulative_rejects_negative_total_trade_quantity() -> None:
    with pytest.raises(ValueError):
        scheduled_cumulative_quantity(
            total_trade_quantity=Decimal("-1.0"),
            elapsed_time=Decimal("30"),
            total_duration=Decimal("120"),
        )


def test_scheduled_deficit_subtracts_confirmed_fills() -> None:
    deficit = scheduled_deficit(
        scheduled_cumulative=Decimal("0.75"),
        confirmed_cumulative_filled=Decimal("0.20"),
    )

    assert deficit == Decimal("0.55")


def test_scheduled_deficit_floors_at_zero_when_ahead_of_schedule() -> None:
    deficit = scheduled_deficit(
        scheduled_cumulative=Decimal("0.75"),
        confirmed_cumulative_filled=Decimal("0.80"),
    )

    assert deficit == Decimal("0")


@pytest.mark.parametrize(
    ("scheduled_cumulative", "confirmed_cumulative_filled"),
    [
        (Decimal("-0.01"), Decimal("0")),
        (Decimal("0"), Decimal("-0.01")),
    ],
)
def test_scheduled_deficit_rejects_negative_inputs(
    scheduled_cumulative: Decimal,
    confirmed_cumulative_filled: Decimal,
) -> None:
    with pytest.raises(ValueError):
        scheduled_deficit(scheduled_cumulative, confirmed_cumulative_filled)


def test_safe_child_quantity_subtracts_all_reserved_exposure() -> None:
    exposure = Exposure(
        live_open_quantity=Decimal("0.10"),
        pending_submit_quantity=Decimal("0.20"),
        pending_cancel_quantity=Decimal("0.30"),
        unknown_order_quantity=Decimal("0.40"),
    )

    quantity = safe_child_quantity(deficit=Decimal("1.50"), exposure=exposure)

    assert quantity == Decimal("0.50")


def test_safe_child_quantity_floors_at_zero() -> None:
    exposure = Exposure(live_open_quantity=Decimal("0.75"))

    quantity = safe_child_quantity(deficit=Decimal("0.50"), exposure=exposure)

    assert quantity == Decimal("0")


def test_safe_child_quantity_rejects_negative_deficit() -> None:
    with pytest.raises(ValueError):
        safe_child_quantity(deficit=Decimal("-0.01"), exposure=Exposure())
