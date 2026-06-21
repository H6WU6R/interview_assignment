from decimal import Decimal

import pytest

from execution.engine import ExposureTracker
from risk.validation import ValidationError


def tracker(target: str = "1") -> ExposureTracker:
    return ExposureTracker(target_quantity=Decimal(target))


def test_unknown_create_exposure_is_reserved_until_reconciled() -> None:
    exposure = tracker()

    exposure.reserve_unknown_create(Decimal("0.4"))

    assert exposure.exposure.unknown_order_quantity == Decimal("0.4")
    assert exposure.available_to_submit() == Decimal("0.6")

    exposure.clear_unknown_create(Decimal("0.1"))

    assert exposure.exposure.unknown_order_quantity == Decimal("0.3")
    assert exposure.available_to_submit() == Decimal("0.7")

    exposure.clear_unknown_create()

    assert exposure.exposure.unknown_order_quantity == Decimal("0")
    assert exposure.available_to_submit() == Decimal("1")


def test_pending_submit_exposure_is_included_in_invariant_and_can_be_released() -> None:
    exposure = tracker()

    exposure.reserve_pending_submit(Decimal("0.7"))

    assert exposure.exposure.pending_submit_quantity == Decimal("0.7")
    assert exposure.available_to_submit() == Decimal("0.3")
    with pytest.raises(ValidationError):
        exposure.check_can_submit(Decimal("0.31"))

    exposure.release_pending_submit(Decimal("0.5"))

    assert exposure.exposure.pending_submit_quantity == Decimal("0.2")
    assert exposure.available_to_submit() == Decimal("0.8")

    exposure.release_pending_submit(Decimal("1"))

    assert exposure.exposure.pending_submit_quantity == Decimal("0")


def test_ambiguous_cancel_moves_live_open_into_pending_cancel_and_keeps_reserved() -> None:
    exposure = tracker()
    exposure.reserve_live_open(Decimal("0.6"))

    exposure.mark_pending_cancel(Decimal("0.4"))

    assert exposure.exposure.live_open_quantity == Decimal("0.2")
    assert exposure.exposure.pending_cancel_quantity == Decimal("0.4")
    assert exposure.available_to_submit() == Decimal("0.4")
    with pytest.raises(ValidationError):
        exposure.check_can_submit(Decimal("0.41"))


def test_pending_cancel_request_larger_than_live_open_does_not_make_live_open_negative() -> None:
    exposure = tracker()
    exposure.reserve_live_open(Decimal("0.3"))

    exposure.mark_pending_cancel(Decimal("0.5"))

    assert exposure.exposure.live_open_quantity == Decimal("0")
    assert exposure.exposure.pending_cancel_quantity == Decimal("0.3")


def test_overfill_invariant_rejects_combined_exposure_over_target() -> None:
    exposure = tracker()
    exposure.apply_fill("trade-1", Decimal("0.2"))
    exposure.reserve_live_open(Decimal("0.2"))
    exposure.reserve_pending_submit(Decimal("0.2"))
    exposure.mark_pending_cancel(Decimal("0.1"))
    exposure.reserve_unknown_create(Decimal("0.2"))

    with pytest.raises(ValidationError):
        exposure.check_can_submit(Decimal("0.21"))


@pytest.mark.parametrize(
    ("reserve", "bucket"),
    [
        (lambda exposure: exposure.reserve_live_open(Decimal("0.8")), "live_open_quantity"),
        (lambda exposure: exposure.reserve_pending_submit(Decimal("0.8")), "pending_submit_quantity"),
        (
            lambda exposure: (
                exposure.reserve_live_open(Decimal("0.8")),
                exposure.mark_pending_cancel(Decimal("0.8")),
            ),
            "pending_cancel_quantity",
        ),
        (lambda exposure: exposure.reserve_unknown_create(Decimal("0.8")), "unknown_order_quantity"),
    ],
)
def test_each_reserved_bucket_contributes_to_overfill(reserve, bucket: str) -> None:
    exposure = tracker()

    reserve(exposure)

    assert getattr(exposure.exposure, bucket) == Decimal("0.8")
    with pytest.raises(ValidationError):
        exposure.check_can_submit(Decimal("0.21"))


def test_available_to_submit_floors_at_zero() -> None:
    exposure = tracker()
    exposure.apply_fill("trade-1", Decimal("0.7"))
    exposure.set_live_open(Decimal("0.5"))

    assert exposure.available_to_submit() == Decimal("0")


def test_duplicate_trade_id_is_not_counted_twice() -> None:
    exposure = tracker()

    exposure.apply_fill("trade-1", Decimal("0.4"))
    exposure.apply_fill("trade-1", Decimal("0.8"))

    assert exposure.exposure.confirmed_filled_quantity == Decimal("0.4")


def test_out_of_order_stale_cumulative_fill_is_ignored() -> None:
    exposure = tracker()

    exposure.apply_fill("trade-1", Decimal("0.7"))
    exposure.apply_fill("trade-2", Decimal("0.5"))

    assert exposure.exposure.confirmed_filled_quantity == Decimal("0.7")
    assert exposure.seen_trade_ids == {"trade-1", "trade-2"}


def test_fill_without_trade_id_uses_monotonic_cumulative_quantity() -> None:
    exposure = tracker()

    exposure.apply_fill(None, Decimal("0.2"))
    exposure.apply_fill(None, Decimal("0.2"))
    exposure.apply_fill(None, Decimal("0.1"))
    exposure.apply_fill(None, Decimal("0.4"))

    assert exposure.exposure.confirmed_filled_quantity == Decimal("0.4")
    assert exposure.seen_trade_ids == set()


@pytest.mark.parametrize(
    "operation",
    [
        lambda exposure: exposure.check_can_submit(Decimal("-0.1")),
        lambda exposure: exposure.reserve_live_open(Decimal("-0.1")),
        lambda exposure: exposure.reserve_pending_submit(Decimal("-0.1")),
        lambda exposure: exposure.release_pending_submit(Decimal("-0.1")),
        lambda exposure: exposure.reserve_unknown_create(Decimal("-0.1")),
        lambda exposure: exposure.clear_unknown_create(Decimal("-0.1")),
        lambda exposure: exposure.mark_pending_cancel(Decimal("-0.1")),
        lambda exposure: exposure.release_pending_cancel(Decimal("-0.1")),
        lambda exposure: exposure.set_live_open(Decimal("-0.1")),
        lambda exposure: exposure.apply_fill("trade-1", Decimal("-0.1")),
    ],
)
def test_negative_quantities_and_cumulative_values_are_rejected(operation) -> None:
    exposure = tracker()

    with pytest.raises(ValueError):
        operation(exposure)
