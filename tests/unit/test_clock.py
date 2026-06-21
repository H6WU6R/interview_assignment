import pytest

from execution.clock import ManualClock


def test_manual_clock_rejects_negative_advance() -> None:
    clock = ManualClock(current=10.0)

    with pytest.raises(ValueError):
        clock.advance(-1.0)

    assert clock.monotonic() == 10.0
