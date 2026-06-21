from decimal import Decimal

import pytest

from execution.clock import ManualClock
from exchanges.simulator import DeterministicSimulator, NoFreshMarketData


async def test_simulator_has_no_actionable_quote_before_first_snapshot() -> None:
    simulator = DeterministicSimulator(clock=ManualClock())

    with pytest.raises(NoFreshMarketData):
        await simulator.get_best_bid_ask("BTCUSDT")


async def test_simulator_returns_fresh_snapshot_after_market_event() -> None:
    clock = ManualClock()
    simulator = DeterministicSimulator(clock=clock)

    await simulator.push_market_data("BTCUSDT", Decimal("100"), Decimal("101"), exchange_event_time=10)
    snapshot = await simulator.get_best_bid_ask("BTCUSDT")

    assert snapshot.bid == Decimal("100")
    assert snapshot.ask == Decimal("101")
    assert snapshot.last_market_event_time_local_monotonic == 0


async def test_simulator_rejects_stale_market_data() -> None:
    clock = ManualClock()
    simulator = DeterministicSimulator(clock=clock)

    await simulator.push_market_data("BTCUSDT", Decimal("100"), Decimal("101"), exchange_event_time=10)
    clock.advance(1.6)

    with pytest.raises(NoFreshMarketData):
        await simulator.get_best_bid_ask("BTCUSDT")


async def test_simulator_rejects_crossed_market_data() -> None:
    simulator = DeterministicSimulator(clock=ManualClock())

    await simulator.push_market_data("BTCUSDT", Decimal("101"), Decimal("101"), exchange_event_time=10)

    with pytest.raises(NoFreshMarketData):
        await simulator.get_best_bid_ask("BTCUSDT")
