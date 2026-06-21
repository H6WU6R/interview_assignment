from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from decimal import Decimal

from exchanges.base import ExchangeAdapter, NoFreshMarketData
from execution.clock import Clock, ManualClock
from execution.models import (
    MarketSnapshot,
    OrderRequest,
    PositionSnapshot,
    ReconciliationResult,
    SymbolRules,
)


@dataclass
class DeterministicSimulator(ExchangeAdapter):
    clock: Clock = field(default_factory=ManualClock)
    position: Decimal = Decimal("0")
    stale_market_data_seconds: float = 1.5
    _market: dict[str, MarketSnapshot] = field(default_factory=dict)
    _market_queue: asyncio.Queue[MarketSnapshot] = field(default_factory=asyncio.Queue)

    async def get_symbol_rules(self, symbol: str) -> SymbolRules:
        return SymbolRules(
            symbol=symbol,
            tick_size=Decimal("0.10"),
            quantity_step=Decimal("0.001"),
            min_quantity=Decimal("0.001"),
            min_notional=Decimal("5"),
            status="TRADING",
            supported_time_in_force=frozenset({"GTC", "GTX"}),
        )

    async def get_position(self, symbol: str) -> PositionSnapshot:
        return PositionSnapshot(symbol=symbol, position=self.position)

    async def push_market_data(
        self,
        symbol: str,
        bid: Decimal,
        ask: Decimal,
        exchange_event_time: int | None = None,
    ) -> None:
        snapshot = MarketSnapshot(
            symbol=symbol,
            bid=bid,
            ask=ask,
            last_market_event_time_exchange=exchange_event_time,
            last_market_event_time_local_monotonic=self.clock.monotonic(),
        )
        self._market[symbol] = snapshot
        await self._market_queue.put(snapshot)

    async def get_best_bid_ask(self, symbol: str) -> MarketSnapshot:
        snapshot = self._market.get(symbol)
        if snapshot is None:
            raise NoFreshMarketData(f"no fresh market data for {symbol}")
        if snapshot.is_crossed:
            raise NoFreshMarketData(f"crossed market data for {symbol}")
        age = self.clock.monotonic() - snapshot.last_market_event_time_local_monotonic
        if age > self.stale_market_data_seconds:
            raise NoFreshMarketData(f"stale market data for {symbol}: age={age}")
        return snapshot

    async def stream_market_data(self) -> AsyncIterator[MarketSnapshot]:
        while True:
            yield await self._market_queue.get()

    async def submit_limit_order(self, order_request: OrderRequest) -> object:
        raise NotImplementedError("order lifecycle is added in the simulator order task")

    async def cancel_order(self, symbol: str, client_order_id: str) -> object:
        raise NotImplementedError("order lifecycle is added in the simulator order task")

    async def get_order_by_client_order_id(self, symbol: str, client_order_id: str) -> object:
        raise NotImplementedError("order lifecycle is added in the simulator order task")

    async def stream_user_events(self) -> AsyncIterator[object]:
        if False:
            yield None
        return

    async def reconcile_orders_and_fills(
        self,
        symbol: str,
        client_order_prefix: str | None = None,
    ) -> ReconciliationResult:
        return ReconciliationResult(orders=[], fills=[], warnings=[])

    async def health_check_streams(self) -> bool:
        return True
