from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Literal

from exchanges.base import ExchangeAdapter, NoFreshMarketData
from execution.clock import Clock, ManualClock
from execution.models import (
    ChildOrder,
    ChildOrderStatus,
    Fill,
    MarketSnapshot,
    OrderRequest,
    PositionSnapshot,
    ReconciliationResult,
    Side,
    SymbolRules,
)
from execution.state_machine import transition_child


class SimulatorOrderRejected(RuntimeError):
    pass


class SimulatorOrderTimeout(RuntimeError):
    pass


@dataclass(frozen=True)
class SimulatorOrderEvent:
    kind: Literal["order_opened", "order_cancelled", "fill"]
    client_order_id: str
    order: ChildOrder | None = None
    fill: Fill | None = None


@dataclass
class DeterministicSimulator(ExchangeAdapter):
    clock: Clock = field(default_factory=ManualClock)
    position: Decimal = Decimal("0")
    stale_market_data_seconds: float = 1.5
    _market: dict[str, MarketSnapshot] = field(default_factory=dict)
    _market_queue: asyncio.Queue[MarketSnapshot] = field(default_factory=asyncio.Queue)
    _symbol_rules: dict[str, SymbolRules] = field(default_factory=dict)
    _orders: dict[tuple[str, str], ChildOrder] = field(default_factory=dict)
    _fills: list[Fill] = field(default_factory=list)
    _user_event_queue: asyncio.Queue[SimulatorOrderEvent] = field(default_factory=asyncio.Queue)
    _trade_sequence: int = 0
    _exchange_order_sequence: int = 0
    _create_timeout_prefixes: list[str] = field(default_factory=list)
    _create_timeout_not_found_prefixes: list[str] = field(default_factory=list)
    _create_timeout_not_found_warnings: set[tuple[str, str]] = field(default_factory=set)
    _fill_during_cancel_scripts: list[tuple[str, Decimal]] = field(default_factory=list)
    _cancel_reconcile_open_prefixes: list[str] = field(default_factory=list)
    _user_stream_healthy: bool = True

    async def get_symbol_rules(self, symbol: str) -> SymbolRules:
        if rules := self._symbol_rules.get(symbol):
            return rules
        return SymbolRules(
            symbol=symbol,
            tick_size=Decimal("0.10"),
            quantity_step=Decimal("0.001"),
            min_quantity=Decimal("0.001"),
            min_notional=Decimal("5"),
            status="TRADING",
            supported_time_in_force=frozenset({"GTC", "GTX"}),
        )

    def set_symbol_rules(self, rules: SymbolRules) -> None:
        self._symbol_rules[rules.symbol] = rules

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

    async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
        snapshot = await self.get_best_bid_ask(order_request.symbol)
        await self._validate_post_only(order_request, snapshot)

        if prefix := self._pop_matching_prefix(
            self._create_timeout_not_found_prefixes,
            order_request.client_order_id,
        ):
            self._create_timeout_not_found_warnings.add((order_request.symbol, prefix))
            raise SimulatorOrderTimeout(f"create timed out for {order_request.client_order_id}")

        order = self._create_open_order(order_request)
        await self._user_event_queue.put(
            SimulatorOrderEvent(
                kind="order_opened",
                client_order_id=order.client_order_id,
                order=order,
            )
        )

        if self._pop_matching_prefix(self._create_timeout_prefixes, order_request.client_order_id):
            raise SimulatorOrderTimeout(f"create timed out for {order_request.client_order_id}")

        return order

    async def cancel_order(self, symbol: str, client_order_id: str) -> ChildOrder:
        order = self._orders.get((symbol, client_order_id))
        if order is None:
            raise KeyError(f"unknown simulator order: {symbol} {client_order_id}")

        if order.status.is_terminal:
            return order

        order.status = transition_child(order.status, ChildOrderStatus.PENDING_CANCEL)

        fill_quantity = self._consume_fill_during_cancel(client_order_id)
        if fill_quantity is not None:
            await self.push_fill(client_order_id, fill_quantity, order.price)

        if order.status.is_terminal:
            return order

        if self._pop_matching_prefix(self._cancel_reconcile_open_prefixes, client_order_id):
            target = self._open_status_for(order)
            if order.status != target:
                order.status = transition_child(order.status, target)
            return order

        if order.status != ChildOrderStatus.PENDING_CANCEL:
            order.status = transition_child(order.status, ChildOrderStatus.PENDING_CANCEL)
        order.status = transition_child(order.status, ChildOrderStatus.CANCELLED)
        await self._user_event_queue.put(
            SimulatorOrderEvent(
                kind="order_cancelled",
                client_order_id=client_order_id,
                order=order,
            )
        )
        return order

    async def get_order_by_client_order_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> ChildOrder | None:
        return self._orders.get((symbol, client_order_id))

    async def push_fill(self, client_order_id: str, fill_quantity: Decimal, price: Decimal) -> Fill:
        if not isinstance(fill_quantity, Decimal) or not isinstance(price, Decimal):
            raise TypeError("simulator fills require Decimal quantity and price")
        if fill_quantity <= Decimal("0"):
            raise ValueError("fill quantity must be positive")

        order = self._order_by_client_order_id(client_order_id)
        if order.status in {
            ChildOrderStatus.CANCELLED,
            ChildOrderStatus.FILLED,
            ChildOrderStatus.REJECTED,
        }:
            raise SimulatorOrderRejected(f"cannot fill terminal order {client_order_id}")
        if fill_quantity > order.remaining_quantity:
            raise SimulatorOrderRejected(f"fill exceeds remaining quantity for {client_order_id}")

        order.confirmed_filled_quantity += fill_quantity
        order.status = transition_child(order.status, self._open_status_for(order))

        self._trade_sequence += 1
        fill = Fill(
            client_order_id=client_order_id,
            trade_id=f"sim_trade_{self._trade_sequence:06d}",
            cumulative_filled_quantity=order.confirmed_filled_quantity,
            last_filled_quantity=fill_quantity,
            last_fill_price=price,
            event_time_ms=self._clock_time_ms(),
            transaction_time_ms=self._clock_time_ms(),
        )
        self._fills.append(fill)
        await self._user_event_queue.put(
            SimulatorOrderEvent(
                kind="fill",
                client_order_id=client_order_id,
                order=order,
                fill=fill,
            )
        )
        return fill

    def stream_user_events(self) -> AsyncIterator[object]:
        async def events() -> AsyncIterator[object]:
            while True:
                yield await self._user_event_queue.get()

        return events()

    async def reconcile_orders_and_fills(
        self,
        symbol: str,
        client_order_prefix: str | None = None,
    ) -> ReconciliationResult:
        self._reject_broad_client_order_prefix(client_order_prefix)

        orders = [
            order
            for (order_symbol, client_order_id), order in self._orders.items()
            if order_symbol == symbol
            and self._client_order_id_matches(client_order_id, client_order_prefix)
        ]
        fills = [
            fill
            for fill in self._fills
            if self._client_order_id_matches(fill.client_order_id, client_order_prefix)
            and self._order_by_client_order_id(fill.client_order_id).symbol == symbol
        ]
        warnings = []
        if (
            client_order_prefix is not None
            and (symbol, client_order_prefix) in self._create_timeout_not_found_warnings
        ):
            warnings.append("CREATE_TIMEOUT_ORDER_NOT_FOUND")
        return ReconciliationResult(orders=orders, fills=fills, warnings=warnings)

    async def health_check_streams(self) -> bool:
        return self._user_stream_healthy

    def script_fill_during_cancel(self, client_order_prefix: str, fill_quantity: Decimal) -> None:
        self._reject_broad_client_order_prefix(client_order_prefix)
        if not isinstance(fill_quantity, Decimal):
            raise TypeError("fill quantity must be Decimal")
        self._fill_during_cancel_scripts.append((client_order_prefix, fill_quantity))

    def script_create_timeout(self, client_order_prefix: str) -> None:
        self._reject_broad_client_order_prefix(client_order_prefix)
        self._create_timeout_prefixes.append(client_order_prefix)

    def script_create_timeout_not_found(self, client_order_prefix: str) -> None:
        self._reject_broad_client_order_prefix(client_order_prefix)
        self._create_timeout_not_found_prefixes.append(client_order_prefix)

    def script_cancel_reconcile_open(self, client_order_prefix: str) -> None:
        self._reject_broad_client_order_prefix(client_order_prefix)
        self._cancel_reconcile_open_prefixes.append(client_order_prefix)

    def set_stream_health(self, *, user_stream_healthy: bool) -> None:
        self._user_stream_healthy = user_stream_healthy

    async def _validate_post_only(
        self,
        order_request: OrderRequest,
        snapshot: MarketSnapshot,
    ) -> None:
        if not order_request.post_only:
            return
        rules = await self.get_symbol_rules(order_request.symbol)
        if "GTX" not in rules.supported_time_in_force:
            raise SimulatorOrderRejected(
                f"{order_request.symbol} does not support GTX/post-only time in force"
            )
        if order_request.side == Side.BUY and order_request.price >= snapshot.ask:
            raise SimulatorOrderRejected("post-only order would cross the current ask")
        if order_request.side == Side.SELL and order_request.price <= snapshot.bid:
            raise SimulatorOrderRejected("post-only order would cross the current bid")

    def _create_open_order(self, order_request: OrderRequest) -> ChildOrder:
        self._exchange_order_sequence += 1
        order = ChildOrder(
            child_order_id=order_request.child_order_id,
            client_order_id=order_request.client_order_id,
            symbol=order_request.symbol,
            side=order_request.side,
            submitted_quantity=order_request.quantity,
            price=order_request.price,
            exchange_order_id=f"sim_order_{self._exchange_order_sequence:06d}",
        )
        order.status = transition_child(order.status, ChildOrderStatus.OPEN)
        self._orders[(order.symbol, order.client_order_id)] = order
        return order

    def _order_by_client_order_id(self, client_order_id: str) -> ChildOrder:
        matches = [
            order
            for (_, stored_client_order_id), order in self._orders.items()
            if stored_client_order_id == client_order_id
        ]
        if len(matches) != 1:
            raise KeyError(f"unknown simulator order: {client_order_id}")
        return matches[0]

    def _pop_matching_prefix(self, prefixes: list[str], client_order_id: str) -> str | None:
        for index, prefix in enumerate(prefixes):
            if client_order_id.startswith(prefix):
                del prefixes[index]
                return prefix
        return None

    def _consume_fill_during_cancel(self, client_order_id: str) -> Decimal | None:
        for index, (prefix, fill_quantity) in enumerate(self._fill_during_cancel_scripts):
            if client_order_id.startswith(prefix):
                del self._fill_during_cancel_scripts[index]
                return fill_quantity
        return None

    def _open_status_for(self, order: ChildOrder) -> ChildOrderStatus:
        if order.remaining_quantity == Decimal("0"):
            return ChildOrderStatus.FILLED
        if order.confirmed_filled_quantity > Decimal("0"):
            return ChildOrderStatus.PARTIALLY_FILLED
        return ChildOrderStatus.OPEN

    def _client_order_id_matches(
        self,
        client_order_id: str,
        client_order_prefix: str | None,
    ) -> bool:
        if client_order_prefix is None:
            return True
        return client_order_id.startswith(client_order_prefix)

    def _clock_time_ms(self) -> int:
        return int(self.clock.utc_now().timestamp() * 1000)

    def _reject_broad_client_order_prefix(self, client_order_prefix: str | None) -> None:
        if client_order_prefix in {"", "ce", "ce_"}:
            raise ValueError("client_order_prefix must be execution-scoped, not a broad ce_ prefix")
