"""Exchange adapter contract and shared exchange exceptions."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from execution.models import (
    MarketSnapshot,
    OrderRequest,
    PositionSnapshot,
    ReconciliationResult,
    SymbolRules,
)


class NoFreshMarketData(RuntimeError):
    """Raised when an adapter cannot provide a usable current market snapshot."""

    pass


class OrderCreateTimeout(RuntimeError):
    """Raised when order creation times out with an uncertain outcome."""

    pass


class OrderCancelTimeout(RuntimeError):
    """Raised when order cancellation times out with a pending outcome."""

    pass


class OrderRejected(RuntimeError):
    """Raised when an exchange rejects an order request."""

    pass


class TerminalOrderRejected(OrderRejected):
    """Raised when an order rejection should terminally fail the child order."""

    pass


class ExchangeAdapter(ABC):
    """Abstract exchange contract shared by the simulator and Binance adapter."""

    @abstractmethod
    async def get_symbol_rules(self, symbol: str) -> SymbolRules:
        raise NotImplementedError

    @abstractmethod
    async def get_position(self, symbol: str) -> PositionSnapshot:
        raise NotImplementedError

    @abstractmethod
    async def get_best_bid_ask(self, symbol: str) -> MarketSnapshot:
        raise NotImplementedError

    @abstractmethod
    def stream_market_data(self) -> AsyncIterator[MarketSnapshot]:
        raise NotImplementedError

    @abstractmethod
    async def submit_limit_order(self, order_request: OrderRequest) -> object:
        raise NotImplementedError

    @abstractmethod
    async def cancel_order(self, symbol: str, client_order_id: str) -> object:
        raise NotImplementedError

    @abstractmethod
    async def get_order_by_client_order_id(self, symbol: str, client_order_id: str) -> object:
        raise NotImplementedError

    @abstractmethod
    def stream_user_events(self) -> AsyncIterator[object]:
        raise NotImplementedError

    @abstractmethod
    async def reconcile_orders_and_fills(
        self,
        symbol: str,
        client_order_prefix: str | None = None,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> ReconciliationResult:
        raise NotImplementedError

    @abstractmethod
    async def health_check_streams(self) -> bool:
        raise NotImplementedError
