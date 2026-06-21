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
    pass


class ExchangeAdapter(ABC):
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
    ) -> ReconciliationResult:
        raise NotImplementedError

    @abstractmethod
    async def health_check_streams(self) -> bool:
        raise NotImplementedError
