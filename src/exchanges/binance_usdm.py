from __future__ import annotations

import hmac
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from hashlib import sha256
from typing import Any
from urllib.parse import urlencode

import httpx

from config import Settings
from exchanges.base import ExchangeAdapter, NoFreshMarketData
from execution.clock import Clock, SystemClock
from execution.models import (
    ChildOrderStatus,
    Environment,
    MarketSnapshot,
    OrderRequest,
    PositionSnapshot,
    ReconciliationResult,
    SymbolRules,
)


BINANCE_USDM_TESTNET_BASE_URL = "https://testnet.binancefuture.com"
BINANCE_USDM_MAINNET_BASE_URL = "https://fapi.binance.com"


def sign_params(params: dict[str, str], secret: str) -> dict[str, str]:
    query = urlencode(params)
    signature = hmac.new(secret.encode(), query.encode(), sha256).hexdigest()
    return {**params, "signature": signature}


def classify_http_status(status_code: int) -> str:
    if status_code == 429:
        return "RATE_LIMIT_BACKOFF"
    if status_code == 418:
        return "VENUE_BAN_HARD_STOP"
    if 500 <= status_code <= 599:
        return "RETRYABLE_READ_OR_UNKNOWN_MUTATION"
    if 400 <= status_code <= 499:
        return "TERMINAL_REJECT"
    return "OK"


def normalize_order_status(raw_status: str) -> ChildOrderStatus:
    status_map = {
        "NEW": ChildOrderStatus.OPEN,
        "PARTIALLY_FILLED": ChildOrderStatus.PARTIALLY_FILLED,
        "FILLED": ChildOrderStatus.FILLED,
        "CANCELED": ChildOrderStatus.CANCELLED,
        "EXPIRED": ChildOrderStatus.CANCELLED,
        "EXPIRED_IN_MATCH": ChildOrderStatus.CANCELLED,
        "REJECTED": ChildOrderStatus.REJECTED,
        "PENDING_CANCEL": ChildOrderStatus.PENDING_CANCEL,
    }
    return status_map.get(raw_status, ChildOrderStatus.UNKNOWN)


def parse_symbol_rules_from_exchange_info(payload: Mapping[str, Any], symbol: str) -> SymbolRules:
    symbol_payload = _find_symbol_payload(payload, symbol)
    filters = {
        filter_payload["filterType"]: filter_payload
        for filter_payload in symbol_payload.get("filters", [])
        if "filterType" in filter_payload
    }
    price_filter = filters["PRICE_FILTER"]
    lot_size = filters["LOT_SIZE"]
    min_notional_filter = filters["MIN_NOTIONAL"]
    min_notional = min_notional_filter.get("notional", min_notional_filter.get("minNotional"))
    if min_notional is None:
        raise KeyError(f"MIN_NOTIONAL filter for {symbol} is missing notional/minNotional")

    return SymbolRules(
        symbol=symbol_payload["symbol"],
        tick_size=Decimal(str(price_filter["tickSize"])),
        quantity_step=Decimal(str(lot_size["stepSize"])),
        min_quantity=Decimal(str(lot_size["minQty"])),
        min_notional=Decimal(str(min_notional)),
        status=str(symbol_payload.get("status", "UNKNOWN")),
        supported_time_in_force=frozenset(str(value) for value in symbol_payload.get("timeInForce", [])),
    )


def parse_exchange_info_rate_limits(payload: Mapping[str, Any]) -> dict[str, int]:
    wanted = {"REQUEST_WEIGHT", "ORDERS"}
    return {
        str(rate_limit["rateLimitType"]): int(rate_limit["limit"])
        for rate_limit in payload.get("rateLimits", [])
        if rate_limit.get("rateLimitType") in wanted
    }


@dataclass
class BinanceUsdmAdapter(ExchangeAdapter):
    settings: Settings = field(default_factory=Settings)
    client: Any | None = None
    clock: Clock = field(default_factory=SystemClock)
    server_time_offset_ms: int = 0
    rate_limits: dict[str, int] = field(default_factory=dict)
    _latest_market: dict[str, MarketSnapshot] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.base_url = self._select_base_url()

    def supports_post_only(self, rules: SymbolRules) -> bool:
        return "GTX" in rules.supported_time_in_force

    def signed_params(self, params: Mapping[str, Any], now_ms: int) -> dict[str, str]:
        if not self.settings.binance_api_secret:
            raise ValueError("Binance API secret is required to sign authenticated REST requests")

        serialized = {key: _serialize_param(value) for key, value in params.items()}
        serialized["timestamp"] = str(now_ms + self.server_time_offset_ms)
        serialized["recvWindow"] = str(self.settings.recv_window_ms)
        return sign_params(serialized, self.settings.binance_api_secret)

    async def get_best_bid_ask(self, symbol: str) -> MarketSnapshot:
        snapshot = self._latest_market.get(symbol)
        if snapshot is None:
            raise NoFreshMarketData(f"no fresh market data for {symbol}")
        if snapshot.is_crossed:
            raise NoFreshMarketData(f"crossed market data for {symbol}")
        age_ms = (self.clock.monotonic() - snapshot.last_market_event_time_local_monotonic) * 1000
        if age_ms > self.settings.stale_market_data_ms:
            raise NoFreshMarketData(f"stale market data for {symbol}: age_ms={age_ms}")
        return snapshot

    def stream_market_data(self) -> AsyncIterator[MarketSnapshot]:
        async def events() -> AsyncIterator[MarketSnapshot]:
            if False:
                yield MarketSnapshot(
                    symbol="",
                    bid=Decimal("0"),
                    ask=Decimal("0"),
                    last_market_event_time_exchange=None,
                    last_market_event_time_local_monotonic=0,
                )

        return events()

    async def submit_limit_order(self, order_request: OrderRequest) -> object:
        raise NotImplementedError("Binance USD-M REST order submission is scheduled for Task 16")

    async def cancel_order(self, symbol: str, client_order_id: str) -> object:
        raise NotImplementedError("Binance USD-M REST order cancellation is scheduled for Task 16")

    async def get_order_by_client_order_id(self, symbol: str, client_order_id: str) -> object:
        raise NotImplementedError("Binance USD-M REST order lookup is scheduled for Task 16")

    async def get_position(self, symbol: str) -> PositionSnapshot:
        raise NotImplementedError("Binance USD-M REST position lookup is scheduled for Task 17")

    async def get_symbol_rules(self, symbol: str) -> SymbolRules:
        url = f"{self.base_url}/fapi/v1/exchangeInfo"
        timeout = httpx.Timeout(5.0)
        params = {"symbol": symbol}

        if self.client is None:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, params=params)
        else:
            response = await self.client.get(url, params=params, timeout=timeout)

        response.raise_for_status()
        data = response.json()
        self.rate_limits = parse_exchange_info_rate_limits(data)
        return parse_symbol_rules_from_exchange_info(data, symbol)

    def stream_user_events(self) -> AsyncIterator[object]:
        async def events() -> AsyncIterator[object]:
            if False:
                yield object()

        return events()

    async def reconcile_orders_and_fills(
        self,
        symbol: str,
        client_order_prefix: str | None = None,
    ) -> ReconciliationResult:
        return ReconciliationResult(
            orders=[],
            fills=[],
            warnings=["BINANCE_REST_RECONCILIATION_NOT_IMPLEMENTED_IN_TASK_15"],
        )

    async def health_check_streams(self) -> bool:
        return True

    def _select_base_url(self) -> str:
        if self.settings.environment == Environment.MAINNET and self.settings.can_trade_mainnet:
            return BINANCE_USDM_MAINNET_BASE_URL
        return BINANCE_USDM_TESTNET_BASE_URL


def _find_symbol_payload(payload: Mapping[str, Any], symbol: str) -> Mapping[str, Any]:
    for symbol_payload in payload.get("symbols", []):
        if symbol_payload.get("symbol") == symbol:
            return symbol_payload
    raise KeyError(f"symbol not found in exchangeInfo: {symbol}")


def _serialize_param(value: Any) -> str:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
