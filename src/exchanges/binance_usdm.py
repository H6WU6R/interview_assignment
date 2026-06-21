from __future__ import annotations

import hmac
import json
import re
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256
from typing import Any
from urllib.parse import urlencode

import httpx
import websockets

from config import Settings
from exchanges.base import (
    ExchangeAdapter,
    NoFreshMarketData,
    OrderCancelTimeout,
    OrderCreateTimeout,
    OrderRejected,
    TerminalOrderRejected,
)
from execution.clock import Clock, SystemClock
from execution import ids
from execution.models import (
    ChildOrderStatus,
    Environment,
    ChildOrder,
    Fill,
    MarketSnapshot,
    OrderRequest,
    PositionSnapshot,
    ReconciliationResult,
    Side,
    SymbolRules,
)


BINANCE_USDM_TESTNET_BASE_URL = "https://demo-fapi.binance.com"
BINANCE_USDM_MAINNET_BASE_URL = "https://fapi.binance.com"
BINANCE_USDM_TESTNET_WS_ROOT = "wss://fstream.binancefuture.com"
BINANCE_USDM_MAINNET_WS_ROOT = "wss://fstream.binance.com"
ORDER_REST_PATH = "/fapi/v1/order"
ORDER_QUERY_PATH = ORDER_REST_PATH
POSITION_RISK_V3_PATH = "/fapi/v3/positionRisk"
OPEN_ORDERS_PATH = "/fapi/v1/openOrders"
ALL_ORDERS_PATH = "/fapi/v1/allOrders"
USER_TRADES_PATH = "/fapi/v1/userTrades"
LISTEN_KEY_PATH = "/fapi/v1/listenKey"
EXECUTION_CLIENT_ORDER_PREFIX_RE = re.compile(r"^ce_[0-9a-f]{12}_$")


class MutationKind(StrEnum):
    CREATE = "CREATE"
    CANCEL = "CANCEL"


class ExchangeTerminalReject(TerminalOrderRejected):
    pass


class UnknownCreateOutcome(OrderCreateTimeout):
    pass


class PendingCancelOutcome(OrderCancelTimeout):
    pass


class RetryableReadFailure(RuntimeError):
    pass


class StreamHealthFailure(RuntimeError):
    pass


def decimal_to_api(value: Decimal) -> str:
    return format(value, "f")


def classify_mutation_timeout(kind: MutationKind) -> str:
    if kind is MutationKind.CREATE:
        return "UNKNOWN_CREATE_OUTCOME"
    if kind is MutationKind.CANCEL:
        return "PENDING_CANCEL_OUTCOME"
    raise ValueError(f"unsupported mutation kind: {kind}")


def build_new_order_params(order_request: OrderRequest, rules: SymbolRules) -> dict[str, str]:
    if len(order_request.client_order_id) > 36 or not ids.CLIENT_ORDER_ID_RE.fullmatch(
        order_request.client_order_id
    ):
        raise ExchangeTerminalReject("INVALID_CLIENT_ORDER_ID")

    if order_request.post_only:
        if "GTX" not in rules.supported_time_in_force:
            raise ExchangeTerminalReject("POST_ONLY_GTX_UNSUPPORTED")
        time_in_force = "GTX"
    else:
        time_in_force = "GTC"

    return {
        "symbol": order_request.symbol,
        "side": order_request.side.value,
        "type": "LIMIT",
        "timeInForce": time_in_force,
        "quantity": decimal_to_api(order_request.quantity),
        "price": decimal_to_api(order_request.price),
        "newClientOrderId": order_request.client_order_id,
    }


def sign_params(params: dict[str, str], secret: str) -> dict[str, str]:
    query = urlencode(params)
    signature = hmac.new(secret.encode(), query.encode(), sha256).hexdigest()
    return {**params, "signature": signature}


def classify_http_status(status_code: int) -> str:
    if status_code == 408:
        return "REQUEST_TIMEOUT_AMBIGUOUS"
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
    _market_stream_symbol: str = "BTCUSDT"

    def __post_init__(self) -> None:
        self.base_url = self._select_base_url()

    @property
    def ws_root_url(self) -> str:
        if self.settings.environment == Environment.MAINNET and self.settings.can_trade_mainnet:
            return BINANCE_USDM_MAINNET_WS_ROOT
        return BINANCE_USDM_TESTNET_WS_ROOT

    @property
    def public_ws_base_url(self) -> str:
        return f"{self.ws_root_url}/public"

    @property
    def private_ws_base_url(self) -> str:
        return f"{self.ws_root_url}/private"

    def supports_post_only(self, rules: SymbolRules) -> bool:
        return "GTX" in rules.supported_time_in_force

    def signed_params(self, params: Mapping[str, Any], now_ms: int) -> dict[str, str]:
        if not self.settings.binance_api_secret:
            raise ValueError("Binance API secret is required to sign authenticated REST requests")

        serialized = {key: _serialize_param(value) for key, value in params.items()}
        serialized["timestamp"] = str(now_ms + self.server_time_offset_ms)
        serialized["recvWindow"] = str(self.settings.recv_window_ms)
        return sign_params(serialized, self.settings.binance_api_secret)

    async def _signed_request(
        self,
        method: str,
        path: str,
        params: Mapping[str, Any],
        mutation_kind: MutationKind | None = None,
    ) -> Any:
        if mutation_kind is not None and self.settings.environment == Environment.MAINNET:
            if not self.settings.can_trade_mainnet:
                raise ExchangeTerminalReject("MAINNET_TRADING_NOT_ALLOWED")
        if not self.settings.binance_api_key:
            raise ValueError("Binance API key is required for authenticated REST requests")
        if not self.settings.binance_api_secret:
            raise ValueError("Binance API secret is required to sign authenticated REST requests")

        signed = self.signed_params(params, now_ms=self._clock_wall_ms())
        headers = {"X-MBX-APIKEY": self.settings.binance_api_key}
        timeout = httpx.Timeout(5.0)
        url = f"{self.base_url}{path}"

        try:
            if self.client is None:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await client.request(method, url, params=signed, headers=headers)
            else:
                response = await self.client.request(
                    method,
                    url,
                    params=signed,
                    headers=headers,
                    timeout=timeout,
                )
        except httpx.TimeoutException as exc:
            if mutation_kind is MutationKind.CREATE:
                raise UnknownCreateOutcome(classify_mutation_timeout(MutationKind.CREATE)) from exc
            if mutation_kind is MutationKind.CANCEL:
                raise PendingCancelOutcome(classify_mutation_timeout(MutationKind.CANCEL)) from exc
            raise RetryableReadFailure("SIGNED_READ_TIMEOUT") from exc

        status = classify_http_status(response.status_code)
        if status == "OK":
            return response.json()
        if response.status_code == 408:
            if mutation_kind is MutationKind.CREATE:
                raise UnknownCreateOutcome(classify_mutation_timeout(MutationKind.CREATE))
            if mutation_kind is MutationKind.CANCEL:
                raise PendingCancelOutcome(classify_mutation_timeout(MutationKind.CANCEL))
            raise RetryableReadFailure("SIGNED_READ_TIMEOUT")
        if response.status_code == 429:
            raise RetryableReadFailure("RATE_LIMIT_BACKOFF")
        if response.status_code == 418:
            raise RuntimeError("VENUE_BAN_HARD_STOP")
        if 500 <= response.status_code <= 599:
            if mutation_kind is MutationKind.CREATE:
                raise UnknownCreateOutcome("UNKNOWN_CREATE_OUTCOME")
            if mutation_kind is MutationKind.CANCEL:
                raise PendingCancelOutcome("PENDING_CANCEL_OUTCOME")
            raise RetryableReadFailure("RETRYABLE_READ_FAILURE")
        if 400 <= response.status_code <= 499:
            reason = _terminal_reject_reason(response)
            if mutation_kind is MutationKind.CREATE and _is_retryable_order_reject_reason(reason):
                raise OrderRejected(reason)
            raise ExchangeTerminalReject(reason)
        raise RuntimeError(f"unexpected Binance HTTP status: {response.status_code}")

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
            url = self.market_stream_url(self._market_stream_symbol)
            async with websockets.connect(url) as websocket:
                async for raw_message in websocket:
                    payload = json.loads(raw_message)
                    snapshot = self.parse_book_ticker(payload)
                    self._latest_market[snapshot.symbol] = snapshot
                    yield snapshot

        return events()

    def set_market_stream_symbol(self, symbol: str) -> None:
        self._market_stream_symbol = symbol.upper()

    def market_stream_url(self, symbol: str) -> str:
        return f"{self.public_ws_base_url}/ws/{symbol.lower()}@bookTicker"

    async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
        rules = await self.get_symbol_rules(order_request.symbol)
        params = build_new_order_params(order_request, rules)
        raw = await self._signed_request(
            "POST",
            ORDER_REST_PATH,
            params,
            mutation_kind=MutationKind.CREATE,
        )
        fallback = ChildOrder(
            child_order_id=order_request.child_order_id,
            client_order_id=order_request.client_order_id,
            symbol=order_request.symbol,
            side=order_request.side,
            submitted_quantity=order_request.quantity,
            price=order_request.price,
        )
        return parse_order(raw, fallback=fallback)

    async def cancel_order(self, symbol: str, client_order_id: str) -> ChildOrder:
        raw = await self._signed_request(
            "DELETE",
            ORDER_REST_PATH,
            {"symbol": symbol, "origClientOrderId": client_order_id},
            mutation_kind=MutationKind.CANCEL,
        )
        return parse_order(raw)

    async def get_order_by_client_order_id(self, symbol: str, client_order_id: str) -> ChildOrder | None:
        try:
            raw = await self._signed_request(
                "GET",
                ORDER_QUERY_PATH,
                {"symbol": symbol, "origClientOrderId": client_order_id},
            )
        except ExchangeTerminalReject as exc:
            if _is_order_not_found_reason(str(exc)):
                return None
            raise
        return parse_order(raw)

    async def get_position(self, symbol: str) -> PositionSnapshot:
        rows = await self._signed_request("GET", POSITION_RISK_V3_PATH, {"symbol": symbol})
        symbol_rows = [row for row in rows if row.get("symbol") == symbol]
        if not symbol_rows:
            return PositionSnapshot(symbol=symbol, position=Decimal("0"))
        for row in symbol_rows:
            if row.get("positionSide", "BOTH") != "BOTH":
                raise ExchangeTerminalReject("HEDGE_MODE_UNSUPPORTED")
        row = symbol_rows[0]
        return PositionSnapshot(
            symbol=symbol,
            position=Decimal(str(row.get("positionAmt", "0"))),
            update_time_ms=_optional_int(row.get("updateTime")),
        )

    async def get_symbol_rules(self, symbol: str) -> SymbolRules:
        url = f"{self.base_url}/fapi/v1/exchangeInfo"
        timeout = httpx.Timeout(5.0)

        if self.client is None:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url)
        else:
            response = await self.client.get(url, timeout=timeout)

        response.raise_for_status()
        data = response.json()
        self.rate_limits = parse_exchange_info_rate_limits(data)
        return parse_symbol_rules_from_exchange_info(data, symbol)

    def stream_user_events(self) -> AsyncIterator[object]:
        async def events() -> AsyncIterator[object]:
            listen_key = await self.create_listen_key()
            url = f"{self.private_ws_base_url}/ws/{listen_key}"
            async with websockets.connect(url) as websocket:
                async for raw_message in websocket:
                    yield self.parse_user_event(json.loads(raw_message))

        return events()

    async def create_listen_key(self) -> str:
        response = await self._api_key_request("POST", LISTEN_KEY_PATH, params=None)
        listen_key = response.get("listenKey")
        if not listen_key:
            raise StreamHealthFailure("LISTEN_KEY_MISSING")
        return str(listen_key)

    async def renew_listen_key(self, listen_key: str) -> None:
        await self._api_key_request("PUT", LISTEN_KEY_PATH, params={"listenKey": listen_key})

    async def _api_key_request(
        self,
        method: str,
        path: str,
        params: Mapping[str, Any] | None,
    ) -> Any:
        if not self.settings.binance_api_key:
            raise ValueError("Binance API key is required for listen-key requests")
        headers = {"X-MBX-APIKEY": self.settings.binance_api_key}
        timeout = httpx.Timeout(5.0)
        url = f"{self.base_url}{path}"
        if self.client is None:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.request(method, url, params=params, headers=headers)
        else:
            response = await self.client.request(
                method,
                url,
                params=params,
                headers=headers,
                timeout=timeout,
            )
        if response.status_code != 200:
            raise StreamHealthFailure(f"LISTEN_KEY_HTTP_{response.status_code}")
        return response.json()

    def parse_book_ticker(self, message: Mapping[str, Any]) -> MarketSnapshot:
        payload = _stream_data(message)
        return MarketSnapshot(
            symbol=str(payload["s"]),
            bid=Decimal(str(payload["b"])),
            ask=Decimal(str(payload["a"])),
            last_market_event_time_exchange=_optional_int(payload.get("E")),
            last_market_event_time_local_monotonic=self.clock.monotonic(),
        )

    def parse_user_event(self, message: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "event_type": message.get("e"),
            "event_time_ms": _optional_int(message.get("E")),
            "transaction_time_ms": _optional_int(message.get("T")),
            "raw": dict(message),
        }

    async def reconcile_orders_and_fills(
        self,
        symbol: str,
        client_order_prefix: str | None = None,
    ) -> ReconciliationResult:
        _require_execution_prefix(client_order_prefix)
        open_orders_raw = await self._signed_request("GET", OPEN_ORDERS_PATH, {"symbol": symbol})
        all_orders_raw = await self._signed_request("GET", ALL_ORDERS_PATH, {"symbol": symbol, "limit": 100})
        trades_raw = await self._signed_request("GET", USER_TRADES_PATH, {"symbol": symbol, "limit": 100})

        orders_by_client_id: dict[str, ChildOrder] = {}
        order_id_to_client_id: dict[str, str] = {}
        for raw_order in [*open_orders_raw, *all_orders_raw]:
            client_order_id = str(raw_order.get("clientOrderId", ""))
            if not client_order_id.startswith(client_order_prefix):
                continue
            order = parse_order(raw_order)
            orders_by_client_id[client_order_id] = order
            if raw_order.get("orderId") is not None:
                order_id_to_client_id[str(raw_order["orderId"])] = client_order_id

        cumulative_by_client_id: dict[str, Decimal] = {}
        fills: list[Fill] = []
        for raw_fill in trades_raw:
            client_order_id = raw_fill.get("clientOrderId")
            if client_order_id is not None:
                client_order_id = str(client_order_id)
            elif raw_fill.get("orderId") is not None:
                client_order_id = order_id_to_client_id.get(str(raw_fill["orderId"]))
            if client_order_id is None or not client_order_id.startswith(client_order_prefix):
                continue

            cumulative = cumulative_by_client_id.get(client_order_id, Decimal("0")) + Decimal(
                str(raw_fill.get("qty", "0"))
            )
            cumulative_by_client_id[client_order_id] = cumulative
            fills.append(parse_fill(raw_fill, client_order_id, cumulative))

        return ReconciliationResult(orders=list(orders_by_client_id.values()), fills=fills)

    async def health_check_streams(self) -> bool:
        return True

    def _select_base_url(self) -> str:
        if self.settings.environment == Environment.MAINNET and self.settings.can_trade_mainnet:
            return BINANCE_USDM_MAINNET_BASE_URL
        return BINANCE_USDM_TESTNET_BASE_URL

    def _clock_wall_ms(self) -> int:
        return int(self.clock.utc_now().timestamp() * 1000)


def _find_symbol_payload(payload: Mapping[str, Any], symbol: str) -> Mapping[str, Any]:
    for symbol_payload in payload.get("symbols", []):
        if symbol_payload.get("symbol") == symbol:
            return symbol_payload
    raise KeyError(f"symbol not found in exchangeInfo: {symbol}")


def _serialize_param(value: Any) -> str:
    if isinstance(value, Decimal):
        return decimal_to_api(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def parse_order(raw: Mapping[str, Any], fallback: ChildOrder | None = None) -> ChildOrder:
    client_order_id = str(raw.get("clientOrderId") or raw.get("origClientOrderId") or _fallback_attr(fallback, "client_order_id", ""))
    raw_status = str(raw.get("status", _fallback_attr(fallback, "raw_status", "UNKNOWN")))
    status = normalize_order_status(raw_status)
    submitted_quantity = Decimal(
        str(raw.get("origQty", raw.get("quantity", _fallback_attr(fallback, "submitted_quantity", "0"))))
    )
    confirmed_filled_quantity = Decimal(
        str(raw.get("executedQty", raw.get("cumQty", _fallback_attr(fallback, "confirmed_filled_quantity", "0"))))
    )
    child = ChildOrder(
        child_order_id=str(_fallback_attr(fallback, "child_order_id", client_order_id)),
        client_order_id=client_order_id,
        symbol=str(raw.get("symbol", _fallback_attr(fallback, "symbol", ""))),
        side=_parse_side(raw.get("side", _fallback_attr(fallback, "side", "BUY"))),
        submitted_quantity=submitted_quantity,
        price=Decimal(str(raw.get("price", _fallback_attr(fallback, "price", "0")))),
        status=status,
        confirmed_filled_quantity=confirmed_filled_quantity,
        exchange_order_id=str(raw["orderId"]) if raw.get("orderId") is not None else _fallback_attr(fallback, "exchange_order_id", None),
        raw_status=raw_status,
        terminal_reason=raw_status if raw_status in {"EXPIRED", "EXPIRED_IN_MATCH", "REJECTED"} else None,
    )
    return child


def parse_fill(
    raw: Mapping[str, Any],
    client_order_id: str,
    cumulative_quantity: Decimal,
) -> Fill:
    return Fill(
        client_order_id=client_order_id,
        trade_id=str(raw["id"]) if raw.get("id") is not None else None,
        cumulative_filled_quantity=cumulative_quantity,
        last_filled_quantity=Decimal(str(raw.get("qty", "0"))),
        last_fill_price=Decimal(str(raw.get("price", "0"))),
        event_time_ms=_optional_int(raw.get("time")),
        transaction_time_ms=_optional_int(raw.get("time")),
    )


def _require_execution_prefix(client_order_prefix: str | None) -> None:
    if not client_order_prefix or not EXECUTION_CLIENT_ORDER_PREFIX_RE.fullmatch(client_order_prefix):
        raise ValueError("client_order_prefix must be execution-scoped: ce_<12 lowercase hex chars>_")


def _parse_side(value: Any) -> Side:
    if isinstance(value, Side):
        return value
    return Side(str(value))


def _fallback_attr(fallback: ChildOrder | None, attr: str, default: Any) -> Any:
    if fallback is None:
        return default
    return getattr(fallback, attr, default)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _stream_data(message: Mapping[str, Any]) -> Mapping[str, Any]:
    data = message.get("data", message)
    if not isinstance(data, Mapping):
        raise ValueError("stream message data must be a mapping")
    return data


def _terminal_reject_reason(response: Any) -> str:
    try:
        payload = response.json()
    except Exception:
        return f"BINANCE_HTTP_{response.status_code}"
    if isinstance(payload, Mapping):
        code = payload.get("code")
        message = payload.get("msg")
        if code is not None and message is not None:
            return f"BINANCE_{code}:{message}"
        if message is not None:
            return str(message)
    return f"BINANCE_HTTP_{response.status_code}"


def _is_order_not_found_reason(reason: str) -> bool:
    return "BINANCE_-2013" in reason or "Order does not exist" in reason


def _is_retryable_order_reject_reason(reason: str) -> bool:
    return reason.startswith("BINANCE_-5022:")
