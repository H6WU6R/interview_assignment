"""Binance USD-M exchange adapter and payload translation helpers."""

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
    """Authenticated Binance mutation category used for timeout classification."""

    CREATE = "CREATE"
    CANCEL = "CANCEL"


class ExchangeTerminalReject(TerminalOrderRejected):
    """Terminal Binance rejection that should fail the child order."""

    pass


class UnknownCreateOutcome(OrderCreateTimeout):
    """Raised when a create-order mutation has an ambiguous outcome."""

    pass


class PendingCancelOutcome(OrderCancelTimeout):
    """Raised when a cancel-order mutation may still be pending."""

    pass


class RetryableReadFailure(RuntimeError):
    """Raised when a signed read can be retried safely."""

    pass


class StreamHealthFailure(RuntimeError):
    """Raised when Binance stream setup or health checks fail."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason
        self.code = reason.split(":", 1)[0]


def decimal_to_api(value: Decimal) -> str:
    """Serialize a Decimal for Binance REST parameters."""

    return format(value, "f")


def classify_mutation_timeout(kind: MutationKind) -> str:
    """Return a conservative reason for an ambiguous mutation timeout."""

    if kind is MutationKind.CREATE:
        return "UNKNOWN_CREATE_OUTCOME"
    if kind is MutationKind.CANCEL:
        return "PENDING_CANCEL_OUTCOME"
    raise ValueError(f"unsupported mutation kind: {kind}")


def build_new_order_params(order_request: OrderRequest, rules: SymbolRules) -> dict[str, str]:
    """Build Binance LIMIT order parameters from an internal order request."""

    if len(order_request.client_order_id) > 36 or not ids.CLIENT_ORDER_ID_RE.fullmatch(
        order_request.client_order_id
    ):
        raise ExchangeTerminalReject("INVALID_CLIENT_ORDER_ID")

    if order_request.post_only:
        if "GTX" not in rules.supported_time_in_force:
            raise ExchangeTerminalReject("POST_ONLY_GTX_UNSUPPORTED")
        time_in_force = "GTX"
    elif order_request.time_in_force is not None:
        time_in_force = order_request.time_in_force.value
        if time_in_force not in rules.supported_time_in_force:
            raise ExchangeTerminalReject(f"{time_in_force}_TIME_IN_FORCE_UNSUPPORTED")
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
    """Return Binance request parameters with an HMAC signature."""

    query = urlencode(params)
    signature = hmac.new(secret.encode(), query.encode(), sha256).hexdigest()
    return {**params, "signature": signature}


def classify_http_status(status_code: int) -> str:
    """Classify a Binance HTTP status into an execution handling category."""

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
    """Map Binance order status text to an internal child order status."""

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
    """Extract symbol trading rules from a Binance exchangeInfo payload."""

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
    """Extract relevant request and order rate limits from exchangeInfo."""

    wanted = {"REQUEST_WEIGHT", "ORDERS"}
    return {
        str(rate_limit["rateLimitType"]): int(rate_limit["limit"])
        for rate_limit in payload.get("rateLimits", [])
        if rate_limit.get("rateLimitType") in wanted
    }


@dataclass
class BinanceUsdmAdapter(ExchangeAdapter):
    """Binance USD-M adapter implementing the exchange contract with REST and stream helpers."""

    settings: Settings = field(default_factory=Settings)
    client: Any | None = None
    clock: Clock = field(default_factory=SystemClock)
    server_time_offset_ms: int = 0
    rate_limits: dict[str, int] = field(default_factory=dict)
    market_stream_healthy: bool = False
    user_stream_healthy: bool = False
    latest_listen_key: str | None = None
    last_stream_error: str | None = None
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
        except (httpx.TransportError, httpx.TimeoutException) as exc:
            if mutation_kind is MutationKind.CREATE:
                raise UnknownCreateOutcome(classify_mutation_timeout(MutationKind.CREATE)) from exc
            if mutation_kind is MutationKind.CANCEL:
                raise PendingCancelOutcome(classify_mutation_timeout(MutationKind.CANCEL)) from exc
            raise RetryableReadFailure("SIGNED_READ_TIMEOUT") from exc

        status = classify_http_status(response.status_code)
        if status == "OK":
            return _json_payload_or_conservative_failure(response, mutation_kind)

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

        error_payload = _json_payload_or_conservative_failure(response, mutation_kind)
        if 500 <= response.status_code <= 599:
            if mutation_kind is not None:
                reason = _terminal_reject_reason(response.status_code, error_payload)
                if _is_specific_terminal_5xx_reject(reason):
                    raise ExchangeTerminalReject(reason)
                if mutation_kind is MutationKind.CREATE:
                    raise UnknownCreateOutcome("UNKNOWN_CREATE_OUTCOME")
                if mutation_kind is MutationKind.CANCEL:
                    raise PendingCancelOutcome("PENDING_CANCEL_OUTCOME")
            raise RetryableReadFailure("RETRYABLE_READ_FAILURE")
        if 400 <= response.status_code <= 499:
            reason = _terminal_reject_reason(response.status_code, error_payload)
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
            self.market_stream_healthy = False
            try:
                async with websockets.connect(url) as websocket:
                    self.market_stream_healthy = True
                    async for raw_message in websocket:
                        payload = json.loads(raw_message)
                        snapshot = self.parse_book_ticker(payload)
                        self._latest_market[snapshot.symbol] = snapshot
                        yield snapshot
            except Exception as exc:
                self.last_stream_error = f"market_stream:{type(exc).__name__}:{exc}"
                raise
            finally:
                self.market_stream_healthy = False

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
            self.user_stream_healthy = False
            try:
                listen_key = await self.create_listen_key()
                url = f"{self.private_ws_base_url}/ws/{listen_key}"
                async with websockets.connect(url) as websocket:
                    self.user_stream_healthy = True
                    async for raw_message in websocket:
                        yield self.parse_user_event(json.loads(raw_message))
            except Exception as exc:
                self.last_stream_error = f"user_stream:{type(exc).__name__}:{exc}"
                raise
            finally:
                self.user_stream_healthy = False

        return events()

    async def create_listen_key(self) -> str:
        response = await self._api_key_request("POST", LISTEN_KEY_PATH, params=None)
        listen_key = response.get("listenKey")
        if not listen_key:
            raise StreamHealthFailure("LISTEN_KEY_MISSING")
        self.latest_listen_key = str(listen_key)
        return self.latest_listen_key

    async def renew_listen_key(self, listen_key: str) -> None:
        try:
            await self._api_key_request("PUT", LISTEN_KEY_PATH, params=None)
        except StreamHealthFailure as exc:
            if exc.code == "LISTEN_KEY_EXPIRED" and self.latest_listen_key == listen_key:
                self.latest_listen_key = None
            raise

    async def _api_key_request(
        self,
        method: str,
        path: str,
        params: Mapping[str, Any] | None,
    ) -> Mapping[str, Any]:
        if not self.settings.binance_api_key:
            raise ValueError("Binance API key is required for listen-key requests")
        headers = {"X-MBX-APIKEY": self.settings.binance_api_key}
        timeout = httpx.Timeout(5.0)
        url = f"{self.base_url}{path}"
        try:
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
        except (httpx.TransportError, httpx.TimeoutException) as exc:
            raise StreamHealthFailure("LISTEN_KEY_RETRYABLE_FAILURE") from exc

        if response.status_code != 200:
            payload = _listen_key_error_payload(response)
            raise StreamHealthFailure(_listen_key_failure_reason(response.status_code, payload))

        try:
            payload = response.json()
        except ValueError as exc:
            raise StreamHealthFailure("LISTEN_KEY_INVALID_JSON") from exc
        if not isinstance(payload, Mapping):
            raise StreamHealthFailure("LISTEN_KEY_MALFORMED_RESPONSE")
        return payload

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

    def reconciliation_from_user_event(self, event: object) -> ReconciliationResult | None:
        return reconciliation_from_user_event(event)

    async def reconcile_orders_and_fills(
        self,
        symbol: str,
        client_order_prefix: str | None = None,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> ReconciliationResult:
        _require_execution_prefix(client_order_prefix)
        open_orders_raw = await self._signed_request("GET", OPEN_ORDERS_PATH, {"symbol": symbol})
        historical_params = _historical_reconciliation_params(
            symbol,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        all_orders_raw = await self._signed_request("GET", ALL_ORDERS_PATH, historical_params)
        trades_raw = await self._signed_request("GET", USER_TRADES_PATH, historical_params)

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
        return self.market_stream_healthy and self.user_stream_healthy

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
    """Parse a Binance order payload into an internal child order."""

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
    """Parse a Binance trade payload into an internal fill."""

    return Fill(
        client_order_id=client_order_id,
        trade_id=str(raw["id"]) if raw.get("id") is not None else None,
        cumulative_filled_quantity=cumulative_quantity,
        last_filled_quantity=Decimal(str(raw.get("qty", "0"))),
        last_fill_price=Decimal(str(raw.get("price", "0"))),
        event_time_ms=_optional_int(raw.get("time")),
        transaction_time_ms=_optional_int(raw.get("time")),
        is_maker=_optional_bool(raw.get("maker")),
    )


def reconciliation_from_user_event(event: object) -> ReconciliationResult | None:
    """Convert a Binance user stream order event into reconciliation data."""

    if not isinstance(event, Mapping):
        return None

    raw = event.get("raw", event)
    if not isinstance(raw, Mapping):
        return None

    event_type = event.get("event_type", raw.get("e"))
    if event_type != "ORDER_TRADE_UPDATE":
        return None

    order_payload = raw.get("o")
    if not isinstance(order_payload, Mapping):
        return None

    client_order_id = str(order_payload.get("c") or "")
    if not client_order_id:
        return None

    raw_status = str(order_payload.get("X", "UNKNOWN"))
    submitted_quantity = Decimal(str(order_payload.get("q", "0")))
    cumulative_filled_quantity = Decimal(str(order_payload.get("z", "0")))
    order_price = _order_update_price(order_payload)
    order = ChildOrder(
        child_order_id=client_order_id,
        client_order_id=client_order_id,
        symbol=str(order_payload.get("s", "")),
        side=_parse_side(order_payload.get("S", "BUY")),
        submitted_quantity=submitted_quantity,
        price=order_price,
        status=normalize_order_status(raw_status),
        confirmed_filled_quantity=cumulative_filled_quantity,
        exchange_order_id=str(order_payload["i"]) if order_payload.get("i") is not None else None,
        raw_status=raw_status,
        terminal_reason=raw_status if raw_status in {"EXPIRED", "EXPIRED_IN_MATCH", "REJECTED"} else None,
    )

    fills: list[Fill] = []
    last_filled_quantity = Decimal(str(order_payload.get("l", "0")))
    if last_filled_quantity > Decimal("0"):
        trade_id = None
        raw_trade_id = order_payload.get("t")
        if raw_trade_id is not None and str(raw_trade_id) != "-1":
            trade_id = str(raw_trade_id)
        fills.append(
            Fill(
                client_order_id=client_order_id,
                trade_id=trade_id,
                cumulative_filled_quantity=cumulative_filled_quantity,
                last_filled_quantity=last_filled_quantity,
                last_fill_price=Decimal(str(order_payload.get("L", order_price))),
                event_time_ms=_optional_int(raw.get("E")),
                transaction_time_ms=_optional_int(order_payload.get("T", raw.get("T"))),
                is_maker=_optional_bool(order_payload.get("m")),
            )
        )

    return ReconciliationResult(orders=[order], fills=fills)


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


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"true", "1"}:
            return True
        if normalized in {"false", "0"}:
            return False
    return bool(value)


def _order_update_price(order_payload: Mapping[str, Any]) -> Decimal:
    price = Decimal(str(order_payload.get("p", "0")))
    if price != Decimal("0"):
        return price
    last_price = Decimal(str(order_payload.get("L", "0")))
    if last_price != Decimal("0"):
        return last_price
    return Decimal("0")


def _stream_data(message: Mapping[str, Any]) -> Mapping[str, Any]:
    data = message.get("data", message)
    if not isinstance(data, Mapping):
        raise ValueError("stream message data must be a mapping")
    return data


def _json_payload_or_conservative_failure(
    response: Any,
    mutation_kind: MutationKind | None,
) -> Any:
    try:
        return response.json()
    except ValueError as exc:
        if mutation_kind is MutationKind.CREATE:
            raise UnknownCreateOutcome("UNKNOWN_CREATE_OUTCOME_INVALID_JSON") from exc
        if mutation_kind is MutationKind.CANCEL:
            raise PendingCancelOutcome("PENDING_CANCEL_OUTCOME_INVALID_JSON") from exc
        raise RetryableReadFailure("SIGNED_READ_INVALID_JSON") from exc


def _terminal_reject_reason(status_code: int, payload: Any) -> str:
    if isinstance(payload, Mapping):
        code = payload.get("code")
        message = payload.get("msg")
        if code is not None and message is not None:
            return f"BINANCE_{code}:{message}"
        if message is not None:
            return str(message)
    return f"BINANCE_HTTP_{status_code}"


def _listen_key_error_payload(response: Any) -> Mapping[str, Any] | None:
    if response.status_code in {408, 418, 429} or 500 <= response.status_code <= 599:
        return None
    try:
        payload = response.json()
    except ValueError as exc:
        raise StreamHealthFailure("LISTEN_KEY_ERROR_INVALID_JSON") from exc
    if not isinstance(payload, Mapping):
        raise StreamHealthFailure("LISTEN_KEY_MALFORMED_ERROR_RESPONSE")
    return payload


def _listen_key_failure_reason(status_code: int, payload: Mapping[str, Any] | None) -> str:
    if status_code == 429:
        return "LISTEN_KEY_RATE_LIMIT_BACKOFF"
    if status_code == 418:
        return "LISTEN_KEY_VENUE_BAN_HARD_STOP"
    if status_code == 408 or 500 <= status_code <= 599:
        return "LISTEN_KEY_RETRYABLE_FAILURE"
    if payload is not None and _is_listen_key_expired_payload(payload):
        return "LISTEN_KEY_EXPIRED"
    if payload is not None:
        return f"LISTEN_KEY_TERMINAL_FAILURE:{_terminal_reject_reason(status_code, payload)}"
    return f"LISTEN_KEY_TERMINAL_FAILURE:BINANCE_HTTP_{status_code}"


def _is_listen_key_expired_payload(payload: Mapping[str, Any]) -> bool:
    code = payload.get("code")
    if code == -1125 or str(code) == "-1125":
        return True
    message = str(payload.get("msg", "")).casefold()
    return "listenkey" in message and (
        "does not exist" in message or "not exist" in message or "expired" in message
    )


def _is_specific_terminal_5xx_reject(reason: str) -> bool:
    match = re.match(r"^BINANCE_(-?\d+):", reason)
    if match is None:
        return False
    code = int(match.group(1))
    ambiguous_server_codes = {-1000, -1001, -1006, -1007, -1008}
    return code not in ambiguous_server_codes


def _historical_reconciliation_params(
    symbol: str,
    *,
    start_time_ms: int | None,
    end_time_ms: int | None,
) -> dict[str, int | str]:
    params: dict[str, int | str] = {"symbol": symbol, "limit": 1000}
    if start_time_ms is not None:
        params["startTime"] = start_time_ms
    if end_time_ms is not None:
        params["endTime"] = end_time_ms
    return params


def _is_order_not_found_reason(reason: str) -> bool:
    return "BINANCE_-2013" in reason or "Order does not exist" in reason


def _is_retryable_order_reject_reason(reason: str) -> bool:
    return reason.startswith("BINANCE_-5022:")
