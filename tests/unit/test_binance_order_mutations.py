from __future__ import annotations

import asyncio
import json
from decimal import Decimal
import os
from pathlib import Path
import subprocess
import sys
from typing import Any

import httpx
import pytest

from config import Settings
from exchanges.base import OrderCreateTimeout, OrderRejected
from exchanges.binance_usdm import (
    LISTEN_KEY_PATH,
    ORDER_QUERY_PATH,
    ORDER_REST_PATH,
    BinanceUsdmAdapter,
    ExchangeTerminalReject,
    MutationKind,
    PendingCancelOutcome,
    RetryableReadFailure,
    UnknownCreateOutcome,
    build_new_order_params,
    classify_mutation_timeout,
    decimal_to_api,
)
from execution.clock import ManualClock
from execution.models import (
    ChildOrderStatus,
    Environment,
    MarketSnapshot,
    OrderRequest,
    Side,
    SymbolRules,
    TimeInForce,
)


class FakeResponse:
    def __init__(self, status_code: int, payload: Any, *, json_error: Exception | None = None) -> None:
        self.status_code = status_code
        self._payload = payload
        self._json_error = json_error
        self.text = str(payload)

    def json(self) -> Any:
        if self._json_error is not None:
            raise self._json_error
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                f"status {self.status_code}",
                request=httpx.Request("GET", "https://example.test"),
                response=httpx.Response(self.status_code),
            )


class RecordingClient:
    def __init__(
        self,
        response: FakeResponse | None = None,
        *,
        timeout: bool = False,
        exception: Exception | None = None,
    ) -> None:
        self.response = response or FakeResponse(200, {})
        self.timeout = timeout
        self.exception = exception
        self.calls: list[dict[str, Any]] = []

    async def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        if self.timeout:
            raise httpx.TimeoutException("timed out")
        if self.exception is not None:
            raise self.exception
        return self.response


class FakeWebSocket:
    def __init__(self, messages: list[str]) -> None:
        self._messages = list(messages)
        self.closed = asyncio.Event()

    async def __aenter__(self) -> FakeWebSocket:
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.closed.set()

    def __aiter__(self) -> FakeWebSocket:
        return self

    async def __anext__(self) -> str:
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)


def rules(*, tif: frozenset[str] = frozenset({"GTC", "GTX"})) -> SymbolRules:
    return SymbolRules(
        symbol="BTCUSDT",
        tick_size=Decimal("0.10"),
        quantity_step=Decimal("0.001"),
        min_quantity=Decimal("0.001"),
        min_notional=Decimal("5"),
        status="TRADING",
        supported_time_in_force=tif,
    )


def order_request(
    *,
    client_order_id: str = "ce_abcdef123456_1",
    post_only: bool = True,
    time_in_force: TimeInForce | None = None,
) -> OrderRequest:
    return OrderRequest(
        execution_id="exec_abcdef1234567890",
        child_order_id="child_0001",
        client_order_id=client_order_id,
        symbol="BTCUSDT",
        side=Side.BUY,
        quantity=Decimal("0.010"),
        price=Decimal("95000.10"),
        post_only=post_only,
        time_in_force=time_in_force,
    )


def authed_adapter(client: RecordingClient | None = None) -> BinanceUsdmAdapter:
    return BinanceUsdmAdapter(
        settings=Settings(
            environment=Environment.TESTNET,
            binance_api_key="fake-key",
            binance_api_secret="fake-secret",
            recv_window_ms=7000,
        ),
        client=client,
        clock=ManualClock(),
    )


def test_new_order_payload_serializes_decimals_and_uses_time_in_force() -> None:
    post_only = build_new_order_params(order_request(post_only=True), rules())
    non_post_only = build_new_order_params(order_request(post_only=False), rules())

    assert post_only == {
        "symbol": "BTCUSDT",
        "side": "BUY",
        "type": "LIMIT",
        "timeInForce": "GTX",
        "quantity": "0.010",
        "price": "95000.10",
        "newClientOrderId": "ce_abcdef123456_1",
    }
    assert non_post_only["timeInForce"] == "GTC"
    assert decimal_to_api(Decimal("1.2300")) == "1.2300"


def test_new_order_payload_rejects_post_only_without_gtx_and_invalid_client_id() -> None:
    with pytest.raises(ExchangeTerminalReject, match="POST_ONLY_GTX_UNSUPPORTED"):
        build_new_order_params(order_request(post_only=True), rules(tif=frozenset({"GTC"})))

    with pytest.raises(ExchangeTerminalReject, match="INVALID_CLIENT_ORDER_ID"):
        build_new_order_params(order_request(client_order_id="INVALID SPACE"), rules())

    with pytest.raises(ExchangeTerminalReject, match="INVALID_CLIENT_ORDER_ID"):
        build_new_order_params(order_request(client_order_id="x" * 37), rules())


def test_new_order_payload_uses_explicit_ioc_and_rejects_unsupported_ioc() -> None:
    request = order_request(post_only=False, time_in_force=TimeInForce.IOC)

    params = build_new_order_params(request, rules(tif=frozenset({"GTC", "GTX", "IOC"})))

    assert params["timeInForce"] == "IOC"
    with pytest.raises(ExchangeTerminalReject, match="IOC_TIME_IN_FORCE_UNSUPPORTED"):
        build_new_order_params(request, rules(tif=frozenset({"GTC", "GTX"})))


def test_timeout_classification_and_exception_hierarchy() -> None:
    assert ORDER_REST_PATH == "/fapi/v1/order"
    assert ORDER_QUERY_PATH == "/fapi/v1/order"
    assert classify_mutation_timeout(MutationKind.CREATE) == "UNKNOWN_CREATE_OUTCOME"
    assert classify_mutation_timeout(MutationKind.CANCEL) == "PENDING_CANCEL_OUTCOME"
    assert issubclass(ExchangeTerminalReject, OrderRejected)
    assert issubclass(UnknownCreateOutcome, OrderCreateTimeout)
    assert issubclass(PendingCancelOutcome, Exception)


async def test_signed_request_timeout_maps_by_mutation_kind() -> None:
    create_adapter = authed_adapter(RecordingClient(timeout=True))
    cancel_adapter = authed_adapter(RecordingClient(timeout=True))
    read_adapter = authed_adapter(RecordingClient(timeout=True))

    with pytest.raises(UnknownCreateOutcome):
        await create_adapter._signed_request("POST", ORDER_REST_PATH, {}, mutation_kind=MutationKind.CREATE)
    with pytest.raises(PendingCancelOutcome):
        await cancel_adapter._signed_request("DELETE", ORDER_REST_PATH, {}, mutation_kind=MutationKind.CANCEL)
    with pytest.raises(RetryableReadFailure):
        await read_adapter._signed_request("GET", ORDER_QUERY_PATH, {})


@pytest.mark.parametrize(
    "transport_error",
    [
        httpx.ConnectError("connect failed"),
        httpx.ReadError("read failed"),
        httpx.WriteError("write failed"),
        httpx.RemoteProtocolError("remote protocol failed"),
        httpx.TransportError("transport failed"),
        httpx.TimeoutException("timed out"),
    ],
)
async def test_signed_request_transport_errors_map_by_operation(transport_error: Exception) -> None:
    with pytest.raises(UnknownCreateOutcome):
        await authed_adapter(RecordingClient(exception=transport_error))._signed_request(
            "POST",
            ORDER_REST_PATH,
            {},
            mutation_kind=MutationKind.CREATE,
        )
    with pytest.raises(PendingCancelOutcome):
        await authed_adapter(RecordingClient(exception=transport_error))._signed_request(
            "DELETE",
            ORDER_REST_PATH,
            {},
            mutation_kind=MutationKind.CANCEL,
        )
    with pytest.raises(RetryableReadFailure):
        await authed_adapter(RecordingClient(exception=transport_error))._signed_request(
            "GET",
            ORDER_QUERY_PATH,
            {},
        )


@pytest.mark.parametrize(
    ("status_code", "expected_exc", "match"),
    [
        (429, RetryableReadFailure, "RATE_LIMIT_BACKOFF"),
        (418, RuntimeError, "VENUE_BAN_HARD_STOP"),
    ],
)
async def test_signed_request_status_only_hard_stops_ignore_malformed_json(
    status_code: int,
    expected_exc: type[Exception],
    match: str,
) -> None:
    def malformed_response() -> FakeResponse:
        return FakeResponse(status_code, "", json_error=ValueError("invalid json"))

    for method, path, mutation_kind in (
        ("POST", ORDER_REST_PATH, MutationKind.CREATE),
        ("DELETE", ORDER_REST_PATH, MutationKind.CANCEL),
        ("GET", ORDER_QUERY_PATH, None),
    ):
        with pytest.raises(expected_exc, match=match):
            await authed_adapter(RecordingClient(malformed_response()))._signed_request(
                method,
                path,
                {},
                mutation_kind=mutation_kind,
            )


async def test_signed_request_http_408_maps_mutations_to_ambiguous_outcome() -> None:
    create_adapter = authed_adapter(RecordingClient(FakeResponse(408, {"code": -1007, "msg": "Timeout"})))
    cancel_adapter = authed_adapter(RecordingClient(FakeResponse(408, {"code": -1007, "msg": "Timeout"})))
    read_adapter = authed_adapter(RecordingClient(FakeResponse(408, {"code": -1007, "msg": "Timeout"})))

    with pytest.raises(UnknownCreateOutcome):
        await create_adapter._signed_request("POST", ORDER_REST_PATH, {}, mutation_kind=MutationKind.CREATE)
    with pytest.raises(PendingCancelOutcome):
        await cancel_adapter._signed_request("DELETE", ORDER_REST_PATH, {}, mutation_kind=MutationKind.CANCEL)
    with pytest.raises(RetryableReadFailure):
        await read_adapter._signed_request("GET", ORDER_QUERY_PATH, {})


async def test_signed_request_invalid_json_after_http_success_maps_conservatively() -> None:
    bad_json = FakeResponse(200, "not-json", json_error=ValueError("invalid json"))

    with pytest.raises(UnknownCreateOutcome):
        await authed_adapter(RecordingClient(bad_json))._signed_request(
            "POST",
            ORDER_REST_PATH,
            {},
            mutation_kind=MutationKind.CREATE,
        )
    with pytest.raises(PendingCancelOutcome):
        await authed_adapter(RecordingClient(bad_json))._signed_request(
            "DELETE",
            ORDER_REST_PATH,
            {},
            mutation_kind=MutationKind.CANCEL,
        )
    with pytest.raises(RetryableReadFailure):
        await authed_adapter(RecordingClient(bad_json))._signed_request(
            "GET",
            ORDER_QUERY_PATH,
            {},
        )


@pytest.mark.parametrize("status_code", [400, 503])
async def test_signed_request_malformed_error_json_maps_conservatively_by_operation(
    status_code: int,
) -> None:
    def malformed_response() -> FakeResponse:
        return FakeResponse(status_code, "not-json", json_error=ValueError("invalid json"))

    with pytest.raises(UnknownCreateOutcome):
        await authed_adapter(RecordingClient(malformed_response()))._signed_request(
            "POST",
            ORDER_REST_PATH,
            {},
            mutation_kind=MutationKind.CREATE,
        )
    with pytest.raises(PendingCancelOutcome):
        await authed_adapter(RecordingClient(malformed_response()))._signed_request(
            "DELETE",
            ORDER_REST_PATH,
            {},
            mutation_kind=MutationKind.CANCEL,
        )
    with pytest.raises(RetryableReadFailure):
        await authed_adapter(RecordingClient(malformed_response()))._signed_request(
            "GET",
            ORDER_QUERY_PATH,
            {},
        )


async def test_signed_create_503_with_specific_terminal_reject_is_not_ambiguous() -> None:
    adapter = authed_adapter(
        RecordingClient(
            FakeResponse(
                503,
                {"code": -2019, "msg": "Margin is insufficient."},
            )
        )
    )

    with pytest.raises(ExchangeTerminalReject, match="Margin is insufficient"):
        await adapter._signed_request("POST", ORDER_REST_PATH, {}, mutation_kind=MutationKind.CREATE)


async def test_signed_create_post_only_reject_remains_retryable_order_reject() -> None:
    adapter = authed_adapter(
        RecordingClient(
            FakeResponse(
                400,
                {
                    "code": -5022,
                    "msg": "Due to the order could not be executed as maker, the Post Only order will be rejected.",
                },
            )
        )
    )

    with pytest.raises(OrderRejected, match="Post Only") as exc_info:
        await adapter._signed_request("POST", ORDER_REST_PATH, {}, mutation_kind=MutationKind.CREATE)
    assert type(exc_info.value) is OrderRejected


async def test_signed_create_non_post_only_4xx_with_maker_text_remains_terminal() -> None:
    adapter = authed_adapter(
        RecordingClient(
            FakeResponse(
                400,
                {
                    "code": -4999,
                    "msg": "Maker account configuration is invalid.",
                },
            )
        )
    )

    with pytest.raises(ExchangeTerminalReject, match="Maker account"):
        await adapter._signed_request("POST", ORDER_REST_PATH, {}, mutation_kind=MutationKind.CREATE)


async def test_signed_create_insufficient_margin_is_terminal_reject() -> None:
    adapter = authed_adapter(
        RecordingClient(
            FakeResponse(
                400,
                {"code": -2019, "msg": "Margin is insufficient."},
            )
        )
    )

    with pytest.raises(ExchangeTerminalReject, match="Margin is insufficient"):
        await adapter._signed_request("POST", ORDER_REST_PATH, {}, mutation_kind=MutationKind.CREATE)


async def test_signed_request_uses_api_key_header_and_signed_params_without_secret() -> None:
    client = RecordingClient(FakeResponse(200, {"ok": True}))
    adapter = authed_adapter(client)

    result = await adapter._signed_request("GET", "/fapi/v1/account", {"symbol": "BTCUSDT"})

    call = client.calls[0]
    assert result == {"ok": True}
    assert call["method"] == "GET"
    assert call["url"].endswith("/fapi/v1/account")
    assert call["headers"] == {"X-MBX-APIKEY": "fake-key"}
    assert call["params"]["symbol"] == "BTCUSDT"
    assert call["params"]["recvWindow"] == "7000"
    assert "signature" in call["params"]
    assert "fake-secret" not in str(call)


async def test_signed_mutation_hard_stops_requested_mainnet_without_explicit_guard() -> None:
    client = RecordingClient(FakeResponse(200, {"unexpected": True}))
    adapter = BinanceUsdmAdapter(
        settings=Settings(
            environment=Environment.MAINNET,
            allow_mainnet_trading=False,
            binance_api_key="fake-key",
            binance_api_secret="fake-secret",
        ),
        client=client,
    )

    with pytest.raises(ExchangeTerminalReject, match="MAINNET_TRADING_NOT_ALLOWED"):
        await adapter._signed_request("POST", ORDER_REST_PATH, {}, mutation_kind=MutationKind.CREATE)

    assert client.calls == []


async def test_submit_cancel_and_query_use_order_endpoint_and_orig_client_id() -> None:
    client = RecordingClient(
        FakeResponse(
            200,
            {
                "symbol": "BTCUSDT",
                "clientOrderId": "ce_abcdef123456_1",
                "orderId": 123,
                "side": "BUY",
                "origQty": "0.010",
                "executedQty": "0.000",
                "price": "95000.10",
                "status": "NEW",
            },
        )
    )
    adapter = authed_adapter(client)

    async def fake_rules(_symbol: str) -> SymbolRules:
        return rules()

    adapter.get_symbol_rules = fake_rules  # type: ignore[method-assign]

    submitted = await adapter.submit_limit_order(order_request())
    cancelled = await adapter.cancel_order("BTCUSDT", "ce_abcdef123456_1")
    queried = await adapter.get_order_by_client_order_id("BTCUSDT", "ce_abcdef123456_1")

    assert submitted.status is ChildOrderStatus.OPEN
    assert cancelled.exchange_order_id == "123"
    assert queried.client_order_id == "ce_abcdef123456_1"
    assert [(call["method"], call["url"].split(adapter.base_url, 1)[1]) for call in client.calls] == [
        ("POST", "/fapi/v1/order"),
        ("DELETE", "/fapi/v1/order"),
        ("GET", "/fapi/v1/order"),
    ]
    assert client.calls[1]["params"]["origClientOrderId"] == "ce_abcdef123456_1"
    assert client.calls[2]["params"]["origClientOrderId"] == "ce_abcdef123456_1"


async def test_query_order_not_found_returns_none_for_create_timeout_reconciliation() -> None:
    client = RecordingClient(FakeResponse(400, {"code": -2013, "msg": "Order does not exist."}))
    adapter = authed_adapter(client)

    order = await adapter.get_order_by_client_order_id("BTCUSDT", "ce_abcdef123456_1")

    assert order is None
    assert client.calls[0]["method"] == "GET"
    assert client.calls[0]["url"].endswith("/fapi/v1/order")
    assert client.calls[0]["params"]["origClientOrderId"] == "ce_abcdef123456_1"


async def test_reconciliation_requires_prefix_filters_manual_orders_and_joins_trades_by_order_id() -> None:
    payloads = {
        ("GET", "/fapi/v1/openOrders"): [
            {
                "symbol": "BTCUSDT",
                "clientOrderId": "ce_abcdef123456_1",
                "orderId": 111,
                "side": "BUY",
                "origQty": "0.010",
                "executedQty": "0.004",
                "price": "95000.10",
                "status": "PARTIALLY_FILLED",
            },
            {
                "symbol": "BTCUSDT",
                "clientOrderId": "manual_order",
                "orderId": 999,
                "side": "BUY",
                "origQty": "1.000",
                "executedQty": "0.000",
                "price": "1.00",
                "status": "NEW",
            },
        ],
        ("GET", "/fapi/v1/allOrders"): [
            {
                "symbol": "BTCUSDT",
                "clientOrderId": "ce_abcdef123456_2",
                "orderId": 222,
                "side": "BUY",
                "origQty": "0.006",
                "executedQty": "0.006",
                "price": "95000.00",
                "status": "FILLED",
            },
            {
                "symbol": "BTCUSDT",
                "clientOrderId": "other_prefix_1",
                "orderId": 333,
                "side": "BUY",
                "origQty": "9",
                "executedQty": "9",
                "price": "9",
                "status": "FILLED",
            },
        ],
        ("GET", "/fapi/v1/userTrades"): [
            {"symbol": "BTCUSDT", "orderId": 222, "id": 1, "qty": "0.002", "price": "95000", "time": 10},
            {"symbol": "BTCUSDT", "orderId": 222, "id": 2, "qty": "0.004", "price": "95001", "time": 11},
            {"symbol": "BTCUSDT", "orderId": 999, "id": 3, "qty": "1", "price": "1", "time": 12},
        ],
    }

    class ReconciliationClient(RecordingClient):
        async def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
            self.calls.append({"method": method, "url": url, **kwargs})
            return FakeResponse(200, payloads[(method, url.split(BinanceUsdmAdapter().base_url, 1)[1])])

    adapter = authed_adapter(ReconciliationClient())

    with pytest.raises(ValueError, match="client_order_prefix"):
        await adapter.reconcile_orders_and_fills("BTCUSDT", client_order_prefix=None)

    result = await adapter.reconcile_orders_and_fills(
        "BTCUSDT",
        client_order_prefix="ce_abcdef123456_",
    )

    assert [order.client_order_id for order in result.orders] == [
        "ce_abcdef123456_1",
        "ce_abcdef123456_2",
    ]
    assert [fill.client_order_id for fill in result.fills] == [
        "ce_abcdef123456_2",
        "ce_abcdef123456_2",
    ]
    assert [fill.cumulative_filled_quantity for fill in result.fills] == [
        Decimal("0.002"),
        Decimal("0.006"),
    ]


async def test_reconciliation_passes_time_window_and_limit_to_historical_endpoints() -> None:
    class EmptyReconciliationClient(RecordingClient):
        async def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
            self.calls.append({"method": method, "url": url, **kwargs})
            return FakeResponse(200, [])

    client = EmptyReconciliationClient()
    adapter = authed_adapter(client)

    result = await adapter.reconcile_orders_and_fills(
        "BTCUSDT",
        client_order_prefix="ce_abcdef123456_",
        start_time_ms=1000,
        end_time_ms=2000,
    )

    assert result.orders == []
    assert result.fills == []
    params_by_path = {
        call["url"].split(adapter.base_url, 1)[1]: call["params"]
        for call in client.calls
    }
    assert params_by_path["/fapi/v1/openOrders"]["symbol"] == "BTCUSDT"
    assert params_by_path["/fapi/v1/allOrders"]["limit"] == "1000"
    assert params_by_path["/fapi/v1/allOrders"]["startTime"] == "1000"
    assert params_by_path["/fapi/v1/allOrders"]["endTime"] == "2000"
    assert params_by_path["/fapi/v1/userTrades"]["limit"] == "1000"
    assert params_by_path["/fapi/v1/userTrades"]["startTime"] == "1000"
    assert params_by_path["/fapi/v1/userTrades"]["endTime"] == "2000"


async def test_position_lookup_rejects_hedge_mode_and_returns_zero_for_missing_symbol() -> None:
    hedge = RecordingClient(
        FakeResponse(
            200,
            [{"symbol": "BTCUSDT", "positionSide": "LONG", "positionAmt": "0.1", "updateTime": 1}],
        )
    )
    with pytest.raises(ExchangeTerminalReject, match="HEDGE_MODE_UNSUPPORTED"):
        await authed_adapter(hedge).get_position("BTCUSDT")

    missing = RecordingClient(FakeResponse(200, [{"symbol": "ETHUSDT", "positionSide": "BOTH", "positionAmt": "1"}]))
    position = await authed_adapter(missing).get_position("BTCUSDT")

    assert position.symbol == "BTCUSDT"
    assert position.position == Decimal("0")


def test_stream_parsers_preserve_exchange_timestamps() -> None:
    clock = ManualClock(current=123.456)
    adapter = BinanceUsdmAdapter(settings=Settings(environment=Environment.TESTNET), clock=clock)

    snapshot = adapter.parse_book_ticker(
        {"stream": "btcusdt@bookTicker", "data": {"s": "BTCUSDT", "b": "100.10", "a": "100.20", "E": 99}}
    )
    event = adapter.parse_user_event({"e": "ORDER_TRADE_UPDATE", "E": 100, "T": 101, "o": {"x": "TRADE"}})

    assert snapshot.symbol == "BTCUSDT"
    assert snapshot.bid == Decimal("100.10")
    assert snapshot.ask == Decimal("100.20")
    assert snapshot.last_market_event_time_exchange == 99
    assert snapshot.last_market_event_time_local_monotonic == 123.456
    assert event["event_type"] == "ORDER_TRADE_UPDATE"
    assert event["event_time_ms"] == 100
    assert event["transaction_time_ms"] == 101
    assert event["raw"]["o"]["x"] == "TRADE"


async def test_market_stream_marks_health_around_iterator_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    websocket = FakeWebSocket(
        [
            json.dumps(
                {
                    "stream": "btcusdt@bookTicker",
                    "data": {"s": "BTCUSDT", "b": "100.10", "a": "100.20", "E": 99},
                }
            )
        ]
    )
    adapter = BinanceUsdmAdapter(settings=Settings(environment=Environment.TESTNET), clock=ManualClock())

    monkeypatch.setattr("exchanges.binance_usdm.websockets.connect", lambda _url: websocket)

    stream = adapter.stream_market_data()
    snapshot = await anext(stream)

    assert snapshot.symbol == "BTCUSDT"
    assert adapter.market_stream_healthy is True

    with pytest.raises(StopAsyncIteration):
        await anext(stream)

    assert adapter.market_stream_healthy is False
    assert websocket.closed.is_set()


async def test_user_stream_creates_listen_key_tracks_health_and_degrades_on_disconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = RecordingClient(FakeResponse(200, {"listenKey": "listen-1"}))
    websocket = FakeWebSocket(
        [
            json.dumps(
                {
                    "e": "ORDER_TRADE_UPDATE",
                    "E": 100,
                    "T": 101,
                    "o": {"x": "NEW"},
                }
            )
        ]
    )
    adapter = authed_adapter(client)

    monkeypatch.setattr("exchanges.binance_usdm.websockets.connect", lambda _url: websocket)

    stream = adapter.stream_user_events()
    event = await anext(stream)

    assert event["event_type"] == "ORDER_TRADE_UPDATE"
    assert adapter.latest_listen_key == "listen-1"
    assert adapter.user_stream_healthy is True
    assert client.calls[0]["method"] == "POST"
    assert client.calls[0]["url"].endswith(LISTEN_KEY_PATH)

    with pytest.raises(StopAsyncIteration):
        await anext(stream)

    assert adapter.user_stream_healthy is False
    assert await adapter.health_check_streams() is False
    assert websocket.closed.is_set()


def test_market_stream_url_uses_requested_symbol_and_public_route() -> None:
    adapter = BinanceUsdmAdapter(settings=Settings(environment=Environment.TESTNET))

    adapter.set_market_stream_symbol("ETHUSDT")

    assert adapter._market_stream_symbol == "ETHUSDT"
    assert adapter.market_stream_url("ETHUSDT") == (
        "wss://fstream.binancefuture.com/public/ws/ethusdt@bookTicker"
    )


def test_testnet_runner_normalizes_symbol_for_rest_and_stream_usage() -> None:
    import importlib.util

    spec = importlib.util.spec_from_file_location("testnet_runner", Path("scripts/testnet_runner.py"))
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.normalize_symbol("ethusdt") == "ETHUSDT"


async def test_testnet_runner_keeps_market_stream_running_until_stopped() -> None:
    import importlib.util

    class FakeMarketAdapter:
        def __init__(self) -> None:
            self.closed = asyncio.Event()
            self.allow_second_snapshot = asyncio.Event()

        def stream_market_data(self):
            async def events():
                try:
                    yield MarketSnapshot(
                        symbol="BTCUSDT",
                        bid=Decimal("100.00"),
                        ask=Decimal("100.10"),
                        last_market_event_time_exchange=1,
                        last_market_event_time_local_monotonic=1.0,
                    )
                    await self.allow_second_snapshot.wait()
                    yield MarketSnapshot(
                        symbol="BTCUSDT",
                        bid=Decimal("100.10"),
                        ask=Decimal("100.20"),
                        last_market_event_time_exchange=2,
                        last_market_event_time_local_monotonic=2.0,
                    )
                finally:
                    self.closed.set()

            return events()

    spec = importlib.util.spec_from_file_location("testnet_runner", Path("scripts/testnet_runner.py"))
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    adapter = FakeMarketAdapter()
    snapshot, task = await module._start_market_stream(adapter, timeout_seconds=0.1)

    assert snapshot.symbol == "BTCUSDT"
    assert not task.done()

    await module._stop_market_stream(task)
    assert adapter.closed.is_set()


def test_testnet_scripts_refuse_without_credentials_and_never_fallback_to_simulator() -> None:
    script = Path("scripts/run_testnet_chase.py")
    env = os.environ.copy()
    env.pop("BINANCE_USDM_API_KEY", None)
    env.pop("BINANCE_USDM_API_SECRET", None)
    env["PYTHON_DOTENV_DISABLED"] = "1"

    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=Path.cwd(),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "never falls back to simulation" in result.stderr
    assert "DeterministicSimulator" not in script.read_text()
    assert "DeterministicSimulator" not in Path("scripts/run_testnet_twap.py").read_text()


def test_testnet_scripts_require_confirm_before_network_work_with_fake_credentials() -> None:
    env = os.environ.copy()
    env["BINANCE_USDM_API_KEY"] = "fake-key"
    env["BINANCE_USDM_API_SECRET"] = "fake-secret"

    result = subprocess.run(
        [sys.executable, "scripts/run_testnet_chase.py"],
        cwd=Path.cwd(),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "--confirm-send-orders" in result.stderr


def test_testnet_runner_exposes_symbol_and_slice_arguments() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/run_testnet_twap.py", "--help"],
        cwd=Path.cwd(),
        text=True,
        capture_output=True,
        check=True,
    )

    assert "--symbol" in result.stdout
    assert "--number-of-slices" in result.stdout
