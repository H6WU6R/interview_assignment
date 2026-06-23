from __future__ import annotations

import hmac
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from urllib.parse import urlencode

import pytest

from config import Settings, load_settings
from exchanges.base import NoFreshMarketData
from exchanges.binance_usdm import (
    BINANCE_USDM_MAINNET_BASE_URL,
    BINANCE_USDM_TESTNET_BASE_URL,
    BinanceUsdmAdapter,
    classify_http_status,
    normalize_order_status,
    parse_exchange_info_rate_limits,
    parse_symbol_rules_from_exchange_info,
    sign_params,
)
from execution.clock import ManualClock
from execution.models import ChildOrderStatus, Environment, MarketSnapshot, SymbolRules


class FakeExchangeInfoResponse:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self.payload


class FakeExchangeInfoClient:
    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.calls: list[dict] = []

    async def get(self, url: str, **kwargs: object) -> FakeExchangeInfoResponse:
        self.calls.append({"url": url, **kwargs})
        return FakeExchangeInfoResponse(self.payload)


def test_mainnet_requires_explicit_allow_flag() -> None:
    blocked = Settings(environment=Environment.MAINNET, allow_mainnet_trading=False)
    allowed = Settings(environment=Environment.MAINNET, allow_mainnet_trading=True)
    testnet = Settings(environment=Environment.TESTNET, allow_mainnet_trading=False)
    blocked_string = Settings(environment="mainnet", allow_mainnet_trading="false")
    allowed_string = Settings(environment="mainnet", allow_mainnet_trading="true")

    assert blocked.can_trade_mainnet is False
    assert allowed.can_trade_mainnet is True
    assert testnet.can_trade_mainnet is False
    assert blocked_string.can_trade_mainnet is False
    assert allowed_string.can_trade_mainnet is True


def test_invalid_mainnet_allow_flag_is_rejected() -> None:
    with pytest.raises(ValueError, match="allow_mainnet_trading"):
        Settings(environment="mainnet", allow_mainnet_trading="definitely")


def test_adapter_base_url_uses_mainnet_only_after_explicit_boolean_allow() -> None:
    blocked = BinanceUsdmAdapter(
        settings=Settings(environment="mainnet", allow_mainnet_trading="false")
    )
    allowed = BinanceUsdmAdapter(
        settings=Settings(environment="mainnet", allow_mainnet_trading=True)
    )

    assert blocked.base_url == BINANCE_USDM_TESTNET_BASE_URL
    assert allowed.base_url == BINANCE_USDM_MAINNET_BASE_URL


def test_sign_params_adds_signature_without_exposing_secret() -> None:
    params = {"symbol": "BTCUSDT", "timestamp": "1710000000000", "recvWindow": "5000"}
    signed = sign_params(params, "super-secret")
    expected_signature = hmac.new(
        b"super-secret",
        urlencode(params).encode(),
        sha256,
    ).hexdigest()

    assert signed is not params
    assert params == {
        "symbol": "BTCUSDT",
        "timestamp": "1710000000000",
        "recvWindow": "5000",
    }
    assert signed["signature"] == expected_signature
    assert signed["signature"] != "super-secret"
    assert "super-secret" not in signed.values()


def test_signed_params_uses_offset_recv_window_and_secret() -> None:
    settings = Settings(
        environment=Environment.TESTNET,
        recv_window_ms=7000,
        binance_api_secret="secret",
    )
    adapter = BinanceUsdmAdapter(settings=settings, clock=ManualClock())
    adapter.server_time_offset_ms = 250

    signed = adapter.signed_params(
        {"symbol": "BTCUSDT", "quantity": Decimal("0.010")}, now_ms=1000
    )

    expected_unsigned = {
        "symbol": "BTCUSDT",
        "quantity": "0.010",
        "timestamp": "1250",
        "recvWindow": "7000",
    }
    assert {k: signed[k] for k in expected_unsigned} == expected_unsigned
    assert (
        signed["signature"]
        == hmac.new(
            b"secret",
            urlencode(expected_unsigned).encode(),
            sha256,
        ).hexdigest()
    )


def test_signed_params_requires_secret() -> None:
    adapter = BinanceUsdmAdapter(settings=Settings(environment=Environment.TESTNET))

    with pytest.raises(ValueError, match="Binance API secret"):
        adapter.signed_params({"symbol": "BTCUSDT"}, now_ms=1000)


def test_classify_http_status_identifies_backoff_and_ban() -> None:
    assert classify_http_status(200) == "OK"
    assert classify_http_status(408) == "REQUEST_TIMEOUT_AMBIGUOUS"
    assert classify_http_status(429) == "RATE_LIMIT_BACKOFF"
    assert classify_http_status(418) == "VENUE_BAN_HARD_STOP"
    assert classify_http_status(503) == "RETRYABLE_READ_OR_UNKNOWN_MUTATION"
    assert classify_http_status(400) == "TERMINAL_REJECT"


@pytest.mark.parametrize(
    ("raw_status", "expected"),
    [
        ("NEW", ChildOrderStatus.OPEN),
        ("PARTIALLY_FILLED", ChildOrderStatus.PARTIALLY_FILLED),
        ("FILLED", ChildOrderStatus.FILLED),
        ("CANCELED", ChildOrderStatus.CANCELLED),
        ("EXPIRED", ChildOrderStatus.CANCELLED),
        ("EXPIRED_IN_MATCH", ChildOrderStatus.CANCELLED),
        ("REJECTED", ChildOrderStatus.REJECTED),
        ("PENDING_CANCEL", ChildOrderStatus.PENDING_CANCEL),
        ("SOMETHING_NEW", ChildOrderStatus.UNKNOWN),
    ],
)
def test_normalize_order_status(raw_status: str, expected: ChildOrderStatus) -> None:
    assert normalize_order_status(raw_status) == expected


def test_post_only_requires_gtx_support() -> None:
    adapter = BinanceUsdmAdapter(settings=Settings(environment=Environment.TESTNET))
    with_gtx = SymbolRules(
        symbol="BTCUSDT",
        tick_size=Decimal("0.10"),
        quantity_step=Decimal("0.001"),
        min_quantity=Decimal("0.001"),
        min_notional=Decimal("100"),
        status="TRADING",
        supported_time_in_force=frozenset({"GTC", "GTX"}),
    )
    without_gtx = SymbolRules(
        symbol="BTCUSDT",
        tick_size=Decimal("0.10"),
        quantity_step=Decimal("0.001"),
        min_quantity=Decimal("0.001"),
        min_notional=Decimal("100"),
        status="TRADING",
        supported_time_in_force=frozenset({"GTC"}),
    )

    assert adapter.supports_post_only(with_gtx) is True
    assert adapter.supports_post_only(without_gtx) is False


def test_exchange_info_parsing_uses_filters_and_rate_limits() -> None:
    payload = {
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "pricePrecision": 2,
                "quantityPrecision": 3,
                "timeInForce": ["GTC", "IOC", "GTX"],
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                    {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                    {"filterType": "MIN_NOTIONAL", "notional": "100.00"},
                ],
            }
        ],
        "rateLimits": [
            {"rateLimitType": "REQUEST_WEIGHT", "limit": 2400},
            {"rateLimitType": "ORDERS", "limit": 1200},
            {"rateLimitType": "RAW_REQUESTS", "limit": 6100},
        ],
    }

    rules = parse_symbol_rules_from_exchange_info(payload, "BTCUSDT")

    assert rules == SymbolRules(
        symbol="BTCUSDT",
        tick_size=Decimal("0.10"),
        quantity_step=Decimal("0.001"),
        min_quantity=Decimal("0.001"),
        min_notional=Decimal("100.00"),
        status="TRADING",
        supported_time_in_force=frozenset({"GTC", "IOC", "GTX"}),
    )
    assert parse_exchange_info_rate_limits(payload) == {
        "REQUEST_WEIGHT": 2400,
        "ORDERS": 1200,
    }


async def test_get_symbol_rules_uses_current_testnet_exchange_info_endpoint_without_params() -> (
    None
):
    payload = {
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "timeInForce": ["GTC", "GTX"],
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                    {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                    {"filterType": "MIN_NOTIONAL", "notional": "100.00"},
                ],
            }
        ],
        "rateLimits": [
            {"rateLimitType": "REQUEST_WEIGHT", "limit": 2400},
            {"rateLimitType": "ORDERS", "limit": 1200},
        ],
    }
    client = FakeExchangeInfoClient(payload)
    adapter = BinanceUsdmAdapter(
        settings=Settings(environment="testnet"), client=client
    )

    rules = await adapter.get_symbol_rules("BTCUSDT")

    assert rules.symbol == "BTCUSDT"
    assert adapter.rate_limits == {"REQUEST_WEIGHT": 2400, "ORDERS": 1200}
    assert (
        client.calls[0]["url"]
        == f"{BINANCE_USDM_TESTNET_BASE_URL}/fapi/v1/exchangeInfo"
    )
    assert "params" not in client.calls[0]


async def test_market_snapshots_require_present_uncrossed_and_fresh_data() -> None:
    clock = ManualClock(current=10)
    adapter = BinanceUsdmAdapter(
        settings=Settings(environment=Environment.TESTNET, stale_market_data_ms=1500),
        clock=clock,
    )

    with pytest.raises(NoFreshMarketData, match="no fresh market data"):
        await adapter.get_best_bid_ask("BTCUSDT")

    adapter._latest_market["BTCUSDT"] = MarketSnapshot(
        symbol="BTCUSDT",
        bid=Decimal("101"),
        ask=Decimal("100"),
        last_market_event_time_exchange=1,
        last_market_event_time_local_monotonic=clock.monotonic(),
    )
    with pytest.raises(NoFreshMarketData, match="crossed market data"):
        await adapter.get_best_bid_ask("BTCUSDT")

    adapter._latest_market["BTCUSDT"] = MarketSnapshot(
        symbol="BTCUSDT",
        bid=Decimal("100"),
        ask=Decimal("101"),
        last_market_event_time_exchange=2,
        last_market_event_time_local_monotonic=clock.monotonic(),
    )
    assert (
        await adapter.get_best_bid_ask("BTCUSDT") == adapter._latest_market["BTCUSDT"]
    )

    clock.advance(1.501)
    with pytest.raises(NoFreshMarketData, match="stale market data"):
        await adapter.get_best_bid_ask("BTCUSDT")


def test_config_loader_parses_example_yaml() -> None:
    settings = load_settings(Path("configs/example.yaml"))

    assert settings.environment == Environment.SIMULATION
    assert settings.allow_mainnet_trading is False
    assert settings.stale_market_data_ms == 1500
    assert settings.recv_window_ms == 5000
    assert settings.binance_api_key is None
    assert settings.binance_api_secret is None
