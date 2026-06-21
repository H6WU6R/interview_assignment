from __future__ import annotations

import os

import pytest

from config import Settings
from exchanges.binance_usdm import BinanceUsdmAdapter


pytestmark = pytest.mark.skipif(
    not os.getenv("BINANCE_USDM_API_KEY") or not os.getenv("BINANCE_USDM_API_SECRET"),
    reason="Binance Testnet credentials are not configured",
)


async def test_binance_testnet_exchange_info_contract() -> None:
    adapter = BinanceUsdmAdapter(
        Settings(
            environment="testnet",
            binance_api_key=os.environ["BINANCE_USDM_API_KEY"],
            binance_api_secret=os.environ["BINANCE_USDM_API_SECRET"],
        )
    )

    rules = await adapter.get_symbol_rules("BTCUSDT")

    assert rules.symbol == "BTCUSDT"
    assert rules.tick_size > 0
    assert rules.quantity_step > 0
    assert rules.status


async def test_binance_testnet_signed_read_and_listen_key_contract() -> None:
    adapter = BinanceUsdmAdapter(
        Settings(
            environment="testnet",
            binance_api_key=os.environ["BINANCE_USDM_API_KEY"],
            binance_api_secret=os.environ["BINANCE_USDM_API_SECRET"],
        )
    )

    position = await adapter.get_position("BTCUSDT")
    listen_key = await adapter.create_listen_key()
    await adapter.renew_listen_key(listen_key)

    assert position.symbol == "BTCUSDT"
    assert isinstance(listen_key, str)
    assert listen_key
