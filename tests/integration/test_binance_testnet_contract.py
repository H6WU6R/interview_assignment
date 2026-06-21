from __future__ import annotations

import pytest

from config import Settings, load_binance_usdm_credentials
from exchanges.binance_usdm import BinanceUsdmAdapter


_credentials = load_binance_usdm_credentials()

pytestmark = pytest.mark.skipif(
    not _credentials.is_configured,
    reason="Binance Testnet credentials are not configured",
)


async def test_binance_testnet_exchange_info_contract() -> None:
    adapter = BinanceUsdmAdapter(
        Settings(
            environment="testnet",
            binance_api_key=_credentials.api_key,
            binance_api_secret=_credentials.api_secret,
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
            binance_api_key=_credentials.api_key,
            binance_api_secret=_credentials.api_secret,
        )
    )

    position = await adapter.get_position("BTCUSDT")
    listen_key = await adapter.create_listen_key()
    await adapter.renew_listen_key(listen_key)

    assert position.symbol == "BTCUSDT"
    assert isinstance(listen_key, str)
    assert listen_key
