from __future__ import annotations

import os

from config import load_allow_mainnet_trading, load_binance_usdm_credentials


def test_load_binance_usdm_credentials_reads_dotenv_values(
    tmp_path, monkeypatch
) -> None:
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text(
        "BINANCE_USDM_API_KEY=dotenv-key\nBINANCE_USDM_API_SECRET=dotenv-secret\n"
    )
    monkeypatch.delenv("BINANCE_USDM_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_USDM_API_SECRET", raising=False)

    credentials = load_binance_usdm_credentials(dotenv_path)

    assert credentials.api_key == "dotenv-key"
    assert credentials.api_secret == "dotenv-secret"
    assert credentials.is_configured
    assert "BINANCE_USDM_API_KEY" not in os.environ
    assert "BINANCE_USDM_API_SECRET" not in os.environ


def test_load_binance_usdm_credentials_does_not_override_exported_values(
    tmp_path, monkeypatch
) -> None:
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text(
        "BINANCE_USDM_API_KEY=dotenv-key\nBINANCE_USDM_API_SECRET=dotenv-secret\n"
    )
    monkeypatch.setenv("BINANCE_USDM_API_KEY", "exported-key")
    monkeypatch.setenv("BINANCE_USDM_API_SECRET", "exported-secret")

    credentials = load_binance_usdm_credentials(dotenv_path)

    assert credentials.api_key == "exported-key"
    assert credentials.api_secret == "exported-secret"
    assert credentials.is_configured


def test_load_binance_usdm_credentials_honors_dotenv_disabled(
    tmp_path, monkeypatch
) -> None:
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text(
        "BINANCE_USDM_API_KEY=dotenv-key\nBINANCE_USDM_API_SECRET=dotenv-secret\n"
    )
    monkeypatch.delenv("BINANCE_USDM_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_USDM_API_SECRET", raising=False)
    monkeypatch.setenv("PYTHON_DOTENV_DISABLED", "1")

    credentials = load_binance_usdm_credentials(dotenv_path)

    assert credentials.api_key is None
    assert credentials.api_secret is None
    assert not credentials.is_configured


def test_load_allow_mainnet_trading_defaults_false_and_reads_dotenv(
    tmp_path, monkeypatch
) -> None:
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text("ALLOW_MAINNET_TRADING=true\n")
    monkeypatch.delenv("ALLOW_MAINNET_TRADING", raising=False)

    assert load_allow_mainnet_trading(tmp_path / "missing.env") is False
    assert load_allow_mainnet_trading(dotenv_path) is True


def test_load_allow_mainnet_trading_prefers_exported_value(
    tmp_path, monkeypatch
) -> None:
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text("ALLOW_MAINNET_TRADING=false\n")
    monkeypatch.setenv("ALLOW_MAINNET_TRADING", "true")

    assert load_allow_mainnet_trading(dotenv_path) is True
