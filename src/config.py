from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any

from dotenv import dotenv_values
import yaml

from execution.models import Environment


DEFAULT_DOTENV_PATH = Path(__file__).resolve().parents[1] / ".env"


@dataclass(frozen=True)
class Settings:
    environment: Environment = Environment.SIMULATION
    allow_mainnet_trading: bool = False
    stale_market_data_ms: int = 1500
    recv_window_ms: int = 5000
    binance_api_key: str | None = None
    binance_api_secret: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.environment, Environment):
            object.__setattr__(self, "environment", Environment(str(self.environment)))
        object.__setattr__(
            self,
            "allow_mainnet_trading",
            _parse_bool(self.allow_mainnet_trading, field_name="allow_mainnet_trading"),
        )

    @property
    def can_trade_mainnet(self) -> bool:
        return self.environment == Environment.MAINNET and self.allow_mainnet_trading is True


@dataclass(frozen=True)
class BinanceUsdmCredentials:
    api_key: str | None
    api_secret: str | None

    @property
    def is_configured(self) -> bool:
        return bool(self.api_key and self.api_secret)


def load_binance_usdm_credentials(dotenv_path: Path | str | None = None) -> BinanceUsdmCredentials:
    dotenv_credentials: dict[str, str | None] = {}
    if not _dotenv_disabled():
        dotenv_credentials = dotenv_values(dotenv_path or DEFAULT_DOTENV_PATH)
    return BinanceUsdmCredentials(
        api_key=_credential_value("BINANCE_USDM_API_KEY", dotenv_credentials),
        api_secret=_credential_value("BINANCE_USDM_API_SECRET", dotenv_credentials),
    )


def load_settings(path: Path) -> Settings:
    payload = yaml.safe_load(path.read_text()) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"settings file must contain a mapping: {path}")

    settings_fields = Settings.__dataclass_fields__.keys()
    values: dict[str, Any] = {key: payload[key] for key in settings_fields if key in payload}
    return Settings(**values)


def _parse_bool(value: Any, *, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y", "on"}:
            return True
        if normalized in {"false", "0", "no", "n", "off"}:
            return False
    raise ValueError(f"{field_name} must be a boolean or boolean string")


def _credential_value(name: str, dotenv_credentials: dict[str, str | None]) -> str | None:
    if name in os.environ:
        return os.environ[name]
    return dotenv_credentials.get(name)


def _dotenv_disabled() -> bool:
    return os.getenv("PYTHON_DOTENV_DISABLED", "").casefold() in {"1", "true", "t", "yes", "y"}
