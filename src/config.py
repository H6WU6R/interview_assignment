from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from execution.models import Environment


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

    @property
    def can_trade_mainnet(self) -> bool:
        return self.environment == Environment.MAINNET and self.allow_mainnet_trading


def load_settings(path: Path) -> Settings:
    payload = yaml.safe_load(path.read_text()) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"settings file must contain a mapping: {path}")

    settings_fields = Settings.__dataclass_fields__.keys()
    values: dict[str, Any] = {key: payload[key] for key in settings_fields if key in payload}
    return Settings(**values)
