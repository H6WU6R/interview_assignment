from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime


class Clock:
    def monotonic(self) -> float:
        raise NotImplementedError

    def utc_now(self) -> datetime:
        raise NotImplementedError


class SystemClock(Clock):
    def monotonic(self) -> float:
        return time.monotonic()

    def utc_now(self) -> datetime:
        return datetime.now(tz=UTC)


@dataclass
class ManualClock(Clock):
    current: float = 0.0

    def monotonic(self) -> float:
        return self.current

    def utc_now(self) -> datetime:
        return datetime.fromtimestamp(self.current, tz=UTC)

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("manual clock cannot advance by negative seconds")
        self.current += seconds
