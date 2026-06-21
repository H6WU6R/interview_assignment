from __future__ import annotations

import asyncio

from execution.models import Algorithm
from testnet_runner import run


async def main() -> None:
    print("BINANCE TESTNET RUNNER: TWAP", flush=True)
    await run(Algorithm.TWAP)


if __name__ == "__main__":
    asyncio.run(main())
