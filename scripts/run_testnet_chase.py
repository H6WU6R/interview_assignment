from __future__ import annotations

import asyncio

from execution.models import Algorithm
from testnet_runner import run


async def main() -> None:
    print("BINANCE TESTNET RUNNER: CHASE", flush=True)
    await run(Algorithm.CHASE)


if __name__ == "__main__":
    asyncio.run(main())
