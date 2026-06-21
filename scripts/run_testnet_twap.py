from __future__ import annotations

import os


def main() -> None:
    print("BINANCE TESTNET DEMO: TWAP", flush=True)
    if not os.getenv("BINANCE_USDM_API_KEY") or not os.getenv("BINANCE_USDM_API_SECRET"):
        raise SystemExit(
            "Missing BINANCE_USDM_API_KEY or BINANCE_USDM_API_SECRET. "
            "This script never falls back to simulation."
        )

    print(
        "Binance Testnet credentials detected. "
        "Task 17 will replace this guard with an explicit --confirm-send-orders evidence runner."
    )


if __name__ == "__main__":
    main()
