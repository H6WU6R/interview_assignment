from __future__ import annotations

import argparse
import asyncio
from contextlib import suppress
from dataclasses import asdict, is_dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from config import Settings, load_binance_usdm_credentials
from exchanges.binance_usdm import BinanceUsdmAdapter
from execution.ids import make_client_order_prefix
from execution.models import (
    Algorithm,
    DeadlinePolicy,
    ExecutionParameters,
    ExecutionRequest,
    ExecutionStatus,
)
from execution.service import ExecutionService
from observability.artifacts import write_execution_artifacts
from observability.logging import to_jsonable


def parse_args(algorithm: Algorithm) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=f"Run a real Binance USD-M testnet {algorithm.value} execution.")
    parser.add_argument("--symbol", default="BTCUSDT", help="USD-M symbol to trade, default BTCUSDT.")
    parser.add_argument("--confirm-send-orders", action="store_true", help="Required before live testnet order sends.")
    parser.add_argument("--target-position", help="Explicit Decimal target position, e.g. 0.001.")
    parser.add_argument("--target-price-lower", help="Explicit Decimal lower price bound.")
    parser.add_argument("--target-price-upper", help="Explicit Decimal upper price bound.")
    parser.add_argument("--duration-seconds", type=int, default=60, help="Execution target duration in seconds.")
    parser.add_argument("--number-of-slices", type=int, default=5, help="TWAP slice count.")
    parser.add_argument("--max-runtime-seconds", type=float, default=30.0, help="Maximum runner runtime.")
    parser.add_argument("--poll-interval-seconds", type=float, default=1.0, help="Delay between engine ticks.")
    parser.add_argument("--market-timeout-seconds", type=float, default=10.0, help="Fresh market snapshot timeout.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/tmp/calais-binance-testnet"),
        help="Directory under which execution artifacts are written.",
    )
    return parser.parse_args()


async def run(algorithm: Algorithm) -> Path:
    args = parse_args(algorithm)
    credentials = load_binance_usdm_credentials()
    if not credentials.is_configured:
        raise SystemExit(
            "Missing BINANCE_USDM_API_KEY or BINANCE_USDM_API_SECRET. "
            "This script never falls back to simulation."
        )
    if not args.confirm_send_orders:
        raise SystemExit("Refusing to send Binance testnet orders without --confirm-send-orders.")

    target_position = _required_decimal(args.target_position, "--target-position")
    lower = _required_decimal(args.target_price_lower, "--target-price-lower")
    upper = _required_decimal(args.target_price_upper, "--target-price-upper")
    symbol = normalize_symbol(args.symbol)

    adapter = BinanceUsdmAdapter(
        Settings(
            environment="testnet",
            binance_api_key=credentials.api_key,
            binance_api_secret=credentials.api_secret,
        )
    )
    adapter.set_market_stream_symbol(symbol)
    service = ExecutionService(adapter, clock=adapter.clock)
    events: list[dict[str, Any]] = []

    market_task: asyncio.Task[Any] | None = None
    try:
        snapshot, market_task = await _start_market_stream(adapter, timeout_seconds=args.market_timeout_seconds)
        events.append({"event": "market_snapshot", "snapshot": _jsonable(snapshot)})

        request = ExecutionRequest(
            environment=adapter.settings.environment,
            symbol=symbol,
            algorithm=algorithm,
            target_position=target_position,
            target_price_lower=lower,
            target_price_upper=upper,
            target_duration_seconds=args.duration_seconds,
            deadline_policy=DeadlinePolicy.AGGRESSIVE_WITHIN_RANGE,
            parameters=ExecutionParameters(number_of_slices=args.number_of_slices),
        )
        execution = await service.create_execution(request)
        events.append({"event": "execution_created", "execution": _record_summary(execution)})

        deadline = adapter.clock.monotonic() + args.max_runtime_seconds
        latest = execution
        while adapter.clock.monotonic() < deadline and latest.status not in {
            ExecutionStatus.COMPLETED,
            ExecutionStatus.PARTIALLY_COMPLETED,
            ExecutionStatus.EXPIRED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.FAILED,
        }:
            latest = await service.run_once(execution.execution_id)
            events.append({"event": "run_once", "execution": _record_summary(latest)})
            if latest.status.is_terminal:
                break
            await asyncio.sleep(args.poll_interval_seconds)

        if not latest.status.is_terminal:
            latest = await service.cancel_execution(execution.execution_id)
            events.append({"event": "cancel_requested", "execution": _record_summary(latest)})
            latest = await service.reconcile_execution(execution.execution_id)
            events.append({"event": "final_reconcile", "execution": _record_summary(latest)})

        prefix = make_client_order_prefix(execution.execution_id)
        reconciliation = await adapter.reconcile_orders_and_fills(symbol, client_order_prefix=prefix)

        artifact_dir = write_execution_artifacts(
            root=args.output_dir,
            execution_id=latest.execution_id,
            request_snapshot=_jsonable(request),
            log_events=events,
            summary=_record_summary(latest),
            child_orders=[_jsonable(child) for child in latest.child_orders],
            fills=[_jsonable(fill) for fill in reconciliation.fills],
            timeline=events,
        )
        print(f"execution_id={latest.execution_id}")
        print(f"status={latest.status.value}")
        print(f"artifact_dir={artifact_dir}")
        return artifact_dir
    finally:
        if market_task is not None:
            await _stop_market_stream(market_task)


async def _start_market_stream(
    adapter: BinanceUsdmAdapter,
    *,
    timeout_seconds: float,
) -> tuple[Any, asyncio.Task[Any]]:
    loop = asyncio.get_running_loop()
    first_snapshot: asyncio.Future[Any] = loop.create_future()

    async def pump() -> None:
        try:
            async for snapshot in adapter.stream_market_data():
                if not first_snapshot.done():
                    first_snapshot.set_result(snapshot)
        except Exception as exc:
            if not first_snapshot.done():
                first_snapshot.set_exception(exc)
            raise

    task = asyncio.create_task(pump())
    try:
        return await asyncio.wait_for(first_snapshot, timeout=timeout_seconds), task
    except BaseException:
        await _stop_market_stream(task)
        raise


async def _stop_market_stream(task: asyncio.Task[Any]) -> None:
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


def normalize_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if not normalized:
        raise SystemExit("--symbol must not be empty.")
    return normalized


def _required_decimal(value: str | None, label: str) -> Decimal:
    if value is None:
        raise SystemExit(f"{label} is required and must be an explicit Decimal value.")
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise SystemExit(f"{label} must be a valid Decimal value.") from exc


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return to_jsonable(asdict(value))
    return to_jsonable(value)


def _record_summary(record: Any) -> dict[str, Any]:
    return {
        "execution_id": record.execution_id,
        "status": record.status,
        "final_reason": record.final_reason,
        "exposure": _jsonable(record.exposure),
        "child_order_count": len(record.child_orders),
    }
