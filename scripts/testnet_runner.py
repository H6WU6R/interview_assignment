from __future__ import annotations

import argparse
import asyncio
from contextlib import suppress
from dataclasses import asdict, is_dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping

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
    user_task: asyncio.Task[Any] | None = None
    try:
        snapshot, market_task = await _start_market_stream(adapter, timeout_seconds=args.market_timeout_seconds)
        events.append(_runtime_event(adapter, "market_snapshot", snapshot=_jsonable(snapshot)))
        symbol_rules = await adapter.get_symbol_rules(symbol)
        symbol_rules_payload = _symbol_rules_payload(symbol, symbol_rules, adapter)
        events.append(_runtime_event(adapter, "symbol_rules_loaded", symbol_rules=symbol_rules_payload))
        active_execution: dict[str, str] = {}
        user_task = await _start_user_stream(
            adapter,
            service,
            events,
            active_execution,
            timeout_seconds=args.market_timeout_seconds,
        )

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
        active_execution["execution_id"] = execution.execution_id
        events.append(_runtime_event(adapter, "execution_created", execution=_record_summary(execution)))

        deadline = adapter.clock.monotonic() + args.max_runtime_seconds
        latest = execution
        while adapter.clock.monotonic() < deadline and latest.status not in {
            ExecutionStatus.COMPLETED,
            ExecutionStatus.PARTIALLY_COMPLETED,
            ExecutionStatus.EXPIRED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.FAILED,
        }:
            _raise_if_stream_task_failed(market_task, user_task)
            latest = await service.run_once(execution.execution_id)
            _raise_if_stream_task_failed(market_task, user_task)
            events.append(_runtime_event(adapter, "run_once", execution=_record_summary(latest)))
            if latest.status.is_terminal:
                break
            await asyncio.sleep(args.poll_interval_seconds)
            _raise_if_stream_task_failed(market_task, user_task)

        if not latest.status.is_terminal:
            _raise_if_stream_task_failed(market_task, user_task)
            latest = await service.cancel_execution(execution.execution_id)
            events.append(_runtime_event(adapter, "cancel_requested", execution=_record_summary(latest)))
            latest = await service.reconcile_execution(execution.execution_id)
            events.append(_runtime_event(adapter, "final_reconcile", execution=_record_summary(latest)))

        prefix = make_client_order_prefix(execution.execution_id)
        reconciliation = await adapter.reconcile_orders_and_fills(symbol, client_order_prefix=prefix)
        events.append(
            _runtime_event(
                adapter,
                "post_run_reconciliation",
                order_count=len(reconciliation.orders),
                fill_count=len(reconciliation.fills),
                warnings=list(reconciliation.warnings),
            )
        )

        artifact_dir = write_execution_artifacts(
            root=args.output_dir,
            execution_id=latest.execution_id,
            request_snapshot=_jsonable(request),
            log_events=events,
            summary=_record_summary(latest),
            child_orders=[_jsonable(child) for child in latest.child_orders],
            fills=[_jsonable(fill) for fill in reconciliation.fills],
            timeline=events,
            twap_slice_ledger=_twap_slice_ledger(latest),
            extra_json_artifacts={
                "symbol_rules.json": symbol_rules_payload,
                "evidence_manifest.json": _evidence_manifest(
                    request=request,
                    record=latest,
                    reconciliation=reconciliation,
                    events=events,
                    adapter=adapter,
                ),
            },
            extra_csv_artifacts={
                "reconciliation_orders.csv": [_jsonable(order) for order in reconciliation.orders],
            },
        )
        print(f"execution_id={latest.execution_id}")
        print(f"status={latest.status.value}")
        print(f"artifact_dir={artifact_dir}")
        return artifact_dir
    finally:
        await _stop_stream_tasks(user_task, market_task)


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
        await _stop_stream_task(task)
        raise


async def _stop_market_stream(task: asyncio.Task[Any]) -> None:
    await _stop_stream_task(task)


async def _start_user_stream(
    adapter: BinanceUsdmAdapter,
    service: ExecutionService,
    events: list[dict[str, Any]],
    active_execution: dict[str, str],
    *,
    timeout_seconds: float,
) -> asyncio.Task[Any]:
    async def pump() -> None:
        async for event in adapter.stream_user_events():
            events.append(_runtime_event(adapter, "user_stream_event", user_event=_jsonable(event)))
            execution_id = active_execution.get("execution_id")
            if execution_id is None:
                continue
            parser = getattr(adapter, "reconciliation_from_user_event", None)
            apply_result = getattr(service, "apply_reconciliation_result", None)
            if not callable(parser) or not callable(apply_result):
                continue
            result = parser(event)
            if result is None or not _reconciliation_result_matches_execution(execution_id, result):
                continue
            updated = await apply_result(execution_id, result)
            events.append(_runtime_event(adapter, "user_stream_applied", execution=_record_summary(updated)))

    task = asyncio.create_task(pump())
    try:
        await asyncio.wait_for(_wait_for_stream_health(adapter, task), timeout=timeout_seconds)
        return task
    except BaseException:
        await _stop_stream_task(task)
        raise


async def _wait_for_stream_health(adapter: BinanceUsdmAdapter, task: asyncio.Task[Any]) -> None:
    while not task.done():
        if await adapter.health_check_streams():
            return
        await asyncio.sleep(0.01)
    await task


async def _stop_stream_task(task: asyncio.Task[Any]) -> None:
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


async def _stop_stream_tasks(*tasks: asyncio.Task[Any] | None) -> None:
    active_tasks = [task for task in tasks if task is not None]
    if not active_tasks:
        return

    results = await asyncio.gather(
        *(_stop_stream_task(task) for task in active_tasks),
        return_exceptions=True,
    )
    for result in results:
        if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
            raise result


def _raise_if_stream_task_failed(*tasks: asyncio.Task[Any] | None) -> None:
    for task in tasks:
        if task is None or not task.done():
            continue
        if task.cancelled():
            raise RuntimeError("stream task was cancelled unexpectedly")
        task.result()
        raise RuntimeError("stream task exited unexpectedly")


def _reconciliation_result_matches_execution(execution_id: str, result: Any) -> bool:
    prefix = make_client_order_prefix(execution_id)
    orders = getattr(result, "orders", [])
    fills = getattr(result, "fills", [])
    return any(getattr(order, "client_order_id", "").startswith(prefix) for order in orders) or any(
        getattr(fill, "client_order_id", "").startswith(prefix) for fill in fills
    )


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


def _runtime_event(adapter: Any, event_name: str, **payload: Any) -> dict[str, Any]:
    clock = getattr(adapter, "clock", None)
    event: dict[str, Any] = {"event": event_name}
    if clock is not None:
        event["utc_timestamp"] = clock.utc_now().isoformat()
        event["monotonic_time"] = Decimal(str(clock.monotonic()))
    event.update(payload)
    return event


def _record_summary(record: Any) -> dict[str, Any]:
    record_summary = getattr(record, "summary", None)
    summary = {
        "execution_id": record.execution_id,
        "status": record.status,
        "final_reason": record.final_reason,
        "exposure": _jsonable(record.exposure),
        "child_order_count": len(record.child_orders),
    }
    if record_summary is not None:
        summary["metrics"] = _jsonable(record_summary.metrics)
    return summary


def _twap_slice_ledger(record: Any) -> list[dict[str, Any]]:
    record_summary = getattr(record, "summary", None)
    if record_summary is None:
        return []
    return record_summary.metrics.get("twap_slice_ledger", [])


def _symbol_rules_payload(symbol: str, rules: Any, adapter: Any) -> dict[str, Any]:
    supported_time_in_force = getattr(rules, "supported_time_in_force", ())
    return {
        "symbol": symbol,
        "rules": {
            "symbol": getattr(rules, "symbol", symbol),
            "tick_size": _jsonable(getattr(rules, "tick_size", None)),
            "quantity_step": _jsonable(getattr(rules, "quantity_step", None)),
            "min_quantity": _jsonable(getattr(rules, "min_quantity", None)),
            "min_notional": _jsonable(getattr(rules, "min_notional", None)),
            "status": getattr(rules, "status", None),
            "supported_time_in_force": sorted(str(value) for value in supported_time_in_force),
        },
        "rate_limits": _jsonable(getattr(adapter, "rate_limits", {})),
    }


def _evidence_manifest(
    *,
    request: ExecutionRequest,
    record: Any,
    reconciliation: Any,
    events: list[dict[str, Any]],
    adapter: Any,
) -> dict[str, Any]:
    reconciliation_orders = list(getattr(reconciliation, "orders", []))
    reconciliation_fills = list(getattr(reconciliation, "fills", []))
    child_orders = list(getattr(record, "child_orders", []))
    client_order_ids = _unique_strings(
        [
            *(_order_client_id(order) for order in child_orders),
            *(_order_client_id(order) for order in reconciliation_orders),
            *(getattr(fill, "client_order_id", None) for fill in reconciliation_fills),
        ]
    )
    exchange_order_ids = _unique_strings(
        [
            *(_exchange_order_id(order) for order in child_orders),
            *(_exchange_order_id(order) for order in reconciliation_orders),
        ]
    )
    reconciled_exchange_order_ids = _unique_strings(
        _exchange_order_id(order) for order in reconciliation_orders
    )
    warnings = list(getattr(reconciliation, "warnings", []))
    return {
        "execution_id": getattr(record, "execution_id", None),
        "environment": _jsonable(request.environment),
        "symbol": request.symbol,
        "algorithm": _jsonable(request.algorithm),
        "final_status": _jsonable(getattr(record, "status", None)),
        "reconciled_order_count": len(reconciliation_orders),
        "reconciled_fill_count": len(reconciliation_fills),
        "client_order_ids": client_order_ids,
        "exchange_order_ids": exchange_order_ids,
        "exchange_order_id_count": len(exchange_order_ids),
        "reconciled_exchange_order_id_count": len(reconciled_exchange_order_ids),
        "exchange_order_evidence_status": (
            "reconciled_exchange_order_ids_observed"
            if reconciled_exchange_order_ids
            else "no_reconciled_exchange_order_ids"
        ),
        "has_private_user_stream_events": _has_event(events, "user_stream_event"),
        "has_user_stream_applied_events": _has_event(events, "user_stream_applied"),
        "warnings": warnings,
        "final_reconciliation_counts": {
            "order_count": len(reconciliation_orders),
            "fill_count": len(reconciliation_fills),
            "warning_count": len(warnings),
        },
        "rate_limits": _jsonable(getattr(adapter, "rate_limits", {})),
    }


def _order_client_id(order: Any) -> str | None:
    value = getattr(order, "client_order_id", None)
    return str(value) if value else None


def _exchange_order_id(order: Any) -> str | None:
    value = getattr(order, "exchange_order_id", None)
    return str(value) if value else None


def _unique_strings(values: Iterable[Any]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for value in values:
        if value is None:
            continue
        text = str(value)
        if text in seen:
            continue
        seen.add(text)
        unique.append(text)
    return unique


def _has_event(events: Iterable[Mapping[str, Any]], event_name: str) -> bool:
    return any(event.get("event") == event_name for event in events)
