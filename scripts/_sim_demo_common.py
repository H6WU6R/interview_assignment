from __future__ import annotations

from collections.abc import Iterable, Mapping
from decimal import Decimal
from pathlib import Path
from typing import Any

from exchanges.simulator import DeterministicSimulator
from execution.clock import ManualClock
from execution.engine import ExecutionRecord
from execution.models import (
    Algorithm,
    ChildOrder,
    DeadlinePolicy,
    Environment,
    ExecutionParameters,
    ExecutionRequest,
    Fill,
)
from execution.service import ExecutionService
from observability.artifacts import write_execution_artifacts


SYMBOL = "BTCUSDT"
DEFAULT_BID = Decimal("50000.00")
DEFAULT_ASK = Decimal("50001.00")


def make_simulator_stack(
    *,
    position: Decimal = Decimal("0"),
    bid: Decimal = DEFAULT_BID,
    ask: Decimal = DEFAULT_ASK,
) -> tuple[ManualClock, DeterministicSimulator, ExecutionService]:
    clock = ManualClock()
    simulator = DeterministicSimulator(clock=clock, position=position)
    service = ExecutionService(simulator, clock=clock)
    return clock, simulator, service


async def seed_market(
    clock: ManualClock,
    simulator: DeterministicSimulator,
    *,
    bid: Decimal = DEFAULT_BID,
    ask: Decimal = DEFAULT_ASK,
) -> None:
    await simulator.push_market_data(
        SYMBOL,
        bid,
        ask,
        exchange_event_time=int(clock.monotonic() * 1000),
    )


def make_request(
    algorithm: Algorithm,
    *,
    target_position: Decimal = Decimal("0.010"),
    lower: Decimal = Decimal("49000"),
    upper: Decimal = Decimal("51000"),
    duration: int = 100,
    parameters: ExecutionParameters | None = None,
) -> ExecutionRequest:
    return ExecutionRequest(
        environment=Environment.SIMULATION,
        symbol=SYMBOL,
        algorithm=algorithm,
        target_position=target_position,
        target_price_lower=lower,
        target_price_upper=upper,
        target_duration_seconds=duration,
        deadline_policy=DeadlinePolicy.CANCEL_REMAINDER,
        parameters=parameters or ExecutionParameters(),
    )


def request_snapshot(record: ExecutionRecord) -> dict[str, Any]:
    parameters = record.request.parameters
    return {
        "execution_id": record.execution_id,
        "environment": record.request.environment,
        "symbol": record.request.symbol,
        "algorithm": record.request.algorithm,
        "side": record.side,
        "target_position": record.request.target_position,
        "target_price_range": {
            "lower": record.request.target_price_lower,
            "upper": record.request.target_price_upper,
        },
        "duration_seconds": record.request.target_duration_seconds,
        "deadline_policy": record.request.deadline_policy,
        "parameters": {
            "reprice_threshold_bps": parameters.reprice_threshold_bps,
            "minimum_reprice_interval_ms": parameters.minimum_reprice_interval_ms,
            "number_of_slices": parameters.number_of_slices,
            "child_order_timeout_seconds": parameters.child_order_timeout_seconds,
            "repricing_mode": parameters.repricing_mode,
        },
    }


def exposure_snapshot(record: ExecutionRecord) -> dict[str, Any]:
    exposure = record.exposure
    return {
        "confirmed_filled_quantity": exposure.confirmed_filled_quantity,
        "live_open_quantity": exposure.live_open_quantity,
        "pending_submit_quantity": exposure.pending_submit_quantity,
        "pending_cancel_quantity": exposure.pending_cancel_quantity,
        "unknown_order_quantity": exposure.unknown_order_quantity,
        "reserved_exposure": exposure.reserved_exposure,
    }


def summary_snapshot(record: ExecutionRecord) -> dict[str, Any]:
    return {
        "execution_id": record.execution_id,
        "final_status": record.status,
        "final_reason": record.final_reason or "",
        "required_quantity": record.required_quantity,
        "side": record.side,
        "exposure": exposure_snapshot(record),
        "child_order_count": len(record.child_orders),
        "client_order_ids": [child.client_order_id for child in record.child_orders],
    }


def child_order_rows(children: Iterable[ChildOrder]) -> list[dict[str, Any]]:
    return [
        {
            "child_order_id": child.child_order_id,
            "client_order_id": child.client_order_id,
            "exchange_order_id": child.exchange_order_id or "",
            "status": child.status,
            "submitted_quantity": child.submitted_quantity,
            "filled_quantity": child.confirmed_filled_quantity,
            "remaining_quantity": child.remaining_quantity,
            "price": child.price,
            "terminal_reason": child.terminal_reason or "",
        }
        for child in children
    ]


def fill_rows(fills: Iterable[Fill]) -> list[dict[str, Any]]:
    return [
        {
            "client_order_id": fill.client_order_id,
            "trade_id": fill.trade_id or "",
            "cumulative_filled_quantity": fill.cumulative_filled_quantity,
            "last_filled_quantity": fill.last_filled_quantity,
            "last_fill_price": fill.last_fill_price,
            "event_time_ms": fill.event_time_ms,
            "transaction_time_ms": fill.transaction_time_ms,
        }
        for fill in fills
    ]


def log_event(
    clock: ManualClock,
    record: ExecutionRecord,
    event: str,
    *,
    child: ChildOrder | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "event": event,
        "execution_id": record.execution_id,
        "status": record.status,
        "monotonic_time": Decimal(str(clock.monotonic())),
        "utc_timestamp": clock.utc_now(),
        "required_quantity": record.required_quantity,
        "final_reason": record.final_reason or "",
        "exposure": exposure_snapshot(record),
    }
    if child is not None:
        payload.update(
            {
                "child_order_id": child.child_order_id,
                "client_order_id": child.client_order_id,
                "child_status": child.status,
                "submitted_quantity": child.submitted_quantity,
                "filled_quantity": child.confirmed_filled_quantity,
                "remaining_quantity": child.remaining_quantity,
                "price": child.price,
            }
        )
    if extra:
        payload.update(dict(extra))
    return payload


def timeline_rows(events: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in events:
        row = dict(event)
        row.pop("exposure", None)
        rows.append(row)
    return rows


def write_artifacts(
    output_root: Path,
    record: ExecutionRecord,
    *,
    log_events: Iterable[Mapping[str, Any]],
    fills: Iterable[Fill],
) -> Path:
    log_events = list(log_events)
    return write_execution_artifacts(
        root=output_root,
        execution_id=record.execution_id,
        request_snapshot=request_snapshot(record),
        log_events=log_events,
        summary=summary_snapshot(record),
        child_orders=child_order_rows(record.child_orders),
        fills=fill_rows(fills),
        timeline=timeline_rows(log_events),
    )


def client_order_ids(record: ExecutionRecord) -> list[str]:
    return [child.client_order_id for child in record.child_orders]
