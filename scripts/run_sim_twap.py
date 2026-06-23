from __future__ import annotations

import argparse
import asyncio
from dataclasses import replace
from pathlib import Path

from execution.models import Algorithm, ChildOrderStatus, DeadlinePolicy, ExecutionParameters

from _sim_demo_common import (
    client_order_ids,
    log_event,
    make_request,
    make_simulator_stack,
    seed_market,
    summary_snapshot,
    write_artifacts,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a deterministic simulator TWAP demo.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/tmp/calais-sim-twap"),
        help="Directory under which execution artifacts are written.",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    print("SIMULATOR DEMO: TWAP")
    clock, simulator, service = make_simulator_stack()
    await seed_market(clock, simulator)

    expected_slices = 5
    request = replace(
        make_request(
            Algorithm.TWAP,
            duration=100,
            parameters=ExecutionParameters(number_of_slices=expected_slices),
        ),
        deadline_policy=DeadlinePolicy.AGGRESSIVE_WITHIN_RANGE,
    )
    execution = await service.create_execution(request)
    events = [
        log_event(
            clock,
            execution,
            "execution_created",
            extra={"schedule": "absolute-time TWAP target over 100 seconds in 5 slices"},
        )
    ]
    print(f"execution_id={execution.execution_id}")
    print("schedule=absolute-time TWAP target over 100 seconds in 5 slices")

    max_ticks = expected_slices + 2
    for _ in range(max_ticks):
        if execution.status.is_terminal:
            break
        clock.advance(20)
        await seed_market(clock, simulator)
        execution = await service.run_once(execution.execution_id)

        open_children = [
            child
            for child in execution.child_orders
            if child.status in {ChildOrderStatus.OPEN, ChildOrderStatus.PARTIALLY_FILLED}
        ]
        if not open_children:
            continue

        child = open_children[-1]
        events.append(log_event(clock, execution, "twap_order_submitted", child=child))
        fill = await simulator.push_fill(child.client_order_id, child.remaining_quantity, child.price)
        events.append(
            log_event(
                clock,
                execution,
                "simulator_fill",
                child=child,
                extra={"trade_id": fill.trade_id},
            )
        )
        execution = await service.reconcile_execution(execution.execution_id)
        events.append(log_event(clock, execution, "filled_reconciled", child=execution.child_orders[-1]))

    if not execution.status.is_terminal:
        raise RuntimeError(
            f"TWAP demo did not complete within {max_ticks} ticks; "
            f"status={execution.status} child_order_count={len(execution.child_orders)}"
        )

    execution = await service.run_once(execution.execution_id)
    events.append(log_event(clock, execution, "result_summary", extra=summary_snapshot(execution)))

    print(f"status=ExecutionStatus.{execution.status.value}")
    print(f"client_order_ids={client_order_ids(execution)}")
    for child in execution.child_orders:
        print(
            "twap_order "
            f"id={child.child_order_id} clientOrderId={child.client_order_id} "
            f"status={child.status} qty={child.submitted_quantity} price={child.price}"
        )
    print(
        "schedule_summary="
        f"elapsed_seconds={clock.monotonic()} required_quantity={execution.required_quantity} "
        f"confirmed={execution.exposure.confirmed_filled_quantity} "
        f"reserved={execution.exposure.reserved_exposure} "
        f"child_order_count={len(execution.child_orders)}"
    )
    print(f"summary={summary_snapshot(execution)}")

    artifact_dir = write_artifacts(
        args.output_dir,
        execution,
        log_events=events,
        fills=simulator._fills,
    )
    print(f"artifact_dir={artifact_dir}")


if __name__ == "__main__":
    asyncio.run(main())
