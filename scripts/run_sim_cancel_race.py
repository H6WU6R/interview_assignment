from __future__ import annotations

import argparse
import asyncio
from decimal import Decimal
from pathlib import Path

from execution.ids import make_client_order_prefix
from execution.models import Algorithm

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
    parser = argparse.ArgumentParser(
        description="Run a deterministic simulator cancel/fill race demo."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/tmp/calais-sim-cancel-race"),
        help="Directory under which execution artifacts are written.",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    print("SIMULATOR DEMO: Cancel/Fill Race")
    clock, simulator, service = make_simulator_stack()
    await seed_market(clock, simulator)

    execution = await service.create_execution(make_request(Algorithm.CHASE))
    prefix = make_client_order_prefix(execution.execution_id)
    simulator.script_fill_during_cancel(prefix, Decimal("0.004"))
    events = [
        log_event(clock, execution, "execution_created", extra={"armed_prefix": prefix})
    ]

    execution = await service.run_once(execution.execution_id)
    events.append(
        log_event(
            clock,
            execution,
            "initial_child_submitted",
            child=execution.child_orders[-1],
        )
    )

    clock.advance(0.6)
    await seed_market(
        clock, simulator, bid=Decimal("50030.00"), ask=Decimal("50031.00")
    )
    execution = await service.run_once(execution.execution_id)
    events.append(
        log_event(
            clock,
            execution,
            "cancel_fill_race_repriced",
            child=execution.child_orders[-1],
            extra={"filled_during_cancel_quantity": Decimal("0.004")},
        )
    )
    race_snapshot = summary_snapshot(execution)

    replacement_child = execution.child_orders[-1]
    await simulator.push_fill(
        replacement_child.client_order_id,
        replacement_child.submitted_quantity,
        replacement_child.price,
    )
    execution = await service.reconcile_execution(execution.execution_id)
    events.append(
        log_event(
            clock,
            execution,
            "replacement_child_filled_terminal",
            child=execution.child_orders[-1],
        )
    )
    events.append(
        log_event(clock, execution, "result_summary", extra=summary_snapshot(execution))
    )

    artifact_dir = write_artifacts(
        args.output_dir,
        execution,
        log_events=events,
        fills=simulator._fills,
        extra_json_artifacts={"execution_snapshot.json": race_snapshot},
    )

    print(f"execution_id={execution.execution_id}")
    print(f"status={execution.status}")
    print(f"client_order_ids={client_order_ids(execution)}")
    print(f"confirmed_filled={execution.exposure.confirmed_filled_quantity}")
    print(f"reserved_exposure={execution.exposure.reserved_exposure}")
    print(f"artifact_dir={artifact_dir}")


if __name__ == "__main__":
    asyncio.run(main())
