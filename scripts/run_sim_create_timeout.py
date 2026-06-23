from __future__ import annotations

import argparse
import asyncio
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
        description="Run a deterministic simulator create-timeout demo."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/tmp/calais-sim-create-timeout"),
        help="Directory under which execution artifacts are written.",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    print("SIMULATOR DEMO: Create Timeout")
    clock, simulator, service = make_simulator_stack()
    await seed_market(clock, simulator)

    execution = await service.create_execution(make_request(Algorithm.CHASE))
    prefix = make_client_order_prefix(execution.execution_id)
    simulator.script_create_timeout(prefix)
    events = [
        log_event(clock, execution, "execution_created", extra={"armed_prefix": prefix})
    ]

    unknown = await service.run_once(execution.execution_id)
    events.append(
        log_event(
            clock, unknown, "create_timeout_unknown", child=unknown.child_orders[-1]
        )
    )

    before_reconcile = await service.run_once(execution.execution_id)
    events.append(
        log_event(
            clock,
            before_reconcile,
            "run_before_reconcile_no_new_client_order_id",
            child=before_reconcile.child_orders[-1],
        )
    )

    reconciled = await service.reconcile_execution(execution.execution_id)
    events.append(
        log_event(
            clock,
            reconciled,
            "reconciled_original_open",
            child=reconciled.child_orders[-1],
        )
    )
    reconciled_snapshot = summary_snapshot(reconciled)

    recovered_child = reconciled.child_orders[-1]
    await simulator.push_fill(
        recovered_child.client_order_id,
        recovered_child.submitted_quantity,
        recovered_child.price,
    )
    completed = await service.reconcile_execution(reconciled.execution_id)
    events.append(
        log_event(
            clock,
            completed,
            "recovered_child_filled_terminal",
            child=completed.child_orders[-1],
        )
    )
    events.append(
        log_event(
            clock, completed, "result_summary", extra=summary_snapshot(completed)
        )
    )

    print(f"execution_id={completed.execution_id}")
    print(f"status={completed.status}")
    print(f"client_order_ids={client_order_ids(completed)}")
    print(f"unknown_before_reconcile={unknown.exposure.unknown_order_quantity}")
    print(f"unknown_after_reconcile={reconciled.exposure.unknown_order_quantity}")
    print(f"live_open_after_reconcile={reconciled.exposure.live_open_quantity}")

    artifact_dir = write_artifacts(
        args.output_dir,
        completed,
        log_events=events,
        fills=simulator._fills,
        extra_json_artifacts={"execution_snapshot.json": reconciled_snapshot},
    )
    print(f"artifact_dir={artifact_dir}")


if __name__ == "__main__":
    asyncio.run(main())
