from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

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
    parser = argparse.ArgumentParser(description="Run a deterministic simulator Chase demo.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/tmp/calais-sim-chase"),
        help="Directory under which execution artifacts are written.",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    print("SIMULATOR DEMO: Chase")
    clock, simulator, service = make_simulator_stack()
    await seed_market(clock, simulator)

    execution = await service.create_execution(make_request(Algorithm.CHASE))
    events = [log_event(clock, execution, "execution_created")]
    execution = await service.run_once(execution.execution_id)
    events.append(log_event(clock, execution, "chase_order_submitted", child=execution.child_orders[-1]))
    events.append(log_event(clock, execution, "result_summary", extra=summary_snapshot(execution)))

    print(f"execution_id={execution.execution_id}")
    print(f"status={execution.status}")
    print(f"client_order_ids={client_order_ids(execution)}")
    for child in execution.child_orders:
        print(
            "child_order "
            f"id={child.child_order_id} clientOrderId={child.client_order_id} "
            f"status={child.status} qty={child.submitted_quantity} price={child.price}"
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
