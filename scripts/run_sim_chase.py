from __future__ import annotations

import asyncio

from execution.models import Algorithm

from _sim_demo_common import (
    client_order_ids,
    make_request,
    make_simulator_stack,
    seed_market,
    summary_snapshot,
)


async def main() -> None:
    print("SIMULATOR DEMO: Chase")
    clock, simulator, service = make_simulator_stack()
    await seed_market(clock, simulator)

    execution = await service.create_execution(make_request(Algorithm.CHASE))
    execution = await service.run_once(execution.execution_id)

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


if __name__ == "__main__":
    asyncio.run(main())
