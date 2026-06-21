from __future__ import annotations

import asyncio

from execution.models import Algorithm

from _sim_demo_common import (
    client_order_ids,
    make_request,
    make_simulator_stack,
    seed_market,
)


async def main() -> None:
    print("SIMULATOR DEMO: TWAP")
    clock, simulator, service = make_simulator_stack()
    await seed_market(clock, simulator)

    execution = await service.create_execution(make_request(Algorithm.TWAP, duration=100))
    print(f"execution_id={execution.execution_id}")
    print("schedule=absolute-time TWAP target over 100 seconds")

    clock.advance(10)
    await seed_market(clock, simulator)
    execution = await service.run_once(execution.execution_id)

    print(f"status={execution.status}")
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
        f"reserved={execution.exposure.reserved_exposure}"
    )


if __name__ == "__main__":
    asyncio.run(main())
