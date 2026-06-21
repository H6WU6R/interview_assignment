import asyncio

from execution.events import ExecutionEventActor


async def test_events_for_one_execution_are_serialized() -> None:
    actor = ExecutionEventActor(execution_id="exec_test")
    seen: list[int] = []

    async def handler(value: int) -> None:
        await asyncio.sleep(0)
        seen.append(value)

    await asyncio.gather(
        actor.apply(lambda: handler(1)),
        actor.apply(lambda: handler(2)),
        actor.apply(lambda: handler(3)),
    )

    assert seen == [1, 2, 3]
