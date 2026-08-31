from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable, Sequence
from typing import TypeVar, cast

_Item = TypeVar("_Item")
_Result = TypeVar("_Result")


async def bounded_map(
    items: Sequence[_Item],
    callback: Callable[[_Item], Awaitable[_Result]],
    *,
    max_concurrent: int,
) -> tuple[_Result, ...]:
    """Map with a fixed worker count while preserving input result order."""
    if max_concurrent < 1:
        raise ValueError("max_concurrent must be positive")
    if not items:
        return ()
    queue = asyncio.Queue[tuple[int, _Item]]()
    for indexed in enumerate(items):
        queue.put_nowait(indexed)
    results: list[_Result | None] = [None] * len(items)

    async def consume() -> None:
        while True:
            try:
                index, item = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                results[index] = await callback(item)
            finally:
                queue.task_done()

    tasks = [
        asyncio.create_task(consume())
        for _ in range(min(max_concurrent, len(items)))
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return tuple(cast(_Result, result) for result in results)


async def bounded_partition_map(
    items: Sequence[_Item],
    callback: Callable[[_Item], Awaitable[_Result]],
    *,
    max_concurrent: int,
    partitions: tuple[tuple[Callable[[_Item], object], int], ...],
) -> tuple[_Result, ...]:
    """Bound concurrency globally and independently for each partition key."""
    if any(limit < 1 or limit > max_concurrent for _, limit in partitions):
        raise ValueError("partition limits must be within global concurrency")
    if not partitions:
        return await bounded_map(items, callback, max_concurrent=max_concurrent)
    pending = list(enumerate(items))
    active = [defaultdict[object, int](int) for _ in partitions]
    results: list[_Result | None] = [None] * len(items)
    condition = asyncio.Condition()

    async def acquire() -> tuple[int, _Item, tuple[object, ...]] | None:
        async with condition:
            while pending:
                for position, (index, item) in enumerate(pending):
                    keys = tuple(selector(item) for selector, _ in partitions)
                    if all(
                        active[partition][key] < partitions[partition][1]
                        for partition, key in enumerate(keys)
                    ):
                        pending.pop(position)
                        for partition, key in enumerate(keys):
                            active[partition][key] += 1
                        return index, item, keys
                await condition.wait()
            return None

    async def release(keys: tuple[object, ...]) -> None:
        async with condition:
            for partition, key in enumerate(keys):
                active[partition][key] -= 1
                if active[partition][key] == 0:
                    del active[partition][key]
            condition.notify_all()

    async def consume() -> None:
        while (claimed := await acquire()) is not None:
            index, item, keys = claimed
            try:
                results[index] = await callback(item)
            finally:
                await release(keys)

    tasks = [
        asyncio.create_task(consume())
        for _ in range(min(max_concurrent, len(items)))
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return tuple(cast(_Result, result) for result in results)
