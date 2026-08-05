from __future__ import annotations

import asyncio


class WorkerWakeGate:
    """Cross-request signal that interrupts a worker idle sleep."""

    def __init__(self) -> None:
        self._event = asyncio.Event()

    def signal(self) -> None:
        self._event.set()

    async def wait(self, idle_seconds: float) -> bool:
        try:
            await asyncio.wait_for(self._event.wait(), timeout=max(0.0, idle_seconds))
        except TimeoutError:
            return False
        self._event.clear()
        return True
