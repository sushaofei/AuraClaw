from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from auraclaw.control.orchestrator import ManagedOrchestrator
from auraclaw.projection.ports import TaskReader
from auraclaw.runtime.harness import AgentHarness
from auraclaw.session.ports import EventStore, OutboxRelayPort

logger = logging.getLogger(__name__)


class RuntimeWorker:
    """In-process worker shared by every resource configuration."""

    def __init__(
        self,
        *,
        event_store: EventStore,
        reader: TaskReader,
        relay: OutboxRelayPort,
        orchestrator: ManagedOrchestrator,
        harness: AgentHarness,
        poll_interval: float = 0.05,
    ) -> None:
        self._event_store = event_store
        self._reader = reader
        self._relay = relay
        self._orchestrator = orchestrator
        self._harness = harness
        self._poll_interval = max(0.01, poll_interval)
        self._stopped = asyncio.Event()

    async def run_once(self) -> int:
        events = await self._event_store.load_all()
        session_keys = {(event.tenant_id, event.session_id) for event in events}
        tasks = []
        for tenant_id, session_id in session_keys:
            task = await self._reader.get_task(tenant_id, session_id)
            if task is not None and task.get("status") in {"pending", "runnable"}:
                tasks.append(task)
        await self._orchestrator.watch(tasks)

        completed = 0
        while assignment := await self._orchestrator.schedule_once():
            await self._harness.execute(assignment)
            await self._relay.relay_once()
            completed += 1
        return completed

    async def run(self) -> None:
        while not self._stopped.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("runtime worker iteration failed")
            with suppress(TimeoutError):
                await asyncio.wait_for(self._stopped.wait(), timeout=self._poll_interval)

    async def stop(self) -> None:
        self._stopped.set()
