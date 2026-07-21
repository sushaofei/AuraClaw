from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from datetime import timedelta

from auraclaw.contracts.errors import FencingTokenError, LeaseConflictError
from auraclaw.control.orchestrator import ManagedOrchestrator
from auraclaw.control.ports import RuntimeAssignment
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
        heartbeat_interval: timedelta | None = None,
    ) -> None:
        self._event_store = event_store
        self._reader = reader
        self._relay = relay
        self._orchestrator = orchestrator
        self._harness = harness
        self._poll_interval = max(0.01, poll_interval)
        self._heartbeat_interval = heartbeat_interval or max(
            timedelta(seconds=1),
            self._orchestrator.lease_ttl / 3,
        )
        self._stopped = asyncio.Event()

    async def run_once(self) -> int:
        recovered = await self._orchestrator.recover()
        if recovered:
            logger.info("recovered %s expired runtime assignment(s)", recovered)

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
            try:
                await self._execute_with_heartbeat(assignment)
                await self._relay.relay_once()
                completed += 1
            except (FencingTokenError, LeaseConflictError):
                task_id = ManagedOrchestrator.task_id(
                    assignment.tenant_id, assignment.session_id, assignment.run_id
                )
                logger.exception(
                    "runtime lost lease for %s; rescheduling assignment", task_id
                )
                await self._orchestrator.release_lost(assignment)
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

    async def _execute_with_heartbeat(self, assignment: RuntimeAssignment) -> None:
        stop = asyncio.Event()

        async def keep_alive() -> None:
            while not stop.is_set():
                try:
                    await asyncio.wait_for(
                        stop.wait(), timeout=self._heartbeat_interval.total_seconds()
                    )
                    return
                except TimeoutError:
                    try:
                        await self._orchestrator.heartbeat(assignment)
                    except (FencingTokenError, LeaseConflictError):
                        logger.warning(
                            "heartbeat failed for session=%s run=%s",
                            assignment.session_id,
                            assignment.run_id,
                        )
                        return

        heartbeats = asyncio.create_task(keep_alive(), name="runtime-lease-heartbeat")
        try:
            await self._harness.execute(assignment)
        finally:
            stop.set()
            await heartbeats
