"""Remote assignment execution, lease renewal and failure disposition."""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import time
from datetime import UTC, datetime, timedelta

from auraclaw.config import Settings
from auraclaw.contracts.errors import FencingTokenError, LeaseConflictError
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.infrastructure.clients.runtime import (
    RemoteRuntimeControlClient,
)
from auraclaw.runtime.harness import AgentHarness

logger = logging.getLogger(__name__)


class RemoteRuntimeWorker:
    def __init__(
        self,
        control: RemoteRuntimeControlClient,
        harness: AgentHarness,
        *,
        heartbeat_interval: timedelta | None = None,
    ) -> None:
        self._control = control
        self._harness = harness
        self._registered = False
        # Must stay below recover_expired's stale-heartbeat window (30s), or a
        # healthy long model call can be mistaken for a dead runtime.
        self._heartbeat_interval = heartbeat_interval or timedelta(seconds=10)
        self._last_heartbeat_at = 0.0

    async def tick(self) -> int:
        now = time.monotonic()
        if not self._registered:
            await self._control.register()
            self._registered = True
            self._last_heartbeat_at = now
        elif now - self._last_heartbeat_at >= self._heartbeat_interval.total_seconds():
            await self._control.heartbeat()
            self._last_heartbeat_at = time.monotonic()
        assignments = await self._control.claim(
            limit=max(1, int(getattr(self._control, "capacity", 1)))
        )
        if assignments:
            await asyncio.gather(
                *(self._run_assignment(assignment) for assignment in assignments)
            )
        return len(assignments)

    async def _run_assignment(self, assignment: RuntimeAssignment) -> None:
        task_id = f"{assignment.tenant_id}:{assignment.session_id}:{assignment.run_id}"
        try:
            await self._execute_with_heartbeat(assignment)
        except (FencingTokenError, LeaseConflictError):
            logger.warning(
                "runtime lost lease for %s; abandoning stale assignment",
                task_id,
                exc_info=True,
            )
            try:
                await self._control.abandon_assignment(
                    task_id,
                    runtime_id=assignment.runtime_id,
                    lease_id=assignment.lease_id,
                    fencing_token=assignment.fencing_token,
                )
            except Exception:
                logger.exception("failed to abandon stale assignment %s", task_id)
            return
        except Exception as exc:
            try:
                if await self._harness.record_failure(assignment, exc):
                    return
            except Exception:
                logger.exception(
                    "failed to record run failure for %s; leaving assignment running",
                    task_id,
                )
                raise
            try:
                await self._control.finish_assignment(task_id, "failed")
            except Exception:
                logger.exception(
                    "failed to disposition assignment %s after recorded failure", task_id
                )
            raise

    async def _execute_with_heartbeat(self, assignment: RuntimeAssignment) -> None:
        stop = asyncio.Event()
        execute = asyncio.create_task(
            self._harness.execute(assignment),
            name=f"runtime-harness-{assignment.run_id}",
        )

        async def keep_alive() -> None:
            failures = 0
            while not stop.is_set():
                delay = self._heartbeat_interval.total_seconds()
                try:
                    renew = getattr(self._control, "renew_assignment", None)
                    if callable(renew):
                        await renew(assignment)
                    else:
                        await self._control.heartbeat()
                    self._last_heartbeat_at = time.monotonic()
                    failures = 0
                except Exception as exc:
                    failures += 1
                    retry_base = min(
                        self._heartbeat_interval.total_seconds(), 1.0
                    )
                    delay = min(
                        self._heartbeat_interval.total_seconds(),
                        retry_base * (2 ** (failures - 1)),
                    )
                    # Transient control-plane blips must not stop keep-alive;
                    # exiting here leaves last_heartbeat_at stale and
                    # recover_expired can reclaim a still-running long call.
                    logger.warning(
                        "runtime heartbeat failed during execute for session=%s run=%s; will retry",
                        assignment.session_id,
                        assignment.run_id,
                        exc_info=True,
                    )
                    expires_at = assignment.execution_claim_expires_at
                    if expires_at is not None and expires_at <= datetime.now(UTC):
                        execute.cancel()
                        raise LeaseConflictError(
                            "execution claim renewal safety window elapsed"
                        ) from exc
                try:
                    await asyncio.wait_for(
                        stop.wait(),
                        timeout=delay,
                    )
                    return
                except TimeoutError:
                    continue

        heartbeats = asyncio.create_task(keep_alive(), name="remote-runtime-heartbeat")
        try:
            done, _ = await asyncio.wait(
                {execute, heartbeats}, return_when=asyncio.FIRST_COMPLETED
            )
            if heartbeats in done:
                await heartbeats
            await execute
        finally:
            stop.set()
            if not heartbeats.done():
                await heartbeats


def runtime_instance_identity(settings: Settings) -> tuple[str, str]:
    hostname = (
        os.getenv("AURACLAW_RUNTIME_INSTANCE_UID")
        or os.getenv("POD_UID")
        or socket.gethostname()
    )
    if settings.deployment_profile == "production":
        runtime_id = settings.runtime_id or f"runtime-{hostname}"
        node_id = (
            hostname
            if settings.runtime_node_id in {None, "local"}
            else settings.runtime_node_id or hostname
        )
        return runtime_id, node_id
    return settings.runtime_id or "runtime-local-1", settings.runtime_node_id or "local"
