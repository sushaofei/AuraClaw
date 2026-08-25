from __future__ import annotations

from typing import Any

from auraclaw.contracts.commands import CommandContext
from auraclaw.gateways.query.waiter import TaskResultWaiter, WaitedResult
from auraclaw.gateways.task.commands import TaskCommandGateway


class SyncInvocationGateway:
    """Create via Task Gateway, then wait on the Result projection."""

    def __init__(
        self,
        commands: TaskCommandGateway,
        waiter: TaskResultWaiter,
    ) -> None:
        self._commands = commands
        self._waiter = waiter

    async def invoke(
        self,
        *,
        goal: str,
        context: CommandContext,
        timeout_seconds: int | None = None,
    ) -> tuple[dict[str, Any], WaitedResult]:
        accepted = await self._commands.create_task(
            goal=goal,
            context=context,
            source="chat",
        )
        session_id = str(accepted["session_id"])
        waited = await self._waiter.wait(
            context.tenant_id,
            session_id,
            timeout_seconds=self._waiter.clamp_timeout(timeout_seconds),
        )
        return accepted, waited
