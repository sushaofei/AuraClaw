from typing import Any

from auraclaw.contracts.approval_mode import ApprovalMode, InteractionMode
from auraclaw.contracts.commands import CommandContext
from auraclaw.session.task_service import TaskService


class TaskCommandGateway:
    """Stable inbound command boundary for task and session mutations."""

    def __init__(self, service: TaskService) -> None:
        self._service = service

    async def create_task(
        self,
        *,
        goal: str,
        context: CommandContext,
        source: str = "chat",
        schedule_id: str | None = None,
        occurrence_id: str | None = None,
        interaction_mode: InteractionMode | None = None,
        approval_mode: ApprovalMode | None = None,
        read_refresh: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        return await self._service.create_task(
            goal=goal,
            context=context,
            source=source,
            schedule_id=schedule_id,
            occurrence_id=occurrence_id,
            interaction_mode=interaction_mode,
            approval_mode=approval_mode,
            read_refresh=read_refresh,
        )

    async def append_message(
        self, *, session_id: str, message: str, context: CommandContext
    ) -> dict[str, Any]:
        return await self._service.append_message(
            session_id=session_id, message=message, context=context
        )

    async def request_run(
        self,
        *,
        session_id: str,
        context: CommandContext,
        approval_mode: ApprovalMode | None = None,
        read_refresh: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        return await self._service.request_run(
            session_id=session_id,
            context=context,
            approval_mode=approval_mode,
            read_refresh=read_refresh,
        )

    async def cancel_task(
        self, *, session_id: str, reason: str, context: CommandContext
    ) -> dict[str, Any]:
        return await self._service.cancel_task(
            session_id=session_id, reason=reason, context=context
        )

    async def close_session(
        self, *, session_id: str, reason: str, context: CommandContext
    ) -> dict[str, Any]:
        return await self._service.close_session(
            session_id=session_id, reason=reason, context=context
        )

    async def resume_task(self, *, session_id: str, context: CommandContext) -> dict[str, Any]:
        return await self._service.resume_task(session_id=session_id, context=context)

    async def record_approval_response(
        self,
        *,
        session_id: str,
        approval_id: str,
        decision: str,
        feedback: str | None,
        context: CommandContext,
    ) -> dict[str, Any]:
        return await self._service.record_approval_response(
            session_id=session_id,
            approval_id=approval_id,
            decision=decision,
            feedback=feedback,
            context=context,
        )
