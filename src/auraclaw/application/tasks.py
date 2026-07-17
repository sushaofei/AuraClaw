from __future__ import annotations

from typing import Any
from uuid import uuid4

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import NotFoundError
from auraclaw.domain.ports import EventStore, ProjectionWriter, TaskReader
from auraclaw.domain.session import SessionAggregate


class TaskService:
    def __init__(
        self,
        *,
        event_store: EventStore,
        projector: ProjectionWriter,
        reader: TaskReader,
    ) -> None:
        self._event_store = event_store
        self._projector = projector
        self._reader = reader

    async def create_task(self, *, goal: str, context: CommandContext) -> dict[str, Any]:
        session_id = f"ses_{uuid4().hex}"
        run_id = f"run_{uuid4().hex}"
        session = SessionAggregate.empty(session_id, context.tenant_id)
        session.create(goal=goal, run_id=run_id)
        response = {
            "session_id": session_id,
            "run_id": run_id,
            "status": "pending",
            "status_url": f"/v1/tasks/{session_id}",
            "result_url": f"/v1/tasks/{session_id}/result",
            "stream_url": f"/v1/streams/{session_id}",
        }
        result = await self._event_store.append(
            root_session_id=session.root_session_id,
            session_id=session.session_id,
            run_id=session.run_id,
            context=context,
            events=session.release_pending_events(),
            command_result=response,
        )
        await self._projector.project(result.events)
        return result.command_result

    async def cancel_task(
        self,
        *,
        session_id: str,
        reason: str,
        context: CommandContext,
    ) -> dict[str, Any]:
        session = await self._load(context.tenant_id, session_id)
        session.cancel(reason)
        response = {
            "session_id": session_id,
            "run_id": session.run_id,
            "status": "cancelled",
        }
        result = await self._event_store.append(
            root_session_id=session.root_session_id,
            session_id=session.session_id,
            run_id=session.run_id,
            context=context,
            events=session.release_pending_events(),
            command_result=response,
        )
        await self._projector.project(result.events)
        return result.command_result

    async def resume_task(
        self,
        *,
        session_id: str,
        context: CommandContext,
    ) -> dict[str, Any]:
        session = await self._load(context.tenant_id, session_id)
        run_id = f"run_{uuid4().hex}"
        session.resume(run_id)
        response = {"session_id": session_id, "run_id": run_id, "status": "pending"}
        result = await self._event_store.append(
            root_session_id=session.root_session_id,
            session_id=session.session_id,
            run_id=run_id,
            context=context,
            events=session.release_pending_events(),
            command_result=response,
        )
        await self._projector.project(result.events)
        return result.command_result

    async def get_task(self, *, tenant_id: str, session_id: str) -> dict[str, Any]:
        task = await self._reader.get_task(tenant_id, session_id)
        if task is None:
            raise NotFoundError(f"Session not found: {session_id}")
        return task

    async def _load(self, tenant_id: str, session_id: str) -> SessionAggregate:
        events = await self._event_store.load(tenant_id, session_id)
        if not events:
            raise NotFoundError(f"Session not found: {session_id}")
        return SessionAggregate.from_events(events)
