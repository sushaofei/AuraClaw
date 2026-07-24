from typing import Any, Protocol

from auraclaw.contracts.errors import NotFoundError
from auraclaw.contracts.events import CanonicalEvent
from auraclaw.gateways.query.transcript import TRANSCRIPT_EVENT_TYPES, build_transcript
from auraclaw.projection.ports import CollaborationReader, TaskReader


class EventReader(Protocol):
    async def load(
        self,
        tenant_id: str,
        session_id: str,
        *,
        from_version: int = 1,
        event_types: list[str] | tuple[str, ...] | None = None,
        limit: int | None = None,
    ) -> list[CanonicalEvent]: ...


class TaskQueryService:
    """Read-only Task, Result and Child views backed exclusively by projections.

    Transcript reads filtered Canonical Events (message/approval types only) so chat
    restore does not pull the full observability Timeline.
    """

    def __init__(
        self,
        reader: TaskReader,
        collaboration: CollaborationReader,
        events: EventReader,
    ) -> None:
        self._reader = reader
        self._collaboration = collaboration
        self._events = events

    async def get_task(self, tenant_id: str, session_id: str) -> dict[str, Any]:
        task = await self._reader.get_task(tenant_id, session_id)
        if task is None:
            raise NotFoundError(f"Session not found: {session_id}")
        return dict(task)

    async def list_children(self, tenant_id: str, root_session_id: str) -> list[dict[str, Any]]:
        children = await self._collaboration.list_children(tenant_id, root_session_id)
        return [dict(item) for item in children]

    async def get_result(self, tenant_id: str, session_id: str) -> dict[str, Any]:
        task = await self.get_task(tenant_id, session_id)
        return {
            "session_id": session_id,
            "run_id": task["run_id"],
            "status": task["run_status"],
            "session_status": task["status"],
            "result_summary": task["result_summary"],
            "result_ref": task["result_ref"],
            "artifact_refs": task["artifact_refs"],
            "error": task["error"],
            "delivery_status": task.get("delivery_status"),
            "delivery_id": task.get("delivery_id"),
            "delivery_attempt_count": task.get("delivery_attempt_count", 0),
            "delivery_response_summary": task.get("delivery_response_summary"),
            "projection_version": task["projection_version"],
        }

    async def get_transcript(self, tenant_id: str, session_id: str) -> dict[str, Any]:
        task = await self.get_task(tenant_id, session_id)
        events = await self._events.load(
            tenant_id,
            session_id,
            event_types=tuple(sorted(TRANSCRIPT_EVENT_TYPES)),
        )
        transcript = build_transcript(events)
        return {
            "session_id": session_id,
            "projection_version": task["projection_version"],
            "status": task["status"],
            "run_status": task["run_status"],
            "messages": transcript["messages"],
            "pending_approval": transcript["pending_approval"],
        }
