from typing import Any

from auraclaw.contracts.errors import NotFoundError
from auraclaw.projection.ports import CollaborationReader, TaskReader


class TaskQueryService:
    """Read-only Task, Result and Child views backed exclusively by projections."""

    def __init__(self, reader: TaskReader, collaboration: CollaborationReader) -> None:
        self._reader = reader
        self._collaboration = collaboration

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
            "status": task["status"],
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
