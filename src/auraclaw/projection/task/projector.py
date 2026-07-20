from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

from auraclaw.contracts.events import CanonicalEvent
from auraclaw.contracts.state import RunStatus, SessionStatus


class ProjectionGapError(RuntimeError):
    pass


class UnsupportedEventError(RuntimeError):
    pass


KNOWN_TASK_EVENTS = {
    "session.created",
    "user.message.appended",
    "run.requested",
    "run.scheduled",
    "run.started",
    "model.output.completed",
    "tool.call.requested",
    "tool.call.completed",
    "tool.call.denied",
    "runtime.failed",
    "runtime.reprovisioned",
    "run.terminated",
    "session.paused",
    "approval.requested",
    "human.response.recorded",
    "approval.approved",
    "approval.rejected",
    "approval.expired",
    "approval.cancelled",
    "run.retry_scheduled",
    "session.resumed",
    "session.closed",
    "run.completed",
    "run.failed",
    "run.cancelled",
    "child.created",
    "dependency.changed",
    "child.delegated",
    "session.handed_off",
    "child.result_published",
    "review.completed",
    "join.completed",
    "parent.result.received",
    "delivery.attempting",
    "delivery.retrying",
    "delivery.succeeded",
    "delivery.failed",
    "delivery.dead_lettered",
}


class InMemoryTaskProjection:
    """Disposable Control/Result read model used by the first vertical slice."""

    def __init__(self) -> None:
        self._tasks: dict[tuple[str, str], dict[str, Any]] = {}
        self._event_ids: set[str] = set()
        self._lock = asyncio.Lock()
        self._poison_events: list[CanonicalEvent] = []

    async def project(self, events: Sequence[CanonicalEvent]) -> None:
        async with self._lock:
            for event in events:
                if event.type not in KNOWN_TASK_EVENTS:
                    self._poison_events.append(event)
                    raise UnsupportedEventError(f"unsupported canonical event: {event.type}")
                if event.event_id in self._event_ids:
                    continue
                key = (event.tenant_id, event.session_id)
                current = self._tasks.get(key)
                current_version = int(current["projection_version"]) if current else 0
                if event.aggregate_version != current_version + 1:
                    raise ProjectionGapError(
                        f"projection gap for {event.session_id}: "
                        f"expected {current_version + 1}, got {event.aggregate_version}"
                    )
                view = dict(current) if current else self._new_view(event)
                self._apply(view, event)
                view["projection_version"] = event.aggregate_version
                view["projected_at"] = event.occurred_at.isoformat()
                self._tasks[key] = view
                self._event_ids.add(event.event_id)

    async def get_task(self, tenant_id: str, session_id: str) -> dict[str, Any] | None:
        task = self._tasks.get((tenant_id, session_id))
        return dict(task) if task else None

    async def clear(self) -> None:
        async with self._lock:
            self._tasks.clear()
            self._event_ids.clear()

    async def poison_events(self) -> list[CanonicalEvent]:
        async with self._lock:
            return list(self._poison_events)

    async def rebuild(self, events: Sequence[CanonicalEvent], tenant_id: str | None = None) -> int:
        async with self._lock:
            if tenant_id is None:
                self._tasks.clear()
                self._event_ids.clear()
            else:
                self._tasks = {
                    key: view for key, view in self._tasks.items() if key[0] != tenant_id
                }
                tenant_event_ids = {
                    event.event_id for event in events if event.tenant_id == tenant_id
                }
                self._event_ids.difference_update(tenant_event_ids)
        selected = [event for event in events if tenant_id is None or event.tenant_id == tenant_id]
        await self.project(selected)
        return len(selected)

    @staticmethod
    def _new_view(event: CanonicalEvent) -> dict[str, Any]:
        return {
            "tenant_id": event.tenant_id,
            "session_id": event.session_id,
            "root_session_id": event.root_session_id,
            "run_id": event.run_id,
            "status": SessionStatus.CREATED.value,
            "run_status": None,
            "progress": 0.0,
            "current_stage": "admission",
            "result_summary": None,
            "result_ref": None,
            "artifact_refs": [],
            "lineage": None,
            "error": None,
            "delivery_status": None,
            "delivery_id": None,
            "delivery_attempt_count": 0,
            "delivery_response_summary": None,
            "projection_version": 0,
        }

    @staticmethod
    def _apply(view: dict[str, Any], event: CanonicalEvent) -> None:
        payload = event.payload
        if event.type == "session.created":
            view.update(
                goal=payload["goal"],
                role=payload.get("role", "root"),
                parent_session_id=payload.get("parent_session_id"),
                status=SessionStatus.CREATED.value,
            )
        elif event.type == "child.created":
            dependencies = list(payload.get("dependency_ids", []))
            view.update(
                goal=payload["goal"],
                role=payload["role"],
                parent_session_id=payload["parent_session_id"],
                dependency_ids=dependencies,
                output_contract=payload["output_contract"],
                owner=None,
                status="blocked" if dependencies else SessionStatus.RUNNABLE.value,
                current_stage="blocked" if dependencies else "scheduling",
            )
        elif event.type == "run.requested":
            child_status = view.get("status")
            view.update(
                run_id=payload["run_id"],
                status=(
                    child_status
                    if view.get("role", "root") != "root"
                    else SessionStatus.PENDING.value
                ),
                current_stage=(
                    view.get("current_stage", "scheduling")
                    if view.get("role", "root") != "root"
                    else "pending"
                ),
                run_status=RunStatus.PENDING.value,
                progress=0.0,
                result_summary=None,
                result_ref=None,
                artifact_refs=[],
                error=None,
                delivery_status=None,
                delivery_id=None,
                delivery_attempt_count=0,
                delivery_response_summary=None,
            )
        elif event.type == "dependency.changed":
            dependencies = list(payload["dependency_ids"])
            view.update(
                dependency_ids=dependencies,
                status="blocked" if dependencies else SessionStatus.RUNNABLE.value,
                current_stage="blocked" if dependencies else "scheduling",
            )
        elif event.type in {"child.delegated", "session.handed_off"}:
            view.update(owner=payload["owner"])
        elif event.type == "run.scheduled":
            view.update(
                status=SessionStatus.RUNNABLE.value,
                run_status=RunStatus.RUNNABLE.value,
                current_stage="scheduling",
            )
        elif event.type == "run.started":
            view.update(
                status=SessionStatus.RUNNING.value,
                run_status=RunStatus.RUNNING.value,
                current_stage="running",
            )
        elif event.type == "session.paused":
            view.update(
                status=SessionStatus.PAUSED.value,
                run_status=RunStatus.PAUSED.value,
                current_stage="paused",
            )
        elif event.type == "approval.requested":
            view.update(
                status=SessionStatus.WAITING_FOR_HUMAN.value,
                run_status=RunStatus.WAITING_FOR_HUMAN.value,
                current_stage="waiting_for_human",
            )
        elif event.type == "approval.approved":
            view.update(
                status=SessionStatus.RUNNABLE.value,
                run_status=RunStatus.RUNNABLE.value,
                current_stage="scheduling",
            )
        elif event.type == "approval.rejected":
            view.update(
                status=SessionStatus.RUNNABLE.value,
                run_status=RunStatus.RUNNABLE.value,
                current_stage="replanning",
            )
        elif event.type == "session.resumed":
            view.update(
                run_id=payload["run_id"],
                status=SessionStatus.PENDING.value,
                run_status=RunStatus.PENDING.value,
                current_stage="pending",
            )
        elif event.type == "run.completed":
            view.update(
                status=(
                    SessionStatus.READY.value
                    if view.get("role", "root") == "root"
                    else SessionStatus.COMPLETED.value
                ),
                run_status=RunStatus.COMPLETED.value,
                progress=1.0,
                current_stage="completed",
                result_summary=payload.get("result_summary"),
                result_ref=payload.get("result_ref"),
                artifact_refs=payload.get("artifact_refs", []),
                lineage=payload.get("lineage"),
            )
        elif event.type == "child.result_published":
            view.update(
                status=SessionStatus.COMPLETED.value,
                run_status=RunStatus.COMPLETED.value,
                progress=1.0,
                current_stage="completed",
                result_summary=payload.get("summary"),
                result_ref=payload.get("result_ref"),
                artifact_refs=payload.get("artifact_refs", []),
            )
        elif event.type == "review.completed":
            view.update(
                status=SessionStatus.COMPLETED.value,
                run_status=RunStatus.COMPLETED.value,
                progress=1.0,
                current_stage="review_completed",
                result_summary=payload.get("decision"),
                result_ref=payload.get("target_result_ref"),
                lineage={"review": payload},
            )
        elif event.type == "run.failed":
            view.update(
                status=(
                    SessionStatus.READY.value
                    if view.get("role", "root") == "root"
                    else SessionStatus.FAILED.value
                ),
                run_status=RunStatus.FAILED.value,
                current_stage="failed",
                error=payload.get("error"),
            )
        elif event.type == "run.cancelled":
            view.update(
                status=(
                    SessionStatus.READY.value
                    if view.get("role", "root") == "root"
                    else SessionStatus.CANCELLED.value
                ),
                run_status=RunStatus.CANCELLED.value,
                current_stage="cancelled",
            )
        elif event.type == "runtime.failed":
            view.update(
                status=SessionStatus.RETRY_WAIT.value,
                run_status=RunStatus.RETRY_WAIT.value,
                current_stage="runtime_recovery",
                error=payload.get("error"),
            )
        elif event.type == "runtime.reprovisioned":
            view.update(
                status=SessionStatus.RUNNABLE.value,
                run_status=RunStatus.RUNNABLE.value,
                current_stage="scheduling",
            )
        elif event.type == "run.terminated":
            view.update(
                status=(
                    SessionStatus.READY.value
                    if view.get("role", "root") == "root"
                    else SessionStatus.FAILED.value
                ),
                run_status=RunStatus.FAILED.value,
                current_stage="terminated",
            )
        elif event.type == "session.closed":
            view.update(status=SessionStatus.CLOSED.value, current_stage="closed")
        elif event.type.startswith("delivery."):
            if event.run_id != view.get("run_id"):
                return
            view.update(
                delivery_status=payload.get("status"),
                delivery_id=payload.get("delivery_id"),
                delivery_attempt_count=payload.get("attempt_count", 0),
                delivery_response_summary=payload.get("response_summary"),
            )
