from __future__ import annotations

import logging
import time
from typing import Any
from uuid import uuid4

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import NotFoundError
from auraclaw.domain.approval import ApprovalAggregate
from auraclaw.domain.session import SessionAggregate
from auraclaw.projection.ports import ApprovalViewReader, TaskReader
from auraclaw.session.ports import (
    AdmissionController,
    AppendResult,
    EventStore,
    HumanApprovalNotifier,
    OutboxRelayPort,
    SessionSnapshot,
)

logger = logging.getLogger(__name__)


class TaskService:
    def __init__(
        self,
        *,
        event_store: EventStore,
        relay: OutboxRelayPort,
        reader: TaskReader,
        admission: AdmissionController,
        approvals: ApprovalViewReader | None = None,
        approval_notifier: HumanApprovalNotifier | None = None,
    ) -> None:
        self._event_store = event_store
        self._relay = relay
        self._reader = reader
        self._admission = admission
        self._approvals = approvals
        self._approval_notifier = approval_notifier

    async def create_task(self, *, goal: str, context: CommandContext) -> dict[str, Any]:
        started = time.perf_counter()
        await self._admission.admit(goal=goal, context=context)
        session_id = f"ses_{uuid4().hex}"
        run_id = f"run_{uuid4().hex}"
        session = SessionAggregate.empty(session_id, context.tenant_id)
        session.create(goal=goal, run_id=run_id, dept_id=context.dept_id)
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
        await self._after_append(session, result)
        logger.info(
            "ttft.create_task session=%s run=%s duration_ms=%.2f",
            session_id,
            run_id,
            (time.perf_counter() - started) * 1_000,
        )
        return result.command_result

    async def append_message(
        self, *, session_id: str, message: str, context: CommandContext
    ) -> dict[str, Any]:
        session = await self._load(context.tenant_id, session_id)
        session.append_message(message=message)
        response = {
            "session_id": session_id,
            "run_id": session.run_id,
            "status": session.status.value if session.status else "created",
            "run_status": session.run_status.value if session.run_status else None,
        }
        result = await self._event_store.append(
            root_session_id=session.root_session_id,
            session_id=session.session_id,
            run_id=session.run_id,
            context=context,
            events=session.release_pending_events(),
            command_result=response,
        )
        await self._after_append(session, result)
        return result.command_result

    async def request_run(self, *, session_id: str, context: CommandContext) -> dict[str, Any]:
        session = await self._load(context.tenant_id, session_id)
        run_id = f"run_{uuid4().hex}"
        session.request_run(run_id)
        response = {
            "session_id": session_id,
            "run_id": run_id,
            "status": "pending",
            "run_status": "pending",
        }
        result = await self._event_store.append(
            root_session_id=session.root_session_id,
            session_id=session.session_id,
            run_id=run_id,
            context=context,
            events=session.release_pending_events(),
            command_result=response,
        )
        await self._after_append(session, result)
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
            "status": session.status.value if session.status else "created",
            "run_status": session.run_status.value if session.run_status else None,
        }
        result = await self._event_store.append(
            root_session_id=session.root_session_id,
            session_id=session.session_id,
            run_id=session.run_id,
            context=context,
            events=session.release_pending_events(),
            command_result=response,
        )
        await self._after_append(session, result)
        return result.command_result

    async def close_session(
        self,
        *,
        session_id: str,
        reason: str,
        context: CommandContext,
    ) -> dict[str, Any]:
        session = await self._load(context.tenant_id, session_id)
        session.close(reason)
        response = {
            "session_id": session_id,
            "run_id": session.run_id,
            "status": "closed",
            "run_status": session.run_status.value if session.run_status else None,
        }
        result = await self._event_store.append(
            root_session_id=session.root_session_id,
            session_id=session.session_id,
            run_id=session.run_id,
            context=context,
            events=session.release_pending_events(),
            command_result=response,
        )
        await self._after_append(session, result)
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
        response = {
            "session_id": session_id,
            "run_id": run_id,
            "status": "pending",
            "run_status": "pending",
        }
        result = await self._event_store.append(
            root_session_id=session.root_session_id,
            session_id=session.session_id,
            run_id=run_id,
            context=context,
            events=session.release_pending_events(),
            command_result=response,
        )
        await self._after_append(session, result)
        return result.command_result

    async def record_approval_response(
        self,
        *,
        session_id: str,
        approval_id: str,
        decision: str,
        feedback: str | None,
        context: CommandContext,
    ) -> dict[str, Any]:
        if self._approvals is None:
            raise NotFoundError(f"Approval not found: {approval_id}")
        record = await self._approvals.get(context.tenant_id, approval_id)
        if record is None or record.session_id != session_id:
            raise NotFoundError(f"Approval not found: {approval_id}")
        decided = ApprovalAggregate.respond(
            record,
            actor_id=context.actor.id,
            decision=decision,
            feedback=feedback,
        )
        session = await self._load(context.tenant_id, session_id)
        session.record_human_response(
            approval_id=approval_id,
            actor_id=context.actor.id,
            decision=decided.status.value,
            feedback=feedback,
        )
        response = {
            "session_id": session_id,
            "run_id": session.run_id,
            "status": session.status.value if session.status else "created",
            "run_status": session.run_status.value if session.run_status else None,
            "approval_id": approval_id,
            "decision": decided.status.value,
        }
        result = await self._event_store.append(
            root_session_id=session.root_session_id,
            session_id=session.session_id,
            run_id=session.run_id,
            context=context,
            events=session.release_pending_events(),
            command_result=response,
        )
        if self._approval_notifier is not None:
            await self._approval_notifier.record_human_response(
                record,
                decision=decided.status.value,
                feedback=feedback,
            )
        await self._after_append(session, result)
        return result.command_result

    async def get_task(self, *, tenant_id: str, session_id: str) -> dict[str, Any]:
        task = await self._reader.get_task(tenant_id, session_id)
        if task is None:
            raise NotFoundError(f"Session not found: {session_id}")
        return task

    async def _load(self, tenant_id: str, session_id: str) -> SessionAggregate:
        snapshot = await self._event_store.get_snapshot(tenant_id, session_id)
        from_version = 1
        if snapshot is not None:
            session = SessionAggregate.from_snapshot(snapshot.state, snapshot.aggregate_version)
            from_version = snapshot.aggregate_version + 1
            events = await self._event_store.load(tenant_id, session_id, from_version=from_version)
            session.replay(events)
            return session
        events = await self._event_store.load(tenant_id, session_id, from_version=from_version)
        if not events:
            raise NotFoundError(f"Session not found: {session_id}")
        return SessionAggregate.from_events(events)

    async def _after_append(self, session: SessionAggregate, result: AppendResult) -> None:
        if not result.deduplicated:
            if result.events:
                session.version = result.events[-1].aggregate_version
            await self._event_store.save_snapshot(
                SessionSnapshot(
                    tenant_id=session.tenant_id,
                    session_id=session.session_id,
                    aggregate_version=session.version,
                    schema_version=1,
                    state=session.snapshot_state(),
                )
            )
            await self._relay.relay_once()
