from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any
from uuid import uuid4

from auraclaw.contracts.approval_mode import ApprovalConfiguration, ApprovalMode, InteractionMode
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import (
    CollaborationValidationError,
    NotFoundError,
    VersionConflictError,
)
from auraclaw.contracts.runtime_options import ReadRefreshGrant
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
        runtime_budget: dict[str, Any] | None = None,
    ) -> None:
        self._event_store = event_store
        self._relay = relay
        self._reader = reader
        self._admission = admission
        self._approvals = approvals
        self._approval_notifier = approval_notifier
        self._runtime_budget = dict(runtime_budget) if runtime_budget else None

    def _refresh_snapshots(self, grants: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
        if grants and (self._runtime_budget or {}).get("policy_version") != "2":
            raise CollaborationValidationError("read refresh requires runtime budget policy v2")
        if grants and (
            len(grants) > 8 or len({g.get("capability_id") for g in grants}) != len(grants)
        ):
            raise CollaborationValidationError(
                "at most eight distinct read refresh grants are allowed"
            )
        return [ReadRefreshGrant.model_validate(g).snapshot() for g in grants or []]

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
        interaction = interaction_mode or (
            InteractionMode.NON_STREAMING if source == "schedule" else InteractionMode.STREAMING
        )
        approval = ApprovalConfiguration.resolve(interaction, approval_mode)
        started = time.perf_counter()
        await self._admission.admit(goal=goal, context=context)
        session_id = f"ses_{uuid4().hex}"
        run_id = f"run_{uuid4().hex}"
        session = SessionAggregate.empty(session_id, context.tenant_id)
        session.create(
            goal=goal,
            run_id=run_id,
            dept_id=context.dept_id,
            source=source,
            schedule_id=schedule_id,
            occurrence_id=occurrence_id,
            approval=approval,
            runtime_budget=self._runtime_budget,
            read_refresh=self._refresh_snapshots(read_refresh),
        )
        response = {
            "session_id": session_id,
            "run_id": run_id,
            "status": "pending",
            "_request_fingerprint": hashlib.sha256(
                json.dumps(
                    {
                        "goal": goal,
                        "source": source,
                        "schedule_id": schedule_id,
                        "occurrence_id": occurrence_id,
                        **approval.public_dict(),
                        **({"read_refresh": read_refresh} if read_refresh else {}),
                    },
                    sort_keys=True,
                ).encode()
            ).hexdigest(),
            **approval.public_dict(),
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
        return {k: v for k, v in result.command_result.items() if k != "_request_fingerprint"}

    async def append_message(
        self, *, session_id: str, message: str, context: CommandContext
    ) -> dict[str, Any]:
        session = await self._load(context.tenant_id, session_id)
        session.append_message(message=message)
        response = {
            **session.approval.public_dict(),
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

    async def request_run(
        self,
        *,
        session_id: str,
        context: CommandContext,
        approval_mode: ApprovalMode | None = None,
        read_refresh: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        fingerprint = hashlib.sha256(
            json.dumps(
                {
                    "session_id": session_id,
                    "approval_mode": approval_mode,
                    **({"read_refresh": read_refresh} if read_refresh else {}),
                },
                sort_keys=True,
            ).encode()
        ).hexdigest()
        for event in await self._event_store.load(
            context.tenant_id, session_id, event_types=("run.requested",)
        ):
            if event.payload.get("command_id") == context.command_id:
                if event.payload.get("request_fingerprint") != fingerprint:
                    raise VersionConflictError(
                        "Run command was reused with a different approval mode"
                    )
                return {
                    "session_id": session_id,
                    "run_id": event.payload["run_id"],
                    "status": "pending",
                    "run_status": "pending",
                    **event.payload.get("approval", {}),
                }
        session = await self._load(context.tenant_id, session_id)
        run_id = f"run_{uuid4().hex}"
        session.request_run(
            run_id,
            approval_mode,
            command_id=context.command_id,
            request_fingerprint=fingerprint,
            runtime_budget=self._runtime_budget,
            read_refresh=self._refresh_snapshots(read_refresh),
        )
        response = {
            "session_id": session_id,
            "run_id": run_id,
            "status": "pending",
            "run_status": "pending",
            "_request_fingerprint": fingerprint,
            **session.approval.public_dict(),
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
        return {k: v for k, v in result.command_result.items() if k != "_request_fingerprint"}

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
            **session.approval.public_dict(),
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
            **session.approval.public_dict(),
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
            **session.approval.public_dict(),
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
        events = await self._event_store.load(context.tenant_id, session_id)
        record = ApprovalAggregate.from_events(
            events,
            tenant_id=context.tenant_id,
            session_id=session_id,
            approval_id=approval_id,
        )
        if record is None and self._approvals is not None:
            projected = await self._approvals.get(context.tenant_id, approval_id)
            if projected is not None and projected.session_id == session_id:
                record = projected
        if record is None:
            raise NotFoundError(f"Approval not found: {approval_id}")
        session = await self._load(context.tenant_id, session_id)
        if record.status.value == decision:
            decided = record
            append_required = False
        else:
            decided = ApprovalAggregate.respond(
                record,
                actor_id=context.actor.id,
                decision=decision,
                feedback=feedback,
            )
            session.record_human_response(
                approval_id=approval_id,
                actor_id=context.actor.id,
                decision=decided.status.value,
                feedback=feedback,
            )
            append_required = True
        response = {
            **session.approval.public_dict(),
            "session_id": session_id,
            "run_id": session.run_id,
            "status": session.status.value if session.status else "created",
            "run_status": session.run_status.value if session.run_status else None,
            "approval_id": approval_id,
            "decision": decided.status.value,
        }
        if append_required:
            result = await self._event_store.append(
                root_session_id=session.root_session_id,
                session_id=session.session_id,
                run_id=session.run_id,
                context=context,
                events=session.release_pending_events(),
                command_result=response,
            )
            response = result.command_result
            await self._after_append(session, result)
        if self._approval_notifier is not None:
            await self._approval_notifier.record_human_response(
                record,
                decision=decided.status.value,
                feedback=feedback,
                actor_id=context.actor.id,
            )
        return response

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
