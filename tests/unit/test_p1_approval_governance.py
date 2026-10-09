import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

import pytest

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import ApprovalValidationError
from auraclaw.contracts.events import Actor, CanonicalEvent, NewEvent
from auraclaw.contracts.tools import ApprovalStatus, RiskLevel
from auraclaw.domain.approval import ApprovalAggregate
from auraclaw.gateways.task.admission import AllowAllAdmissionController
from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore
from auraclaw.projection.approval.projector import InMemoryApprovalProjection
from auraclaw.projection.relay import OutboxRelay
from auraclaw.projection.task.projector import InMemoryTaskProjection
from auraclaw.session.task_service import TaskService


class _FanoutProjection:
    def __init__(
        self,
        task: InMemoryTaskProjection,
        approval: InMemoryApprovalProjection,
    ) -> None:
        self._task = task
        self._approval = approval

    async def project(self, events: Sequence[CanonicalEvent]) -> None:
        await self._task.project(events)
        await self._approval.project(events)


def test_approval_quorum_rejects_duplicate_votes_and_rejection_is_terminal() -> None:
    now = datetime(2026, 10, 7, 1, 0, tzinfo=UTC)
    record = ApprovalAggregate.request(
        tenant_id="tenant-1",
        session_id="session-1",
        run_id="run-1",
        digest="digest-1",
        tool_name="write-record",
        redacted_arguments={},
        risk=RiskLevel.HIGH,
        reason="two-person control",
        expected_effect="write",
        policy_version="policy-v2",
        assigned_approvers=("approver-1", "approver-2"),
        required_approvals=2,
        ttl=timedelta(hours=1),
        escalation_after=timedelta(minutes=15),
        now=now,
    )

    first, terminal = ApprovalAggregate.vote(
        record,
        actor_id="approver-1",
        decision="approved",
        feedback="first approval",
        now=now + timedelta(minutes=1),
    )
    assert terminal is False
    assert first.status is ApprovalStatus.WAITING
    with pytest.raises(ApprovalValidationError, match="already voted"):
        ApprovalAggregate.vote(
            first,
            actor_id="approver-1",
            decision="approved",
            feedback=None,
            now=now + timedelta(minutes=2),
        )

    rejected, terminal = ApprovalAggregate.vote(
        first,
        actor_id="approver-2",
        decision="rejected",
        feedback="risk unresolved",
        now=now + timedelta(minutes=3),
    )
    assert terminal is True
    assert rejected.status is ApprovalStatus.REJECTED
    assert len(rejected.votes) == 2


def test_sla_escalates_once_then_expires_and_resumes_session() -> None:
    async def scenario() -> None:
        now = datetime(2026, 10, 7, 2, 0, tzinfo=UTC)
        store = InMemoryEventStore()
        tasks = InMemoryTaskProjection()
        approvals = InMemoryApprovalProjection()
        relay = OutboxRelay(store, _FanoutProjection(tasks, approvals))
        service = TaskService(
            event_store=store,
            relay=relay,
            reader=tasks,
            admission=AllowAllAdmissionController(),
            approvals=approvals,
        )
        created = await service.create_task(
            goal="controlled operation",
            context=CommandContext(
                command_id="create-controlled-operation",
                tenant_id="tenant-1",
                actor=Actor(type="user", id="requester"),
                correlation_id="corr-1",
                expected_version=0,
                operation="create_task",
            ),
        )
        session_id = str(created["session_id"])
        run_id = str(created["run_id"])
        approval = ApprovalAggregate.request(
            tenant_id="tenant-1",
            session_id=session_id,
            run_id=run_id,
            digest="digest-sla",
            tool_name="controlled-operation",
            redacted_arguments={},
            risk=RiskLevel.HIGH,
            reason="approval required",
            expected_effect="write",
            policy_version="policy-v2",
            assigned_approvers=("approver-1",),
            ttl=timedelta(hours=1),
            escalation_after=timedelta(minutes=15),
            now=now,
        )
        await store.append(
            root_session_id=session_id,
            session_id=session_id,
            run_id=run_id,
            context=CommandContext(
                command_id="request-approval",
                tenant_id="tenant-1",
                actor=Actor(type="runtime", id="runtime-1"),
                correlation_id=run_id,
                expected_version=2,
                operation="runtime.approval.requested",
            ),
            events=[NewEvent(type="approval.requested", payload=approval.as_event_payload())],
            command_result={"approval_id": approval.approval_id},
        )
        await relay.relay_once()

        assert (
            await service.process_due_approval_slas(now=now + timedelta(minutes=16))
            == 1
        )
        escalated = await approvals.get("tenant-1", approval.approval_id)
        assert escalated is not None
        assert escalated.escalation_level == 1
        assert escalated.escalation_at is None
        assert escalated.status is ApprovalStatus.WAITING

        assert await service.process_due_approval_slas(now=now + timedelta(hours=2)) == 1
        expired = await approvals.get("tenant-1", approval.approval_id)
        assert expired is not None
        assert expired.status is ApprovalStatus.EXPIRED
        task = await tasks.get_task("tenant-1", session_id)
        assert task is not None
        assert task["status"] == "runnable"

        delivery_types = {
            item.event.type for item in await store.pending_delivery_outbox()
        }
        assert {"approval.requested", "approval.escalated", "approval.expired"} <= (
            delivery_types
        )

    asyncio.run(scenario())
