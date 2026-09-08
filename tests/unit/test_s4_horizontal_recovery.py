import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.delivery import ResultSinkConfig, SinkResponse
from auraclaw.contracts.events import Actor, CanonicalEvent, NewEvent
from auraclaw.contracts.state import Visibility
from auraclaw.control.orchestrator import (
    ManagedOrchestrator,
    RegisteredRuntimeProvisioner,
)
from auraclaw.control.ports import RunnableItem, RuntimeAssignment, RuntimeInstance
from auraclaw.control.runnable_feed import RunnableFeedConsumer
from auraclaw.infrastructure.delivery.memory_job_store import InMemoryDeliveryJobStore
from auraclaw.infrastructure.persistence.memory_control_store import (
    InMemoryControlStateStore,
)
from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore


class _RecordingOrchestratorSession:
    def __init__(self) -> None:
        self.appended: list[tuple[RuntimeAssignment, list[NewEvent]]] = []

    async def append(
        self,
        assignment: RuntimeAssignment,
        events: list[NewEvent],
        *,
        command_id: str,
        operation: str,
        expected_version: int | None = None,
    ) -> list[CanonicalEvent]:
        del command_id, operation, expected_version
        self.appended.append((assignment, events))
        return []


@pytest.mark.asyncio
async def test_outbox_replicas_preserve_per_session_order() -> None:
    events = InMemoryEventStore()
    await events.append(
        root_session_id="session-ordered",
        session_id="session-ordered",
        run_id="run-ordered",
        context=CommandContext(
            command_id="ordered-events",
            tenant_id="tenant-ordered",
            actor=Actor(type="user", id="user-ordered"),
            correlation_id="run-ordered",
            expected_version=0,
            operation="ordered_events",
        ),
        events=(
            NewEvent(type="session.created", payload={"goal": "ordered"}),
            NewEvent(type="run.requested", payload={"run_id": "run-ordered"}),
        ),
        command_result={},
    )

    first = await events.claim_outbox(
        "projection", "projection-a", limit=10, claim_ttl=timedelta(seconds=30)
    )
    assert [record.event.aggregate_version for record in first] == [1]
    assert await events.claim_outbox(
        "projection", "projection-b", limit=10, claim_ttl=timedelta(seconds=30)
    ) == []
    assert await events.disposition_outbox(
        "projection",
        "projection-a",
        first[0].outbox_id,
        first[0].claim_token,
        "ack",
    )
    second = await events.claim_outbox(
        "projection", "projection-b", limit=10, claim_ttl=timedelta(seconds=30)
    )
    assert [record.event.aggregate_version for record in second] == [2]


@pytest.mark.asyncio
async def test_control_claim_expiry_fences_old_orchestrator_replica() -> None:
    store = InMemoryControlStateStore()
    item = RunnableItem(
        task_id="task-s4",
        tenant_id="tenant-s4",
        root_session_id="session-s4",
        session_id="session-s4",
        run_id="run-s4",
        source_version=1,
    )
    assert await store.enqueue(item)
    old_claim = (
        await store.claim("orchestrator-a", claim_ttl=timedelta(0), limit=1)
    )[0]
    new_claim = (
        await store.claim(
            "orchestrator-b", claim_ttl=timedelta(seconds=30), limit=1
        )
    )[0]
    assert old_claim.claim_token != new_claim.claim_token

    lease = await store.acquire_lease(
        "session:tenant-s4:session-s4",
        "orchestrator-b",
        ttl=timedelta(seconds=30),
    )
    assert lease is not None
    assignment = RuntimeAssignment(
        tenant_id="tenant-s4",
        root_session_id="session-s4",
        session_id="session-s4",
        run_id="run-s4",
        runtime_id="runtime-s4",
        lease_id=lease.lease_id,
        fencing_token=lease.fencing_token,
        role="root",
        resource_profile={},
    )
    assert not await store.assign(
        item.task_id, assignment, claim_token=old_claim.claim_token
    )
    assert await store.assign(
        item.task_id, assignment, claim_token=new_claim.claim_token
    )
    await store.reschedule(
        item.task_id,
        worker_id="orchestrator-a",
        claim_token=old_claim.claim_token,
    )
    assert await store.claim("orchestrator-c", limit=1) == []


@pytest.mark.asyncio
async def test_delivery_claim_expiry_recovers_and_rejects_old_worker() -> None:
    store = InMemoryDeliveryJobStore()
    sink = ResultSinkConfig(
        sink_id="sink-s4",
        tenant_id="tenant-s4",
        session_id="session-s4",
        sink_type="test",
        target_ref="managed://test",
    )
    event = CanonicalEvent(
        event_id="event-s4",
        tenant_id="tenant-s4",
        root_session_id="session-s4",
        session_id="session-s4",
        run_id="run-s4",
        aggregate_version=1,
        type="run.completed",
        occurred_at=datetime.now(UTC),
        actor=Actor(type="runtime", id="runtime-s4"),
        correlation_id="run-s4",
        causation_id="run-s4",
        visibility=Visibility.USER,
        schema_version=1,
        payload={"result_summary": "done"},
    )
    await store.register_sink(sink)
    created = await store.create_job(event, sink)
    old_claim = (
        await store.claim_due(
            worker_id="delivery-a", claim_ttl=timedelta(0), limit=1
        )
    )[0]
    new_claim = (
        await store.claim_due(
            worker_id="delivery-b",
            claim_ttl=timedelta(seconds=30),
            limit=1,
        )
    )[0]
    assert new_claim.delivery_id == created.delivery_id
    assert new_claim.attempt_count == 2
    assert old_claim.claim_token != new_claim.claim_token
    with pytest.raises(RuntimeError, match="claim"):
        await store.record_attempt(
            old_claim,
            SinkResponse(True),
            next_attempt_at=None,
            max_attempts=5,
        )
    completed = await store.record_attempt(
        new_claim,
        SinkResponse(True),
        next_attempt_at=None,
        max_attempts=5,
    )
    assert completed.status.value == "succeeded"


@pytest.mark.asyncio
async def test_delivery_replicas_preserve_per_session_sink_order() -> None:
    store = InMemoryDeliveryJobStore()
    sink = ResultSinkConfig(
        sink_id="sink-ordered",
        tenant_id="tenant-ordered",
        session_id="session-ordered",
        sink_type="test",
        target_ref="managed://ordered",
    )
    await store.register_sink(sink)

    async def create(version: int) -> None:
        await store.create_job(
            CanonicalEvent(
                event_id=f"event-ordered-{version}",
                tenant_id="tenant-ordered",
                root_session_id="session-ordered",
                session_id="session-ordered",
                run_id="run-ordered",
                aggregate_version=version,
                type="run.completed",
                occurred_at=datetime.now(UTC),
                actor=Actor(type="runtime", id="runtime-ordered"),
                correlation_id="run-ordered",
                causation_id="run-ordered",
                visibility=Visibility.USER,
                schema_version=1,
                payload={"result_summary": str(version)},
            ),
            sink,
        )

    await create(1)
    await create(2)
    first = await store.claim_due(
        worker_id="delivery-a", claim_ttl=timedelta(seconds=30), limit=10
    )
    assert [job.event_id for job in first] == ["event-ordered-1"]
    assert await store.claim_due(
        worker_id="delivery-b", claim_ttl=timedelta(seconds=30), limit=10
    ) == []
    await store.record_attempt(
        first[0], SinkResponse(True), next_attempt_at=None, max_attempts=5
    )
    second = await store.claim_due(
        worker_id="delivery-b", claim_ttl=timedelta(seconds=30), limit=10
    )
    assert [job.event_id for job in second] == ["event-ordered-2"]


@pytest.mark.asyncio
async def test_runnable_feed_and_orchestrator_replicas_schedule_once_without_log_scan() -> None:
    events = InMemoryEventStore()
    control = InMemoryControlStateStore()
    context = CommandContext(
        command_id="create-s4-feed",
        tenant_id="tenant-s4-feed",
        actor=Actor(type="user", id="user-s4"),
        correlation_id="run-s4-feed",
        expected_version=0,
        operation="create_task",
    )
    await events.append(
        root_session_id="session-s4-feed",
        session_id="session-s4-feed",
        run_id="run-s4-feed",
        context=context,
        events=(
            NewEvent(
                type="session.created",
                payload={"goal": "feed", "role": "root"},
            ),
            NewEvent(type="run.requested", payload={"run_id": "run-s4-feed"}),
        ),
        command_result={},
    )
    feed_a = RunnableFeedConsumer(events, control, worker_id="orchestrator-a")
    feed_b = RunnableFeedConsumer(events, control, worker_id="orchestrator-b")
    ingested = await asyncio.gather(feed_a.run_once(), feed_b.run_once())
    assert sum(ingested) == 1

    for runtime_id in ("runtime-a", "runtime-b"):
        await control.register_runtime(
            RuntimeInstance(
                runtime_id=runtime_id,
                runtime_type="agent",
                role="root",
                node_id=runtime_id,
                capabilities={},
                capacity=1,
            )
        )
    session = _RecordingOrchestratorSession()
    orchestrators = (
        ManagedOrchestrator(
            orchestrator_id="orchestrator-a",
            control_store=control,
            session=session,
            provisioner=RegisteredRuntimeProvisioner(control),
        ),
        ManagedOrchestrator(
            orchestrator_id="orchestrator-b",
            control_store=control,
            session=session,
            provisioner=RegisteredRuntimeProvisioner(control),
        ),
    )
    assignments = await asyncio.gather(
        *(orchestrator.schedule_once() for orchestrator in orchestrators)
    )
    scheduled = [assignment for assignment in assignments if assignment is not None]
    assert len(scheduled) == 1
    assert scheduled[0].runtime_id in {"runtime-a", "runtime-b"}
    assert len(session.appended) == 1


@pytest.mark.asyncio
async def test_child_runnable_inherits_root_user_instead_of_coordinator_actor() -> None:
    events = InMemoryEventStore()
    root_context = CommandContext(
        command_id="create-owner-root",
        tenant_id="tenant-owner",
        actor=Actor(type="user", id="owner-user"),
        correlation_id="corr-owner",
        expected_version=0,
        operation="create_task",
    )
    await events.append(
        root_session_id="session-owner-root",
        session_id="session-owner-root",
        run_id="run-owner-root",
        context=root_context,
        events=(
            NewEvent(
                type="session.created",
                payload={"role": "root", "dept_id": "88"},
            ),
            NewEvent(type="run.requested", payload={"run_id": "run-owner-root"}),
        ),
        command_result={},
    )
    root_outbox = await events.claim_outbox(
        "control", "drain-root", limit=10, claim_ttl=timedelta(seconds=30)
    )
    for record in root_outbox:
        assert await events.disposition_outbox(
            "control",
            "drain-root",
            record.outbox_id,
            record.claim_token,
            "ack",
        )

    coordinator_context = CommandContext(
        command_id="create-owner-child",
        tenant_id="tenant-owner",
        actor=Actor(type="coordinator", id="coordinator-runtime"),
        correlation_id="corr-owner",
        expected_version=0,
        operation="collaboration.create_child",
    )
    await events.append(
        root_session_id="session-owner-root",
        session_id="session-owner-child",
        run_id="run-owner-child",
        context=coordinator_context,
        events=(
            NewEvent(
                type="child.created",
                payload={
                    "role": "worker",
                    "root_session_id": "session-owner-root",
                },
            ),
            NewEvent(type="run.requested", payload={"run_id": "run-owner-child"}),
        ),
        command_result={},
    )
    control = InMemoryControlStateStore()
    feed = RunnableFeedConsumer(events, control, worker_id="owner-feed")
    assert await feed.run_once() == 1
    claimed = await control.claim(
        "owner-orchestrator", claim_ttl=timedelta(seconds=30), limit=10
    )
    assert len(claimed) == 1
    assert claimed[0].item.session_id == "session-owner-child"
    assert claimed[0].item.user_id == "owner-user"
    assert claimed[0].item.dept_id == "88"
