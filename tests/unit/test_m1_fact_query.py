import asyncio

import pytest

from auraclaw.application.maintenance import ProjectionMaintenanceService
from auraclaw.application.tasks import AllowAllAdmissionController, TaskService
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.events import Actor, CanonicalEvent, NewEvent, utc_now
from auraclaw.contracts.state import Visibility
from auraclaw.infrastructure.memory import InMemoryEventStore
from auraclaw.projections.relay import OutboxRelay
from auraclaw.projections.tasks import (
    InMemoryTaskProjection,
    ProjectionGapError,
    UnsupportedEventError,
)


def _context(command_id: str, *, operation: str = "create_task") -> CommandContext:
    return CommandContext(
        command_id=command_id,
        tenant_id="tenant-1",
        actor=Actor(type="user", id="user-1"),
        correlation_id="corr-1",
        expected_version=0,
        operation=operation,
    )


def _service() -> tuple[TaskService, InMemoryEventStore, InMemoryTaskProjection]:
    store = InMemoryEventStore()
    projection = InMemoryTaskProjection()
    service = TaskService(
        event_store=store,
        relay=OutboxRelay(store, projection),
        reader=projection,
        admission=AllowAllAdmissionController(),
    )
    return service, store, projection


def test_100_concurrent_idempotent_creates_produce_one_root_session() -> None:
    async def scenario() -> None:
        service, store, _ = _service()
        responses = await asyncio.gather(
            *(service.create_task(goal="same task", context=_context("stable")) for _ in range(100))
        )

        assert len({response["session_id"] for response in responses}) == 1
        events = await store.load_all("tenant-1")
        assert [event.type for event in events] == ["session.created", "run.requested"]
        assert await store.pending_outbox() == []

    asyncio.run(scenario())


def test_snapshot_restores_session_and_projection_rebuild_is_deterministic() -> None:
    async def scenario() -> None:
        service, store, projection = _service()
        created = await service.create_task(goal="rebuild me", context=_context("create"))
        session_id = str(created["session_id"])
        snapshot = await store.get_snapshot("tenant-1", session_id)
        assert snapshot is not None
        assert snapshot.aggregate_version == 2

        before = await projection.get_task("tenant-1", session_id)
        await projection.clear()
        assert await projection.get_task("tenant-1", session_id) is None
        rebuilt = await ProjectionMaintenanceService(store, projection).rebuild_tasks("tenant-1")
        after = await projection.get_task("tenant-1", session_id)

        assert rebuilt == 2
        assert before is not None and after is not None
        for field in ("session_id", "status", "goal", "projection_version"):
            assert before[field] == after[field]

    asyncio.run(scenario())


def test_event_and_outbox_are_committed_together() -> None:
    async def scenario() -> None:
        store = InMemoryEventStore()
        result = await store.append(
            root_session_id="ses-1",
            session_id="ses-1",
            run_id="run-1",
            context=_context("atomic"),
            events=[NewEvent(type="session.created", payload={"goal": "atomic"})],
            command_result={"session_id": "ses-1"},
        )
        outbox = await store.pending_outbox()
        assert len(result.events) == len(outbox) == 1
        assert result.events[0].event_id == outbox[0].event_id

    asyncio.run(scenario())


def test_projector_rejects_gap_and_unknown_critical_event() -> None:
    async def scenario() -> None:
        projection = InMemoryTaskProjection()
        base = dict(
            event_id="evt-1",
            tenant_id="tenant-1",
            root_session_id="ses-1",
            session_id="ses-1",
            run_id=None,
            occurred_at=utc_now(),
            actor=Actor(type="user", id="user-1"),
            correlation_id="corr-1",
            causation_id="cmd-1",
            visibility=Visibility.INTERNAL,
            schema_version=1,
            payload={},
        )
        with pytest.raises(ProjectionGapError):
            await projection.project(
                [CanonicalEvent(aggregate_version=2, type="run.requested", **base)]
            )
        with pytest.raises(UnsupportedEventError):
            await projection.project(
                [CanonicalEvent(aggregate_version=1, type="future.critical", **base)]
            )

    asyncio.run(scenario())
