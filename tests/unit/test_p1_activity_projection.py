import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

from auraclaw.contracts.events import Actor, CanonicalEvent
from auraclaw.contracts.state import Visibility
from auraclaw.gateways.query.reader import TaskQueryService
from auraclaw.projection.activity import InMemoryActivityProjection


def _event(version: int, *, event_type: str = "user.message.appended") -> CanonicalEvent:
    return CanonicalEvent(
        event_id=f"evt-activity-cache-{version}",
        tenant_id="tenant-activity-cache",
        root_session_id="session-activity-cache",
        session_id="session-activity-cache",
        run_id="run-activity-cache",
        aggregate_version=version,
        type=event_type,
        occurred_at=datetime(2026, 10, 7, tzinfo=UTC) + timedelta(milliseconds=version),
        actor=Actor(type="user", id="activity-user"),
        correlation_id="activity-cache",
        causation_id=f"cause-{version}",
        visibility=Visibility.USER,
        schema_version=1,
        payload={"message": f"message {version}"},
    )


class _TaskReader:
    def __init__(self, version: int) -> None:
        self.version = version

    async def get_task(self, tenant_id: str, session_id: str) -> dict[str, Any]:
        return {
            "tenant_id": tenant_id,
            "session_id": session_id,
            "projection_version": self.version,
            "status": "ready",
            "run_status": "completed",
        }


class _UnusedCollaboration:
    pass


class _FailingEventReader:
    async def load(self, *args: Any, **kwargs: Any) -> list[CanonicalEvent]:
        raise AssertionError("complete Activity projection must not scan Canonical Events")


class _FallbackEventReader:
    def __init__(self, events: list[CanonicalEvent]) -> None:
        self.events = events
        self.calls = 0

    async def load(self, *args: Any, **kwargs: Any) -> list[CanonicalEvent]:
        self.calls += 1
        return self.events


def test_long_session_activity_reads_bounded_page_from_precomputed_projection() -> None:
    async def scenario() -> None:
        event_count = 10_000
        projection = InMemoryActivityProjection()
        await projection.project([_event(version) for version in range(1, event_count + 1)])
        query = TaskQueryService(
            _TaskReader(event_count),  # type: ignore[arg-type]
            _UnusedCollaboration(),  # type: ignore[arg-type]
            _FailingEventReader(),
            projection,
        )

        page = await query.get_activity(
            "tenant-activity-cache",
            "session-activity-cache",
            after_version=9_799,
            limit=200,
        )

        assert page["source_version"] == event_count
        assert page["cache_status"] == "hit"
        assert len(page["nodes"]) == 200
        assert page["next_after_version"] == 9_999
        assert page["has_more"] is True

    asyncio.run(scenario())


def test_partial_projection_falls_back_to_canonical_events_until_rebuilt() -> None:
    async def scenario() -> None:
        event = _event(50)
        projection = InMemoryActivityProjection()
        await projection.project([event])
        events = _FallbackEventReader([event])
        query = TaskQueryService(
            _TaskReader(50),  # type: ignore[arg-type]
            _UnusedCollaboration(),  # type: ignore[arg-type]
            events,
            projection,
        )

        page = await query.get_activity(
            "tenant-activity-cache", "session-activity-cache", limit=10
        )

        assert events.calls == 1
        assert page["cache_status"] == "fallback"
        assert page["source_version"] == 50
        assert [node["updated_version"] for node in page["nodes"]] == [50]

    asyncio.run(scenario())


def test_activity_projection_rebuild_restores_complete_cache() -> None:
    async def scenario() -> None:
        projection = InMemoryActivityProjection()
        await projection.project([_event(3)])
        assert (
            await projection.get_activity_page(
                "tenant-activity-cache",
                "session-activity-cache",
                after_version=0,
                limit=10,
                min_source_version=3,
            )
            is None
        )

        events = [_event(version) for version in range(1, 4)]
        assert await projection.rebuild(events, "tenant-activity-cache") == 3
        page = await projection.get_activity_page(
            "tenant-activity-cache",
            "session-activity-cache",
            after_version=0,
            limit=10,
            min_source_version=3,
        )
        assert page is not None
        assert page["source_version"] == 3
        assert len(page["nodes"]) == 3

    asyncio.run(scenario())


def test_activity_projection_keeps_approval_governance_updates() -> None:
    async def scenario() -> None:
        projection = InMemoryActivityProjection()
        events = [
            replace(
                _event(1, event_type="approval.requested"),
                payload={"approval_id": "approval-1"},
            ),
            replace(
                _event(2, event_type="approval.vote.recorded"),
                payload={"approval_id": "approval-1", "decision": "approve"},
            ),
            replace(
                _event(3, event_type="approval.delegated"),
                payload={"approval_id": "approval-1", "reason": "delegate"},
            ),
            replace(
                _event(4, event_type="approval.escalated"),
                payload={"approval_id": "approval-1", "reason": "sla"},
            ),
        ]
        await projection.project(events)
        page = await projection.get_activity_page(
            "tenant-activity-cache",
            "session-activity-cache",
            after_version=0,
            limit=10,
            min_source_version=4,
        )
        assert page is not None
        assert len(page["nodes"]) == 1
        assert page["nodes"][0]["updated_version"] == 4
        assert page["nodes"][0]["status"] == "waiting"

    asyncio.run(scenario())
