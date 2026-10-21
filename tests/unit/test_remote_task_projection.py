from datetime import UTC, datetime

import pytest

from auraclaw.contracts.events import Actor, CanonicalEvent
from auraclaw.contracts.state import Visibility
from auraclaw.infrastructure.clients.session import RemoteTaskProjection


class _SessionFeed:
    async def load(self, tenant_id: str, session_id: str) -> list[CanonicalEvent]:
        return [
            CanonicalEvent(
                event_id="evt-1",
                tenant_id=tenant_id,
                root_session_id=session_id,
                session_id=session_id,
                run_id="run-1",
                aggregate_version=1,
                type="session.created",
                occurred_at=datetime.now(UTC),
                actor=Actor(type="user", id="user-1"),
                correlation_id="corr-1",
                causation_id="cause-1",
                visibility=Visibility.INTERNAL,
                schema_version=1,
                payload={"goal": "hello"},
            )
        ]


@pytest.mark.asyncio
async def test_remote_task_projection_rebuilds_view_from_session_feed() -> None:
    view = await RemoteTaskProjection(_SessionFeed()).get_task("tenant-1", "ses-1")

    assert view is not None
    assert view["session_id"] == "ses-1"
    assert view["goal"] == "hello"
    assert view["projection_version"] == 1


@pytest.mark.asyncio
async def test_distributed_collaboration_uses_session_feed_without_canonical_sql() -> None:
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock, MagicMock

    from auraclaw.infrastructure.projection.postgres_collaboration_store import (
        PostgresCollaborationProjection,
    )
    from auraclaw.projection.collaboration.projector import COLLABORATION_EVENTS

    events = await _SessionFeed().load("tenant-1", "ses-1")
    feed = MagicMock()
    feed.load_root = AsyncMock(return_value=events)
    connection = MagicMock()
    connection.fetchval = AsyncMock(return_value=events[0].event_id)
    connection.execute = AsyncMock()
    connection.fetch = AsyncMock(side_effect=AssertionError("Canonical SQL is forbidden"))

    @asynccontextmanager
    async def acquired():
        yield connection

    @asynccontextmanager
    async def transaction():
        yield

    connection.transaction = transaction
    pool = MagicMock()
    pool.acquire = acquired
    projection = PostgresCollaborationProjection("postgresql://unused/db", event_store=feed)
    projection.pool = AsyncMock(return_value=pool)
    await projection.project(events)
    feed.load_root.assert_awaited_once_with(
        "tenant-1", "ses-1", event_types=tuple(sorted(COLLABORATION_EVENTS))
    )
    connection.fetch.assert_not_awaited()
    assert any("INSERT INTO projection.collaboration_view" in c.args[0]
               for c in connection.execute.await_args_list)
    # Duplicate delivery does not reload facts or rewrite the view.
    connection.fetchval.return_value = None
    await projection.project(events)
    assert feed.load_root.await_count == 1
    # A full root-feed page must use the complete, paginated tenant feed.
    from dataclasses import replace
    feed.load_root.return_value = events * 5000
    feed.load_all = AsyncMock(return_value=[
        events[0], replace(events[0], root_session_id="another-root", session_id="other")
    ])
    connection.fetchval.return_value = events[0].event_id
    await projection.project(events)
    feed.load_all.assert_awaited_once_with("tenant-1")
