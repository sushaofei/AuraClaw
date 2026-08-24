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
