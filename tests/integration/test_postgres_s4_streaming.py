import asyncio
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.config import get_settings
from auraclaw.infrastructure.kafka.runtime_events import PostgresRuntimeEventStore
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url
from auraclaw.runtime.ports import RuntimeEvent

SETTINGS = get_settings()
DATABASE_URL = asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (ROOT / "migrations/0011_s4_streaming_state.sql").read_text()
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="PostgreSQL test URL not configured")


async def _apply_migration() -> None:
    assert DATABASE_URL is not None
    connection = await asyncpg.connect(DATABASE_URL)
    try:
        if await connection.fetchval(
            "SELECT to_regclass('streaming.runtime_event')"
        ) is None:
            await connection.execute(MIGRATION)
    finally:
        await connection.close()


def _event(tenant_id: str, session_id: str, sequence: int) -> RuntimeEvent:
    return RuntimeEvent(
        event_id=f"event-{uuid4().hex}",
        tenant_id=tenant_id,
        root_session_id=session_id,
        session_id=session_id,
        run_id=f"run-{session_id}",
        sequence=sequence,
        type="runtime.progress",
        timestamp=datetime.now(UTC),
        payload={"sequence": sequence},
        durable=False,
        visibility="user",
    )


def test_postgres_streaming_sequence_replay_and_gateway_handoff() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        await _apply_migration()
        suffix = uuid4().hex
        tenant_id = f"tenant-stream-{suffix}"
        session_id = f"session-stream-{suffix}"
        store_a = PostgresRuntimeEventStore(
            DATABASE_URL, owner_id="gateway-a", retention_events=3, poll_interval=0.01
        )
        store_b = PostgresRuntimeEventStore(
            DATABASE_URL, owner_id="gateway-b", retention_events=3, poll_interval=0.01
        )
        try:
            sequences = await asyncio.gather(
                *(store_a.next_sequence(tenant_id, session_id) for _ in range(3)),
                *(store_b.next_sequence(tenant_id, session_id) for _ in range(2)),
            )
            assert sorted(sequences) == [1, 2, 3, 4, 5]
            for sequence in sorted(sequences):
                await store_a.publish(_event(tenant_id, session_id, sequence))

            expired = await store_a.subscribe(
                tenant_id, session_id, after_sequence=1
            )
            assert expired.replay_missed
            await expired.close()

            handoff = await store_b.subscribe(
                tenant_id, session_id, after_sequence=4
            )
            assert [event.sequence for event in handoff.initial] == [5]
            stream = handoff.events()
            assert (await anext(stream)).sequence == 5

            ingested = await store_a.ingest(_event(tenant_id, session_id, 1))
            assert ingested.sequence == 6
            live = await asyncio.wait_for(anext(stream), timeout=2)
            assert live.sequence == 6
            await stream.aclose()

            connection = await asyncpg.connect(DATABASE_URL)
            try:
                count = await connection.fetchval(
                    """SELECT count(*) FROM streaming.connection_registry
                       WHERE tenant_id = $1 AND session_id = $2""",
                    tenant_id,
                    session_id,
                )
                assert count == 0
            finally:
                await connection.close()
            assert [event.sequence for event in await store_b.events(tenant_id, session_id)] == [
                4,
                5,
                6,
            ]
        finally:
            await store_a.close()
            await store_b.close()

    asyncio.run(scenario())
