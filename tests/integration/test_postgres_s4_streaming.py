import asyncio
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.config import get_settings
from auraclaw.contracts.errors import ServiceDrainingError
from auraclaw.infrastructure.kafka.runtime_events import PostgresRuntimeEventStore
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url
from auraclaw.runtime.ports import RuntimeEvent

SETTINGS = get_settings()
DATABASE_URL = asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (ROOT / "migrations/0011_s4_streaming_state.sql").read_text()
OWNERSHIP_MIGRATION = (
    ROOT / "migrations/0070_streaming_connection_ownership.sql"
).read_text()
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="PostgreSQL test URL not configured")


async def _apply_migration() -> None:
    assert DATABASE_URL is not None
    connection = await asyncpg.connect(DATABASE_URL)
    try:
        if await connection.fetchval(
            "SELECT to_regclass('streaming.runtime_event')"
        ) is None:
            await connection.execute(MIGRATION)
        if await connection.fetchval(
            "SELECT to_regclass('streaming.gateway_instance')"
        ) is None:
            await connection.execute(OWNERSHIP_MIGRATION)
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


def test_postgres_streaming_wakes_immediately_and_preserves_slow_deltas() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        await _apply_migration()
        suffix = uuid4().hex
        tenant_id = f"tenant-stream-wakeup-{suffix}"
        session_id = f"session-stream-wakeup-{suffix}"
        store = PostgresRuntimeEventStore(
            DATABASE_URL,
            owner_id="gateway-wakeup",
            retention_events=10,
            connection_queue_size=1,
            poll_interval=1.0,
        )
        try:
            subscription = await store.subscribe(tenant_id, session_id, after_sequence=0)
            stream = subscription.events()

            await store.publish(_event(tenant_id, session_id, 1))
            first = await asyncio.wait_for(anext(stream), timeout=0.25)
            assert first.sequence == 1

            await store.publish(_event(tenant_id, session_id, 2))
            await store.publish(_event(tenant_id, session_id, 3))
            second = await asyncio.wait_for(anext(stream), timeout=0.25)
            third = await asyncio.wait_for(anext(stream), timeout=0.25)
            assert [second.sequence, third.sequence] == [2, 3]
            await stream.aclose()
        finally:
            await store.close()

    asyncio.run(scenario())


def test_postgres_streaming_fences_live_owner_and_sweeps_orphans_on_takeover() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        await _apply_migration()
        suffix = uuid4().hex
        owner_id = f"gateway-takeover-{suffix}"
        tenant_id = f"tenant-takeover-{suffix}"
        session_id = f"session-takeover-{suffix}"
        first = PostgresRuntimeEventStore(
            DATABASE_URL,
            owner_id=owner_id,
            gateway_heartbeat_interval=60,
            poll_interval=0.01,
        )
        replacement = PostgresRuntimeEventStore(
            DATABASE_URL,
            owner_id=owner_id,
            gateway_heartbeat_interval=60,
            poll_interval=0.01,
        )
        connection = await asyncpg.connect(DATABASE_URL)
        subscription = None
        try:
            await first.start()
            with pytest.raises(RuntimeError, match="already active"):
                await replacement.start()
            generation = await connection.fetchval(
                "SELECT generation FROM streaming.gateway_instance WHERE owner_id=$1",
                owner_id,
            )
            await connection.execute(
                """INSERT INTO streaming.connection_registry
                       (connection_id, tenant_id, session_id, owner_id,
                        owner_generation, expires_at)
                   VALUES ($1,$2,$3,$4,$5,now() + interval '1 hour')""",
                f"orphan-{suffix}",
                tenant_id,
                session_id,
                owner_id,
                generation,
            )
            await connection.execute(
                """UPDATE streaming.gateway_instance
                   SET expires_at=now() - interval '1 second'
                   WHERE owner_id=$1""",
                owner_id,
            )
            await replacement.start()
            assert await connection.fetchval(
                "SELECT count(*) FROM streaming.connection_registry WHERE owner_id=$1",
                owner_id,
            ) == 0
            with pytest.raises(ServiceDrainingError, match="ownership was lost"):
                await first.ensure_accepting()

            subscription = await replacement.subscribe(
                tenant_id, session_id, after_sequence=0
            )
            events = subscription.events()
            assert await replacement.begin_drain(retry_after_seconds=7) == 1
            with pytest.raises(ServiceDrainingError, match="draining"):
                await replacement.ensure_accepting()
            with pytest.raises(StopAsyncIteration):
                await anext(events)
        finally:
            if subscription is not None:
                await subscription.close()
            await first.close()
            await replacement.close()
            await connection.close()

    asyncio.run(scenario())
