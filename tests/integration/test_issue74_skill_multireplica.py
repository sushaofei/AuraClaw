from __future__ import annotations

import asyncio
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.action.skill_lifecycle_events import (
    SkillLifecycleSignalApplier,
    SkillLifecycleSignalRelay,
)
from auraclaw.config import get_settings
from auraclaw.infrastructure.kafka.skill_lifecycle_events import (
    KafkaSkillLifecycleSignalConsumer,
    KafkaSkillLifecycleSignalPublisher,
)
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url
from auraclaw.infrastructure.persistence.postgres_skill_lifecycle_events import (
    PostgresSkillLifecycleSignalStore,
)

SETTINGS = get_settings()
DATABASE_URL = (
    asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
)
pytestmark = pytest.mark.skipif(
    DATABASE_URL is None or not SETTINGS.kafka_enabled,
    reason="PostgreSQL and Kafka test hosts are required",
)


class _ReplicaRebuilder:
    def __init__(self) -> None:
        self.revisions: list[str] = []

    async def rebuild_tenant(self, tenant_id: str) -> tuple[int, tuple[str, ...]]:
        self.revisions.append(tenant_id)
        return 1, ()


@pytest.mark.parametrize("replica_count", (1, 2, 4))
def test_postgres_outbox_kafka_broadcast_reaches_every_replica(
    replica_count: int,
) -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        suffix = uuid4().hex
        tenant_id = f"tenant-issue74-{suffix}"
        topic = SETTINGS.kafka_skill_lifecycle_topic
        store = PostgresSkillLifecycleSignalStore(DATABASE_URL)
        publisher = KafkaSkillLifecycleSignalPublisher(
            SETTINGS.kafka_bootstrap_servers, topic=topic
        )
        rebuilders = [_ReplicaRebuilder() for _ in range(replica_count)]
        consumers = [
            KafkaSkillLifecycleSignalConsumer(
                SETTINGS.kafka_bootstrap_servers,
                topic=topic,
                replica_id=f"issue74-{suffix}-{index}",
                target=SkillLifecycleSignalApplier(rebuilder=rebuilder),
            )
            for index, rebuilder in enumerate(rebuilders)
        ]
        relay = SkillLifecycleSignalRelay(
            signals=store,
            publisher=publisher,
            owner=f"issue74-relay-{suffix}",
        )
        try:
            await asyncio.gather(*(consumer.start() for consumer in consumers))
            # Allow unique consumer groups to receive partition assignment before publish.
            await asyncio.sleep(0.5)
            await store.enqueue(
                tenant_id=tenant_id,
                change_type="skill.lifecycle.snapshot_changed",
                snapshot_digest="sha256:issue74",
                origin_replica="issue74-origin",
            )
            assert await relay.run_once() == 1
            for _ in range(200):
                if all(rebuilder.revisions == [tenant_id] for rebuilder in rebuilders):
                    break
                await asyncio.sleep(0.05)
            assert all(rebuilder.revisions == [tenant_id] for rebuilder in rebuilders)
            assert len({consumer.group_id for consumer in consumers}) == replica_count
        finally:
            await asyncio.gather(
                *(consumer.close() for consumer in consumers),
                return_exceptions=True,
            )
            await publisher.close()
            await store.close()
            connection = await asyncpg.connect(DATABASE_URL)
            try:
                await connection.execute(
                    "DELETE FROM hands.skill_lifecycle_broadcast_outbox WHERE tenant_id=$1",
                    tenant_id,
                )
                await connection.execute(
                    "DELETE FROM hands.skill_lifecycle_revision WHERE tenant_id=$1",
                    tenant_id,
                )
            finally:
                await connection.close()

    asyncio.run(asyncio.wait_for(scenario(), timeout=60))
