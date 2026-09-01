from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from auraclaw.action.skill_lifecycle_events import (
    BroadcastingSkillStateProjector,
    InMemorySkillLifecycleSignalStore,
    SkillLifecycleSignal,
    SkillLifecycleSignalApplier,
    SkillLifecycleSignalRelay,
)
from auraclaw.infrastructure.kafka.skill_lifecycle_events import (
    KafkaSkillLifecycleSignalConsumer,
    skill_lifecycle_group_id,
)


class _Rebuilder:
    def __init__(self, digest: str = "sha256:a") -> None:
        self.digest = digest
        self.tenants: list[str] = []

    async def rebuild_tenant(self, tenant_id: str) -> tuple[int, tuple[str, ...]]:
        self.tenants.append(tenant_id)
        return 1, ()

    def snapshot_digest(self, tenant_id: str) -> str:
        assert tenant_id == "tenant-a"
        return self.digest


class _Publisher:
    def __init__(self, *, fail_once: bool = False) -> None:
        self.fail_once = fail_once
        self.signals: list[SkillLifecycleSignal] = []

    async def publish(self, signal: SkillLifecycleSignal) -> None:
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("kafka unavailable")
        self.signals.append(signal)


def test_signal_is_published_once_and_applied_by_every_other_replica() -> None:
    async def scenario() -> None:
        store = InMemorySkillLifecycleSignalStore()
        origin = _Rebuilder()
        projector = BroadcastingSkillStateProjector(
            rebuilder=origin,
            signals=store,
            replica_id="hands-a",
        )
        await projector.rebuild_tenant("tenant-a")

        publisher = _Publisher()
        relay = SkillLifecycleSignalRelay(
            signals=store,
            publisher=publisher,
            owner="relay-a",
        )
        assert await relay.run_once() == 1
        assert len(publisher.signals) == 1
        signal = publisher.signals[0]
        assert signal.revision == 1
        assert signal.snapshot_digest == "sha256:a"

        local = _Rebuilder()
        peer_b = _Rebuilder()
        peer_c = _Rebuilder()
        appliers = (
            SkillLifecycleSignalApplier(rebuilder=local),
            SkillLifecycleSignalApplier(rebuilder=peer_b),
            SkillLifecycleSignalApplier(rebuilder=peer_c),
        )
        assert await asyncio.gather(*(item.apply(signal) for item in appliers)) == [
            True,
            True,
            True,
        ]
        assert local.tenants == ["tenant-a"]
        assert peer_b.tenants == ["tenant-a"]
        assert peer_c.tenants == ["tenant-a"]

    asyncio.run(scenario())


def test_signal_revision_fences_duplicate_and_out_of_order_delivery() -> None:
    async def scenario() -> None:
        rebuilder = _Rebuilder()
        applier = SkillLifecycleSignalApplier(rebuilder=rebuilder)

        def signal(revision: int) -> SkillLifecycleSignal:
            return SkillLifecycleSignal(
                event_id=f"event-{revision}",
                tenant_id="tenant-a",
                revision=revision,
                change_type="skill.lifecycle.snapshot_changed",
                snapshot_digest=f"sha256:{revision}",
                origin_replica="hands-a",
                occurred_at=datetime.now(UTC),
            )

        assert await applier.apply(signal(2)) is True
        assert await applier.apply(signal(1)) is False
        assert await applier.apply(signal(2)) is False
        assert rebuilder.tenants == ["tenant-a"]
        assert applier.applied_revision("tenant-a") == 2

    asyncio.run(scenario())


def test_failed_kafka_publish_keeps_outbox_retryable() -> None:
    async def scenario() -> None:
        store = InMemorySkillLifecycleSignalStore()
        await store.enqueue(
            tenant_id="tenant-a",
            change_type="skill.lifecycle.snapshot_changed",
            snapshot_digest="sha256:a",
            origin_replica="hands-a",
        )
        publisher = _Publisher(fail_once=True)
        relay = SkillLifecycleSignalRelay(
            signals=store,
            publisher=publisher,
            owner="relay-a",
        )

        assert await relay.run_once() == 0
        assert await relay.run_once() == 1
        assert len(publisher.signals) == 1

    asyncio.run(scenario())


def test_each_replica_uses_a_distinct_kafka_consumer_group() -> None:
    assert skill_lifecycle_group_id("hands-a") != skill_lifecycle_group_id(
        "hands-b"
    )
    assert skill_lifecycle_group_id("hands/a") == (
        "action-hands-skill-lifecycle-hands-a"
    )


def test_kafka_consumer_can_be_composed_without_a_running_event_loop() -> None:
    consumer = KafkaSkillLifecycleSignalConsumer(
        "kafka.example:9092",
        topic="skill-lifecycle",
        replica_id="hands-a",
        target=SkillLifecycleSignalApplier(rebuilder=_Rebuilder()),
    )

    assert consumer.group_id == "action-hands-skill-lifecycle-hands-a"
    assert consumer.ready is False
