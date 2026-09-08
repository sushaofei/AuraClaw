from __future__ import annotations

import asyncio
import json
import logging
from contextlib import suppress
from datetime import datetime

from aiokafka import (  # type: ignore[import-untyped]
    AIOKafkaConsumer,
    AIOKafkaProducer,
    TopicPartition,
)

from auraclaw.action.skill_lifecycle_events import (
    SkillLifecycleSignal,
    SkillLifecycleSignalApplier,
    SkillLifecycleSignalPublisher,
)


class KafkaSkillLifecycleSignalPublisher(SkillLifecycleSignalPublisher):
    def __init__(self, bootstrap_servers: str, *, topic: str) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._producer: AIOKafkaProducer | None = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        async with self._lock:
            if self._producer is not None:
                return
            producer = AIOKafkaProducer(
                bootstrap_servers=self._bootstrap_servers,
                compression_type="gzip",
                enable_idempotence=True,
                request_timeout_ms=10_000,
            )
            await producer.start()
            self._producer = producer

    async def publish(self, signal: SkillLifecycleSignal) -> None:
        await self.start()
        assert self._producer is not None
        await self._producer.send_and_wait(
            self._topic,
            json.dumps(_signal_dict(signal), separators=(",", ":")).encode(),
            key=signal.tenant_id.encode(),
        )

    async def close(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None


class KafkaSkillLifecycleSignalConsumer:
    """A unique group per replica gives Kafka broadcast semantics."""

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        topic: str,
        replica_id: str,
        target: SkillLifecycleSignalApplier,
    ) -> None:
        self._group_id = skill_lifecycle_group_id(replica_id)
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._consumer: AIOKafkaConsumer | None = None
        self._target = target
        self._task: asyncio.Task[None] | None = None

    @property
    def group_id(self) -> str:
        return self._group_id

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        if self._consumer is not None:
            await self._consumer.stop()
        consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            enable_auto_commit=False,
            auto_offset_reset="latest",
            request_timeout_ms=10_000,
        )
        try:
            await consumer.start()
        except Exception:
            await consumer.stop()
            raise
        self._consumer = consumer
        self._task = asyncio.create_task(
            self._run(), name=f"{self._group_id}-consumer"
        )

    async def _run(self) -> None:
        logger = logging.getLogger(__name__)
        consumer = self._consumer
        assert consumer is not None
        try:
            async for message in consumer:
                try:
                    data = json.loads(message.value.decode())
                    await self._target.apply(_signal_from_dict(dict(data)))
                    await consumer.commit()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "Skill lifecycle signal failed; retrying partition=%s offset=%s",
                        getattr(message, "partition", None),
                        getattr(message, "offset", None),
                    )
                    consumer.seek(
                        TopicPartition(message.topic, message.partition),
                        message.offset,
                    )
                    await asyncio.sleep(0.25)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Skill lifecycle signal consumer stopped")

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

    @property
    def ready(self) -> bool:
        return self._task is not None and not self._task.done()


def skill_lifecycle_group_id(replica_id: str) -> str:
    normalized = "".join(
        character if character.isalnum() or character in "._-" else "-"
        for character in replica_id
    )[:128]
    if not normalized:
        raise ValueError("Skill lifecycle replica id is invalid")
    return f"action-hands-skill-lifecycle-{normalized}"


def _signal_dict(signal: SkillLifecycleSignal) -> dict[str, object]:
    return {
        "schema_version": 1,
        "event_id": signal.event_id,
        "tenant_id": signal.tenant_id,
        "revision": signal.revision,
        "change_type": signal.change_type,
        "snapshot_digest": signal.snapshot_digest,
        "origin_replica": signal.origin_replica,
        "occurred_at": signal.occurred_at.isoformat(),
    }


def _signal_from_dict(data: dict[str, object]) -> SkillLifecycleSignal:
    if int(str(data.get("schema_version", 0))) != 1:
        raise ValueError("Skill lifecycle signal schema is unsupported")
    revision = int(str(data["revision"]))
    if revision < 1:
        raise ValueError("Skill lifecycle signal revision is invalid")
    occurred_at = datetime.fromisoformat(str(data["occurred_at"]))
    if occurred_at.tzinfo is None:
        raise ValueError("Skill lifecycle signal timestamp is invalid")
    tenant_id = str(data["tenant_id"])
    if not 1 <= len(tenant_id) <= 256:
        raise ValueError("Skill lifecycle signal tenant is invalid")
    return SkillLifecycleSignal(
        event_id=str(data["event_id"]),
        tenant_id=tenant_id,
        revision=revision,
        change_type=str(data["change_type"]),
        snapshot_digest=(
            None
            if data.get("snapshot_digest") is None
            else str(data["snapshot_digest"])
        ),
        origin_replica=str(data["origin_replica"]),
        occurred_at=occurred_at,
    )
