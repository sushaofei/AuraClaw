import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from auraclaw.config import get_settings
from auraclaw.infrastructure.runtime_events import (
    KafkaRuntimeEventProducer,
    KafkaStreamingIngestor,
    ReplayRuntimeEventBus,
)
from auraclaw.runtime.ports import RuntimeEvent

SETTINGS = get_settings()
pytestmark = pytest.mark.skipif(
    not SETTINGS.kafka_enabled,
    reason="Kafka test host not configured",
)


def test_kafka_runtime_event_round_trip_uses_public_sequence() -> None:
    async def scenario() -> None:
        suffix = uuid4().hex
        tenant_id = f"tenant-kafka-m5-{suffix}"
        session_id = f"session-kafka-m5-{suffix}"
        bus = ReplayRuntimeEventBus()
        ingestor = KafkaStreamingIngestor(
            SETTINGS.kafka_bootstrap_servers,
            topic=SETTINGS.kafka_runtime_topic,
            group_id=f"streaming-ingestor-test-{suffix}",
            target=bus,
        )
        producer = KafkaRuntimeEventProducer(
            SETTINGS.kafka_bootstrap_servers,
            topic=SETTINGS.kafka_runtime_topic,
        )
        try:
            await asyncio.wait_for(ingestor.start(), timeout=15)
            await asyncio.wait_for(
                producer.publish(
                    RuntimeEvent(
                        event_id=f"rte_{suffix}",
                        tenant_id=tenant_id,
                        root_session_id=session_id,
                        session_id=session_id,
                        run_id=f"run-{suffix}",
                        sequence=1,
                        type="runtime.progress",
                        timestamp=datetime.now(UTC),
                        payload={"step": 1},
                        visibility="user",
                    )
                ),
                timeout=15,
            )
            received: list[RuntimeEvent] = []
            for _ in range(100):
                received = await bus.events(tenant_id, session_id)
                if received:
                    break
                await asyncio.sleep(0.05)
            assert len(received) == 1
            assert received[0].sequence == 1
            assert received[0].event_id == f"rte_{suffix}"
        finally:
            await asyncio.wait_for(producer.close(), timeout=15)
            await asyncio.wait_for(ingestor.close(), timeout=15)
            await bus.close()

    asyncio.run(asyncio.wait_for(scenario(), timeout=45))
