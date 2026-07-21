from __future__ import annotations

import asyncio
import json
from collections import defaultdict, deque
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import suppress
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # type: ignore[import-untyped]

from auraclaw.runtime.ports import RuntimeEvent, RuntimeEventPublisher

_SENSITIVE_KEYS = {
    "access_token",
    "api_key",
    "authorization",
    "client_secret",
    "cookie",
    "credential",
    "password",
    "refresh_token",
    "secret",
    "token",
}
_NON_CRITICAL_TYPES = {"model.output.delta", "runtime.progress", "typing", "heartbeat"}


class RuntimeEventRejectedError(ValueError):
    pass


def _safe_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): "[REDACTED]" if str(key).lower() in _SENSITIVE_KEYS else _safe_payload(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_safe_payload(item) for item in value]
    return value


def runtime_event_dict(event: RuntimeEvent) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "tenant_id": event.tenant_id,
        "root_session_id": event.root_session_id,
        "session_id": event.session_id,
        "run_id": event.run_id,
        "sequence": event.sequence,
        "type": event.type,
        "timestamp": event.timestamp.isoformat(),
        "payload": _safe_payload(event.payload),
        "durable": event.durable,
        "visibility": event.visibility,
    }


def runtime_event_from_dict(data: dict[str, Any]) -> RuntimeEvent:
    return RuntimeEvent(
        event_id=str(data["event_id"]),
        tenant_id=str(data["tenant_id"]),
        root_session_id=str(data["root_session_id"]),
        session_id=str(data["session_id"]),
        run_id=str(data["run_id"]),
        sequence=int(data["sequence"]),
        type=str(data["type"]),
        timestamp=datetime.fromisoformat(str(data["timestamp"])),
        payload=dict(data.get("payload", {})),
        durable=bool(data.get("durable", False)),
        visibility=str(data.get("visibility", "internal")),
    )


def public_cursor(event: RuntimeEvent) -> str:
    return f"{event.session_id}:{event.sequence}"


class RuntimeEventProducerSDK:
    """Validates, sequences and coalesces ephemeral events before publication."""

    def __init__(
        self,
        publisher: RuntimeEventPublisher,
        *,
        max_event_bytes: int = 256_000,
        delta_flush_bytes: int = 512,
    ) -> None:
        self._publisher = publisher
        self._max_event_bytes = max_event_bytes
        self._delta_flush_bytes = delta_flush_bytes
        # Public SSE cursors are Session-scoped, so sequences must remain monotonic
        # when a Session starts a second or later Run.
        self._sequences: dict[tuple[str, str], int] = defaultdict(int)
        self._deltas: dict[tuple[str, str, str], str] = defaultdict(str)
        self._lock = asyncio.Lock()

    async def publish(
        self,
        *,
        tenant_id: str,
        root_session_id: str,
        session_id: str,
        run_id: str,
        event_type: str,
        payload: dict[str, Any],
        visibility: str = "internal",
        durable: bool = False,
    ) -> RuntimeEvent | None:
        if visibility == "secret":
            raise RuntimeEventRejectedError("secret runtime events cannot be published")
        key = (tenant_id, session_id, run_id)
        safe = dict(_safe_payload(payload))
        async with self._lock:
            if event_type == "model.output.delta":
                self._deltas[key] += str(safe.get("delta", ""))
                if len(self._deltas[key].encode()) < self._delta_flush_bytes:
                    return None
                safe = {"delta": self._deltas.pop(key)}
            return await self._publish_locked(
                key=key,
                root_session_id=root_session_id,
                event_type=event_type,
                payload=safe,
                visibility=visibility,
                durable=durable,
            )

    async def flush(
        self, *, tenant_id: str, root_session_id: str, session_id: str, run_id: str
    ) -> RuntimeEvent | None:
        key = (tenant_id, session_id, run_id)
        async with self._lock:
            delta = self._deltas.pop(key, "")
            if not delta:
                return None
            return await self._publish_locked(
                key=key,
                root_session_id=root_session_id,
                event_type="model.output.delta",
                payload={"delta": delta},
                visibility="user",
                durable=False,
            )

    async def _publish_locked(
        self,
        *,
        key: tuple[str, str, str],
        root_session_id: str,
        event_type: str,
        payload: dict[str, Any],
        visibility: str,
        durable: bool,
    ) -> RuntimeEvent:
        sequence_key = (key[0], key[1])
        self._sequences[sequence_key] += 1
        event = RuntimeEvent(
            event_id=f"rte_{uuid4().hex}",
            tenant_id=key[0],
            root_session_id=root_session_id,
            session_id=key[1],
            run_id=key[2],
            sequence=self._sequences[sequence_key],
            type=event_type,
            timestamp=datetime.now(UTC),
            payload=payload,
            durable=durable,
            visibility=visibility,
        )
        encoded = json.dumps(runtime_event_dict(event), separators=(",", ":")).encode()
        if len(encoded) > self._max_event_bytes:
            self._sequences[sequence_key] -= 1
            raise RuntimeEventRejectedError("runtime event exceeds configured size limit")
        await self._publisher.publish(event)
        return event


class SDKRuntimeEventPublisher:
    """Adapts Harness RuntimeEvent writes to the producer SDK's validated API."""

    def __init__(self, sdk: RuntimeEventProducerSDK) -> None:
        self._sdk = sdk

    async def publish(self, event: RuntimeEvent) -> None:
        await self._sdk.publish(
            tenant_id=event.tenant_id,
            root_session_id=event.root_session_id,
            session_id=event.session_id,
            run_id=event.run_id,
            event_type=event.type,
            payload=event.payload,
            visibility=event.visibility,
            durable=event.durable,
        )


class RuntimeSubscription:
    def __init__(
        self,
        initial: list[RuntimeEvent],
        queue: asyncio.Queue[RuntimeEvent | None],
        close: Callable[[], Awaitable[None]],
        *,
        replay_missed: bool,
    ) -> None:
        self.initial = initial
        self.replay_missed = replay_missed
        self._queue = queue
        self._close = close

    async def events(self) -> AsyncIterator[RuntimeEvent]:
        try:
            for initial_event in self.initial:
                yield initial_event
            while True:
                queued_event = await self._queue.get()
                if queued_event is None:
                    return
                yield queued_event
        finally:
            await self._close()


class ReplayRuntimeEventBus:
    """Bounded replay buffer and non-blocking fan-out used by a Gateway instance."""

    def __init__(self, *, retention_events: int = 1_000, connection_queue_size: int = 128) -> None:
        self._retention_events = retention_events
        self._queue_size = connection_queue_size
        self._events: dict[tuple[str, str], deque[RuntimeEvent]] = defaultdict(
            lambda: deque(maxlen=self._retention_events)
        )
        self._event_ids: set[str] = set()
        self._subscribers: dict[tuple[str, str], set[asyncio.Queue[RuntimeEvent | None]]] = (
            defaultdict(set)
        )
        self._lock = asyncio.Lock()

    async def publish(self, event: RuntimeEvent) -> None:
        if event.visibility == "secret":
            raise RuntimeEventRejectedError("secret runtime events cannot enter replay")
        event = replace(event, payload=dict(_safe_payload(event.payload)))
        key = (event.tenant_id, event.session_id)
        async with self._lock:
            if event.event_id in self._event_ids:
                return
            existing = self._events[key]
            if existing and event.sequence <= existing[-1].sequence:
                raise RuntimeEventRejectedError("runtime event sequence must increase")
            if len(existing) == existing.maxlen and existing:
                self._event_ids.discard(existing[0].event_id)
            existing.append(event)
            self._event_ids.add(event.event_id)
            for queue in tuple(self._subscribers[key]):
                if queue.full():
                    if event.type in _NON_CRITICAL_TYPES:
                        continue
                    with suppress(asyncio.QueueEmpty):
                        queue.get_nowait()
                with suppress(asyncio.QueueFull):
                    queue.put_nowait(event)

    async def subscribe(
        self, tenant_id: str, session_id: str, *, after_sequence: int | None = None
    ) -> RuntimeSubscription:
        key = (tenant_id, session_id)
        queue: asyncio.Queue[RuntimeEvent | None] = asyncio.Queue(maxsize=self._queue_size)
        async with self._lock:
            retained = list(self._events[key])
            replay_missed = bool(
                after_sequence is not None
                and retained
                and after_sequence < retained[0].sequence - 1
            )
            if after_sequence is None:
                initial = retained
            elif not replay_missed:
                initial = [event for event in retained if event.sequence > after_sequence]
            else:
                initial = []
            self._subscribers[key].add(queue)

        async def close() -> None:
            async with self._lock:
                self._subscribers[key].discard(queue)

        return RuntimeSubscription(initial, queue, close, replay_missed=replay_missed)

    async def events(self, tenant_id: str, session_id: str) -> list[RuntimeEvent]:
        async with self._lock:
            return list(self._events[(tenant_id, session_id)])

    async def close(self) -> None:
        async with self._lock:
            for subscribers in self._subscribers.values():
                for queue in subscribers:
                    with suppress(asyncio.QueueFull):
                        queue.put_nowait(None)
            self._subscribers.clear()


class KafkaRuntimeEventProducer:
    """Kafka producer adapter; browser cursors never expose Kafka offsets."""

    def __init__(self, bootstrap_servers: str, *, topic: str) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._producer: AIOKafkaProducer | None = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        async with self._lock:
            if self._producer is None:
                producer = AIOKafkaProducer(
                    bootstrap_servers=self._bootstrap_servers,
                    compression_type="gzip",
                    enable_idempotence=True,
                    request_timeout_ms=10_000,
                )
                await producer.start()
                self._producer = producer

    async def publish(self, event: RuntimeEvent) -> None:
        await self.start()
        assert self._producer is not None
        value = json.dumps(runtime_event_dict(event), separators=(",", ":")).encode()
        await self._producer.send_and_wait(
            self._topic,
            value,
            key=event.root_session_id.encode(),
        )

    async def close(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    @property
    def ready(self) -> bool:
        return self._producer is not None


class KafkaStreamingIngestor:
    """One service-level consumer feeds a Gateway replay/router buffer."""

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        topic: str,
        group_id: str,
        target: ReplayRuntimeEventBus,
    ) -> None:
        self._consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="latest",
            request_timeout_ms=10_000,
        )
        self._target = target
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is not None:
            return
        await self._consumer.start()
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        try:
            async for message in self._consumer:
                data = json.loads(message.value.decode())
                await self._target.publish(runtime_event_from_dict(dict(data)))
                await self._consumer.commit()
        except asyncio.CancelledError:
            raise

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        await self._consumer.stop()

    @property
    def ready(self) -> bool:
        return self._task is not None and not self._task.done()
