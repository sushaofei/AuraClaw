from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict, deque
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import uuid4

import asyncpg  # type: ignore[import-untyped]
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # type: ignore[import-untyped]

from auraclaw.contracts.observability import MetricPoint
from auraclaw.infrastructure.persistence.postgres_common import (
    LazyPool,
    json_dumps,
    json_loads,
)
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


class RuntimeSequenceAllocator(Protocol):
    async def next_sequence(self, tenant_id: str, session_id: str) -> int: ...


class MetricWriter(Protocol):
    async def write_metric(self, metric: MetricPoint) -> None: ...


class _KeyedPublishLocks:
    def __init__(self) -> None:
        self._guard = asyncio.Lock()
        self._entries: dict[tuple[str, str], tuple[asyncio.Lock, int]] = {}

    @asynccontextmanager
    async def hold(
        self, key: tuple[str, str], *, wait_seconds: float
    ) -> AsyncIterator[None]:
        async with self._guard:
            lock, references = self._entries.get(key, (asyncio.Lock(), 0))
            self._entries[key] = (lock, references + 1)
        try:
            await asyncio.wait_for(lock.acquire(), timeout=wait_seconds)
        except BaseException:
            await self._remove(key)
            raise
        try:
            yield
        finally:
            lock.release()
            await self._remove(key)

    async def _remove(self, key: tuple[str, str]) -> None:
        async with self._guard:
            lock, references = self._entries[key]
            if references == 1:
                self._entries.pop(key, None)
            else:
                self._entries[key] = (lock, references - 1)


class InMemoryRuntimeSequenceAllocator:
    def __init__(self) -> None:
        self._sequences: dict[tuple[str, str], int] = defaultdict(int)

    async def next_sequence(self, tenant_id: str, session_id: str) -> int:
        key = (tenant_id, session_id)
        self._sequences[key] += 1
        return self._sequences[key]


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
        sequence_allocator: RuntimeSequenceAllocator | None = None,
        max_event_bytes: int = 256_000,
        delta_flush_bytes: int = 512,
        max_concurrent: int = 64,
        max_queued: int = 1_024,
        queue_timeout_seconds: float = 5.0,
        publish_timeout_seconds: float = 10.0,
        metric_writer: MetricWriter | None = None,
    ) -> None:
        if min(max_concurrent, max_queued) < 1:
            raise ValueError("Runtime Event capacity limits must be positive")
        if queue_timeout_seconds <= 0 or publish_timeout_seconds <= 0:
            raise ValueError("Runtime Event timeouts must be positive")
        self._publisher = publisher
        self._sequence_allocator = sequence_allocator or InMemoryRuntimeSequenceAllocator()
        self._max_event_bytes = max_event_bytes
        self._delta_flush_bytes = delta_flush_bytes
        self._deltas: dict[tuple[str, str, str], str] = defaultdict(str)
        self._locks = _KeyedPublishLocks()
        self._capacity = asyncio.Semaphore(max_concurrent)
        self._max_queued = max_queued
        self._queue_timeout_seconds = queue_timeout_seconds
        self._publish_timeout_seconds = publish_timeout_seconds
        self._metric_writer = metric_writer
        self._queue_guard = asyncio.Lock()
        self._queued = 0
        self._inflight = 0

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
        async with self._publish_slot(tenant_id, session_id):
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
        async with self._publish_slot(tenant_id, session_id):
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
        sequence = await self._sequence_allocator.next_sequence(key[0], key[1])
        event = RuntimeEvent(
            event_id=f"rte_{uuid4().hex}",
            tenant_id=key[0],
            root_session_id=root_session_id,
            session_id=key[1],
            run_id=key[2],
            sequence=sequence,
            type=event_type,
            timestamp=datetime.now(UTC),
            payload=payload,
            durable=durable,
            visibility=visibility,
        )
        encoded = json.dumps(runtime_event_dict(event), separators=(",", ":")).encode()
        if len(encoded) > self._max_event_bytes:
            raise RuntimeEventRejectedError("runtime event exceeds configured size limit")
        try:
            await asyncio.wait_for(
                self._publisher.publish(event), timeout=self._publish_timeout_seconds
            )
        except TimeoutError as exc:
            await self._emit(
                "runtime.event.publish.timeout.count",
                1.0,
                key[0],
                session_id=key[1],
            )
            raise RuntimeEventRejectedError("runtime event publish timed out") from exc
        return event

    @asynccontextmanager
    async def _publish_slot(
        self, tenant_id: str, session_id: str
    ) -> AsyncIterator[None]:
        queued_at = asyncio.get_running_loop().time()
        async with self._queue_guard:
            if self._queued >= self._max_queued:
                rejected = True
                queue_depth = self._queued
            else:
                rejected = False
                self._queued += 1
                queue_depth = self._queued
        if rejected:
            await self._emit(
                "runtime.event.backpressure.count",
                1.0,
                tenant_id,
                session_id=session_id,
                reason="queue_full",
            )
            raise RuntimeEventRejectedError("runtime event publish queue is full")
        capacity_acquired = False
        started = False
        try:
            await self._emit(
                "runtime.event.queue.depth",
                float(queue_depth),
                tenant_id,
                session_id=session_id,
            )
            remaining = self._remaining(queued_at)
            async with self._locks.hold(
                (tenant_id, session_id), wait_seconds=remaining
            ):
                await asyncio.wait_for(
                    self._capacity.acquire(), timeout=self._remaining(queued_at)
                )
                capacity_acquired = True
                async with self._queue_guard:
                    self._queued -= 1
                    self._inflight += 1
                    started = True
                    queue_depth = self._queued
                    inflight = self._inflight
                await self._emit(
                    "runtime.event.queue.depth",
                    float(queue_depth),
                    tenant_id,
                    session_id=session_id,
                )
                await self._emit(
                    "runtime.event.in_flight",
                    float(inflight),
                    tenant_id,
                    session_id=session_id,
                )
                await self._emit(
                    "runtime.event.queue.latency.seconds",
                    asyncio.get_running_loop().time() - queued_at,
                    tenant_id,
                    session_id=session_id,
                )
                yield
        except TimeoutError as exc:
            await self._emit(
                "runtime.event.backpressure.count",
                1.0,
                tenant_id,
                session_id=session_id,
                reason="queue_timeout",
            )
            raise RuntimeEventRejectedError(
                "runtime event publish queue wait timed out"
            ) from exc
        finally:
            if capacity_acquired:
                self._capacity.release()
            async with self._queue_guard:
                if started:
                    self._inflight -= 1
                else:
                    self._queued -= 1
                queue_depth = self._queued
                inflight = self._inflight
            await self._emit(
                "runtime.event.queue.depth",
                float(queue_depth),
                tenant_id,
                session_id=session_id,
            )
            await self._emit(
                "runtime.event.in_flight",
                float(inflight),
                tenant_id,
                session_id=session_id,
            )

    def _remaining(self, queued_at: float) -> float:
        elapsed = asyncio.get_running_loop().time() - queued_at
        return max(0.0, self._queue_timeout_seconds - elapsed)

    async def _emit(
        self,
        name: str,
        value: float,
        tenant_id: str,
        **labels: str,
    ) -> None:
        if self._metric_writer is None:
            return
        try:
            await asyncio.wait_for(
                self._metric_writer.write_metric(
                    MetricPoint(
                        name=name,
                        value=value,
                        observed_at=datetime.now(UTC),
                        tenant_id=tenant_id,
                        labels=labels,
                    )
                ),
                timeout=0.1,
            )
        except Exception:
            return


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
        on_dequeue: Callable[[], None] | None = None,
    ) -> None:
        self.initial = initial
        self.replay_missed = replay_missed
        self._queue = queue
        self._close = close
        self._closed = False
        self._on_dequeue = on_dequeue

    async def events(self) -> AsyncIterator[RuntimeEvent]:
        try:
            for initial_event in self.initial:
                yield initial_event
            while True:
                queued_event = await self._queue.get()
                if self._on_dequeue is not None:
                    self._on_dequeue()
                if queued_event is None:
                    return
                yield queued_event
        finally:
            await self.close()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
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


class PostgresRuntimeEventStore(LazyPool):
    """Shared sequence, replay and connection state for horizontally scaled gateways."""

    def __init__(
        self,
        database_url: str,
        *,
        owner_id: str | None = None,
        retention_events: int = 1_000,
        connection_queue_size: int = 128,
        connection_ttl: timedelta = timedelta(seconds=30),
        poll_interval: float = 0.1,
    ) -> None:
        super().__init__(database_url)
        self._owner_id = owner_id or f"streaming-{uuid4().hex}"
        self._retention_events = retention_events
        self._queue_size = connection_queue_size
        self._connection_ttl = connection_ttl
        self._poll_interval = poll_interval
        self._subscriptions: dict[str, asyncio.Task[None]] = {}
        self._subscription_keys: dict[str, tuple[str, str]] = {}
        self._subscription_wakeups: dict[str, asyncio.Event] = {}
        self._closed = False

    async def next_sequence(self, tenant_id: str, session_id: str) -> int:
        pool = await self.pool()
        value = await pool.fetchval(
            """INSERT INTO streaming.session_sequence
                   (tenant_id, session_id, last_sequence)
               VALUES ($1, $2, 1)
               ON CONFLICT (tenant_id, session_id) DO UPDATE SET
                   last_sequence = streaming.session_sequence.last_sequence + 1,
                   updated_at = now()
               RETURNING last_sequence""",
            tenant_id,
            session_id,
        )
        return int(value)

    async def publish(self, event: RuntimeEvent) -> None:
        if event.visibility == "secret":
            raise RuntimeEventRejectedError("secret runtime events cannot enter replay")
        event = replace(event, payload=dict(_safe_payload(event.payload)))
        pool = await self.pool()
        try:
            async with pool.acquire() as connection, connection.transaction():
                status = await connection.execute(
                    """INSERT INTO streaming.runtime_event
                           (tenant_id, session_id, sequence, event_id,
                            root_session_id, run_id, event_type, occurred_at,
                            payload, durable, visibility)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11)
                       ON CONFLICT (event_id) DO NOTHING""",
                    event.tenant_id,
                    event.session_id,
                    event.sequence,
                    event.event_id,
                    event.root_session_id,
                    event.run_id,
                    event.type,
                    event.timestamp,
                    json_dumps(event.payload),
                    event.durable,
                    event.visibility,
                )
                # asyncpg returns "INSERT 0 0" for an idempotent conflict.
                if status.rsplit(" ", 1)[-1] == "0":
                    return
                # Keep the newest N events without exposing deleted sequence gaps.
                await connection.execute(
                    """DELETE FROM streaming.runtime_event AS victim
                       WHERE victim.tenant_id = $1 AND victim.session_id = $2
                         AND NOT EXISTS (
                             SELECT 1 FROM (
                                 SELECT sequence FROM streaming.runtime_event
                                 WHERE tenant_id = $1 AND session_id = $2
                                 ORDER BY sequence DESC
                                 LIMIT $3
                             ) AS kept
                             WHERE kept.sequence = victim.sequence
                         )""",
                    event.tenant_id,
                    event.session_id,
                    self._retention_events,
                )
        except asyncpg.UniqueViolationError as exc:
            raise RuntimeEventRejectedError(
                "runtime event sequence is already occupied"
            ) from exc
        self._wake_local_subscriptions(event.tenant_id, event.session_id)

    async def ingest(self, event: RuntimeEvent) -> RuntimeEvent:
        """Assign the public cursor at the shared Kafka ingestion boundary."""
        sequence = await self.next_sequence(event.tenant_id, event.session_id)
        event = replace(event, sequence=sequence)
        await self.publish(event)
        return event

    async def subscribe(
        self, tenant_id: str, session_id: str, *, after_sequence: int | None = None
    ) -> RuntimeSubscription:
        if self._closed:
            raise RuntimeError("runtime event store is closed")
        pool = await self.pool()
        connection_id = f"conn_{uuid4().hex}"
        queue: asyncio.Queue[RuntimeEvent | None] = asyncio.Queue(maxsize=self._queue_size)
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute(
                "DELETE FROM streaming.connection_registry WHERE expires_at <= now()"
            )
            rows = await connection.fetch(
                """SELECT * FROM streaming.runtime_event
                   WHERE tenant_id = $1 AND session_id = $2
                   ORDER BY sequence""",
                tenant_id,
                session_id,
            )
            retained = [self._event(row) for row in rows]
            replay_missed = bool(
                after_sequence is not None
                and retained
                and after_sequence < retained[0].sequence - 1
            )
            if after_sequence is None:
                initial = retained
            elif replay_missed:
                initial = []
            else:
                initial = [event for event in retained if event.sequence > after_sequence]
            cursor = max(
                after_sequence or 0,
                max((event.sequence for event in initial), default=0),
            )
            await connection.execute(
                """INSERT INTO streaming.connection_registry
                       (connection_id, tenant_id, session_id, owner_id,
                        cursor_sequence, expires_at)
                   VALUES ($1, $2, $3, $4, $5, now() + $6::interval)""",
                connection_id,
                tenant_id,
                session_id,
                self._owner_id,
                cursor,
                self._connection_ttl,
            )
        wakeup = asyncio.Event()
        self._subscription_keys[connection_id] = (tenant_id, session_id)
        self._subscription_wakeups[connection_id] = wakeup
        task = asyncio.create_task(
            self._poll(connection_id, tenant_id, session_id, cursor, queue, wakeup)
        )
        self._subscriptions[connection_id] = task

        async def close() -> None:
            polling = self._subscriptions.pop(connection_id, None)
            self._subscription_keys.pop(connection_id, None)
            self._subscription_wakeups.pop(connection_id, None)
            if polling is not None and polling is not asyncio.current_task():
                polling.cancel()
                with suppress(asyncio.CancelledError):
                    await polling
            current_pool = await self.pool()
            await current_pool.execute(
                "DELETE FROM streaming.connection_registry WHERE connection_id = $1",
                connection_id,
            )

        return RuntimeSubscription(
            initial,
            queue,
            close,
            replay_missed=replay_missed,
            on_dequeue=wakeup.set,
        )

    def _wake_local_subscriptions(self, tenant_id: str, session_id: str) -> None:
        key = (tenant_id, session_id)
        for connection_id, subscription_key in tuple(self._subscription_keys.items()):
            if subscription_key == key:
                wakeup = self._subscription_wakeups.get(connection_id)
                if wakeup is not None:
                    wakeup.set()

    async def _poll(
        self,
        connection_id: str,
        tenant_id: str,
        session_id: str,
        cursor: int,
        queue: asyncio.Queue[RuntimeEvent | None],
        wakeup: asyncio.Event,
    ) -> None:
        pool = await self.pool()
        try:
            while True:
                wakeup.clear()
                rows = await pool.fetch(
                    """SELECT * FROM streaming.runtime_event
                       WHERE tenant_id = $1 AND session_id = $2 AND sequence > $3
                       ORDER BY sequence LIMIT $4""",
                    tenant_id,
                    session_id,
                    cursor,
                    self._queue_size,
                )
                for row in rows:
                    event = self._event(row)
                    if queue.full():
                        break
                    queue.put_nowait(event)
                    cursor = event.sequence
                await pool.execute(
                    """UPDATE streaming.connection_registry
                       SET cursor_sequence = $2, heartbeat_at = now(),
                           expires_at = now() + $3::interval
                       WHERE connection_id = $1 AND owner_id = $4""",
                    connection_id,
                    cursor,
                    self._connection_ttl,
                    self._owner_id,
                )
                if rows and not queue.full() and cursor < self._event(rows[-1]).sequence:
                    continue
                try:
                    await asyncio.wait_for(wakeup.wait(), timeout=self._poll_interval)
                except TimeoutError:
                    pass
        except asyncio.CancelledError:
            raise
        except Exception:
            with suppress(asyncio.QueueFull):
                queue.put_nowait(None)

    async def events(self, tenant_id: str, session_id: str) -> list[RuntimeEvent]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT * FROM streaming.runtime_event
               WHERE tenant_id = $1 AND session_id = $2 ORDER BY sequence""",
            tenant_id,
            session_id,
        )
        return [self._event(row) for row in rows]

    async def close(self) -> None:
        self._closed = True
        tasks = tuple(self._subscriptions.values())
        self._subscriptions.clear()
        self._subscription_keys.clear()
        self._subscription_wakeups.clear()
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task
        if self._pool is not None:
            await self._pool.execute(
                "DELETE FROM streaming.connection_registry WHERE owner_id = $1",
                self._owner_id,
            )
        await super().close()

    @staticmethod
    def _event(row: asyncpg.Record) -> RuntimeEvent:
        return RuntimeEvent(
            event_id=str(row["event_id"]),
            tenant_id=str(row["tenant_id"]),
            root_session_id=str(row["root_session_id"]),
            session_id=str(row["session_id"]),
            run_id=str(row["run_id"]),
            sequence=int(row["sequence"]),
            type=str(row["event_type"]),
            timestamp=row["occurred_at"],
            payload=dict(json_loads(row["payload"])),
            durable=bool(row["durable"]),
            visibility=str(row["visibility"]),
        )


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
        target: RuntimeEventPublisher,
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
        logger = logging.getLogger(__name__)
        try:
            async for message in self._consumer:
                try:
                    data = json.loads(message.value.decode())
                    event = runtime_event_from_dict(dict(data))
                    ingest = getattr(self._target, "ingest", None)
                    if ingest is None:
                        await self._target.publish(event)
                    else:
                        await ingest(event)
                    await self._consumer.commit()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "runtime event ingest failed; skipping offset=%s",
                        getattr(message, "offset", None),
                    )
                    with suppress(Exception):
                        await self._consumer.commit()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("runtime event consumer stopped")

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
