import asyncio
import hashlib
import hmac
import json
from datetime import UTC, datetime, timedelta
from time import perf_counter

import httpx
import pytest

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.delivery import DeliveryJob, ResultSinkConfig, SinkResponse
from auraclaw.contracts.errors import NotFoundError
from auraclaw.contracts.events import Actor, NewEvent
from auraclaw.contracts.state import Visibility
from auraclaw.delivery.worker import CircuitBreaker, ResultDeliveryWorker
from auraclaw.gateways.streaming.gateway import StreamingGateway
from auraclaw.gateways.task.admission import AllowAllAdmissionController
from auraclaw.infrastructure.delivery import (
    InMemoryDeliveryJobStore,
    ParentSessionResultSink,
    StaticDeliverySecretResolver,
    WebhookResultSink,
)
from auraclaw.infrastructure.kafka.runtime_events import (
    ReplayRuntimeEventBus,
    RuntimeEventProducerSDK,
    RuntimeEventRejectedError,
)
from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore
from auraclaw.projection.relay import OutboxRelay
from auraclaw.projection.task.projector import InMemoryTaskProjection
from auraclaw.runtime.ports import RuntimeEvent
from auraclaw.session.task_service import TaskService


class RecordingSink:
    sink_type = "recording"

    def __init__(self, responses: list[SinkResponse]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    async def deliver(self, job: DeliveryJob, config: ResultSinkConfig) -> SinkResponse:
        self.calls.append(job.delivery_id)
        return self.responses.pop(0)


def test_runtime_producer_sequences_coalesces_redacts_and_limits_events() -> None:
    async def scenario() -> None:
        bus = ReplayRuntimeEventBus()
        producer = RuntimeEventProducerSDK(
            bus,
            delta_flush_bytes=5,
            max_event_bytes=1_000,
        )
        common = {
            "tenant_id": "tenant-m5",
            "root_session_id": "session-m5",
            "session_id": "session-m5",
            "run_id": "run-m5",
        }
        assert await producer.publish(
            **common,
            event_type="model.output.delta",
            payload={"delta": "ab"},
            visibility="user",
        ) is None
        first = await producer.publish(
            **common,
            event_type="model.output.delta",
            payload={"delta": "cde"},
            visibility="user",
        )
        assert first is not None
        second = await producer.publish(
            **common,
            event_type="tool.progress",
            payload={"step": 1, "secret": "must-not-leak"},
            visibility="user",
        )
        assert second is not None
        assert first.sequence == 1 and first.payload == {"delta": "abcde"}
        assert second.sequence == 2 and second.payload["secret"] == "[REDACTED]"
        next_run = await producer.publish(
            **{**common, "run_id": "run-m5-2"},
            event_type="model.output.delta",
            payload={"delta": "second run"},
            visibility="user",
        )
        assert next_run is not None and next_run.sequence == 3
        with pytest.raises(RuntimeEventRejectedError, match="secret runtime"):
            await producer.publish(
                **common,
                event_type="unsafe",
                payload={},
                visibility="secret",
            )
        with pytest.raises(RuntimeEventRejectedError, match="size limit"):
            await producer.publish(
                **common,
                event_type="oversized",
                payload={"value": "x" * 2_000},
                visibility="user",
            )

    asyncio.run(scenario())


def test_runtime_event_slow_session_does_not_block_another_session() -> None:
    class SessionPublisher:
        def __init__(self) -> None:
            self.slow_started = asyncio.Event()
            self.release = asyncio.Event()
            self.events: list[RuntimeEvent] = []

        async def publish(self, event: RuntimeEvent) -> None:
            if event.session_id == "session-slow":
                self.slow_started.set()
                await self.release.wait()
            self.events.append(event)

    async def scenario() -> None:
        target = SessionPublisher()
        producer = RuntimeEventProducerSDK(target, max_concurrent=2)
        slow = asyncio.create_task(
            producer.publish(
                tenant_id="tenant-m5",
                root_session_id="session-slow",
                session_id="session-slow",
                run_id="run-slow",
                event_type="runtime.progress",
                payload={"step": "slow"},
            )
        )
        await asyncio.wait_for(target.slow_started.wait(), timeout=1)
        fast = await asyncio.wait_for(
            producer.publish(
                tenant_id="tenant-m5",
                root_session_id="session-fast",
                session_id="session-fast",
                run_id="run-fast",
                event_type="runtime.progress",
                payload={"step": "fast"},
            ),
            timeout=1,
        )
        assert fast is not None and fast.session_id == "session-fast"
        target.release.set()
        assert (await slow) is not None

    asyncio.run(scenario())


def test_runtime_event_same_session_preserves_send_order() -> None:
    class OrderedPublisher:
        def __init__(self) -> None:
            self.first_started = asyncio.Event()
            self.release = asyncio.Event()
            self.sequences: list[int] = []

        async def publish(self, event: RuntimeEvent) -> None:
            if not self.sequences:
                self.first_started.set()
                await self.release.wait()
            self.sequences.append(event.sequence)

    async def scenario() -> None:
        target = OrderedPublisher()
        producer = RuntimeEventProducerSDK(target, max_concurrent=2)
        common = {
            "tenant_id": "tenant-m5",
            "root_session_id": "session-ordered",
            "session_id": "session-ordered",
            "run_id": "run-ordered",
            "event_type": "runtime.progress",
            "visibility": "user",
        }
        first = asyncio.create_task(producer.publish(**common, payload={"step": 1}))
        await asyncio.wait_for(target.first_started.wait(), timeout=1)
        second = asyncio.create_task(producer.publish(**common, payload={"step": 2}))
        await asyncio.sleep(0.02)
        assert not second.done()
        target.release.set()
        results = await asyncio.gather(first, second)
        assert [result.sequence for result in results if result is not None] == [1, 2]
        assert target.sequences == [1, 2]

    asyncio.run(scenario())


def test_runtime_event_timeout_releases_keyed_state() -> None:
    class TimeoutOncePublisher:
        def __init__(self) -> None:
            self.calls = 0

        async def publish(self, event: RuntimeEvent) -> None:
            del event
            self.calls += 1
            if self.calls == 1:
                await asyncio.sleep(60)

    async def scenario() -> None:
        target = TimeoutOncePublisher()
        producer = RuntimeEventProducerSDK(
            target,
            publish_timeout_seconds=0.05,
            queue_timeout_seconds=0.2,
        )
        common = {
            "tenant_id": "tenant-m5",
            "root_session_id": "session-timeout",
            "session_id": "session-timeout",
            "run_id": "run-timeout",
            "event_type": "runtime.progress",
            "visibility": "user",
        }
        with pytest.raises(RuntimeEventRejectedError, match="publish timed out"):
            await producer.publish(**common, payload={"step": 1})
        recovered = await producer.publish(**common, payload={"step": 2})
        assert recovered is not None and recovered.sequence == 2
        assert producer._locks._entries == {}  # type: ignore[attr-defined]

    asyncio.run(scenario())


def test_runtime_event_same_session_queue_wait_is_bounded() -> None:
    class BlockingPublisher:
        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.release = asyncio.Event()
            self.calls = 0

        async def publish(self, event: RuntimeEvent) -> None:
            del event
            self.calls += 1
            self.started.set()
            await self.release.wait()

    async def scenario() -> None:
        target = BlockingPublisher()
        producer = RuntimeEventProducerSDK(
            target,
            max_concurrent=2,
            max_queued=2,
            queue_timeout_seconds=0.05,
            publish_timeout_seconds=2,
        )
        common = {
            "tenant_id": "tenant-m5",
            "root_session_id": "session-queue-timeout",
            "session_id": "session-queue-timeout",
            "run_id": "run-queue-timeout",
            "event_type": "runtime.progress",
            "visibility": "user",
        }
        owner = asyncio.create_task(producer.publish(**common, payload={"step": 1}))
        await asyncio.wait_for(target.started.wait(), timeout=1)
        with pytest.raises(RuntimeEventRejectedError, match="queue wait timed out"):
            await producer.publish(**common, payload={"step": 2})
        assert target.calls == 1
        target.release.set()
        assert (await owner) is not None
        assert producer._locks._entries == {}  # type: ignore[attr-defined]

    asyncio.run(scenario())


def test_runtime_event_cancellation_releases_session_key() -> None:
    class CancelOncePublisher:
        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.calls = 0

        async def publish(self, event: RuntimeEvent) -> None:
            del event
            self.calls += 1
            if self.calls == 1:
                self.started.set()
                await asyncio.sleep(60)

    async def scenario() -> None:
        target = CancelOncePublisher()
        producer = RuntimeEventProducerSDK(target)
        common = {
            "tenant_id": "tenant-m5",
            "root_session_id": "session-cancel",
            "session_id": "session-cancel",
            "run_id": "run-cancel",
            "event_type": "runtime.progress",
            "visibility": "user",
        }
        cancelled = asyncio.create_task(
            producer.publish(**common, payload={"step": 1})
        )
        await asyncio.wait_for(target.started.wait(), timeout=1)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        recovered = await producer.publish(**common, payload={"step": 2})
        assert recovered is not None and recovered.sequence == 2
        assert producer._locks._entries == {}  # type: ignore[attr-defined]

    asyncio.run(scenario())


def test_runtime_delta_publish_and_flush_do_not_cross_runs_or_lose_data() -> None:
    async def scenario() -> None:
        bus = ReplayRuntimeEventBus()
        producer = RuntimeEventProducerSDK(bus, delta_flush_bytes=100)
        common = {
            "tenant_id": "tenant-m5",
            "root_session_id": "session-delta-race",
            "session_id": "session-delta-race",
            "visibility": "user",
        }
        await producer.publish(
            **common,
            run_id="run-a",
            event_type="model.output.delta",
            payload={"delta": "a1"},
        )
        publish_a2 = asyncio.create_task(
            producer.publish(
                **common,
                run_id="run-a",
                event_type="model.output.delta",
                payload={"delta": "a2"},
            )
        )
        flush_a = asyncio.create_task(
            producer.flush(
                tenant_id="tenant-m5",
                root_session_id="session-delta-race",
                session_id="session-delta-race",
                run_id="run-a",
            )
        )
        flush_b = asyncio.create_task(
            producer.flush(
                tenant_id="tenant-m5",
                root_session_id="session-delta-race",
                session_id="session-delta-race",
                run_id="run-b",
            )
        )
        await publish_a2
        emitted_a = await flush_a
        emitted_b = await flush_b
        trailing = await producer.flush(
            tenant_id="tenant-m5",
            root_session_id="session-delta-race",
            session_id="session-delta-race",
            run_id="run-a",
        )
        deltas = [
            event.payload["delta"]
            for event in (emitted_a, trailing)
            if event is not None
        ]
        assert "".join(deltas) == "a1a2"
        assert emitted_b is None

    asyncio.run(scenario())


def test_streaming_gateway_authorizes_replays_and_signals_expired_cursor() -> None:
    async def scenario() -> None:
        projection = InMemoryTaskProjection()
        store = InMemoryEventStore()
        relay = OutboxRelay(store, projection)
        service = TaskService(
            event_store=store,
            relay=relay,
            reader=projection,
            admission=AllowAllAdmissionController(),
        )
        response = await service.create_task(
            goal="stream me",
            context=CommandContext(
                command_id="create-stream",
                tenant_id="tenant-m5",
                actor=Actor(type="user", id="user"),
                correlation_id="corr-stream",
                expected_version=0,
                operation="create_task",
            ),
        )
        session_id = str(response["session_id"])
        bus = ReplayRuntimeEventBus(retention_events=2, connection_queue_size=1)
        producer = RuntimeEventProducerSDK(bus, delta_flush_bytes=1)
        for index in range(1, 4):
            await producer.publish(
                tenant_id="tenant-m5",
                root_session_id=session_id,
                session_id=session_id,
                run_id=str(response["run_id"]),
                event_type="runtime.progress",
                payload={"step": index},
                visibility="user",
            )
        gateway = StreamingGateway(reader=projection, bus=bus)
        replay = await gateway.subscribe(
            tenant_id="tenant-m5",
            session_id=session_id,
            last_event_id=f"{session_id}:2",
        )
        assert [event.sequence for event in replay.initial] == [3]
        expired = await gateway.subscribe(
            tenant_id="tenant-m5",
            session_id=session_id,
            last_event_id=f"{session_id}:0",
        )
        assert expired.replay_missed and expired.initial == []
        initial = await gateway.subscribe(
            tenant_id="tenant-m5",
            session_id=session_id,
            last_event_id=None,
        )
        assert [event.sequence for event in initial.initial] == [2, 3]
        initial_events = initial.events()
        assert (await anext(initial_events)).sequence == 2
        await initial_events.aclose()
        slow = await gateway.subscribe(
            tenant_id="tenant-m5",
            session_id=session_id,
            last_event_id=f"{session_id}:3",
        )
        for index in range(4, 7):
            await producer.publish(
                tenant_id="tenant-m5",
                root_session_id=session_id,
                session_id=session_id,
                run_id=str(response["run_id"]),
                event_type="runtime.progress",
                payload={"step": index},
                visibility="user",
            )
        await producer.publish(
            tenant_id="tenant-m5",
            root_session_id=session_id,
            session_id=session_id,
            run_id=str(response["run_id"]),
            event_type="approval.requested",
            payload={"approval_id": "approval-m5"},
            visibility="user",
        )
        slow_events = slow.events()
        important = await asyncio.wait_for(anext(slow_events), timeout=1)
        assert important.type == "approval.requested"
        await slow_events.aclose()
        with pytest.raises(NotFoundError, match="Session not found"):
            await gateway.authorize(tenant_id="foreign", session_id=session_id)

    asyncio.run(scenario())


def test_streaming_gateway_paces_consecutive_model_deltas() -> None:
    class Reader:
        async def get_task(self, tenant_id: str, session_id: str) -> dict[str, str]:
            return {"tenant_id": tenant_id, "session_id": session_id}

    class Subscription:
        initial: list[RuntimeEvent] = []
        replay_missed = False

        async def events(self):  # type: ignore[no-untyped-def]
            for sequence in range(1, 4):
                yield RuntimeEvent(
                    event_id=f"event-{sequence}",
                    tenant_id="tenant-m5",
                    root_session_id="session-m5",
                    session_id="session-m5",
                    run_id="run-m5",
                    sequence=sequence,
                    type="model.output.delta",
                    timestamp=datetime.now(UTC),
                    payload={"delta": str(sequence)},
                    visibility="user",
                )

    class Bus:
        async def subscribe(
            self,
            tenant_id: str,
            session_id: str,
            *,
            after_sequence: int | None = None,
        ) -> Subscription:
            del tenant_id, session_id, after_sequence
            return Subscription()

    async def scenario() -> None:
        interval = 0.02
        gateway = StreamingGateway(
            reader=Reader(),  # type: ignore[arg-type]
            bus=Bus(),  # type: ignore[arg-type]
            delta_min_interval=interval,
        )
        stream = gateway.sse(
            tenant_id="tenant-m5",
            session_id="session-m5",
            last_event_id=None,
        )
        emitted_at: list[float] = []
        try:
            for _ in range(3):
                await anext(stream)
                emitted_at.append(perf_counter())
        finally:
            await stream.aclose()

        gaps = [
            later - earlier
            for earlier, later in zip(emitted_at, emitted_at[1:], strict=False)
        ]
        assert all(gap >= interval * 0.8 for gap in gaps)

    asyncio.run(scenario())


def test_parent_session_sink_dlq_and_circuit_breaker_are_observable() -> None:
    async def scenario() -> None:
        event_store = InMemoryEventStore()
        projection = InMemoryTaskProjection()
        relay = OutboxRelay(event_store, projection)
        tasks = TaskService(
            event_store=event_store,
            relay=relay,
            reader=projection,
            admission=AllowAllAdmissionController(),
        )

        async def create(command: str) -> dict[str, object]:
            return await tasks.create_task(
                goal=command,
                context=CommandContext(
                    command_id=command,
                    tenant_id="tenant-m5-extra",
                    actor=Actor(type="user", id="user"),
                    correlation_id=command,
                    expected_version=0,
                    operation="create_task",
                ),
            )

        parent = await create("parent")
        source = await create("source")
        source_id = str(source["session_id"])
        completed = await event_store.append(
            root_session_id=source_id,
            session_id=source_id,
            run_id=str(source["run_id"]),
            context=CommandContext(
                command_id="source-complete",
                tenant_id="tenant-m5-extra",
                actor=Actor(type="runtime", id="runtime"),
                correlation_id="source",
                expected_version=2,
                operation="runtime.run.completed",
            ),
            events=[NewEvent(type="run.completed", payload={"result_summary": "child done"})],
            command_result={},
        )
        parent_config = ResultSinkConfig(
            sink_id="parent-sink",
            tenant_id="tenant-m5-extra",
            session_id=source_id,
            sink_type="parent_session",
            target_ref=str(parent["session_id"]),
        )
        parent_store = InMemoryDeliveryJobStore()
        parent_job = await parent_store.create_job(completed.events[0], parent_config)
        response = await ParentSessionResultSink(event_store, relay).deliver(
            parent_job, parent_config
        )
        assert response.succeeded
        parent_events = await event_store.load(
            "tenant-m5-extra", str(parent["session_id"])
        )
        assert parent_events[-1].type == "parent.result.received"

        delivery_store = InMemoryDeliveryJobStore()
        retry_config = ResultSinkConfig(
            sink_id="dlq-sink",
            tenant_id="tenant-m5-extra",
            session_id=source_id,
            sink_type="recording",
            target_ref="managed://dlq",
        )
        await delivery_store.register_sink(retry_config)
        retrying = RecordingSink(
            [SinkResponse(False, True, "timeout"), SinkResponse(False, True, "timeout")]
        )
        worker = ResultDeliveryWorker(
            outbox=event_store,
            event_store=event_store,
            relay=relay,
            store=delivery_store,
            adapters=[retrying],
            max_attempts=2,
            base_retry_delay=timedelta(0),
        )
        assert await worker.run_once() == 1
        assert await worker.run_once() == 1
        dlq_job = next(
            job
            for job in await delivery_store.list_jobs("tenant-m5-extra", source_id)
            if job.sink_id == "dlq-sink"
        )
        assert dlq_job.status.value == "dead_lettered"
        view = await projection.get_task("tenant-m5-extra", source_id)
        assert view is not None and view["delivery_status"] == "dead_lettered"

        breaker = CircuitBreaker(failure_threshold=2, reset_after=timedelta(seconds=1))
        now = completed.events[0].occurred_at
        failed = SinkResponse(False, True, "HTTP 503")
        breaker.record("sink", failed, now)
        breaker.record("sink", failed, now)
        assert not breaker.allow("sink", now)
        assert breaker.allow("sink", now + timedelta(seconds=2))

    asyncio.run(scenario())


def test_delivery_worker_recovers_retries_and_deduplicates_business_delivery() -> None:
    async def scenario() -> None:
        event_store = InMemoryEventStore()
        projection = InMemoryTaskProjection()
        relay = OutboxRelay(event_store, projection)
        tasks = TaskService(
            event_store=event_store,
            relay=relay,
            reader=projection,
            admission=AllowAllAdmissionController(),
        )
        accepted = await tasks.create_task(
            goal="deliver reliably",
            context=CommandContext(
                command_id="create-delivery",
                tenant_id="tenant-m5",
                actor=Actor(type="user", id="user"),
                correlation_id="corr-delivery",
                expected_version=0,
                operation="create_task",
            ),
        )
        session_id = str(accepted["session_id"])
        completed = await event_store.append(
            root_session_id=session_id,
            session_id=session_id,
            run_id=str(accepted["run_id"]),
            context=CommandContext(
                command_id="complete-delivery",
                tenant_id="tenant-m5",
                actor=Actor(type="runtime", id="runtime"),
                correlation_id="corr-delivery",
                expected_version=2,
                operation="runtime.run.completed",
            ),
            events=[
                NewEvent(
                    type="run.completed",
                    visibility=Visibility.USER,
                    payload={"result_summary": "done", "artifact_refs": ["artifact://1"]},
                )
            ],
            command_result={"status": "completed"},
        )
        await relay.relay_once()
        store = InMemoryDeliveryJobStore()
        sink_config = ResultSinkConfig(
            sink_id="sink-recording",
            tenant_id="tenant-m5",
            session_id=session_id,
            sink_type="recording",
            target_ref="managed://sink",
        )
        await store.register_sink(sink_config)
        adapter = RecordingSink(
            [
                SinkResponse(False, True, "HTTP 503"),
                SinkResponse(True, summary="HTTP 204"),
            ]
        )
        first_worker = ResultDeliveryWorker(
            outbox=event_store,
            event_store=event_store,
            relay=relay,
            store=store,
            adapters=[adapter],
            base_retry_delay=timedelta(0),
        )
        assert await first_worker.run_once() == 1
        jobs = await store.list_jobs("tenant-m5", session_id)
        assert len(jobs) == 1 and jobs[0].status.value == "retry_wait"

        # A fresh Worker instance recovers the durable retry job after a service stop.
        restarted_worker = ResultDeliveryWorker(
            outbox=event_store,
            event_store=event_store,
            relay=relay,
            store=store,
            adapters=[adapter],
            base_retry_delay=timedelta(0),
        )
        assert await restarted_worker.run_once() == 1
        jobs = await store.list_jobs("tenant-m5", session_id)
        assert jobs[0].status.value == "succeeded"
        assert jobs[0].attempt_count == 2
        assert adapter.calls == [jobs[0].delivery_id, jobs[0].delivery_id]
        assert await store.create_job(completed.events[0], sink_config) == jobs[0]
        assert len(await store.list_jobs("tenant-m5", session_id)) == 1
        view = await projection.get_task("tenant-m5", session_id)
        assert view is not None
        assert view["delivery_status"] == "succeeded"
        assert view["delivery_attempt_count"] == 2
        adapter.responses.append(SinkResponse(True, summary="manual HTTP 204"))
        assert await restarted_worker.redeliver("tenant-m5", jobs[0].delivery_id)
        redelivered = await store.get_job("tenant-m5", jobs[0].delivery_id)
        assert redelivered is not None and redelivered.attempt_count == 3
        assert len(await store.attempts(jobs[0].delivery_id)) == 3
        canonical_types = [
            event.type for event in await event_store.load("tenant-m5", session_id)
        ]
        assert canonical_types.count("delivery.succeeded") == 2

    asyncio.run(scenario())


def test_delivery_sink_circuit_is_shared_and_allows_one_half_open_probe() -> None:
    async def scenario() -> None:
        store = InMemoryDeliveryJobStore()
        sink = ResultSinkConfig(
            sink_id="shared-circuit",
            tenant_id="tenant-circuit",
            session_id="session-circuit",
            sink_type="recording",
            target_ref="managed://shared-circuit",
        )
        await store.register_sink(sink)
        failed = SinkResponse(False, True, "HTTP 503")
        permit = await store.acquire_sink_circuit(
            sink.tenant_id,
            sink.sink_id,
            worker_id="worker-a",
            failure_threshold=2,
            reset_after=timedelta(0),
            probe_ttl=timedelta(seconds=10),
        )
        assert permit.allowed
        await store.record_sink_circuit_result(
            sink.tenant_id,
            sink.sink_id,
            failed,
            failure_threshold=2,
            reset_after=timedelta(0),
            probe_token=permit.probe_token,
        )
        permit = await store.acquire_sink_circuit(
            sink.tenant_id,
            sink.sink_id,
            worker_id="worker-b",
            failure_threshold=2,
            reset_after=timedelta(0),
            probe_ttl=timedelta(seconds=10),
        )
        assert permit.allowed
        opened = await store.record_sink_circuit_result(
            sink.tenant_id,
            sink.sink_id,
            failed,
            failure_threshold=2,
            reset_after=timedelta(0),
            probe_token=permit.probe_token,
        )
        assert opened.state == "open" and opened.failure_count == 2

        permits = await asyncio.gather(
            *(
                store.acquire_sink_circuit(
                    sink.tenant_id,
                    sink.sink_id,
                    worker_id=worker_id,
                    failure_threshold=2,
                    reset_after=timedelta(0),
                    probe_ttl=timedelta(seconds=10),
                )
                for worker_id in ("worker-a", "worker-b")
            )
        )
        probes = [item for item in permits if item.allowed]
        assert len(probes) == 1
        assert probes[0].state == "half_open" and probes[0].probe_token
        assert (await store.get_sink_circuit(sink.tenant_id, sink.sink_id)).probe_owner in {
            "worker-a",
            "worker-b",
        }

        closed = await store.record_sink_circuit_result(
            sink.tenant_id,
            sink.sink_id,
            SinkResponse(True, summary="HTTP 204"),
            failure_threshold=2,
            reset_after=timedelta(0),
            probe_token=probes[0].probe_token,
        )
        assert closed.state == "closed" and closed.failure_count == 0

    asyncio.run(scenario())


def test_webhook_uses_stable_idempotency_and_hmac_signature() -> None:
    async def scenario() -> None:
        captured: dict[str, str] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["idempotency"] = request.headers["Idempotency-Key"]
            captured["signature"] = request.headers["X-AuraClaw-Signature"]
            captured["timestamp"] = request.headers["X-AuraClaw-Timestamp"]
            captured["body"] = request.content.decode()
            return httpx.Response(204)

        store = InMemoryDeliveryJobStore()
        sink = ResultSinkConfig(
            sink_id="signed-webhook",
            tenant_id="tenant-m5",
            session_id="session-m5",
            sink_type="webhook",
            target_ref="https://example.invalid/hook",
            credential_ref="credential://webhook",
        )
        event_store = InMemoryEventStore()
        event = (
            await event_store.append(
                root_session_id="session-m5",
                session_id="session-m5",
                run_id="run-m5",
                context=CommandContext(
                    command_id="event",
                    tenant_id="tenant-m5",
                    actor=Actor(type="runtime", id="runtime"),
                    correlation_id="corr",
                    expected_version=0,
                    operation="test.event",
                ),
                events=[NewEvent(type="run.completed", payload={"result_summary": "ok"})],
                command_result={},
            )
        ).events[0]
        job = await store.create_job(event, sink)
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = WebhookResultSink(
            StaticDeliverySecretResolver({("tenant-m5", "credential://webhook"): "secret"}),
            client=client,
        )
        try:
            response = await adapter.deliver(job, sink)
        finally:
            await client.aclose()
        assert response.succeeded
        assert captured["idempotency"] == job.delivery_id
        body = captured["body"].encode()
        expected = hmac.new(
            b"secret",
            captured["timestamp"].encode() + b"." + body,
            hashlib.sha256,
        ).hexdigest()
        assert captured["signature"] == f"sha256={expected}"
        assert json.loads(captured["body"])["delivery_id"] == job.delivery_id

    asyncio.run(scenario())
