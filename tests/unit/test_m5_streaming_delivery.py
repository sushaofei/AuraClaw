import asyncio
import hashlib
import hmac
import json
from datetime import timedelta

import httpx
import pytest

from auraclaw.application.delivery import CircuitBreaker, ResultDeliveryWorker
from auraclaw.application.streaming import StreamingGateway
from auraclaw.application.tasks import AllowAllAdmissionController, TaskService
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.delivery import DeliveryJob, ResultSinkConfig, SinkResponse
from auraclaw.contracts.errors import NotFoundError
from auraclaw.contracts.events import Actor, NewEvent
from auraclaw.contracts.state import Visibility
from auraclaw.infrastructure.delivery import (
    InMemoryDeliveryJobStore,
    ParentSessionResultSink,
    StaticDeliverySecretResolver,
    WebhookResultSink,
)
from auraclaw.infrastructure.memory import InMemoryEventStore
from auraclaw.infrastructure.runtime_events import (
    ReplayRuntimeEventBus,
    RuntimeEventProducerSDK,
    RuntimeEventRejectedError,
)
from auraclaw.projections.relay import OutboxRelay
from auraclaw.projections.tasks import InMemoryTaskProjection


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
