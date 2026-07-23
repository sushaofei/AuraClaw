from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.delivery import DeliveryJob, DeliveryStatus, SinkResponse
from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.events import Actor, NewEvent
from auraclaw.contracts.state import Visibility
from auraclaw.delivery.ports import (
    DeliveryOutboxSource,
    DeliveryStore,
    ResultSinkAdapter,
)
from auraclaw.session.ports import EventStore, OutboxRelayPort


class CircuitBreaker:
    def __init__(
        self,
        *,
        failure_threshold: int = 3,
        reset_after: timedelta = timedelta(seconds=30),
    ) -> None:
        self._failure_threshold = failure_threshold
        self._reset_after = reset_after
        self._failures: dict[str, int] = {}
        self._opened_at: dict[str, datetime] = {}

    def allow(self, sink_id: str, now: datetime) -> bool:
        opened = self._opened_at.get(sink_id)
        if opened is None:
            return True
        if now - opened >= self._reset_after:
            self._opened_at.pop(sink_id, None)
            self._failures[sink_id] = 0
            return True
        return False

    def record(self, sink_id: str, response: SinkResponse, now: datetime) -> None:
        if response.succeeded:
            self._failures[sink_id] = 0
            self._opened_at.pop(sink_id, None)
            return
        if not response.retryable:
            return
        failures = self._failures.get(sink_id, 0) + 1
        self._failures[sink_id] = failures
        if failures >= self._failure_threshold:
            self._opened_at[sink_id] = now


class ResultDeliveryWorker:
    """Creates idempotent jobs from Outbox and executes recoverable sink delivery."""

    def __init__(
        self,
        *,
        outbox: DeliveryOutboxSource,
        event_store: EventStore,
        relay: OutboxRelayPort,
        store: DeliveryStore,
        adapters: Sequence[ResultSinkAdapter],
        max_attempts: int = 5,
        base_retry_delay: timedelta = timedelta(seconds=1),
        circuit_breaker: CircuitBreaker | None = None,
        worker_id: str | None = None,
        claim_ttl: timedelta = timedelta(seconds=30),
    ) -> None:
        self._outbox = outbox
        self._event_store = event_store
        self._relay = relay
        self._store = store
        self._adapters = {adapter.sink_type: adapter for adapter in adapters}
        self._max_attempts = max_attempts
        self._base_retry_delay = base_retry_delay
        self._circuit = circuit_breaker or CircuitBreaker()
        self._worker_id = worker_id or f"delivery-{uuid4().hex}"
        self._claim_ttl = claim_ttl

    async def ingest_once(self, *, limit: int = 100) -> int:
        ingested = 0
        records = await self._outbox.pending_delivery_outbox()
        for record in records[:limit]:
            try:
                sinks = await self._store.list_sinks(
                    record.event.tenant_id,
                    record.event.session_id,
                    record.event.type,
                )
                for sink in sinks:
                    await self._store.create_job(record.event, sink)
            except Exception:
                await self._outbox.mark_outbox_failed(record.outbox_id)
                continue
            await self._outbox.mark_outbox_published(record.outbox_id)
            ingested += 1
        return ingested

    async def run_once(self, *, limit: int = 100) -> int:
        await self.ingest_once(limit=limit)
        jobs = await self._store.claim_due(
            worker_id=self._worker_id,
            claim_ttl=self._claim_ttl,
            limit=limit,
        )
        for job in jobs:
            await self._deliver(job)
        return len(jobs)

    async def redeliver(self, tenant_id: str, delivery_id: str) -> bool:
        job = await self._store.begin_redelivery(
            tenant_id,
            delivery_id,
            worker_id=self._worker_id,
            claim_ttl=self._claim_ttl,
        )
        if job is None:
            return False
        await self._deliver(job)
        return True

    async def _deliver(self, job: DeliveryJob) -> None:
        config = await self._store.get_sink(job.tenant_id, job.sink_id)
        adapter = self._adapters.get(job.sink_type)
        now = datetime.now(UTC)
        if config is None or adapter is None:
            response = SinkResponse(False, False, "delivery adapter or sink config not found")
        elif not self._circuit.allow(job.sink_id, now):
            response = SinkResponse(False, True, "sink circuit is open")
        else:
            await self._write_status(job, DeliveryStatus.ATTEMPTING, "attempting")
            try:
                response = await adapter.deliver(job, config)
            except Exception as exc:
                response = SinkResponse(False, True, type(exc).__name__)
        self._circuit.record(job.sink_id, response, now)
        delay = self._base_retry_delay * (2 ** max(0, job.attempt_count - 1))
        updated = await self._store.record_attempt(
            job,
            response,
            next_attempt_at=now + delay,
            max_attempts=self._max_attempts,
        )
        await self._write_status(updated, updated.status, response.summary)

    async def _write_status(
        self, job: DeliveryJob, status: DeliveryStatus, summary: str
    ) -> None:
        event_type = {
            DeliveryStatus.ATTEMPTING: "delivery.attempting",
            DeliveryStatus.RETRY_WAIT: "delivery.retrying",
            DeliveryStatus.SUCCEEDED: "delivery.succeeded",
            DeliveryStatus.FAILED: "delivery.failed",
            DeliveryStatus.DEAD_LETTERED: "delivery.dead_lettered",
        }.get(status)
        if event_type is None:
            return
        for _ in range(5):
            events = await self._event_store.load(job.tenant_id, job.session_id)
            if not events:
                return
            try:
                result = await self._event_store.append(
                    root_session_id=job.root_session_id,
                    session_id=job.session_id,
                    run_id=job.run_id,
                    context=CommandContext(
                        command_id=(
                            f"delivery:{job.delivery_id}:{job.attempt_count}:{status.value}"
                        ),
                        tenant_id=job.tenant_id,
                        actor=Actor(type="delivery", id="result-delivery-worker"),
                        correlation_id=job.delivery_id,
                        expected_version=len(events),
                        operation=f"delivery.{status.value}",
                    ),
                    events=[
                        NewEvent(
                            type=event_type,
                            visibility=Visibility.USER,
                            payload={
                                "delivery_id": job.delivery_id,
                                "sink_id": job.sink_id,
                                "status": status.value,
                                "attempt_count": job.attempt_count,
                                "response_summary": summary[:2_000],
                            },
                        )
                    ],
                    command_result={
                        "delivery_id": job.delivery_id,
                        "status": status.value,
                    },
                )
            except VersionConflictError:
                continue
            if not result.deduplicated:
                await self._relay.relay_once()
            return
        raise VersionConflictError(
            f"could not append delivery status after concurrent Session writes: {job.delivery_id}"
        )
