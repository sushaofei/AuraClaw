from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from auraclaw.contracts.delivery import (
    DeliveryAttempt,
    DeliveryJob,
    ResultSinkConfig,
    SinkResponse,
)
from auraclaw.contracts.events import CanonicalEvent


@dataclass(frozen=True)
class SinkCircuitPermit:
    allowed: bool
    state: str
    generation: int
    probe_token: str | None = None


@dataclass(frozen=True)
class SinkCircuitSnapshot:
    state: str
    failure_count: int
    generation: int
    open_until: datetime | None = None
    probe_owner: str | None = None


class DeliveryOutboxItem(Protocol):
    outbox_id: int
    event: CanonicalEvent


class DeliveryOutboxSource(Protocol):
    async def pending_delivery_outbox(self) -> Sequence[DeliveryOutboxItem]: ...

    async def mark_outbox_published(self, outbox_id: int) -> None: ...

    async def mark_outbox_failed(self, outbox_id: int) -> None: ...


class DeliveryStore(Protocol):
    async def register_sink(self, sink: ResultSinkConfig) -> None: ...

    async def get_sink(self, tenant_id: str, sink_id: str) -> ResultSinkConfig | None: ...

    async def list_sinks(
        self, tenant_id: str, session_id: str, event_type: str
    ) -> list[ResultSinkConfig]: ...

    async def create_job(self, event: CanonicalEvent, sink: ResultSinkConfig) -> DeliveryJob: ...

    async def claim_due(
        self, *, worker_id: str, claim_ttl: timedelta, limit: int
    ) -> list[DeliveryJob]: ...

    async def record_attempt(
        self,
        job: DeliveryJob,
        response: SinkResponse,
        *,
        next_attempt_at: datetime | None,
        max_attempts: int,
    ) -> DeliveryJob: ...

    async def get_job(self, tenant_id: str, delivery_id: str) -> DeliveryJob | None: ...

    async def list_jobs(self, tenant_id: str, session_id: str) -> list[DeliveryJob]: ...

    async def attempts(self, delivery_id: str) -> list[DeliveryAttempt]: ...

    async def begin_redelivery(
        self,
        tenant_id: str,
        delivery_id: str,
        *,
        worker_id: str,
        claim_ttl: timedelta,
    ) -> DeliveryJob | None: ...

    async def acquire_sink_circuit(
        self,
        tenant_id: str,
        sink_id: str,
        *,
        worker_id: str,
        failure_threshold: int,
        reset_after: timedelta,
        probe_ttl: timedelta,
    ) -> SinkCircuitPermit: ...

    async def record_sink_circuit_result(
        self,
        tenant_id: str,
        sink_id: str,
        response: SinkResponse,
        *,
        failure_threshold: int,
        reset_after: timedelta,
        probe_token: str | None,
    ) -> SinkCircuitSnapshot: ...

    async def get_sink_circuit(
        self, tenant_id: str, sink_id: str
    ) -> SinkCircuitSnapshot | None: ...


class DeliverySecretResolver(Protocol):
    async def resolve(self, tenant_id: str, credential_ref: str) -> str: ...


class ResultSinkAdapter(Protocol):
    sink_type: str

    async def deliver(
        self, job: DeliveryJob, config: ResultSinkConfig
    ) -> SinkResponse: ...
