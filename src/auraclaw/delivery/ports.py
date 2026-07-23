from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Protocol

from auraclaw.contracts.delivery import (
    DeliveryAttempt,
    DeliveryJob,
    ResultSinkConfig,
    SinkResponse,
)
from auraclaw.contracts.events import CanonicalEvent


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


class DeliverySecretResolver(Protocol):
    async def resolve(self, tenant_id: str, credential_ref: str) -> str: ...


class ResultSinkAdapter(Protocol):
    sink_type: str

    async def deliver(
        self, job: DeliveryJob, config: ResultSinkConfig
    ) -> SinkResponse: ...
