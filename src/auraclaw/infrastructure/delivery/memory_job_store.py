from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from uuid import uuid4

from auraclaw.contracts.delivery import (
    DeliveryAttempt,
    DeliveryJob,
    DeliveryStatus,
    ResultSinkConfig,
    SinkResponse,
)
from auraclaw.contracts.events import CanonicalEvent, utc_now
from auraclaw.delivery.ports import SinkCircuitPermit, SinkCircuitSnapshot
from auraclaw.infrastructure.delivery.common import build_delivery_job


@dataclass
class _MemoryCircuit:
    state: str = "closed"
    failure_count: int = 0
    open_until: datetime | None = None
    generation: int = 0
    probe_owner: str | None = None
    probe_token: str | None = None
    probe_expires_at: datetime | None = None


class InMemoryDeliveryJobStore:
    def __init__(self) -> None:
        self._sinks: dict[tuple[str, str], ResultSinkConfig] = {}
        self._jobs: dict[str, DeliveryJob] = {}
        self._attempts: list[DeliveryAttempt] = []
        self._circuits: dict[tuple[str, str], _MemoryCircuit] = {}
        self._lock = asyncio.Lock()

    async def register_sink(self, sink: ResultSinkConfig) -> None:
        async with self._lock:
            self._sinks[(sink.tenant_id, sink.sink_id)] = sink

    async def get_sink(self, tenant_id: str, sink_id: str) -> ResultSinkConfig | None:
        return self._sinks.get((tenant_id, sink_id))

    async def list_sinks(
        self, tenant_id: str, session_id: str, event_type: str
    ) -> list[ResultSinkConfig]:
        return [
            sink
            for (item_tenant, _), sink in self._sinks.items()
            if item_tenant == tenant_id
            and sink.session_id == session_id
            and sink.enabled
            and event_type in sink.event_types
        ]

    async def create_job(self, event: CanonicalEvent, sink: ResultSinkConfig) -> DeliveryJob:
        job = build_delivery_job(event, sink)
        async with self._lock:
            return self._jobs.setdefault(job.delivery_id, job)

    async def claim_due(
        self,
        *,
        worker_id: str,
        claim_ttl: timedelta,
        limit: int,
        max_per_tenant: int | None = None,
    ) -> list[DeliveryJob]:
        now = utc_now()
        async with self._lock:
            active = sorted(
                (
                    job
                    for job in self._jobs.values()
                    if job.status
                    in {
                        DeliveryStatus.PENDING,
                        DeliveryStatus.RETRY_WAIT,
                        DeliveryStatus.ATTEMPTING,
                    }
                ),
                key=lambda job: (job.created_at, job.delivery_id),
            )
            due: list[DeliveryJob] = []
            blocked: set[tuple[str, str, str]] = set()
            tenant_claims: dict[str, int] = {}
            for job in active:
                order_key = (job.tenant_id, job.session_id, job.sink_id)
                if order_key in blocked:
                    continue
                blocked.add(order_key)
                claim_expired = (
                    job.status is DeliveryStatus.ATTEMPTING
                    and job.claim_expires_at is not None
                    and job.claim_expires_at <= now
                )
                available = job.status in {
                    DeliveryStatus.PENDING,
                    DeliveryStatus.RETRY_WAIT,
                } or claim_expired
                if (
                    max_per_tenant is not None
                    and tenant_claims.get(job.tenant_id, 0) >= max_per_tenant
                ):
                    continue
                if available and (
                    job.next_attempt_at is None or job.next_attempt_at <= now
                ):
                    due.append(job)
                    tenant_claims[job.tenant_id] = (
                        tenant_claims.get(job.tenant_id, 0) + 1
                    )
                    if len(due) >= limit:
                        break
            claimed: list[DeliveryJob] = []
            for job in due:
                updated = replace(
                    job,
                    status=DeliveryStatus.ATTEMPTING,
                    attempt_count=job.attempt_count + 1,
                    claimed_by=worker_id,
                    claim_token=uuid4().hex,
                    claim_expires_at=now + claim_ttl,
                    claim_heartbeat_at=now,
                    side_effect_started_at=None,
                    reconciliation_reason=None,
                )
                self._jobs[job.delivery_id] = updated
                claimed.append(updated)
            return claimed

    async def renew_claim(
        self, job: DeliveryJob, *, claim_ttl: timedelta
    ) -> bool:
        now = utc_now()
        async with self._lock:
            current = self._jobs.get(job.delivery_id)
            if (
                current is None
                or current.claimed_by != job.claimed_by
                or current.claim_token != job.claim_token
                or current.claim_expires_at is None
                or current.claim_expires_at <= now
                or current.status is not DeliveryStatus.ATTEMPTING
            ):
                return False
            self._jobs[job.delivery_id] = replace(
                current,
                claim_expires_at=now + claim_ttl,
                claim_heartbeat_at=now,
            )
            return True

    async def begin_side_effect(self, job: DeliveryJob) -> bool:
        now = utc_now()
        async with self._lock:
            current = self._jobs.get(job.delivery_id)
            if (
                current is None
                or current.claimed_by != job.claimed_by
                or current.claim_token != job.claim_token
                or current.claim_expires_at is None
                or current.claim_expires_at <= now
                or current.status is not DeliveryStatus.ATTEMPTING
            ):
                return False
            self._jobs[job.delivery_id] = replace(
                current, side_effect_started_at=now
            )
            return True

    async def mark_reconciling(self, job: DeliveryJob, *, reason: str) -> bool:
        async with self._lock:
            current = self._jobs.get(job.delivery_id)
            if (
                current is None
                or current.claimed_by != job.claimed_by
                or current.claim_token != job.claim_token
                or current.side_effect_started_at is None
            ):
                return False
            self._jobs[job.delivery_id] = replace(
                current,
                status=DeliveryStatus.RECONCILING,
                reconciliation_reason=reason[:128],
                claimed_by=None,
                claim_token=None,
                claim_expires_at=None,
            )
            return True

    async def record_attempt(
        self,
        job: DeliveryJob,
        response: SinkResponse,
        *,
        next_attempt_at: datetime | None,
        max_attempts: int,
    ) -> DeliveryJob:
        completed_at = utc_now()
        async with self._lock:
            current = self._jobs.get(job.delivery_id)
            if (
                current is None
                or current.claimed_by != job.claimed_by
                or current.claim_token != job.claim_token
                or current.claim_expires_at is None
                or current.claim_expires_at <= completed_at
            ):
                raise RuntimeError("delivery claim is no longer valid")
            if response.succeeded:
                status = DeliveryStatus.SUCCEEDED
            elif response.retryable and current.attempt_count < max_attempts:
                status = DeliveryStatus.RETRY_WAIT
            elif response.retryable:
                status = DeliveryStatus.DEAD_LETTERED
            else:
                status = DeliveryStatus.FAILED
            updated = replace(
                current,
                status=status,
                next_attempt_at=(
                    next_attempt_at if status is DeliveryStatus.RETRY_WAIT else None
                ),
                last_response_summary=response.summary[:2_000],
                completed_at=(
                    completed_at
                    if status
                    in {
                        DeliveryStatus.SUCCEEDED,
                        DeliveryStatus.FAILED,
                        DeliveryStatus.DEAD_LETTERED,
                    }
                    else None
                ),
                claimed_by=None,
                claim_token=None,
                claim_expires_at=None,
                claim_heartbeat_at=None,
            )
            attempt = DeliveryAttempt(
                delivery_id=job.delivery_id,
                attempt_number=current.attempt_count,
                started_at=completed_at,
                completed_at=completed_at,
                outcome=status.value,
                response_summary=response.summary[:2_000],
                retryable=response.retryable,
            )
            self._jobs[job.delivery_id] = updated
            self._attempts.append(attempt)
        return updated

    async def get_job(self, tenant_id: str, delivery_id: str) -> DeliveryJob | None:
        job = self._jobs.get(delivery_id)
        return job if job is not None and job.tenant_id == tenant_id else None

    async def list_jobs(self, tenant_id: str, session_id: str) -> list[DeliveryJob]:
        return [
            job
            for job in self._jobs.values()
            if job.tenant_id == tenant_id and job.session_id == session_id
        ]

    async def attempts(self, delivery_id: str) -> list[DeliveryAttempt]:
        return [attempt for attempt in self._attempts if attempt.delivery_id == delivery_id]

    async def begin_redelivery(
        self,
        tenant_id: str,
        delivery_id: str,
        *,
        worker_id: str,
        claim_ttl: timedelta,
    ) -> DeliveryJob | None:
        async with self._lock:
            job = self._jobs.get(delivery_id)
            if job is None or job.tenant_id != tenant_id:
                return None
            now = utc_now()
            updated = replace(
                job,
                status=DeliveryStatus.ATTEMPTING,
                attempt_count=job.attempt_count + 1,
                next_attempt_at=None,
                completed_at=None,
                claimed_by=worker_id,
                claim_token=uuid4().hex,
                claim_expires_at=now + claim_ttl,
                claim_heartbeat_at=now,
                side_effect_started_at=None,
                reconciliation_reason=None,
            )
            self._jobs[delivery_id] = updated
            return updated

    async def acquire_sink_circuit(
        self,
        tenant_id: str,
        sink_id: str,
        *,
        worker_id: str,
        failure_threshold: int,
        reset_after: timedelta,
        probe_ttl: timedelta,
    ) -> SinkCircuitPermit:
        del failure_threshold, reset_after
        now = utc_now()
        async with self._lock:
            circuit = self._circuits.setdefault((tenant_id, sink_id), _MemoryCircuit())
            if circuit.state == "closed":
                return SinkCircuitPermit(True, "closed", circuit.generation)
            if circuit.state == "open" and (
                circuit.open_until is None or circuit.open_until > now
            ):
                return SinkCircuitPermit(False, "open", circuit.generation)
            if circuit.state == "half_open" and (
                circuit.probe_expires_at is None or circuit.probe_expires_at > now
            ):
                return SinkCircuitPermit(False, "half_open", circuit.generation)
            token = uuid4().hex
            circuit.state = "half_open"
            circuit.generation += 1
            circuit.probe_owner = worker_id
            circuit.probe_token = token
            circuit.probe_expires_at = now + probe_ttl
            return SinkCircuitPermit(
                True, "half_open", circuit.generation, probe_token=token
            )

    async def record_sink_circuit_result(
        self,
        tenant_id: str,
        sink_id: str,
        response: SinkResponse,
        *,
        failure_threshold: int,
        reset_after: timedelta,
        probe_token: str | None,
    ) -> SinkCircuitSnapshot:
        now = utc_now()
        async with self._lock:
            circuit = self._circuits.setdefault((tenant_id, sink_id), _MemoryCircuit())
            if circuit.state == "half_open" and circuit.probe_token != probe_token:
                return self._circuit_snapshot(circuit)
            if response.succeeded:
                changed = circuit.state != "closed" or circuit.failure_count != 0
                circuit.state = "closed"
                circuit.failure_count = 0
                circuit.open_until = None
                circuit.probe_owner = None
                circuit.probe_token = None
                circuit.probe_expires_at = None
                circuit.generation += int(changed)
            elif response.retryable:
                circuit.failure_count += 1
                if (
                    circuit.state == "half_open"
                    or circuit.failure_count >= failure_threshold
                ):
                    circuit.state = "open"
                    circuit.failure_count = max(
                        circuit.failure_count, failure_threshold
                    )
                    circuit.open_until = now + reset_after
                    circuit.probe_owner = None
                    circuit.probe_token = None
                    circuit.probe_expires_at = None
                    circuit.generation += 1
            return self._circuit_snapshot(circuit)

    async def get_sink_circuit(
        self, tenant_id: str, sink_id: str
    ) -> SinkCircuitSnapshot | None:
        circuit = self._circuits.get((tenant_id, sink_id))
        return None if circuit is None else self._circuit_snapshot(circuit)

    @staticmethod
    def _circuit_snapshot(circuit: _MemoryCircuit) -> SinkCircuitSnapshot:
        return SinkCircuitSnapshot(
            state=circuit.state,
            failure_count=circuit.failure_count,
            generation=circuit.generation,
            open_until=circuit.open_until,
            probe_owner=circuit.probe_owner,
        )
