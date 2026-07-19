from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from dataclasses import replace
from datetime import UTC, datetime

import asyncpg  # type: ignore[import-untyped]
import httpx

from auraclaw.application.delivery import DeliverySecretResolver
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.delivery import (
    DeliveryAttempt,
    DeliveryJob,
    DeliveryStatus,
    ResultSinkConfig,
    SinkResponse,
)
from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.events import Actor, CanonicalEvent, NewEvent, utc_now
from auraclaw.contracts.state import Visibility
from auraclaw.domain.ports import EventStore, OutboxRelayPort
from auraclaw.infrastructure.postgres import _decode_json, _json, _LazyPool


def delivery_id_for(event_id: str, sink_id: str) -> str:
    digest = hashlib.sha256(f"{event_id}:{sink_id}".encode()).hexdigest()
    return f"del_{digest[:32]}"


def build_delivery_job(event: CanonicalEvent, sink: ResultSinkConfig) -> DeliveryJob:
    return DeliveryJob(
        delivery_id=delivery_id_for(event.event_id, sink.sink_id),
        event_id=event.event_id,
        tenant_id=event.tenant_id,
        root_session_id=event.root_session_id,
        session_id=event.session_id,
        run_id=event.run_id,
        sink_id=sink.sink_id,
        sink_type=sink.sink_type,
        sink_target_ref=sink.target_ref,
        payload={
            "delivery_id": delivery_id_for(event.event_id, sink.sink_id),
            "event_id": event.event_id,
            "tenant_id": event.tenant_id,
            "root_session_id": event.root_session_id,
            "session_id": event.session_id,
            "run_id": event.run_id,
            "event_type": event.type,
            "occurred_at": event.occurred_at.isoformat(),
            "result_summary": event.payload.get("result_summary"),
            "result_ref": event.payload.get("result_ref"),
            "artifact_refs": event.payload.get("artifact_refs", []),
            "error": event.payload.get("error"),
        },
        status=DeliveryStatus.PENDING,
        attempt_count=0,
        next_attempt_at=utc_now(),
        last_response_summary=None,
        created_at=utc_now(),
    )


class StaticDeliverySecretResolver:
    """Test adapter. Production implementations resolve refs through Credential Proxy."""

    def __init__(self, secrets: dict[tuple[str, str], str]) -> None:
        self._secrets = secrets

    async def resolve(self, tenant_id: str, credential_ref: str) -> str:
        return self._secrets[(tenant_id, credential_ref)]


class WebhookResultSink:
    sink_type = "webhook"

    def __init__(
        self,
        secrets: DeliverySecretResolver,
        *,
        client: httpx.AsyncClient | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._secrets = secrets
        self._client = client
        self._timeout = timeout

    async def deliver(self, job: DeliveryJob, config: ResultSinkConfig) -> SinkResponse:
        if config.credential_ref is None:
            return SinkResponse(False, False, "webhook credential_ref is required")
        secret = await self._secrets.resolve(job.tenant_id, config.credential_ref)
        body = json.dumps(job.payload, separators=(",", ":"), sort_keys=True).encode()
        timestamp = str(int(datetime.now(UTC).timestamp()))
        signature = hmac.new(
            secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256
        ).hexdigest()
        headers = {
            "Content-Type": "application/json",
            "Idempotency-Key": job.delivery_id,
            "X-AuraClaw-Timestamp": timestamp,
            "X-AuraClaw-Signature": f"sha256={signature}",
        }
        try:
            if self._client is not None:
                response = await self._client.post(
                    config.target_ref, content=body, headers=headers, timeout=self._timeout
                )
            else:
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        config.target_ref, content=body, headers=headers, timeout=self._timeout
                    )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            return SinkResponse(False, True, type(exc).__name__)
        if 200 <= response.status_code < 300:
            return SinkResponse(True, summary=f"HTTP {response.status_code}")
        retryable = response.status_code == 429 or response.status_code >= 500
        return SinkResponse(False, retryable, f"HTTP {response.status_code}")


class ParentSessionResultSink:
    sink_type = "parent_session"

    def __init__(self, event_store: EventStore, relay: OutboxRelayPort) -> None:
        self._event_store = event_store
        self._relay = relay

    async def deliver(self, job: DeliveryJob, config: ResultSinkConfig) -> SinkResponse:
        for _ in range(5):
            parent_events = await self._event_store.load(job.tenant_id, config.target_ref)
            if not parent_events:
                return SinkResponse(False, False, "parent Session not found")
            first = parent_events[0]
            try:
                result = await self._event_store.append(
                    root_session_id=first.root_session_id,
                    session_id=config.target_ref,
                    run_id=first.run_id,
                    context=CommandContext(
                        command_id=f"parent-delivery:{job.delivery_id}",
                        tenant_id=job.tenant_id,
                        actor=Actor(type="delivery", id="parent-session-sink"),
                        correlation_id=job.delivery_id,
                        expected_version=len(parent_events),
                        operation="delivery.parent_session",
                    ),
                    events=[
                        NewEvent(
                            type="parent.result.received",
                            visibility=Visibility.INTERNAL,
                            payload={
                                "delivery_id": job.delivery_id,
                                "source_session_id": job.session_id,
                                "result_summary": job.payload.get("result_summary"),
                                "result_ref": job.payload.get("result_ref"),
                                "artifact_refs": job.payload.get("artifact_refs", []),
                            },
                        )
                    ],
                    command_result={"delivery_id": job.delivery_id},
                )
            except VersionConflictError:
                continue
            if not result.deduplicated:
                await self._relay.relay_once()
            return SinkResponse(True, summary="parent Session acknowledged")
        return SinkResponse(False, True, "parent Session write conflict")


class InMemoryDeliveryJobStore:
    def __init__(self) -> None:
        self._sinks: dict[tuple[str, str], ResultSinkConfig] = {}
        self._jobs: dict[str, DeliveryJob] = {}
        self._attempts: list[DeliveryAttempt] = []
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

    async def claim_due(self, *, limit: int) -> list[DeliveryJob]:
        now = utc_now()
        async with self._lock:
            due = sorted(
                (
                    job
                    for job in self._jobs.values()
                    if job.status in {DeliveryStatus.PENDING, DeliveryStatus.RETRY_WAIT}
                    and (job.next_attempt_at is None or job.next_attempt_at <= now)
                ),
                key=lambda job: (job.next_attempt_at or job.created_at, job.delivery_id),
            )[:limit]
            claimed: list[DeliveryJob] = []
            for job in due:
                updated = replace(
                    job,
                    status=DeliveryStatus.ATTEMPTING,
                    attempt_count=job.attempt_count + 1,
                )
                self._jobs[job.delivery_id] = updated
                claimed.append(updated)
            return claimed

    async def record_attempt(
        self,
        job: DeliveryJob,
        response: SinkResponse,
        *,
        next_attempt_at: datetime | None,
        max_attempts: int,
    ) -> DeliveryJob:
        completed_at = utc_now()
        if response.succeeded:
            status = DeliveryStatus.SUCCEEDED
        elif response.retryable and job.attempt_count < max_attempts:
            status = DeliveryStatus.RETRY_WAIT
        elif response.retryable:
            status = DeliveryStatus.DEAD_LETTERED
        else:
            status = DeliveryStatus.FAILED
        updated = replace(
            job,
            status=status,
            next_attempt_at=next_attempt_at if status is DeliveryStatus.RETRY_WAIT else None,
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
        )
        attempt = DeliveryAttempt(
            delivery_id=job.delivery_id,
            attempt_number=job.attempt_count,
            started_at=completed_at,
            completed_at=completed_at,
            outcome=status.value,
            response_summary=response.summary[:2_000],
            retryable=response.retryable,
        )
        async with self._lock:
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
        self, tenant_id: str, delivery_id: str
    ) -> DeliveryJob | None:
        async with self._lock:
            job = self._jobs.get(delivery_id)
            if job is None or job.tenant_id != tenant_id:
                return None
            updated = replace(
                job,
                status=DeliveryStatus.ATTEMPTING,
                attempt_count=job.attempt_count + 1,
                next_attempt_at=None,
                completed_at=None,
            )
            self._jobs[delivery_id] = updated
            return updated


class PostgresDeliveryJobStore(_LazyPool):
    async def register_sink(self, sink: ResultSinkConfig) -> None:
        pool = await self.pool()
        await pool.execute(
            """INSERT INTO delivery.sink_config
            (sink_id, tenant_id, session_id, sink_type, target_ref, credential_ref,
             event_types, enabled)
            VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8)
            ON CONFLICT (tenant_id, sink_id) DO UPDATE SET
              session_id=EXCLUDED.session_id, sink_type=EXCLUDED.sink_type,
              target_ref=EXCLUDED.target_ref, credential_ref=EXCLUDED.credential_ref,
              event_types=EXCLUDED.event_types, enabled=EXCLUDED.enabled,
              updated_at=now()""",
            sink.sink_id,
            sink.tenant_id,
            sink.session_id,
            sink.sink_type,
            sink.target_ref,
            sink.credential_ref,
            _json(sink.event_types),
            sink.enabled,
        )

    async def get_sink(self, tenant_id: str, sink_id: str) -> ResultSinkConfig | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            "SELECT * FROM delivery.sink_config WHERE tenant_id=$1 AND sink_id=$2",
            tenant_id,
            sink_id,
        )
        return self._sink(row) if row is not None else None

    async def list_sinks(
        self, tenant_id: str, session_id: str, event_type: str
    ) -> list[ResultSinkConfig]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT * FROM delivery.sink_config
            WHERE tenant_id=$1 AND session_id=$2 AND enabled
              AND event_types ? $3 ORDER BY sink_id""",
            tenant_id,
            session_id,
            event_type,
        )
        return [self._sink(row) for row in rows]

    async def create_job(self, event: CanonicalEvent, sink: ResultSinkConfig) -> DeliveryJob:
        job = build_delivery_job(event, sink)
        pool = await self.pool()
        await pool.execute(
            """INSERT INTO delivery.delivery_job
            (delivery_id,event_id,tenant_id,root_session_id,session_id,run_id,sink_id,
             sink_type,sink_target_ref,payload_ref,status,attempt_count,next_attempt_at,
             created_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14)
            ON CONFLICT (delivery_id) DO NOTHING""",
            job.delivery_id,
            job.event_id,
            job.tenant_id,
            job.root_session_id,
            job.session_id,
            job.run_id,
            job.sink_id,
            job.sink_type,
            job.sink_target_ref,
            _json(job.payload),
            job.status.value,
            job.attempt_count,
            job.next_attempt_at,
            job.created_at,
        )
        stored = await self.get_job(job.tenant_id, job.delivery_id)
        assert stored is not None
        return stored

    async def claim_due(self, *, limit: int) -> list[DeliveryJob]:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            rows = await connection.fetch(
                """SELECT * FROM delivery.delivery_job
                WHERE status IN ('pending','retry_wait') AND next_attempt_at <= now()
                ORDER BY next_attempt_at, delivery_id FOR UPDATE SKIP LOCKED LIMIT $1""",
                limit,
            )
            if not rows:
                return []
            ids = [str(row["delivery_id"]) for row in rows]
            updated = await connection.fetch(
                """UPDATE delivery.delivery_job SET status='attempting',
                attempt_count=attempt_count+1 WHERE delivery_id=ANY($1::text[])
                RETURNING *""",
                ids,
            )
            return [self._job(row) for row in updated]

    async def record_attempt(
        self,
        job: DeliveryJob,
        response: SinkResponse,
        *,
        next_attempt_at: datetime | None,
        max_attempts: int,
    ) -> DeliveryJob:
        if response.succeeded:
            status = DeliveryStatus.SUCCEEDED
        elif response.retryable and job.attempt_count < max_attempts:
            status = DeliveryStatus.RETRY_WAIT
        elif response.retryable:
            status = DeliveryStatus.DEAD_LETTERED
        else:
            status = DeliveryStatus.FAILED
        now = utc_now()
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            row = await connection.fetchrow(
                """UPDATE delivery.delivery_job SET status=$2,next_attempt_at=$3,
                last_response_summary=$4,completed_at=$5 WHERE delivery_id=$1 RETURNING *""",
                job.delivery_id,
                status.value,
                next_attempt_at if status is DeliveryStatus.RETRY_WAIT else None,
                response.summary[:2_000],
                now
                if status
                in {
                    DeliveryStatus.SUCCEEDED,
                    DeliveryStatus.FAILED,
                    DeliveryStatus.DEAD_LETTERED,
                }
                else None,
            )
            await connection.execute(
                """INSERT INTO delivery.delivery_attempt
                (delivery_id,attempt_number,started_at,completed_at,outcome,
                 response_summary,retryable)
                VALUES ($1,$2,$3,$4,$5,$6,$7)""",
                job.delivery_id,
                job.attempt_count,
                now,
                now,
                status.value,
                response.summary[:2_000],
                response.retryable,
            )
        assert row is not None
        return self._job(row)

    async def get_job(self, tenant_id: str, delivery_id: str) -> DeliveryJob | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            "SELECT * FROM delivery.delivery_job WHERE tenant_id=$1 AND delivery_id=$2",
            tenant_id,
            delivery_id,
        )
        return self._job(row) if row is not None else None

    async def list_jobs(self, tenant_id: str, session_id: str) -> list[DeliveryJob]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT * FROM delivery.delivery_job WHERE tenant_id=$1 AND session_id=$2
            ORDER BY created_at,delivery_id""",
            tenant_id,
            session_id,
        )
        return [self._job(row) for row in rows]

    async def attempts(self, delivery_id: str) -> list[DeliveryAttempt]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT * FROM delivery.delivery_attempt WHERE delivery_id=$1
            ORDER BY attempt_number""",
            delivery_id,
        )
        return [
            DeliveryAttempt(
                delivery_id=str(row["delivery_id"]),
                attempt_number=int(row["attempt_number"]),
                started_at=row["started_at"],
                completed_at=row["completed_at"],
                outcome=str(row["outcome"]),
                response_summary=str(row["response_summary"]),
                retryable=bool(row["retryable"]),
            )
            for row in rows
        ]

    async def begin_redelivery(
        self, tenant_id: str, delivery_id: str
    ) -> DeliveryJob | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """UPDATE delivery.delivery_job SET status='attempting',
            attempt_count=attempt_count+1,next_attempt_at=NULL,completed_at=NULL
            WHERE tenant_id=$1 AND delivery_id=$2 RETURNING *""",
            tenant_id,
            delivery_id,
        )
        return self._job(row) if row is not None else None

    @staticmethod
    def _sink(row: asyncpg.Record) -> ResultSinkConfig:
        return ResultSinkConfig(
            sink_id=str(row["sink_id"]),
            tenant_id=str(row["tenant_id"]),
            session_id=str(row["session_id"]),
            sink_type=str(row["sink_type"]),
            target_ref=str(row["target_ref"]),
            event_types=tuple(_decode_json(row["event_types"])),
            credential_ref=row["credential_ref"],
            enabled=bool(row["enabled"]),
        )

    @staticmethod
    def _job(row: asyncpg.Record) -> DeliveryJob:
        return DeliveryJob(
            delivery_id=str(row["delivery_id"]),
            event_id=str(row["event_id"]),
            tenant_id=str(row["tenant_id"]),
            root_session_id=str(row["root_session_id"]),
            session_id=str(row["session_id"]),
            run_id=str(row["run_id"]) if row["run_id"] is not None else None,
            sink_id=str(row["sink_id"]),
            sink_type=str(row["sink_type"]),
            sink_target_ref=str(row["sink_target_ref"]),
            payload=dict(_decode_json(row["payload_ref"])),
            status=DeliveryStatus(str(row["status"])),
            attempt_count=int(row["attempt_count"]),
            next_attempt_at=row["next_attempt_at"],
            last_response_summary=row["last_response_summary"],
            created_at=row["created_at"],
            completed_at=row["completed_at"],
        )
