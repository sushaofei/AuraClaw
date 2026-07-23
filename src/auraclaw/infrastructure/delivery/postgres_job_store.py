from __future__ import annotations

from datetime import datetime, timedelta
from uuid import uuid4

import asyncpg  # type: ignore[import-untyped]

from auraclaw.contracts.delivery import (
    DeliveryAttempt,
    DeliveryJob,
    DeliveryStatus,
    ResultSinkConfig,
    SinkResponse,
)
from auraclaw.contracts.errors import LeaseConflictError
from auraclaw.contracts.events import CanonicalEvent, utc_now
from auraclaw.infrastructure.delivery.common import build_delivery_job
from auraclaw.infrastructure.persistence.postgres_common import (
    LazyPool as _LazyPool,
)
from auraclaw.infrastructure.persistence.postgres_common import (
    json_dumps as _json,
)
from auraclaw.infrastructure.persistence.postgres_common import (
    json_loads as _decode_json,
)


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

    async def claim_due(
        self, *, worker_id: str, claim_ttl: timedelta, limit: int
    ) -> list[DeliveryJob]:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            rows = await connection.fetch(
                """SELECT * FROM delivery.delivery_job
                WHERE (
                    (status IN ('pending','retry_wait') AND next_attempt_at <= now())
                    OR (status='attempting' AND claim_expires_at <= now())
                )
                AND NOT EXISTS (
                    SELECT 1 FROM delivery.delivery_job earlier
                    WHERE earlier.tenant_id=delivery_job.tenant_id
                      AND earlier.session_id=delivery_job.session_id
                      AND earlier.sink_id=delivery_job.sink_id
                      AND earlier.status IN ('pending','retry_wait','attempting')
                      AND (earlier.created_at,earlier.delivery_id)
                        < (delivery_job.created_at,delivery_job.delivery_id)
                )
                ORDER BY created_at, delivery_id
                FOR UPDATE SKIP LOCKED LIMIT $1""",
                limit,
            )
            if not rows:
                return []
            updated = []
            for row in rows:
                claimed = await connection.fetchrow(
                    """UPDATE delivery.delivery_job SET status='attempting',
                    attempt_count=attempt_count+1,claimed_by=$2,claim_token=$3,
                    claim_expires_at=now() + $4::interval
                    WHERE delivery_id=$1 RETURNING *""",
                    str(row["delivery_id"]),
                    worker_id,
                    uuid4().hex,
                    claim_ttl,
                )
                assert claimed is not None
                updated.append(claimed)
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
                last_response_summary=$4,completed_at=$5,claimed_by=NULL,
                claim_token=NULL,claim_expires_at=NULL
                WHERE delivery_id=$1 AND claimed_by=$6 AND claim_token=$7
                  AND claim_expires_at > now() RETURNING *""",
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
                job.claimed_by,
                job.claim_token,
            )
            if row is None:
                raise LeaseConflictError(
                    f"delivery claim is no longer owned: {job.delivery_id}"
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
        self,
        tenant_id: str,
        delivery_id: str,
        *,
        worker_id: str,
        claim_ttl: timedelta,
    ) -> DeliveryJob | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """UPDATE delivery.delivery_job SET status='attempting',
            attempt_count=attempt_count+1,next_attempt_at=NULL,completed_at=NULL,
            claimed_by=$3,claim_token=$4,claim_expires_at=now() + $5::interval
            WHERE tenant_id=$1 AND delivery_id=$2 RETURNING *""",
            tenant_id,
            delivery_id,
            worker_id,
            uuid4().hex,
            claim_ttl,
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
            claimed_by=row["claimed_by"],
            claim_token=row["claim_token"],
            claim_expires_at=row["claim_expires_at"],
        )
