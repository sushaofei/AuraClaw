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
from auraclaw.delivery.ports import SinkCircuitPermit, SinkCircuitSnapshot
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
        self,
        *,
        worker_id: str,
        claim_ttl: timedelta,
        limit: int,
        max_per_tenant: int | None = None,
    ) -> list[DeliveryJob]:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            rows = await connection.fetch(
                """WITH eligible AS (
                SELECT delivery_job.delivery_id,
                       row_number() OVER (
                           PARTITION BY delivery_job.tenant_id
                           ORDER BY delivery_job.created_at,delivery_job.delivery_id
                       ) AS tenant_rank
                FROM delivery.delivery_job AS delivery_job
                WHERE ((delivery_job.status IN ('pending','retry_wait')
                        AND delivery_job.next_attempt_at <= now())
                       OR (delivery_job.status='attempting'
                           AND delivery_job.claim_expires_at <= now()))
                  AND NOT EXISTS (
                    SELECT 1 FROM delivery.delivery_job AS earlier
                    WHERE earlier.tenant_id=delivery_job.tenant_id
                      AND earlier.session_id=delivery_job.session_id
                      AND earlier.sink_id=delivery_job.sink_id
                      AND earlier.status IN ('pending','retry_wait','attempting')
                      AND (earlier.created_at,earlier.delivery_id)
                        < (delivery_job.created_at,delivery_job.delivery_id)
                )), chosen AS (
                    SELECT delivery_id,tenant_rank FROM eligible
                    WHERE tenant_rank <= $2
                    ORDER BY tenant_rank,delivery_id
                    LIMIT $1
                )
                SELECT delivery_job.* FROM delivery.delivery_job AS delivery_job
                JOIN chosen USING (delivery_id)
                ORDER BY chosen.tenant_rank,delivery_job.created_at,
                         delivery_job.delivery_id
                FOR UPDATE OF delivery_job SKIP LOCKED""",
                limit,
                max_per_tenant or limit,
            )
            if not rows:
                return []
            updated = []
            for row in rows:
                claimed = await connection.fetchrow(
                    """UPDATE delivery.delivery_job SET status='attempting',
                    attempt_count=attempt_count+1,claimed_by=$2,claim_token=$3,
                    claim_expires_at=now() + $4::interval,claim_heartbeat_at=now(),
                    side_effect_started_at=NULL,reconciliation_reason=NULL
                    WHERE delivery_id=$1 RETURNING *""",
                    str(row["delivery_id"]),
                    worker_id,
                    uuid4().hex,
                    claim_ttl,
                )
                assert claimed is not None
                updated.append(claimed)
            return [self._job(row) for row in updated]

    async def renew_claim(
        self, job: DeliveryJob, *, claim_ttl: timedelta
    ) -> bool:
        pool = await self.pool()
        status = await pool.execute(
            """UPDATE delivery.delivery_job
               SET claim_expires_at=now()+$4::interval,claim_heartbeat_at=now()
               WHERE delivery_id=$1 AND claimed_by=$2 AND claim_token=$3
                 AND status='attempting' AND claim_expires_at > now()""",
            job.delivery_id,
            job.claimed_by,
            job.claim_token,
            claim_ttl,
        )
        return bool(status.rsplit(" ", 1)[-1] == "1")

    async def begin_side_effect(self, job: DeliveryJob) -> bool:
        pool = await self.pool()
        status = await pool.execute(
            """UPDATE delivery.delivery_job SET side_effect_started_at=now()
               WHERE delivery_id=$1 AND claimed_by=$2 AND claim_token=$3
                 AND status='attempting' AND claim_expires_at > now()""",
            job.delivery_id,
            job.claimed_by,
            job.claim_token,
        )
        return bool(status.rsplit(" ", 1)[-1] == "1")

    async def mark_reconciling(self, job: DeliveryJob, *, reason: str) -> bool:
        pool = await self.pool()
        status = await pool.execute(
            """UPDATE delivery.delivery_job
               SET status='reconciling',reconciliation_reason=$4,
                   claimed_by=NULL,claim_token=NULL,claim_expires_at=NULL
               WHERE delivery_id=$1 AND claimed_by=$2 AND claim_token=$3
                 AND side_effect_started_at IS NOT NULL""",
            job.delivery_id,
            job.claimed_by,
            job.claim_token,
            reason[:128],
        )
        return bool(status.rsplit(" ", 1)[-1] == "1")

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
                claim_token=NULL,claim_expires_at=NULL,claim_heartbeat_at=NULL
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
            claimed_by=$3,claim_token=$4,claim_expires_at=now() + $5::interval,
            claim_heartbeat_at=now(),side_effect_started_at=NULL,
            reconciliation_reason=NULL
            WHERE tenant_id=$1 AND delivery_id=$2 RETURNING *""",
            tenant_id,
            delivery_id,
            worker_id,
            uuid4().hex,
            claim_ttl,
        )
        return self._job(row) if row is not None else None

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
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """INSERT INTO delivery.sink_circuit_state (tenant_id,sink_id)
                VALUES ($1,$2) ON CONFLICT (tenant_id,sink_id) DO NOTHING""",
                tenant_id,
                sink_id,
            )
            row = await connection.fetchrow(
                """SELECT *,open_until > now() AS open_active,
                          probe_expires_at > now() AS probe_active
                FROM delivery.sink_circuit_state
                WHERE tenant_id=$1 AND sink_id=$2 FOR UPDATE""",
                tenant_id,
                sink_id,
            )
            assert row is not None
            state = str(row["state"])
            generation = int(row["generation"])
            if state == "closed":
                return SinkCircuitPermit(True, state, generation)
            if state == "open" and bool(row["open_active"]):
                return SinkCircuitPermit(False, state, generation)
            if state == "half_open" and bool(row["probe_active"]):
                return SinkCircuitPermit(False, state, generation)
            probe_token = uuid4().hex
            updated = await connection.fetchrow(
                """UPDATE delivery.sink_circuit_state
                SET state='half_open',generation=generation+1,probe_owner=$3,
                    probe_token=$4,probe_expires_at=now()+$5::interval,
                    updated_at=now()
                WHERE tenant_id=$1 AND sink_id=$2 RETURNING *""",
                tenant_id,
                sink_id,
                worker_id,
                probe_token,
                probe_ttl,
            )
            assert updated is not None
            return SinkCircuitPermit(
                True,
                "half_open",
                int(updated["generation"]),
                probe_token=probe_token,
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
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            row = await connection.fetchrow(
                """SELECT * FROM delivery.sink_circuit_state
                WHERE tenant_id=$1 AND sink_id=$2 FOR UPDATE""",
                tenant_id,
                sink_id,
            )
            if row is None:
                row = await connection.fetchrow(
                    """INSERT INTO delivery.sink_circuit_state (tenant_id,sink_id)
                    VALUES ($1,$2) RETURNING *""",
                    tenant_id,
                    sink_id,
                )
            assert row is not None
            state = str(row["state"])
            failures = int(row["failure_count"])
            if state == "half_open" and row["probe_token"] != probe_token:
                return self._circuit(row)
            if response.succeeded:
                changed = state != "closed" or failures != 0
                row = await connection.fetchrow(
                    """UPDATE delivery.sink_circuit_state
                    SET state='closed',failure_count=0,open_until=NULL,
                        generation=generation+$3,probe_owner=NULL,probe_token=NULL,
                        probe_expires_at=NULL,updated_at=now()
                    WHERE tenant_id=$1 AND sink_id=$2 RETURNING *""",
                    tenant_id,
                    sink_id,
                    int(changed),
                )
            elif response.retryable:
                failures += 1
                should_open = state == "half_open" or failures >= failure_threshold
                if should_open:
                    row = await connection.fetchrow(
                        """UPDATE delivery.sink_circuit_state
                        SET state='open',failure_count=$3,
                            open_until=now()+$4::interval,generation=generation+1,
                            probe_owner=NULL,probe_token=NULL,probe_expires_at=NULL,
                            updated_at=now()
                        WHERE tenant_id=$1 AND sink_id=$2 RETURNING *""",
                        tenant_id,
                        sink_id,
                        max(failures, failure_threshold),
                        reset_after,
                    )
                else:
                    row = await connection.fetchrow(
                        """UPDATE delivery.sink_circuit_state
                        SET failure_count=$3,updated_at=now()
                        WHERE tenant_id=$1 AND sink_id=$2 RETURNING *""",
                        tenant_id,
                        sink_id,
                        failures,
                    )
            assert row is not None
            return self._circuit(row)

    async def get_sink_circuit(
        self, tenant_id: str, sink_id: str
    ) -> SinkCircuitSnapshot | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM delivery.sink_circuit_state
            WHERE tenant_id=$1 AND sink_id=$2""",
            tenant_id,
            sink_id,
        )
        return None if row is None else self._circuit(row)

    @staticmethod
    def _circuit(row: asyncpg.Record) -> SinkCircuitSnapshot:
        return SinkCircuitSnapshot(
            state=str(row["state"]),
            failure_count=int(row["failure_count"]),
            generation=int(row["generation"]),
            open_until=row["open_until"],
            probe_owner=row["probe_owner"],
        )

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
            claim_heartbeat_at=row["claim_heartbeat_at"],
            side_effect_started_at=row["side_effect_started_at"],
            reconciliation_reason=row["reconciliation_reason"],
        )
