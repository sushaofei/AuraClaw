import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.config import get_settings
from auraclaw.contracts.delivery import ResultSinkConfig, SinkResponse
from auraclaw.contracts.events import Actor, CanonicalEvent
from auraclaw.contracts.state import Visibility
from auraclaw.infrastructure.delivery import PostgresDeliveryJobStore
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url as _asyncpg_url

SETTINGS = get_settings()
DATABASE_URL = _asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = tuple(
    (ROOT / path).read_text()
    for path in (
        "migrations/0001_initial.sql",
        "migrations/0002_m1_fact_query.sql",
        "migrations/0003_m2_managed_runtime.sql",
        "migrations/0004_m3_tool_artifact_approval.sql",
        "migrations/0005_m4_collaboration_review.sql",
        "migrations/0006_m5_streaming_delivery.sql",
        "migrations/0010_s4_claim_recovery.sql",
    )
)
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="PostgreSQL test URL not configured")


async def _apply_migrations() -> None:
    assert DATABASE_URL is not None
    connection = await asyncpg.connect(DATABASE_URL, timeout=10)
    try:
        checks = (
            "session_core.canonical_event",
            "projection.poison_event",
            "control.runtime_checkpoint",
            "projection.approval_view",
            "projection.collaboration_view",
        )
        for migration, relation in zip(MIGRATIONS[:-2], checks, strict=True):
            if await connection.fetchval("SELECT to_regclass($1)", relation) is None:
                await connection.execute(migration)
        m5_ready = await connection.fetchval(
            """SELECT to_regclass('delivery.sink_config') IS NOT NULL
            AND EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema='projection' AND table_name='task_view'
                  AND column_name='delivery_status'
            )
            AND EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname='delivery_job_event_id_sink_id_key'
            )"""
        )
        if not m5_ready:
            await connection.execute(MIGRATIONS[-2])
        if await connection.fetchval(
            """SELECT 1 FROM information_schema.columns
            WHERE table_schema='delivery' AND table_name='delivery_job'
              AND column_name='claim_token'"""
        ) is None:
            await connection.execute(MIGRATIONS[-1])
    finally:
        await connection.close()


def test_postgres_delivery_job_survives_restart_and_duplicate_outbox() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        await _apply_migrations()
        suffix = uuid4().hex
        tenant_id = f"tenant-pg-m5-{suffix}"
        delivery_store = PostgresDeliveryJobStore(DATABASE_URL)
        session_id = f"session-pg-m5-{suffix}"
        event = CanonicalEvent(
            event_id=f"event-pg-m5-{suffix}",
            tenant_id=tenant_id,
            root_session_id=session_id,
            session_id=session_id,
            run_id=f"run-pg-m5-{suffix}",
            aggregate_version=3,
            type="run.completed",
            occurred_at=datetime.now(UTC),
            actor=Actor(type="runtime", id="runtime"),
            correlation_id=suffix,
            causation_id=f"command-{suffix}",
            visibility=Visibility.USER,
            schema_version=1,
            payload={"result_summary": "persisted"},
        )
        sink = ResultSinkConfig(
            sink_id=f"sink-{suffix}",
            tenant_id=tenant_id,
            session_id=session_id,
            sink_type="restartable",
            target_ref="managed://restartable",
        )
        try:
            await delivery_store.register_sink(sink)
            created = await delivery_store.create_job(event, sink)
            duplicate = await delivery_store.create_job(event, sink)
            assert duplicate.delivery_id == created.delivery_id
            following = await delivery_store.create_job(
                replace(
                    event,
                    event_id=f"event-pg-m5-following-{suffix}",
                    aggregate_version=4,
                ),
                sink,
            )
            claimed = await delivery_store.claim_due(
                worker_id="delivery-a",
                claim_ttl=timedelta(seconds=30),
                limit=1,
            )
            assert len(claimed) == 1 and claimed[0].attempt_count == 1
            assert await delivery_store.claim_due(
                worker_id="delivery-competing",
                claim_ttl=timedelta(seconds=30),
                limit=10,
            ) == []
            retrying = await delivery_store.record_attempt(
                claimed[0],
                SinkResponse(False, True, "HTTP 503"),
                next_attempt_at=datetime.now(UTC),
                max_attempts=5,
            )
            assert retrying.status.value == "retry_wait"
            await delivery_store.close()

            restarted_store = PostgresDeliveryJobStore(DATABASE_URL)
            recovered = await restarted_store.claim_due(
                worker_id="delivery-b",
                claim_ttl=timedelta(seconds=30),
                limit=1,
            )
            assert len(recovered) == 1 and recovered[0].attempt_count == 2
            succeeded = await restarted_store.record_attempt(
                recovered[0],
                SinkResponse(True, summary="HTTP 204"),
                next_attempt_at=None,
                max_attempts=5,
            )
            jobs = await restarted_store.list_jobs(tenant_id, session_id)
            assert len(jobs) == 2
            assert succeeded.status.value == "succeeded" and jobs[0].attempt_count == 2
            assert len(await restarted_store.attempts(created.delivery_id)) == 2
            next_claim = await restarted_store.claim_due(
                worker_id="delivery-b",
                claim_ttl=timedelta(seconds=30),
                limit=10,
            )
            assert [job.delivery_id for job in next_claim] == [following.delivery_id]
            await restarted_store.record_attempt(
                next_claim[0],
                SinkResponse(True, summary="HTTP 204"),
                next_attempt_at=None,
                max_attempts=5,
            )
            await restarted_store.close()
        finally:
            cleanup = await asyncpg.connect(DATABASE_URL, timeout=10)
            try:
                await cleanup.execute(
                    "DELETE FROM delivery.delivery_attempt WHERE delivery_id IN "
                    "(SELECT delivery_id FROM delivery.delivery_job WHERE tenant_id=$1)",
                    tenant_id,
                )
                await cleanup.execute(
                    "DELETE FROM delivery.delivery_job WHERE tenant_id=$1", tenant_id
                )
                await cleanup.execute(
                    "DELETE FROM delivery.sink_config WHERE tenant_id=$1", tenant_id
                )
            finally:
                await cleanup.close()

    asyncio.run(asyncio.wait_for(scenario(), timeout=60))
