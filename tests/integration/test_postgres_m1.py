import asyncio
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.application.maintenance import ProjectionMaintenanceService
from auraclaw.application.tasks import AllowAllAdmissionController, TaskService
from auraclaw.config import get_settings
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.events import Actor
from auraclaw.infrastructure.postgres import (
    PostgresEventStore,
    PostgresTaskProjection,
    _asyncpg_url,
)
from auraclaw.projections.relay import OutboxRelay

SETTINGS = get_settings()
DATABASE_URL = _asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = tuple(
    (ROOT / path).read_text()
    for path in ("migrations/0001_initial.sql", "migrations/0002_m1_fact_query.sql")
)
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="PostgreSQL test URL not configured")


def test_postgres_concurrent_idempotency_outbox_snapshot_and_rebuild() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        try:
            events_table = await connection.fetchval(
                "SELECT to_regclass('session_core.canonical_event')"
            )
            if events_table is None:
                await connection.execute(MIGRATIONS[0])
            role_column = await connection.fetchval(
                """SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'projection' AND table_name = 'task_view'
                  AND column_name = 'role'"""
            )
            if role_column is None:
                await connection.execute(MIGRATIONS[1])
        finally:
            await connection.close()

        tenant_id = f"tenant-pg-{uuid4().hex}"
        store = PostgresEventStore(DATABASE_URL)
        projection = PostgresTaskProjection(DATABASE_URL)
        service = TaskService(
            event_store=store,
            relay=OutboxRelay(store, projection),
            reader=projection,
            admission=AllowAllAdmissionController(),
        )
        context = CommandContext(
            command_id="same-command",
            tenant_id=tenant_id,
            actor=Actor(type="user", id="postgres-test"),
            correlation_id="corr-pg",
            expected_version=0,
            operation="create_task",
        )
        try:
            responses = await asyncio.gather(
                *(service.create_task(goal="postgres task", context=context) for _ in range(100))
            )
            assert len({response["session_id"] for response in responses}) == 1
            session_id = str(responses[0]["session_id"])
            events = await store.load_all(tenant_id)
            assert [event.type for event in events] == ["session.created", "run.requested"]
            assert await store.pending_outbox() == []

            snapshot = await store.get_snapshot(tenant_id, session_id)
            assert snapshot is not None and snapshot.aggregate_version == 2
            before = await projection.get_task(tenant_id, session_id)
            rebuilt = await ProjectionMaintenanceService(store, projection).rebuild_tasks(tenant_id)
            after = await projection.get_task(tenant_id, session_id)
            assert rebuilt == 2
            assert before is not None and after is not None
            assert before["projection_version"] == after["projection_version"] == 2
            assert before["status"] == after["status"] == "pending"
            assert before["artifact_refs"] == after["artifact_refs"] == []
            assert before["result_ref"] is after["result_ref"] is None
            assert before["error"] is after["error"] is None
        finally:
            await store.close()
            await projection.close()
            cleanup = await asyncpg.connect(DATABASE_URL)
            try:
                event_ids = await cleanup.fetch(
                    "SELECT event_id FROM session_core.canonical_event WHERE tenant_id = $1",
                    tenant_id,
                )
                ids = [str(row["event_id"]) for row in event_ids]
                await cleanup.execute(
                    "DELETE FROM projection.task_view WHERE tenant_id = $1", tenant_id
                )
                await cleanup.execute(
                    "DELETE FROM projection.poison_event WHERE tenant_id = $1", tenant_id
                )
                await cleanup.execute(
                    """DELETE FROM projection.projector_checkpoint
                    WHERE projector_id = 'task' AND partition_id LIKE $1""",
                    f"{tenant_id}:%",
                )
                if ids:
                    await cleanup.execute(
                        """DELETE FROM projection.processed_event
                        WHERE projector_id = 'task' AND event_id = ANY($1::text[])""",
                        ids,
                    )
                    await cleanup.execute(
                        "DELETE FROM session_core.outbox WHERE event_id = ANY($1::text[])", ids
                    )
                await cleanup.execute(
                    "DELETE FROM session_core.snapshot WHERE tenant_id = $1", tenant_id
                )
                await cleanup.execute(
                    "DELETE FROM session_core.canonical_event WHERE tenant_id = $1", tenant_id
                )
                await cleanup.execute(
                    "DELETE FROM session_core.session_head WHERE tenant_id = $1", tenant_id
                )
                await cleanup.execute(
                    "DELETE FROM session_core.command_dedup WHERE tenant_id = $1", tenant_id
                )
            finally:
                await cleanup.close()

    asyncio.run(scenario())
