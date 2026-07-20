import asyncio
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.config import get_settings
from auraclaw.contracts.collaboration import (
    ChildResult,
    ChildSpec,
    CollaborationRole,
    OutputContract,
)
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.events import Actor
from auraclaw.gateways.task.admission import AllowAllAdmissionController
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url as _asyncpg_url
from auraclaw.infrastructure.persistence.postgres_event_store import PostgresEventStore
from auraclaw.infrastructure.projection.postgres_collaboration_store import (
    PostgresCollaborationProjection,
)
from auraclaw.infrastructure.projection.postgres_task_store import PostgresTaskProjection
from auraclaw.projection.approval.projector import CompositeProjection
from auraclaw.projection.relay import OutboxRelay
from auraclaw.session.collaboration_service import CollaborationService
from auraclaw.session.task_service import TaskService

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
        "migrations/0008_multi_run_sessions.sql",
    )
)
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="PostgreSQL test URL not configured")


async def _apply_migrations() -> None:
    assert DATABASE_URL is not None
    connection = await asyncpg.connect(DATABASE_URL)
    try:
        checks = (
            "session_core.canonical_event",
            "projection.poison_event",
            "control.runtime_checkpoint",
            "projection.approval_view",
            "projection.collaboration_view",
        )
        for migration, relation in zip(MIGRATIONS[:-1], checks, strict=True):
            if await connection.fetchval("SELECT to_regclass($1)", relation) is None:
                await connection.execute(migration)
        if await connection.fetchval(
            """SELECT 1 FROM information_schema.columns
            WHERE table_schema='projection' AND table_name='task_view'
              AND column_name='run_status'"""
        ) is None:
            await connection.execute(MIGRATIONS[-1])
    finally:
        await connection.close()


def test_postgres_collaboration_projection_rebuilds_runnable_and_result() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        await _apply_migrations()
        suffix = uuid4().hex
        tenant_id = f"tenant-pg-m4-{suffix}"
        store = PostgresEventStore(DATABASE_URL)
        tasks = PostgresTaskProjection(DATABASE_URL)
        collaboration = PostgresCollaborationProjection(DATABASE_URL)
        relay = OutboxRelay(store, CompositeProjection(tasks, collaboration))
        task_service = TaskService(
            event_store=store,
            relay=relay,
            reader=tasks,
            admission=AllowAllAdmissionController(),
        )
        service = CollaborationService(event_store=store, relay=relay)
        try:
            root = await task_service.create_task(
                goal="postgres M4",
                context=CommandContext(
                    command_id=f"root-{suffix}",
                    tenant_id=tenant_id,
                    actor=Actor(type="user", id="user"),
                    correlation_id=suffix,
                    expected_version=0,
                    operation="create_task",
                ),
            )
            root_id = str(root["session_id"])
            child = await service.create_child(
                root_session_id=root_id,
                parent_session_id=root_id,
                spec=ChildSpec(
                    task_key="postgres-worker",
                    role=CollaborationRole.WORKER,
                    goal="produce result",
                    output_contract=OutputContract(require_artifacts=True),
                ),
                context=CommandContext(
                    command_id=f"child-{suffix}",
                    tenant_id=tenant_id,
                    actor=Actor(type="coordinator", id="coordinator"),
                    correlation_id=suffix,
                    expected_version=0,
                    operation="collaboration.create_child",
                ),
            )
            child_id = str(child["session_id"])
            runnable = await collaboration.list_runnable(tenant_id, root_id)
            assert [item["session_id"] for item in runnable] == [child_id]

            child_events = await store.load(tenant_id, child_id)
            await service.publish_child_result(
                root_session_id=root_id,
                child_session_id=child_id,
                child_result=ChildResult(
                    summary="done",
                    result_ref="result://postgres",
                    artifact_refs=("artifact://postgres",),
                ),
                context=CommandContext(
                    command_id=f"publish-{suffix}",
                    tenant_id=tenant_id,
                    actor=Actor(type="worker", id="worker"),
                    correlation_id=suffix,
                    expected_version=len(child_events),
                    operation="collaboration.publish_result",
                ),
            )
            events = await store.load_all(tenant_id)
            await collaboration.rebuild(events, tenant_id)
            view = await collaboration.get(tenant_id, child_id)
            assert view is not None
            assert view["status"] == "completed"
            assert view["artifact_refs"] == ["artifact://postgres"]
        finally:
            await collaboration.close()
            await tasks.close()
            await store.close()
            cleanup = await asyncpg.connect(DATABASE_URL)
            try:
                event_ids = await cleanup.fetch(
                    "SELECT event_id FROM session_core.canonical_event WHERE tenant_id=$1",
                    tenant_id,
                )
                ids = [str(row["event_id"]) for row in event_ids]
                await cleanup.execute(
                    "DELETE FROM projection.collaboration_view WHERE tenant_id=$1", tenant_id
                )
                await cleanup.execute(
                    "DELETE FROM projection.task_view WHERE tenant_id=$1", tenant_id
                )
                await cleanup.execute(
                    "DELETE FROM session_core.outbox WHERE event_id IN "
                    "(SELECT event_id FROM session_core.canonical_event WHERE tenant_id=$1)",
                    tenant_id,
                )
                await cleanup.execute(
                    "DELETE FROM session_core.canonical_event WHERE tenant_id=$1", tenant_id
                )
                await cleanup.execute(
                    "DELETE FROM session_core.snapshot WHERE tenant_id=$1", tenant_id
                )
                await cleanup.execute(
                    "DELETE FROM session_core.session_head WHERE tenant_id=$1", tenant_id
                )
                await cleanup.execute(
                    "DELETE FROM session_core.command_dedup WHERE tenant_id=$1", tenant_id
                )
                if ids:
                    await cleanup.execute(
                        "DELETE FROM projection.processed_event WHERE event_id=ANY($1::text[])",
                        ids,
                    )
            finally:
                await cleanup.close()

    asyncio.run(scenario())
