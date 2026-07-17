import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.config import get_settings
from auraclaw.contracts.errors import FencingTokenError
from auraclaw.infrastructure.control_postgres import PostgresControlStateStore
from auraclaw.infrastructure.postgres import _asyncpg_url
from auraclaw.runtime.ports import (
    RunnableItem,
    RuntimeAssignment,
    RuntimeCheckpoint,
    RuntimeInstance,
)

SETTINGS = get_settings()
DATABASE_URL = _asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = tuple(
    (ROOT / path).read_text()
    for path in (
        "migrations/0001_initial.sql",
        "migrations/0002_m1_fact_query.sql",
        "migrations/0003_m2_managed_runtime.sql",
    )
)
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="PostgreSQL test URL not configured")


async def _apply_migrations() -> None:
    assert DATABASE_URL is not None
    connection = await asyncpg.connect(DATABASE_URL)
    try:
        if await connection.fetchval("SELECT to_regclass('control.runtime_lease')") is None:
            await connection.execute(MIGRATIONS[0])
        if await connection.fetchval(
            """SELECT 1 FROM information_schema.columns
            WHERE table_schema='projection' AND table_name='task_view' AND column_name='role'"""
        ) is None:
            await connection.execute(MIGRATIONS[1])
        if await connection.fetchval(
            "SELECT to_regclass('control.runtime_checkpoint')"
        ) is None:
            await connection.execute(MIGRATIONS[2])
    finally:
        await connection.close()


def test_postgres_control_claim_lease_fencing_checkpoint_and_capacity() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        await _apply_migrations()
        suffix = uuid4().hex
        tenant_id = f"tenant-pg-m2-{suffix}"
        session_id = f"ses-pg-m2-{suffix}"
        run_id = f"run-pg-m2-{suffix}"
        task_id = f"{tenant_id}:{session_id}:{run_id}"
        resource_id = f"session:{tenant_id}:{session_id}"
        scope = f"tenant:{tenant_id}"
        store_a = PostgresControlStateStore(DATABASE_URL)
        store_b = PostgresControlStateStore(DATABASE_URL)
        try:
            item = RunnableItem(
                task_id=task_id,
                tenant_id=tenant_id,
                root_session_id=session_id,
                session_id=session_id,
                run_id=run_id,
                source_version=2,
            )
            assert await store_a.enqueue(item)
            claims = await asyncio.gather(
                store_a.claim("orch-a", limit=1), store_b.claim("orch-b", limit=1)
            )
            assert sum(len(batch) for batch in claims) == 1

            lease = await store_a.acquire_lease(
                resource_id, "orch-a", ttl=timedelta(seconds=5)
            )
            assert lease is not None
            assert await store_b.acquire_lease(
                resource_id, "orch-b", ttl=timedelta(seconds=1)
            ) is None
            runtime = RuntimeInstance(
                runtime_id=f"runtime-{suffix}",
                runtime_type="agent",
                role="root",
                node_id="pg-test",
                capabilities={},
                capacity=1,
            )
            await store_a.register_runtime(runtime)
            assignment = RuntimeAssignment(
                tenant_id=tenant_id,
                root_session_id=session_id,
                session_id=session_id,
                run_id=run_id,
                runtime_id=runtime.runtime_id,
                lease_id=lease.lease_id,
                fencing_token=lease.fencing_token,
                role="root",
                resource_profile={},
            )
            assert await store_a.assign(task_id, assignment)
            assert await store_a.get_assignment(task_id) == assignment
            await store_a.heartbeat(runtime.runtime_id, lease.fencing_token)
            assert await store_a.reserve_capacity(scope, 2, limit=2)
            assert not await store_b.reserve_capacity(scope, 1, limit=2)
            await store_a.release_capacity(scope, 1)
            assert await store_b.reserve_capacity(scope, 1, limit=2)

            checkpoint = RuntimeCheckpoint(
                tenant_id=tenant_id,
                session_id=session_id,
                run_id=run_id,
                fencing_token=lease.fencing_token,
                phase="model_pending",
                state={"model_call_id": "stable"},
                updated_at=datetime.now(UTC),
            )
            await store_a.save_checkpoint(checkpoint)
            restored = await store_b.load_checkpoint(tenant_id, session_id, run_id)
            assert restored is not None and restored.state == {"model_call_id": "stable"}

            failure_connection = await asyncpg.connect(DATABASE_URL)
            try:
                await failure_connection.execute(
                    """UPDATE control.runtime_lease
                    SET expires_at=now() - interval '1 second' WHERE resource_id=$1""",
                    resource_id,
                )
            finally:
                await failure_connection.close()
            assert await store_b.recover_expired() == 1
            replacement = await store_b.acquire_lease(
                resource_id, "orch-b", ttl=timedelta(seconds=1)
            )
            assert replacement is not None
            assert replacement.fencing_token > lease.fencing_token
            with pytest.raises(FencingTokenError):
                await store_a.assert_fencing(resource_id, lease.fencing_token)
        finally:
            await store_a.close()
            await store_b.close()
            connection = await asyncpg.connect(DATABASE_URL)
            try:
                await connection.execute(
                    "DELETE FROM control.runtime_cancellation WHERE tenant_id=$1", tenant_id
                )
                await connection.execute(
                    "DELETE FROM control.runtime_checkpoint WHERE tenant_id=$1", tenant_id
                )
                await connection.execute(
                    "DELETE FROM control.assignment WHERE task_id=$1", task_id
                )
                await connection.execute(
                    "DELETE FROM control.runnable_item WHERE task_id=$1", task_id
                )
                await connection.execute(
                    "DELETE FROM control.runtime_instance WHERE runtime_id=$1",
                    f"runtime-{suffix}",
                )
                await connection.execute(
                    "DELETE FROM control.capacity_reservation WHERE scope=$1", scope
                )
                await connection.execute(
                    "DELETE FROM control.runtime_lease WHERE resource_id=$1", resource_id
                )
            finally:
                await connection.close()

    asyncio.run(scenario())
