import asyncio
import json
import time
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.config import get_settings
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url
from auraclaw.infrastructure.projection.postgres_activity_store import (
    PostgresActivityProjection,
)

SETTINGS = get_settings()
DATABASE_URL = asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (ROOT / "migrations/0071_activity_projection_cache.sql").read_text()
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="PostgreSQL test URL not configured")


async def _apply_migration(connection: asyncpg.Connection) -> None:
    if await connection.fetchval(
        "SELECT to_regclass('projection.activity_state')"
    ) is None:
        await connection.execute(MIGRATION)


def test_postgres_activity_projection_uses_bounded_index_page_for_long_session() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        suffix = uuid4().hex
        tenant_id = f"tenant-activity-{suffix}"
        session_id = f"session-activity-{suffix}"
        connection = await asyncpg.connect(DATABASE_URL)
        store = PostgresActivityProjection(DATABASE_URL)
        try:
            await _apply_migration(connection)
            await connection.execute(
                """INSERT INTO projection.activity_state
                (tenant_id,session_id,source_version,source_event_id,complete,projected_at)
                VALUES ($1,$2,10000,'evt-10000',true,now())""",
                tenant_id,
                session_id,
            )
            await connection.execute(
                """INSERT INTO projection.activity_node
                (tenant_id,session_id,node_id,sequence,updated_version,node,projected_at)
                SELECT $1,$2,'node-' || value,value,value,
                       jsonb_build_object(
                           'id','node-' || value,'type','tool','status','completed',
                           'title','load test','summary','ok','sequence',value,
                           'updated_version',value,'run_id',NULL,
                           'started_at','2026-10-07T00:00:00+00:00',
                           'completed_at','2026-10-07T00:00:00+00:00',
                           'duration_ms',0,'detail','{}'::jsonb,
                           'correlation','{}'::jsonb),now()
                FROM generate_series(1,10000) AS value""",
                tenant_id,
                session_id,
            )

            plan = await connection.fetchval(
                """EXPLAIN (FORMAT JSON)
                SELECT node FROM projection.activity_node
                WHERE tenant_id=$1 AND session_id=$2 AND updated_version > 9799
                ORDER BY updated_version,sequence,node_id LIMIT 201""",
                tenant_id,
                session_id,
            )
            assert "activity_node_incremental_page_idx" in json.dumps(plan)

            started = time.perf_counter()
            page = await store.get_activity_page(
                tenant_id,
                session_id,
                after_version=9_799,
                limit=200,
                min_source_version=10_000,
            )
            elapsed_ms = (time.perf_counter() - started) * 1_000
            assert page is not None
            assert len(page["nodes"]) == 200
            assert page["next_after_version"] == 9_999
            assert page["has_more"] is True
            assert elapsed_ms < 1_000
        finally:
            await connection.execute(
                "DELETE FROM projection.activity_node WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM projection.activity_state WHERE tenant_id=$1", tenant_id
            )
            await store.close()
            await connection.close()

    asyncio.run(scenario())
