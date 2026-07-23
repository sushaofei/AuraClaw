import asyncio
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.config import get_settings
from auraclaw.contracts.tools import ToolInvocation, ToolResult, ToolResultStatus
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url
from auraclaw.infrastructure.persistence.postgres_invocation_store import (
    PostgresInvocationStore,
)

SETTINGS = get_settings()
DATABASE_URL = asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (ROOT / "migrations/0009_s3_owner_boundaries.sql").read_text()
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="PostgreSQL test URL not configured")


def test_hands_replicas_atomically_deduplicate_and_recover_invocation() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        await connection.execute(MIGRATION)
        suffix = uuid4().hex
        tenant_id = f"tenant-hands-s4-{suffix}"
        invocation = ToolInvocation(
            tool_invocation_id=f"invocation-{suffix}",
            tenant_id=tenant_id,
            root_session_id=f"root-{suffix}",
            session_id=f"session-{suffix}",
            run_id=f"run-{suffix}",
            tool_name="managed",
            tool_version="1",
            arguments={"value": 1},
            expected_side_effect="write",
            idempotency_key=f"idem-{suffix}",
            deadline=None,
            fencing_token=1,
            actor_id="runtime-s4",
        )
        store_a = PostgresInvocationStore(DATABASE_URL)
        store_b = PostgresInvocationStore(DATABASE_URL)
        try:
            starts = await asyncio.gather(
                store_a.begin(invocation, "digest-s4"),
                store_b.begin(invocation, "digest-s4"),
            )
            assert sum(result.cached_result is None for result in starts) == 1
            recovered = next(result.cached_result for result in starts if result.cached_result)
            assert recovered.status is ToolResultStatus.UNKNOWN

            completed = ToolResult(
                status=ToolResultStatus.SUCCESS,
                content={"replica": "winner"},
                side_effect_status="completed",
            )
            await store_a.complete(invocation, completed)
            assert (await store_b.begin(invocation, "digest-s4")).cached_result == completed
        finally:
            await store_a.close()
            await store_b.close()
            await connection.execute(
                "DELETE FROM hands.invocation WHERE tenant_id=$1", tenant_id
            )
            await connection.close()

    asyncio.run(scenario())
