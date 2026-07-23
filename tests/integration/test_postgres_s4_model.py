import asyncio
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.config import get_settings
from auraclaw.contracts.internal import ModelGenerateResponse
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url
from auraclaw.infrastructure.persistence.postgres_model_store import PostgresModelStateStore

SETTINGS = get_settings()
DATABASE_URL = asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (ROOT / "migrations/0012_s4_model_state.sql").read_text()
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="PostgreSQL test URL not configured")


def test_model_replicas_share_idempotency_and_hourly_quota() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        if await connection.fetchval(
            "SELECT to_regclass('model_gateway.model_call')"
        ) is None:
            await connection.execute(MIGRATION)
        suffix = uuid4().hex
        tenant_id = f"tenant-model-s4-{suffix}"
        store_a = PostgresModelStateStore(DATABASE_URL)
        store_b = PostgresModelStateStore(DATABASE_URL)
        common = {
            "tenant_id": tenant_id,
            "model_call_id": f"call-{suffix}",
            "run_id": f"run-{suffix}",
            "request_digest": "digest-a",
            "reserved_tokens": 7,
            "token_limit": 10,
        }
        try:
            reservations = await asyncio.gather(
                store_a.reserve(**common), store_b.reserve(**common)
            )
            assert sorted(item.status for item in reservations) == [
                "in_progress",
                "reserved",
            ]
            response = ModelGenerateResponse(
                model_call_id=common["model_call_id"],
                provider="test",
                model="test-v1",
                completed_output="done",
                usage={"total_tokens": 5},
            )
            await store_a.complete(
                tenant_id=tenant_id,
                model_call_id=common["model_call_id"],
                response=response,
            )
            cached = await store_b.reserve(**common)
            assert cached.status == "completed"
            assert cached.cached_response == response

            over_quota = await store_b.reserve(
                **{
                    **common,
                    "model_call_id": f"call-over-{suffix}",
                    "request_digest": "digest-over",
                    "reserved_tokens": 6,
                }
            )
            assert over_quota.status == "quota_exceeded"
            retryable = await store_b.reserve(
                **{
                    **common,
                    "model_call_id": f"call-fail-{suffix}",
                    "request_digest": "digest-fail",
                    "reserved_tokens": 5,
                }
            )
            assert retryable.status == "reserved"
            await store_b.fail(
                tenant_id=tenant_id,
                model_call_id=f"call-fail-{suffix}",
                error_code="temporary",
            )
            retried = await store_a.reserve(
                **{
                    **common,
                    "model_call_id": f"call-fail-{suffix}",
                    "request_digest": "digest-fail",
                    "reserved_tokens": 5,
                }
            )
            assert retried.status == "reserved"
        finally:
            await store_a.close()
            await store_b.close()
            await connection.execute(
                "DELETE FROM model_gateway.model_call WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM model_gateway.usage_budget WHERE tenant_id=$1", tenant_id
            )
            await connection.close()

    asyncio.run(scenario())
