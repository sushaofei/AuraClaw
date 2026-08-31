import asyncio
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.config import get_settings
from auraclaw.contracts.errors import LeaseConflictError, VersionConflictError
from auraclaw.contracts.internal import ModelGenerateResponse
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url
from auraclaw.infrastructure.persistence.postgres_model_store import PostgresModelStateStore

SETTINGS = get_settings()
DATABASE_URL = asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (ROOT / "migrations/0012_s4_model_state.sql").read_text()
LIFECYCLE_MIGRATION = (
    ROOT / "migrations/0048_model_call_execution_lifecycle.sql"
).read_text()
LIFECYCLE_DOWN = (
    ROOT / "migrations/0048_model_call_execution_lifecycle.down.sql"
).read_text()
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="PostgreSQL test URL not configured")


def test_model_replicas_share_idempotency_and_hourly_quota() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        if await connection.fetchval(
            "SELECT to_regclass('model_gateway.model_call')"
        ) is None:
            await connection.execute(MIGRATION)
        await connection.execute(LIFECYCLE_MIGRATION)
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


def test_model_call_owner_heartbeat_and_cross_replica_cancel_are_persistent() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        if await connection.fetchval(
            "SELECT to_regclass('model_gateway.model_call')"
        ) is None:
            await connection.execute(MIGRATION)
        await connection.execute(LIFECYCLE_MIGRATION)
        suffix = uuid4().hex
        tenant_id = f"tenant-model-lifecycle-{suffix}"
        store_a = PostgresModelStateStore(DATABASE_URL)
        store_b = PostgresModelStateStore(DATABASE_URL)
        call_id = f"call-{suffix}"
        run_id = f"run-{suffix}"
        common = {
            "tenant_id": tenant_id,
            "model_call_id": call_id,
            "run_id": run_id,
            "request_digest": "digest-a",
            "reserved_tokens": 3,
            "token_limit": 20,
            "actor": "agent-runtime",
            "correlation_id": run_id,
            "causation_id": call_id,
        }
        try:
            owned = await store_a.reserve(
                **common,
                execution_owner="gateway-a",
                claim_ttl=timedelta(seconds=30),
            )
            assert owned.status == "reserved"
            assert owned.claim_token
            duplicate = await store_b.reserve(
                **common,
                execution_owner="gateway-b",
                claim_ttl=timedelta(seconds=30),
            )
            assert duplicate.status == "in_progress"
            conflict = await store_b.reserve(
                **{**common, "request_digest": "different-digest"},
                execution_owner="gateway-b",
                claim_ttl=timedelta(seconds=30),
            )
            assert conflict.status == "conflict"

            requested = await store_b.request_cancel(
                tenant_id=tenant_id,
                model_call_id=call_id,
                run_id=run_id,
                actor="agent-runtime",
                correlation_id=run_id,
                causation_id=f"cancel-{call_id}",
            )
            assert requested.status == "cancel_requested"
            assert requested.execution_owner == "gateway-a"
            audit = await connection.fetchrow(
                """SELECT actor,correlation_id,causation_id,cancel_actor,
                          cancel_correlation_id,cancel_causation_id
                   FROM model_gateway.model_call
                   WHERE tenant_id=$1 AND model_call_id=$2""",
                tenant_id,
                call_id,
            )
            assert audit is not None
            assert tuple(audit) == (
                "agent-runtime",
                run_id,
                call_id,
                "agent-runtime",
                run_id,
                f"cancel-{call_id}",
            )
            heartbeat = await store_a.heartbeat(
                tenant_id=tenant_id,
                model_call_id=call_id,
                execution_owner="gateway-a",
                claim_token=owned.claim_token,
                claim_ttl=timedelta(seconds=30),
            )
            assert heartbeat.owned
            assert heartbeat.cancel_requested
            assert await store_a.mark_cancelled(
                tenant_id=tenant_id,
                model_call_id=call_id,
                execution_owner="gateway-a",
                claim_token=owned.claim_token,
                usage={"total_tokens": 1},
            )
            assert await connection.fetchval(
                "SELECT tokens_used FROM model_gateway.usage_budget WHERE tenant_id=$1",
                tenant_id,
            ) == 1
            repeated = await store_b.request_cancel(
                tenant_id=tenant_id,
                model_call_id=call_id,
                run_id=run_id,
            )
            assert repeated.status == "cancelled"
            assert repeated.requested
            with pytest.raises(LeaseConflictError):
                await store_a.complete(
                    tenant_id=tenant_id,
                    model_call_id=call_id,
                    response=ModelGenerateResponse(
                        model_call_id=call_id,
                        provider="test",
                        model="test-v1",
                        completed_output="late",
                        usage={"total_tokens": 3},
                    ),
                    claim_token=owned.claim_token,
                )

            other_tenant = await store_b.request_cancel(
                tenant_id=f"other-{tenant_id}",
                model_call_id=call_id,
                run_id=run_id,
            )
            assert other_tenant.status == "not_found"

            mismatch_id = f"mismatch-{suffix}"
            await store_a.reserve(
                **{**common, "model_call_id": mismatch_id, "request_digest": "digest-b"},
                execution_owner="gateway-a",
            )
            with pytest.raises(VersionConflictError):
                await store_b.request_cancel(
                    tenant_id=tenant_id,
                    model_call_id=mismatch_id,
                    run_id="wrong-run",
                )

            stale_id = f"stale-{suffix}"
            stale = await store_a.reserve(
                **{**common, "model_call_id": stale_id, "request_digest": "digest-c"},
                execution_owner="gateway-a",
            )
            assert stale.status == "reserved"
            await connection.execute(
                """UPDATE model_gateway.model_call
                   SET claim_expires_at=now()-interval '1 second'
                   WHERE tenant_id=$1 AND model_call_id=$2""",
                tenant_id,
                stale_id,
            )
            reserved_before = await connection.fetchval(
                "SELECT tokens_reserved FROM model_gateway.usage_budget WHERE tenant_id=$1",
                tenant_id,
            )
            uncertain = await store_b.request_cancel(
                tenant_id=tenant_id,
                model_call_id=stale_id,
                run_id=run_id,
            )
            assert uncertain.status == "reconciling"
            reserved_after = await connection.fetchval(
                "SELECT tokens_reserved FROM model_gateway.usage_budget WHERE tenant_id=$1",
                tenant_id,
            )
            assert reserved_after == reserved_before
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


def test_model_call_execution_lifecycle_migration_roundtrip() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        try:
            if await connection.fetchval(
                "SELECT to_regclass('model_gateway.model_call')"
            ) is None:
                await connection.execute(MIGRATION)
            await connection.execute(LIFECYCLE_MIGRATION)
            await connection.execute(LIFECYCLE_DOWN)
            assert not await connection.fetchval(
                """SELECT EXISTS(SELECT 1 FROM information_schema.columns
                WHERE table_schema='model_gateway' AND table_name='model_call'
                  AND column_name='execution_owner')"""
            )
            await connection.execute(LIFECYCLE_MIGRATION)
            assert await connection.fetchval(
                """SELECT count(*)=15 FROM information_schema.columns
                WHERE table_schema='model_gateway' AND table_name='model_call'
                  AND column_name IN (
                    'execution_owner','claim_token','started_at','heartbeat_at',
                    'claim_expires_at','cancel_requested_at','cancelled_at',
                    'completed_at','provider_request_ref','actor','correlation_id',
                    'causation_id','cancel_actor','cancel_correlation_id',
                    'cancel_causation_id')"""
            )
        finally:
            await connection.close()

    asyncio.run(scenario())
