"""Run against an explicitly provisioned database at migration 0064 or later."""

import asyncio
from decimal import Decimal
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.config import get_settings
from auraclaw.contracts.internal import ModelGenerateResponse
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url
from auraclaw.infrastructure.persistence.postgres_model_store import PostgresModelStateStore
from auraclaw.model_gateway.pricing import quote

settings = get_settings()
DATABASE_URL = asyncpg_url(settings.resolved_database_url) if settings.postgres_enabled else None
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="PostgreSQL URL not configured")
PROFILE = {
    "provider": "fixture",
    "model": "fixture-model",
    "currency": "TEST",
    "version": "v1",
    "max_input_tokens": 8192,
    "input_per_million": "1",
    "output_per_million": "2",
}


def test_concurrent_cost_reservations_settle_once_and_unknown_retains_budget():
    async def scenario():
        assert DATABASE_URL is not None
        tenant = "cost-ledger-" + uuid4().hex
        store = PostgresModelStateStore(DATABASE_URL)
        connection = await asyncpg.connect(DATABASE_URL)
        common = dict(
            tenant_id=tenant,
            run_id="r",
            reserved_tokens=100,
            token_limit=10000,
            cost_reservation=quote(PROFILE, [], [], 100, 0.01),
        )
        try:
            first, second = await asyncio.gather(
                store.reserve(**common, model_call_id="a", request_digest="a"),
                store.reserve(**common, model_call_id="b", request_digest="b"),
            )
            assert {first.status, second.status} == {"reserved", "cost_quota_exceeded"}
            winner = "a" if first.status == "reserved" else "b"
            claim = first.claim_token if winner == "a" else second.claim_token
            response = ModelGenerateResponse(
                model_call_id=winner,
                provider="fixture",
                model="fixture-model",
                completed_output="done",
                usage={"input_tokens": 10, "output_tokens": 20, "total_tokens": 30},
            )
            await store.complete(
                tenant_id=tenant, model_call_id=winner, response=response, claim_token=claim
            )
            await store.complete(
                tenant_id=tenant, model_call_id=winner, response=response, claim_token=claim
            )
            row = await connection.fetchrow(
                "SELECT * FROM model_gateway.run_cost_budget WHERE tenant_id=$1", tenant
            )
            assert row["cost_used"] == Decimal("0.00005")
            assert row["cost_reserved"] == 0
            cached = await store.reserve(**common, model_call_id=winner, request_digest=winner)
            assert cached.status == "completed" and cached.cached_response.usage["cost"] == 0.00005
            unknown = await store.reserve(
                **common, model_call_id="unknown", request_digest="unknown"
            )
            assert unknown.status == "reserved"
            await store.fail(
                tenant_id=tenant,
                model_call_id="unknown",
                error_code="transport_lost",
                claim_token=unknown.claim_token,
            )
            replay = await store.reserve(
                **common, model_call_id="unknown", request_digest="unknown"
            )
            assert replay.status == "reconciling"
            # Hour rollover cannot erase an unresolved reservation.
            await connection.execute(
                "UPDATE model_gateway.usage_budget "
                "SET window_started_at=now()-interval '2 hours' WHERE tenant_id=$1",
                tenant,
            )
            blocked = await store.reserve(
                **common, model_call_id="after-rollover", request_digest="after-rollover"
            )
            assert blocked.status == "cost_quota_exceeded"
            row = await connection.fetchrow(
                "SELECT * FROM model_gateway.usage_budget WHERE tenant_id=$1", tenant
            )
            assert row["tokens_reserved"] == 100
        finally:
            await store.close()
            await connection.close()

    asyncio.run(scenario())


def test_root_tree_admission_is_atomic_across_distinct_child_sessions():
    from dataclasses import replace

    from auraclaw.contracts.commands import CommandContext
    from auraclaw.contracts.errors import CollaborationValidationError
    from auraclaw.contracts.events import Actor, NewEvent
    from auraclaw.infrastructure.persistence.postgres_event_store import PostgresEventStore
    from auraclaw.infrastructure.projection.postgres_task_store import PostgresTaskProjection

    async def scenario():
        assert DATABASE_URL is not None
        tenant = "tree-ledger-" + uuid4().hex
        store = PostgresEventStore(DATABASE_URL)
        projection = PostgresTaskProjection(DATABASE_URL)
        context = CommandContext(
            tenant_id=tenant,
            command_id="root",
            expected_version=0,
            actor=Actor(type="user", id="fixture"),
            correlation_id="fixture",
            operation="create_task",
        )
        try:
            result = await store.append(
                root_session_id="s",
                session_id="s",
                run_id="r",
                context=context,
                command_result={},
                events=[
                    NewEvent(type="session.created", payload={"goal": "fixture"}),
                    NewEvent(
                        type="run.requested",
                        payload={
                            "run_id": "r",
                            "budget": {
                                "policy_version": "2",
                                "max_steps": 4,
                                "max_output_tokens": 10,
                                "tree_max_steps": 8,
                                "tree_max_output_tokens": 20,
                            },
                        },
                    ),
                ],
            )
            await projection.project(result.events)

            async def child(index):
                return await store.append(
                    root_session_id="s",
                    session_id=f"child-{index}",
                    run_id=f"run-{index}",
                    context=replace(context, command_id=f"child-{index}"),
                    command_result={},
                    events=[
                        NewEvent(
                            type="child.created",
                            payload={
                                "parent_session_id": "s",
                                "runtime_budget": {"max_steps": 4, "max_output_tokens": 10},
                            },
                        ),
                        NewEvent(type="run.requested", payload={"run_id": f"run-{index}"}),
                    ],
                )

            outcomes = await asyncio.gather(child(1), child(2), return_exceptions=True)
            assert sum(isinstance(r, CollaborationValidationError) for r in outcomes) == 1
            assert sum(not isinstance(r, BaseException) for r in outcomes) == 1
            reserved = await store.append(
                root_session_id="s",
                session_id="s",
                run_id="r",
                context=replace(context, command_id="reserve", expected_version=2),
                command_result={},
                events=[
                    NewEvent(
                        type="runtime.budget.reserved",
                        payload={"reservation_id": "model-1", "kind": "model", "output_tokens": 5},
                    )
                ],
            )
            await projection.project(reserved.events)
            view = await projection.get_task(tenant, "s")
            assert view["runtime_budget"]["usage"]["steps_used"] == 1
            assert "_facts" not in view["runtime_budget"]
        finally:
            await projection.close()
            await store.close()

    asyncio.run(scenario())
