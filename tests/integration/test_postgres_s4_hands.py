import asyncio
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.action.policy import PolicyEngine
from auraclaw.action.tool_gateway import ToolGateway, ToolRegistry
from auraclaw.config import get_settings
from auraclaw.contracts.tools import (
    RiskLevel,
    ToolCapability,
    ToolInvocation,
    ToolPermission,
    ToolResult,
    ToolResultStatus,
)
from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage
from auraclaw.infrastructure.hands.local import LocalHandsService
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url
from auraclaw.infrastructure.persistence.postgres_invocation_store import (
    PostgresInvocationStore,
)
from auraclaw.projection.approval.projector import InMemoryApprovalProjection

SETTINGS = get_settings()
DATABASE_URL = asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
ROOT = Path(__file__).resolve().parents[2]
MIGRATION = "\n".join(
    (ROOT / path).read_text()
    for path in (
        "migrations/0009_s3_owner_boundaries.sql",
        "migrations/0044_hands_invocation_claims.sql",
    )
)
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
                store_a.begin(
                    invocation,
                    "digest-s4",
                    owner="hands-a",
                    claim_token="claim-a",
                    claim_ttl=timedelta(seconds=30),
                ),
                store_b.begin(
                    invocation,
                    "digest-s4",
                    owner="hands-b",
                    claim_token="claim-b",
                    claim_ttl=timedelta(seconds=30),
                ),
            )
            assert sum(result.acquired for result in starts) == 1
            recovered = next(result.cached_result for result in starts if result.cached_result)
            assert recovered.error_code == "invocation_in_progress"

            completed = ToolResult(
                status=ToolResultStatus.SUCCESS,
                content={"replica": "winner"},
                side_effect_status="completed",
            )
            winner = next(result for result in starts if result.acquired)
            assert winner.claim_token is not None
            winner_store = store_a if winner.claim_token == "claim-a" else store_b
            assert await winner_store.complete(
                invocation, completed, claim_token=winner.claim_token
            )
            cached = await store_b.begin(
                invocation,
                "digest-s4",
                owner="hands-b",
                claim_token="claim-after-complete",
                claim_ttl=timedelta(seconds=30),
            )
            assert cached.cached_result == completed
            colliding = replace(
                invocation, idempotency_key=f"different-idempotency-{suffix}"
            )
            collision = await store_b.begin(
                colliding,
                "digest-s4",
                owner="hands-b",
                claim_token="claim-primary-collision",
                claim_ttl=timedelta(seconds=30),
            )
            assert collision.conflict
        finally:
            await store_a.close()
            await store_b.close()
            await connection.execute(
                "DELETE FROM hands.invocation WHERE tenant_id=$1", tenant_id
            )
            await connection.close()

    asyncio.run(scenario())


def test_hands_approval_and_cancel_state_survive_replica_changes() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        await connection.execute(MIGRATION)
        suffix = uuid4().hex
        tenant_id = f"tenant-hands-recovery-{suffix}"
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
            started = await store_a.begin(
                invocation,
                "digest-approval",
                owner="hands-a",
                claim_token="claim-waiting",
                claim_ttl=timedelta(seconds=30),
            )
            assert started.acquired
            waiting = ToolResult(
                status=ToolResultStatus.DENIED,
                summary="approval required",
                metadata={"approval_request": {"approval_id": "approval-1"}},
                error_code="approval_required",
            )
            assert await store_a.wait_for_approval(
                invocation, waiting, claim_token="claim-waiting"
            )
            replay = await store_b.begin(
                invocation,
                "digest-approval",
                owner="hands-b",
                claim_token="claim-replay",
                claim_ttl=timedelta(seconds=30),
            )
            assert replay.cached_result == waiting

            approved = replace(invocation, approval_id="approval-1")
            resumed = await store_b.begin(
                approved,
                "digest-approval",
                owner="hands-b",
                claim_token="claim-approved",
                claim_ttl=timedelta(seconds=30),
            )
            assert resumed.acquired
            assert await store_a.request_cancel(
                tenant_id, invocation.tool_invocation_id
            )
            assert await store_a.request_cancel(
                tenant_id, invocation.tool_invocation_id
            )
            assert await store_b.is_cancel_requested(
                approved, claim_token="claim-approved"
            )
            cancelled = ToolResult(
                status=ToolResultStatus.CANCELLED,
                summary="cancelled across replicas",
                error_code="tool_cancelled",
                side_effect_status="unknown",
            )
            assert await store_b.complete(
                approved, cancelled, claim_token="claim-approved"
            )
            status = await store_a.get_status(
                tenant_id, invocation.tool_invocation_id
            )
            assert status is not None
            assert status.status == "cancelled"
            assert status.cancel_requested

            abandoned = replace(
                invocation,
                tool_invocation_id=f"abandoned-{suffix}",
                idempotency_key=f"abandoned-idem-{suffix}",
                approval_id=None,
            )
            assert (
                await store_a.begin(
                    abandoned,
                    "digest-abandoned",
                    owner="hands-a",
                    claim_token="claim-abandoned",
                    claim_ttl=timedelta(seconds=30),
                )
            ).acquired
            await connection.execute(
                """UPDATE hands.invocation SET execution_claim_expires_at=
                   now()-interval '1 second'
                WHERE tenant_id=$1 AND tool_invocation_id=$2""",
                tenant_id,
                abandoned.tool_invocation_id,
            )
            takeover = await store_b.begin(
                abandoned,
                "digest-abandoned",
                owner="hands-b",
                claim_token="claim-takeover",
                claim_ttl=timedelta(seconds=30),
            )
            assert takeover.acquired
            assert takeover.claim_token == "claim-takeover"
        finally:
            await store_a.close()
            await store_b.close()
            await connection.execute(
                "DELETE FROM hands.invocation WHERE tenant_id=$1", tenant_id
            )
            await connection.close()

    asyncio.run(scenario())


def test_cancel_sent_to_another_hands_replica_stops_the_owner() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        await connection.execute(MIGRATION)
        suffix = uuid4().hex
        tenant_id = f"tenant-hands-cancel-{suffix}"
        invocation = ToolInvocation(
            tool_invocation_id=f"invocation-{suffix}",
            tenant_id=tenant_id,
            root_session_id=f"root-{suffix}",
            session_id=f"session-{suffix}",
            run_id=f"run-{suffix}",
            tool_name="managed",
            tool_version="1",
            arguments={"value": 1},
            expected_side_effect="read",
            idempotency_key=f"idem-{suffix}",
            deadline=None,
            fencing_token=1,
            actor_id="runtime-s4",
        )
        started = asyncio.Event()

        async def slow(arguments: dict[str, object]) -> dict[str, object]:
            started.set()
            await asyncio.sleep(60)
            return arguments

        capability = ToolCapability(
            name="managed",
            version="1",
            description="cross-replica cancellation test",
            input_schema={"type": "object"},
            output_schema={"type": "object"},
            permission=ToolPermission.READ_ONLY,
            risk_level=RiskLevel.LOW,
        )
        store_a = PostgresInvocationStore(DATABASE_URL)
        store_b = PostgresInvocationStore(DATABASE_URL)

        def gateway(store: PostgresInvocationStore, instance_id: str) -> ToolGateway:
            return ToolGateway(
                registry=ToolRegistry((capability,)),
                policy=PolicyEngine(),
                approvals=InMemoryApprovalProjection(),
                hands=LocalHandsService(
                    workspace_root=ROOT, handlers={"managed": slow}
                ),
                artifacts=ArtifactStore(
                    InMemoryObjectStorage(), signing_key=b"hands-cross-replica-key"
                ),
                invocation_store=store,
                instance_id=instance_id,
                execution_claim_ttl=timedelta(seconds=2),
                cancellation_poll_interval=0.01,
            )

        owner = gateway(store_a, "hands-a")
        other = gateway(store_b, "hands-b")
        try:
            running = asyncio.create_task(owner.execute(invocation))
            await asyncio.wait_for(started.wait(), timeout=2)
            assert await other.cancel(
                invocation.tool_invocation_id, tenant_id=tenant_id
            )
            result = await asyncio.wait_for(running, timeout=2)
            assert result.status is ToolResultStatus.CANCELLED
            status = await store_b.get_status(tenant_id, invocation.tool_invocation_id)
            assert status is not None
            assert status.status == "cancelled"
            assert status.cancel_requested
        finally:
            await store_a.close()
            await store_b.close()
            await connection.execute(
                "DELETE FROM hands.invocation WHERE tenant_id=$1", tenant_id
            )
            await connection.close()

    asyncio.run(scenario())


def test_same_key_waiter_timeout_does_not_stop_owner_heartbeat() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        await connection.execute(MIGRATION)
        suffix = uuid4().hex
        tenant_id = f"tenant-hands-waiter-{suffix}"
        invocation = ToolInvocation(
            tool_invocation_id=f"invocation-{suffix}",
            tenant_id=tenant_id,
            root_session_id=f"root-{suffix}",
            session_id=f"session-{suffix}",
            run_id=f"run-{suffix}",
            tool_name="managed",
            tool_version="1",
            arguments={"value": 1},
            expected_side_effect="read",
            idempotency_key=f"idem-{suffix}",
            deadline=None,
            fencing_token=1,
            actor_id="runtime-s4",
        )
        started = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def slow(arguments: dict[str, object]) -> dict[str, object]:
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            return arguments

        capability = ToolCapability(
            name="managed",
            version="1",
            description="same-key waiter heartbeat test",
            input_schema={"type": "object"},
            output_schema={"type": "object"},
            permission=ToolPermission.READ_ONLY,
            risk_level=RiskLevel.LOW,
        )
        store = PostgresInvocationStore(DATABASE_URL)
        gateway = ToolGateway(
            registry=ToolRegistry((capability,)),
            policy=PolicyEngine(),
            approvals=InMemoryApprovalProjection(),
            hands=LocalHandsService(workspace_root=ROOT, handlers={"managed": slow}),
            artifacts=ArtifactStore(
                InMemoryObjectStorage(), signing_key=b"hands-waiter-heartbeat-key"
            ),
            invocation_store=store,
            instance_id="hands-waiter-owner",
            execution_claim_ttl=timedelta(seconds=1.2),
            cancellation_poll_interval=0.01,
            max_concurrent=1,
            max_concurrent_per_tenant=1,
            queue_timeout=0.05,
        )
        owner: asyncio.Task[ToolResult] | None = None
        try:
            owner = asyncio.create_task(gateway.execute(invocation))
            await asyncio.wait_for(started.wait(), timeout=2)
            waiter = await asyncio.wait_for(gateway.execute(invocation), timeout=2)
            assert waiter.error_code == "hands_capacity_exhausted"
            assert waiter.metadata["capacity_reason"] == "queue_timeout"
            await asyncio.sleep(1.5)
            release.set()
            result = await asyncio.wait_for(owner, timeout=2)
            assert result.status is ToolResultStatus.SUCCESS
            assert calls == 1
        finally:
            release.set()
            if owner is not None and not owner.done():
                owner.cancel()
                await asyncio.gather(owner, return_exceptions=True)
            await store.close()
            await connection.execute(
                "DELETE FROM hands.invocation WHERE tenant_id=$1", tenant_id
            )
            await connection.close()

    asyncio.run(scenario())
