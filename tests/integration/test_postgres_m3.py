import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.config import get_settings
from auraclaw.contracts.events import Actor, CanonicalEvent
from auraclaw.contracts.state import Visibility
from auraclaw.infrastructure.postgres import PostgresApprovalProjection, _asyncpg_url

SETTINGS = get_settings()
DATABASE_URL = _asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = tuple(
    (ROOT / f"migrations/000{version}_{name}.sql").read_text()
    for version, name in (
        (1, "initial"),
        (2, "m1_fact_query"),
        (3, "m2_managed_runtime"),
        (4, "m3_tool_artifact_approval"),
    )
)
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="PostgreSQL test URL not configured")


async def _apply_migrations() -> None:
    assert DATABASE_URL is not None
    connection = await asyncpg.connect(DATABASE_URL)
    try:
        if await connection.fetchval("SELECT to_regclass('session_core.canonical_event')") is None:
            await connection.execute(MIGRATIONS[0])
        if await connection.fetchval("SELECT to_regclass('projection.poison_event')") is None:
            await connection.execute(MIGRATIONS[1])
        if await connection.fetchval("SELECT to_regclass('control.runtime_checkpoint')") is None:
            await connection.execute(MIGRATIONS[2])
        if await connection.fetchval("SELECT to_regclass('projection.approval_view')") is None:
            await connection.execute(MIGRATIONS[3])
    finally:
        await connection.close()


def test_postgres_approval_projection_is_rebuildable_from_canonical_events() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        await _apply_migrations()
        suffix = uuid4().hex
        tenant_id = f"tenant-pg-m3-{suffix}"
        session_id = f"session-pg-m3-{suffix}"
        approval_id = f"approval-pg-m3-{suffix}"
        occurred_at = datetime.now(UTC)

        def event(event_type: str, payload: dict[str, object], version: int) -> CanonicalEvent:
            return CanonicalEvent(
                event_id=f"event-pg-m3-{suffix}-{version}",
                tenant_id=tenant_id,
                root_session_id=session_id,
                session_id=session_id,
                run_id=f"run-pg-m3-{suffix}",
                aggregate_version=version,
                type=event_type,
                occurred_at=occurred_at,
                actor=Actor(type="runtime", id="runtime-pg-m3"),
                correlation_id=f"corr-pg-m3-{suffix}",
                causation_id=f"cause-pg-m3-{suffix}-{version}",
                visibility=Visibility.INTERNAL,
                schema_version=1,
                payload=payload,
            )

        requested = event(
            "approval.requested",
            {
                "approval_id": approval_id,
                "run_id": f"run-pg-m3-{suffix}",
                "action_digest": "digest-pg-m3",
                "tool_name": "managed",
                "redacted_arguments": {"target": "safe"},
                "risk": "high",
                "reason": "write",
                "expected_effect": "write",
                "allowed_decisions": ["approved", "rejected"],
                "assigned_approvers": [],
                "policy_version": "m3-v1",
                "expires_at": (occurred_at + timedelta(hours=1)).isoformat(),
                "status": "waiting",
            },
            3,
        )
        approved = event(
            "approval.approved",
            {"approval_id": approval_id, "decision": "approved"},
            4,
        )
        projection = PostgresApprovalProjection(DATABASE_URL)
        try:
            await projection.project([requested, approved, approved])
            record = await projection.get(tenant_id, approval_id)
            assert record is not None and record.status.value == "approved"
            matched = await projection.find_approved(
                tenant_id, session_id, "digest-pg-m3", "m3-v1"
            )
            assert matched is not None and matched.approval_id == approval_id
        finally:
            await projection.close()
            cleanup = await asyncpg.connect(DATABASE_URL)
            try:
                await cleanup.execute(
                    "DELETE FROM projection.approval_view WHERE tenant_id=$1", tenant_id
                )
                await cleanup.execute(
                    """DELETE FROM projection.processed_event
                    WHERE projector_id='approval' AND event_id LIKE $1""",
                    f"event-pg-m3-{suffix}-%",
                )
            finally:
                await cleanup.close()

    asyncio.run(scenario())
