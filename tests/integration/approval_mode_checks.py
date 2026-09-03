"""Approval-mode checks run against the migration suite's isolated PostgreSQL cluster."""

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from auraclaw.contracts.approval_mode import ApprovalMode, InteractionMode
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.events import Actor
from auraclaw.contracts.internal import (
    InternalRequestContext,
    PolicyEvaluateRequest,
    ServiceIdentity,
)
from auraclaw.contracts.tools import PolicyDecision
from auraclaw.domain.session import SessionAggregate
from auraclaw.gateways.task.admission import AllowAllAdmissionController
from auraclaw.infrastructure.persistence.postgres_event_store import PostgresEventStore
from auraclaw.infrastructure.projection.postgres_task_store import PostgresTaskProjection
from auraclaw.policy.approval_modes import ApprovalModeResolver, ReviewResult
from auraclaw.projection.relay import OutboxRelay
from auraclaw.session.ports import SessionSnapshot
from auraclaw.session.task_service import TaskService


async def check_approval_modes(connection, database_url: str, migration_dir: Path) -> None:
    from auraclaw.infrastructure.projection.postgres_approval_store import (
        PostgresApprovalProjection,
    )

    approvals = PostgresApprovalProjection(database_url)
    try:
        assert (
            await approvals.find_approved(
                "approval-pg", "missing", "digest", "v1", run_id="current"
            )
            is None
        )
    finally:
        await approvals.close()
    store = PostgresEventStore(database_url)
    second_store = PostgresEventStore(database_url)
    projection = PostgresTaskProjection(database_url)
    try:
        service = TaskService(
            event_store=store,
            reader=projection,
            relay=OutboxRelay(store, projection),
            admission=AllowAllAdmissionController(),
        )
        ctx = CommandContext(
            command_id="approval-pg",
            tenant_id="approval-pg",
            actor=Actor(type="user", id="test"),
            expected_version=0,
            correlation_id="test",
            operation="create_task",
        )
        accepted = await service.create_task(
            goal="read a sensitive report",
            context=ctx,
            interaction_mode=InteractionMode.NON_STREAMING,
            approval_mode=ApprovalMode.AUTO_REVIEW,
        )
        with pytest.raises(VersionConflictError):
            await service.create_task(
                goal="read a sensitive report", context=ctx, approval_mode=ApprovalMode.FULL_ACCESS
            )
        sid = accepted["session_id"]
        aggregate = SessionAggregate.from_events(await store.load("approval-pg", sid))
        await store.save_snapshot(
            SessionSnapshot(
                tenant_id="approval-pg",
                session_id=sid,
                aggregate_version=aggregate.version,
                schema_version=1,
                state=aggregate.snapshot_state(),
            )
        )
        snapshot = await second_store.get_snapshot("approval-pg", sid)
        assert (
            SessionAggregate.from_snapshot(
                snapshot.state, snapshot.aggregate_version
            ).approval.effective_approval_mode
            == ApprovalMode.AUTO_REVIEW
        )
        reviewer = AsyncMock()
        reviewer.review.return_value = ReviewResult(approved=True, reason="safe authorized read")
        request = PolicyEvaluateRequest(
            context=InternalRequestContext(
                tenant_id="approval-pg",
                service_identity=ServiceIdentity.ACTION_HANDS,
                request_id="pg-review",
                correlation_id="test",
                causation_id="test",
            ),
            session_id=sid,
            run_id=accepted["run_id"],
            subject="test",
            action="sensitive.read",
            resource="report",
            input_digest="args",
        )
        first = await ApprovalModeResolver(store, reviewer).resolve(
            request, PolicyDecision.REQUIRE_APPROVAL, "v1"
        )
        second = await ApprovalModeResolver(second_store).resolve(
            request, PolicyDecision.REQUIRE_APPROVAL, "v1"
        )
        assert first == second and first[0] == PolicyDecision.ALLOW
        assert reviewer.review.await_count == 1
        events = await second_store.load("approval-pg", sid)
        await projection.project(events)
        assert (await projection.get_task("approval-pg", sid))[
            "effective_approval_mode"
        ] == "auto_review"
        # Projection is disposable: field rollback/up and canonical rebuild restore mode.
        await connection.execute((migration_dir / "0059_approval_modes.down.sql").read_text())
        await connection.execute((migration_dir / "0059_approval_modes.sql").read_text())
        await projection.rebuild(events, tenant_id="approval-pg")
        restored = await projection.get_task("approval-pg", sid)
        assert restored["effective_approval_mode"] == "auto_review"
        assert restored["approval_mode_revision"] == 1
    finally:
        await store.close()
        await second_store.close()
        await projection.close()
