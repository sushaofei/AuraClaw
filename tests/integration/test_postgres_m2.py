import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.config import get_settings
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import FencingTokenError, LeaseConflictError
from auraclaw.contracts.events import Actor, CanonicalEvent, NewEvent
from auraclaw.control.orchestrator import (
    ManagedOrchestrator,
    RegisteredRuntimeProvisioner,
)
from auraclaw.control.ports import (
    RunnableItem,
    RuntimeAssignment,
    RuntimeBudget,
    RuntimeCheckpoint,
    RuntimeInstance,
)
from auraclaw.control.runnable_feed import RunnableFeedConsumer
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url as _asyncpg_url
from auraclaw.infrastructure.persistence.postgres_control_store import PostgresControlStateStore
from auraclaw.infrastructure.persistence.postgres_event_store import PostgresEventStore

SETTINGS = get_settings()
DATABASE_URL = _asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = tuple(
    (ROOT / path).read_text()
    for path in (
        "migrations/0001_initial.sql",
        "migrations/0002_m1_fact_query.sql",
        "migrations/0003_m2_managed_runtime.sql",
        "migrations/0010_s4_claim_recovery.sql",
        "migrations/0040_runtime_execution_claims.sql",
    )
)
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="PostgreSQL test URL not configured")


class _RecordingSession:
    def __init__(self) -> None:
        self.calls: list[RuntimeAssignment] = []

    async def append(
        self,
        assignment: RuntimeAssignment,
        events: list[NewEvent],
        *,
        command_id: str,
        operation: str,
        expected_version: int | None = None,
    ) -> list[CanonicalEvent]:
        del events, command_id, operation, expected_version
        self.calls.append(assignment)
        return []


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
        if await connection.fetchval(
            """SELECT 1 FROM information_schema.columns
            WHERE table_schema='control' AND table_name='runnable_item'
              AND column_name='claim_token'"""
        ) is None:
            await connection.execute(MIGRATIONS[3])
        if await connection.fetchval(
            """SELECT 1 FROM information_schema.columns
            WHERE table_schema='control' AND table_name='runtime_instance'
              AND column_name='registration_id'"""
        ) is None:
            await connection.execute(MIGRATIONS[4])
    finally:
        await connection.close()


@pytest.mark.parametrize("policy_version", ["1", "2"])
def test_postgres_control_claim_lease_fencing_checkpoint_and_capacity(policy_version: str) -> None:
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
                budget=RuntimeBudget(policy_version=policy_version),
            )
            assert await store_a.enqueue(item)
            claims = await asyncio.gather(
                store_a.claim("orch-a", limit=1), store_b.claim("orch-b", limit=1)
            )
            assert sum(len(batch) for batch in claims) == 1
            runnable_claim = next(batch[0] for batch in claims if batch)

            lease = await store_a.acquire_lease(
                resource_id,
                runnable_claim.claimed_by,
                ttl=timedelta(seconds=30),
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
            with pytest.raises(LeaseConflictError, match="already registered"):
                await store_b.register_runtime(
                    RuntimeInstance(
                        **{
                            **runtime.__dict__,
                            "registration_id": f"replacement-{suffix}",
                        }
                    )
                )
            assignment = RuntimeAssignment(
                tenant_id=tenant_id,
                root_session_id=session_id,
                session_id=session_id,
                run_id=run_id,
                runtime_id=runtime.runtime_id,
                lease_id=lease.lease_id,
                fencing_token=lease.fencing_token,
                role="root",
                resource_profile={}, user_id="user-a", dept_id="dept-a",
                budget=item.budget,
            )
            assert await store_a.assign(
                task_id,
                assignment,
                claim_token=runnable_claim.claim_token,
            )
            assert await store_a.get_assignment(task_id) == assignment
            await store_a.heartbeat(runtime.runtime_id, lease.fencing_token)
            if policy_version == "2":
                assert await store_a.select_runtime(item) is None
                assert await store_a.claim_assignments(runtime.runtime_id, runtime.role) == []
                await store_a.register_runtime(replace(
                    runtime, capabilities={"budget_policy_v2": True}))
                # Assigned capacity is already reserved.
                assert (await store_a.select_runtime(item)) is None
            claimed_assignments = await store_a.claim_assignments(
                runtime.runtime_id, runtime.role
            )
            assert len(claimed_assignments) == 1
            execution = claimed_assignments[0].assignment
            assert execution.execution_claim_token is not None
            assert await store_b.claim_assignments(runtime.runtime_id, runtime.role) == []
            renewed = await store_a.renew_assignment_claim(
                task_id,
                runtime_id=runtime.runtime_id,
                registration_id=runtime.registration_id,
                execution_claim_token=execution.execution_claim_token,
                lease_id=execution.lease_id,
                fencing_token=execution.fencing_token,
            )
            assert renewed.lease_expires_at is not None
            assert renewed.execution_claim_expires_at is not None
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
            await store_a.suspend_assignment(task_id, "waiting_children")
            assert await store_b.list_waiting_assignments(limit=0) == ()
            waiting = await store_b.list_waiting_assignments(limit=10)
            assert len(waiting) == 1
            assert waiting[0].session_id == assignment.session_id
            assert waiting[0].run_id == assignment.run_id
            assert await store_b.wake_assignment(task_id)
            await store_a.suspend_assignment(task_id, "waiting_for_tool")
            tool_waiting = await store_b.list_waiting_assignments(status="waiting_for_tool")
            assert len(tool_waiting) == 1 and tool_waiting[0].run_id == assignment.run_id
            restored_identity = await store_b.get_assignment(task_id)
            assert restored_identity is not None
            assert (restored_identity.user_id, restored_identity.dept_id) == ("user-a", "dept-a")
            assert await store_b.wake_assignment(task_id)
            assert not await store_a.wake_assignment(task_id)
            assert await store_a.list_waiting_assignments(status="waiting_for_tool") == ()
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


def test_postgres_runnable_feed_and_two_orchestrators_schedule_exactly_once() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        await _apply_migrations()
        suffix = uuid4().hex
        tenant_id = f"tenant-feed-s4-{suffix}"
        session_id = f"session-feed-s4-{suffix}"
        run_id = f"run-feed-s4-{suffix}"
        events = PostgresEventStore(DATABASE_URL)
        control = PostgresControlStateStore(DATABASE_URL)
        session = _RecordingSession()
        try:
            await events.append(
                root_session_id=session_id,
                session_id=session_id,
                run_id=run_id,
                context=CommandContext(
                    command_id=f"create-{suffix}",
                    tenant_id=tenant_id,
                    actor=Actor(type="user", id="integration"),
                    correlation_id=run_id,
                    expected_version=0,
                    operation="create_task",
                ),
                events=(
                    NewEvent(
                        type="session.created",
                        payload={"goal": "feed", "role": "root"},
                    ),
                    NewEvent(type="run.requested", payload={"run_id": run_id}),
                ),
                command_result={},
            )
            projection_destination = f"projection-test-{suffix}"
            isolation_connection = await asyncpg.connect(DATABASE_URL)
            try:
                await isolation_connection.execute(
                    """UPDATE session_core.outbox SET destination=$2
                    WHERE destination='projection' AND event_id IN
                      (SELECT event_id FROM session_core.canonical_event WHERE tenant_id=$1)""",
                    tenant_id,
                    projection_destination,
                )
            finally:
                await isolation_connection.close()
            projection_claims = await asyncio.gather(
                events.claim_outbox(
                    projection_destination,
                    f"projection-a-{suffix}",
                    limit=10,
                    claim_ttl=timedelta(seconds=30),
                ),
                events.claim_outbox(
                    projection_destination,
                    f"projection-b-{suffix}",
                    limit=10,
                    claim_ttl=timedelta(seconds=30),
                ),
            )
            first_projection = next(batch[0] for batch in projection_claims if batch)
            assert sum(len(batch) for batch in projection_claims) == 1
            assert first_projection.event.aggregate_version == 1
            assert await events.disposition_outbox(
                projection_destination,
                f"projection-{'a' if projection_claims[0] else 'b'}-{suffix}",
                first_projection.outbox_id,
                first_projection.claim_token,
                "ack",
            )
            second_projection = await events.claim_outbox(
                projection_destination,
                f"projection-c-{suffix}",
                limit=10,
                claim_ttl=timedelta(seconds=30),
            )
            assert [record.event.aggregate_version for record in second_projection] == [2]
            feeds = (
                RunnableFeedConsumer(events, control, worker_id=f"orch-a-{suffix}"),
                RunnableFeedConsumer(events, control, worker_id=f"orch-b-{suffix}"),
            )
            assert sum(await asyncio.gather(*(feed.run_once() for feed in feeds))) == 1
            for runtime_id in (f"runtime-a-{suffix}", f"runtime-b-{suffix}"):
                await control.register_runtime(
                    RuntimeInstance(
                        runtime_id=runtime_id,
                        runtime_type="agent",
                        role="root",
                        node_id=runtime_id,
                        capabilities={},
                        capacity=1,
                    )
                )
            orchestrators = tuple(
                ManagedOrchestrator(
                    orchestrator_id=f"orch-{index}-{suffix}",
                    control_store=control,
                    session=session,
                    provisioner=RegisteredRuntimeProvisioner(control),
                )
                for index in range(2)
            )
            assignments = await asyncio.gather(
                *(orchestrator.schedule_once() for orchestrator in orchestrators)
            )
            assert sum(assignment is not None for assignment in assignments) == 1
            assert len(session.calls) == 1
        finally:
            await events.close()
            await control.close()
            connection = await asyncpg.connect(DATABASE_URL)
            try:
                await connection.execute(
                    "DELETE FROM control.assignment WHERE tenant_id=$1", tenant_id
                )
                await connection.execute(
                    "DELETE FROM control.runnable_item WHERE tenant_id=$1", tenant_id
                )
                await connection.execute(
                    "DELETE FROM control.runtime_lease WHERE resource_id=$1",
                    f"session:{tenant_id}:{session_id}",
                )
                await connection.execute(
                    "DELETE FROM control.runtime_instance WHERE runtime_id LIKE $1",
                    f"%-{suffix}",
                )
                await connection.execute(
                    """DELETE FROM session_core.outbox WHERE event_id IN
                    (SELECT event_id FROM session_core.canonical_event WHERE tenant_id=$1)""",
                    tenant_id,
                )
                await connection.execute(
                    "DELETE FROM session_core.canonical_event WHERE tenant_id=$1", tenant_id
                )
                await connection.execute(
                    "DELETE FROM session_core.command_dedup WHERE tenant_id=$1", tenant_id
                )
                await connection.execute(
                    "DELETE FROM session_core.session_head WHERE tenant_id=$1", tenant_id
                )
            finally:
                await connection.close()

    asyncio.run(scenario())


def test_postgres_skill_terminal_is_exclusive_across_committers_and_restart() -> None:
    from auraclaw.runtime.event_committer import CanonicalEventCommitter

    async def scenario() -> None:
        assert DATABASE_URL is not None
        suffix = uuid4().hex
        assignment = RuntimeAssignment(
            tenant_id=f"terminal-{suffix}", root_session_id="root", session_id="session",
            run_id="run", runtime_id="runtime", lease_id="lease", fencing_token=1,
            role="worker", resource_profile={},
        )
        first, second = PostgresEventStore(DATABASE_URL), PostgresEventStore(DATABASE_URL)

        class Session:
            def __init__(self, store):
                self.store = store

            async def load(self, assignment):
                return await self.store.load(assignment.tenant_id, assignment.session_id)

            async def append(self, assignment, events, *, command_id, operation,
                             expected_version=None):
                result = await self.store.append(
                    root_session_id=assignment.root_session_id, session_id=assignment.session_id,
                    run_id=assignment.run_id, events=events, command_result={},
                    context=CommandContext(
                        command_id=command_id, operation=operation, tenant_id=assignment.tenant_id,
                        actor=Actor(type="runtime", id="runtime"), correlation_id="terminal-test",
                        causation_id="test", expected_version=expected_version or 0,
                    ),
                )
                return result.events

        class Guard:
            async def check(self, assignment):
                pass

        try:
            a = CanonicalEventCommitter(Session(first), Guard())
            b = CanonicalEventCommitter(Session(second), Guard())
            await a.append_capability_event(assignment, NewEvent(
                type="skill.failed", payload={"skill_activation_id": "activation"},
            ))
            # A stale checkpoint/chat completion cannot overwrite the canonical failure.
            await asyncio.gather(*(b.append_capability_event(assignment, NewEvent(
                type=kind, payload={"skill_activation_id": "activation"},
            )) for kind in ("skill.failed", "skill.completed", "skill.cancelled")))
            events = await first.load(assignment.tenant_id, assignment.session_id)
            assert [event.type for event in events] == ["skill.failed"]
        finally:
            await first.close()
            await second.close()
            connection = await asyncpg.connect(DATABASE_URL)
            await connection.execute(
                "DELETE FROM session_core.outbox WHERE event_id IN "
                "(SELECT event_id FROM session_core.canonical_event WHERE tenant_id=$1)",
                assignment.tenant_id,
            )
            for table in ("canonical_event", "command_dedup", "session_head"):
                await connection.execute(f"DELETE FROM session_core.{table} WHERE tenant_id=$1",
                                         assignment.tenant_id)
            await connection.close()
    asyncio.run(scenario())
