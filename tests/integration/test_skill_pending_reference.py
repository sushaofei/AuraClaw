from __future__ import annotations

import asyncio
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.config import get_settings
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.events import Actor, NewEvent
from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url
from auraclaw.infrastructure.persistence.postgres_event_store import PostgresEventStore

SETTINGS = get_settings()
DATABASE_URL = asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None


@pytest.mark.parametrize(
    "postgres",
    [
        False,
        pytest.param(
            True,
            marks=pytest.mark.skipif(DATABASE_URL is None, reason="Explicit PostgreSQL required"),
        ),
    ],
)
def test_terminal_run_retains_unknown_write_and_releases_settled_activation(postgres: bool) -> None:
    async def scenario() -> None:
        tenant = "skill-pending-" + uuid4().hex
        store = PostgresEventStore(DATABASE_URL) if postgres else InMemoryEventStore()
        version = 0

        async def append(kind, **payload):
            nonlocal version
            await store.append(
                root_session_id="session",
                session_id="session",
                run_id="run",
                context=CommandContext(
                    tenant_id=tenant,
                    command_id=f"cmd-{version}",
                    expected_version=version,
                    actor=Actor(type="runtime", id="fixture"),
                    correlation_id="test",
                    causation_id="test",
                    operation="test",
                ),
                events=[NewEvent(type=kind, payload=payload)],
                command_result={},
            )
            version += 1

        async def referenced():
            return await store.has_active_skill_reference(
                tenant, "platform", "example", "sha256:old"
            )

        try:
            await append(
                "skill.activated",
                skill_activation_id="activation",
                activation={
                    "binding": {
                        "publisher": "platform",
                        "skill_name": "example",
                        "package_digest": "sha256:old",
                    }
                },
            )
            assert await referenced()
            await append("skill.completed", skill_activation_id="activation")
            assert not await referenced()  # The Run can continue with unrelated work.
            await append(
                "skill.activated",
                skill_activation_id="second",
                activation={
                    "binding": {
                        "publisher": "platform",
                        "skill_name": "example",
                        "package_digest": "sha256:old",
                    }
                },
            )
            receipt = {"skill_activation_id": "second", "tool_invocation_id": "write"}
            await append("skill.invocation.requested", **receipt, invocation_cycle=1)
            await append("skill.invocation.settled", **receipt, invocation_cycle=1)
            await append("skill.invocation.requested", **receipt, invocation_cycle=2)
            await append("run.cancelled", run_id="run")
            assert (
                await referenced()
            )  # A previous approval cycle's receipt cannot release this write.
            await append("skill.failed", skill_activation_id="second")
            assert await referenced()  # Even an erroneous terminal must not hide unknown effects.
            await append("skill.invocation.settled", **receipt, invocation_cycle=2)
            assert not await referenced()
        finally:
            if postgres:
                await store.close()
                connection = await asyncpg.connect(DATABASE_URL)
                await connection.execute(
                    "DELETE FROM session_core.outbox WHERE event_id IN "
                    "(SELECT event_id FROM session_core.canonical_event WHERE tenant_id=$1)",
                    tenant,
                )
                for table in (
                    "command_dedup",
                    "canonical_event",
                    "session_snapshot",
                    "session_head",
                ):
                    if await connection.fetchval("SELECT to_regclass($1)", f"session_core.{table}"):
                        await connection.execute(
                            f"DELETE FROM session_core.{table} WHERE tenant_id=$1", tenant
                        )
                await connection.close()

    asyncio.run(scenario())
