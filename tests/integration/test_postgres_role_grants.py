import asyncio
import os
from pathlib import Path

import asyncpg
import pytest
from dotenv import dotenv_values

from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url

ROOT = Path(__file__).resolve().parents[2]
DOTENV = dotenv_values(ROOT / ".env")

ROLE_TARGETS = {
    "SESSION_DATABASE_URL": (
        "auraclaw_session",
        "session_core.session_head",
        "tenant_id",
    ),
    "PROJECTION_DATABASE_URL": (
        "auraclaw_projection",
        "projection.task_view",
        "tenant_id",
    ),
    "CONTROL_DATABASE_URL": (
        "auraclaw_control",
        "control.runtime_lease",
        "resource_id",
    ),
    "DELIVERY_DATABASE_URL": (
        "auraclaw_delivery",
        "delivery.delivery_job",
        "tenant_id",
    ),
    "HANDS_DATABASE_URL": ("auraclaw_hands", "hands.invocation", "tenant_id"),
    "POLICY_DATABASE_URL": ("auraclaw_policy", "policy.decision", "tenant_id"),
    "CREDENTIAL_DATABASE_URL": (
        "auraclaw_credential",
        "credential.reference",
        "tenant_id",
    ),
    "ARTIFACT_DATABASE_URL": (
        "auraclaw_artifact",
        "artifact.metadata",
        "tenant_id",
    ),
}
QUERY_ROLE = ("TASK_QUERY_DATABASE_URL", "auraclaw_task_query_ro")


def _configured_url(name: str) -> str | None:
    value = os.getenv(name) or DOTENV.get(name)
    return asyncpg_url(value) if value else None


async def _assert_hardened_login(
    connection: asyncpg.Connection, expected_role: str
) -> None:
    role = await connection.fetchrow(
        """SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolinherit
        FROM pg_roles WHERE rolname = current_user"""
    )
    assert role is not None
    assert role["rolname"] == expected_role
    assert not role["rolsuper"]
    assert not role["rolcreatedb"]
    assert not role["rolcreaterole"]
    assert not role["rolinherit"]


async def _assert_owner_dml(
    connection: asyncpg.Connection, table: str, update_column: str
) -> None:
    await connection.execute(f"SELECT 1 FROM {table} WHERE FALSE")
    await connection.execute(f"INSERT INTO {table} SELECT * FROM {table} WHERE FALSE")
    await connection.execute(
        f"UPDATE {table} SET {update_column} = {update_column} WHERE FALSE"
    )
    await connection.execute(f"DELETE FROM {table} WHERE FALSE")


async def _assert_write_denied(
    connection: asyncpg.Connection, table: str, update_column: str
) -> None:
    statements = (
        f"INSERT INTO {table} SELECT * FROM {table} WHERE FALSE",
        f"UPDATE {table} SET {update_column} = {update_column} WHERE FALSE",
        f"DELETE FROM {table} WHERE FALSE",
    )
    for statement in statements:
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.execute(statement)


def test_production_roles_enforce_owner_and_query_boundaries() -> None:
    required_names = (*ROLE_TARGETS, QUERY_ROLE[0])
    urls = {name: _configured_url(name) for name in required_names}
    missing = [name for name, url in urls.items() if url is None]
    if missing:
        pytest.skip("production role DSNs not configured: " + ", ".join(missing))

    async def scenario() -> None:
        tables = tuple(target[1] for target in ROLE_TARGETS.values())
        for env_name, (expected_role, owner_table, update_column) in ROLE_TARGETS.items():
            url = urls[env_name]
            assert url is not None
            connection = await asyncpg.connect(url)
            try:
                await _assert_hardened_login(connection, expected_role)
                await _assert_owner_dml(connection, owner_table, update_column)
                for foreign_table in tables:
                    if foreign_table == owner_table:
                        continue
                    with pytest.raises(asyncpg.InsufficientPrivilegeError):
                        await connection.execute(
                            f"SELECT 1 FROM {foreign_table} WHERE FALSE"
                        )
            finally:
                await connection.close()

        query_url = urls[QUERY_ROLE[0]]
        assert query_url is not None
        query_connection = await asyncpg.connect(query_url)
        try:
            await _assert_hardened_login(query_connection, QUERY_ROLE[1])
            await query_connection.execute(
                "SELECT 1 FROM projection.task_view WHERE FALSE"
            )
            await _assert_write_denied(
                query_connection, "projection.task_view", "tenant_id"
            )
            for table in tables:
                if table == "projection.task_view":
                    continue
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await query_connection.execute(f"SELECT 1 FROM {table} WHERE FALSE")
        finally:
            await query_connection.close()

    asyncio.run(scenario())
