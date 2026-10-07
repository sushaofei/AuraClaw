import asyncio
import os
from pathlib import Path

import asyncpg
import pytest
from dotenv import dotenv_values

from auraclaw.config import get_settings
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url

ROOT = Path(__file__).resolve().parents[2]
DOTENV = (
    {}
    if os.environ.get("AURACLAW_DISABLE_ENV_FILE") == "1"
    else dotenv_values(ROOT / ".env.dev")
)
SETTINGS = get_settings()

ROLE_TARGETS = {
    "AURACLAW_SESSION_DATABASE_URL": (
        "auraclaw_session",
        "session_core.session_head",
        "tenant_id",
    ),
    "AURACLAW_PROJECTION_DATABASE_URL": (
        "auraclaw_projection",
        "projection.task_view",
        "tenant_id",
    ),
    "AURACLAW_ORCHESTRATOR_DATABASE_URL": (
        "auraclaw_control",
        "control.runtime_lease",
        "resource_id",
    ),
    "AURACLAW_DELIVERY_DATABASE_URL": (
        "auraclaw_delivery",
        "delivery.delivery_job",
        "tenant_id",
    ),
    "AURACLAW_ACTION_HANDS_DATABASE_URL": (
        "auraclaw_hands",
        "hands.invocation",
        "tenant_id",
    ),
    "AURACLAW_POLICY_DATABASE_URL": (
        "auraclaw_policy",
        "policy.decision",
        "tenant_id",
    ),
    "AURACLAW_CREDENTIAL_PROXY_DATABASE_URL": (
        "auraclaw_credential",
        "credential.reference",
        "tenant_id",
    ),
    "AURACLAW_ARTIFACT_DATABASE_URL": (
        "auraclaw_artifact",
        "artifact.metadata",
        "tenant_id",
    ),
    "AURACLAW_STREAMING_DATABASE_URL": (
        "auraclaw_streaming",
        "streaming.runtime_event",
        "tenant_id",
    ),
    "AURACLAW_MODEL_GATEWAY_DATABASE_URL": (
        "auraclaw_model",
        "model_gateway.model_call",
        "tenant_id",
    ),
}
TASK_API_ROLE = ("AURACLAW_TASK_API_DATABASE_URL", "auraclaw_task_api")


def _configured_url(name: str) -> str | None:
    value = os.getenv(name) or DOTENV.get(name)
    if not value:
        return None
    return asyncpg_url(value)


async def _catalog_has_roles(database_url: str, roles: tuple[str, ...]) -> bool:
    connection = await asyncpg.connect(database_url)
    try:
        installed = await connection.fetch(
            "SELECT rolname FROM pg_roles WHERE rolname=ANY($1::text[])",
            list(roles),
        )
        return {str(row["rolname"]) for row in installed} == set(roles)
    finally:
        await connection.close()


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


async def _assert_catalog_role(
    connection: asyncpg.Connection,
    expected_role: str,
    owner_table: str,
    tables: tuple[str, ...],
) -> None:
    role = await connection.fetchrow(
        """SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolinherit
        FROM pg_roles WHERE rolname=$1""",
        expected_role,
    )
    assert role is not None
    assert not role["rolsuper"]
    assert not role["rolcreatedb"]
    assert not role["rolcreaterole"]
    assert not role["rolinherit"]
    for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE"):
        assert await connection.fetchval(
            "SELECT has_table_privilege($1,$2,$3)",
            expected_role,
            owner_table,
            privilege,
        )
    if expected_role == "auraclaw_streaming":
        for table in (
            "streaming.gateway_instance",
            "streaming.connection_registry",
        ):
            for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE"):
                assert await connection.fetchval(
                    "SELECT has_table_privilege($1,$2,$3)",
                    expected_role,
                    table,
                    privilege,
                )
    for foreign_table in tables:
        if foreign_table == owner_table:
            continue
        allowed = (
            expected_role == "auraclaw_streaming"
            and foreign_table == "projection.task_view"
        )
        assert bool(
            await connection.fetchval(
                "SELECT has_table_privilege($1,$2,'SELECT')",
                expected_role,
                foreign_table,
            )
        ) is allowed


async def _assert_owner_dml(
    connection: asyncpg.Connection, table: str, update_column: str
) -> None:
    await connection.execute(f"SELECT 1 FROM {table} WHERE FALSE")
    await connection.execute(f"INSERT INTO {table} SELECT * FROM {table} WHERE FALSE")
    await connection.execute(
        f"UPDATE {table} SET {update_column} = {update_column} WHERE FALSE"
    )
    await connection.execute(f"DELETE FROM {table} WHERE FALSE")


async def _assert_task_api_privileges(connection: asyncpg.Connection) -> None:
    role = TASK_API_ROLE[1]
    assert await connection.fetchval(
        "SELECT has_table_privilege($1,'projection.task_view','SELECT')", role
    )
    assert not await connection.fetchval(
        "SELECT has_table_privilege($1,'projection.task_view','UPDATE')", role
    )
    for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE"):
        assert await connection.fetchval(
            "SELECT has_table_privilege($1,'hands.invocation',$2)", role, privilege
        )
    assert await connection.fetchval(
        "SELECT has_table_privilege($1,'observability.audit_event','SELECT')", role
    )
    assert not await connection.fetchval(
        "SELECT has_table_privilege($1,'observability.audit_event','UPDATE')", role
    )
    for table in (
        "session_core.session_head",
        "control.runtime_lease",
        "delivery.delivery_job",
        "policy.decision",
        "credential.reference",
        "artifact.metadata",
        "streaming.runtime_event",
        "streaming.gateway_instance",
        "model_gateway.model_call",
    ):
        assert not await connection.fetchval(
            "SELECT has_table_privilege($1,$2,'SELECT')", role, table
        )


def test_production_roles_enforce_owner_and_task_api_boundaries() -> None:
    if not SETTINGS.postgres_enabled:
        pytest.skip("PostgreSQL role grant matrix requires postgres primary storage")
    required_names = (*ROLE_TARGETS, TASK_API_ROLE[0])
    urls = {name: _configured_url(name) for name in required_names}
    missing = [name for name, url in urls.items() if url is None]
    fallback_url = (
        asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
    )
    if missing and fallback_url is None:
        pytest.skip("production role DSNs and catalog connection are not configured")
    if missing and fallback_url is not None:
        required_roles = tuple(
            [target[0] for target in ROLE_TARGETS.values()] + [TASK_API_ROLE[1]]
        )
        if not asyncio.run(_catalog_has_roles(fallback_url, required_roles)):
            pytest.skip("optional PostgreSQL production roles are not installed")

    async def scenario() -> None:
        tables = tuple(target[1] for target in ROLE_TARGETS.values())
        catalog_connection = (
            await asyncpg.connect(fallback_url) if missing and fallback_url else None
        )
        for env_name, (expected_role, owner_table, update_column) in ROLE_TARGETS.items():
            url = urls[env_name]
            if url is None:
                assert catalog_connection is not None
                await _assert_catalog_role(
                    catalog_connection, expected_role, owner_table, tables
                )
                continue
            connection = await asyncpg.connect(url)
            try:
                await _assert_hardened_login(connection, expected_role)
                await _assert_owner_dml(connection, owner_table, update_column)
                if expected_role == "auraclaw_streaming":
                    await _assert_owner_dml(
                        connection,
                        "streaming.gateway_instance",
                        "owner_id",
                    )
                for foreign_table in tables:
                    if foreign_table == owner_table:
                        continue
                    if (
                        expected_role == "auraclaw_streaming"
                        and foreign_table == "projection.task_view"
                    ):
                        await connection.execute(
                            "SELECT 1 FROM projection.task_view WHERE FALSE"
                        )
                        continue
                    with pytest.raises(asyncpg.InsufficientPrivilegeError):
                        await connection.execute(
                            f"SELECT 1 FROM {foreign_table} WHERE FALSE"
                        )
            finally:
                await connection.close()

        task_api_url = urls[TASK_API_ROLE[0]]
        task_api_connection = (
            await asyncpg.connect(task_api_url) if task_api_url else catalog_connection
        )
        try:
            assert task_api_connection is not None
            if task_api_url:
                await _assert_hardened_login(task_api_connection, TASK_API_ROLE[1])
            await _assert_task_api_privileges(task_api_connection)
        finally:
            if task_api_url and task_api_connection is not None:
                await task_api_connection.close()
            if catalog_connection is not None:
                await catalog_connection.close()

    asyncio.run(scenario())
