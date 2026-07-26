"""MySQL production-role grant matrix against a real primary database."""

from __future__ import annotations

import asyncio
import os
from urllib.parse import quote, unquote, urlparse

import aiomysql  # type: ignore[import-untyped]
import pytest
from pymysql.err import OperationalError

from auraclaw.config import get_settings
from auraclaw.infrastructure.persistence.mysql_roles import apply_mysql_roles

ROLE_TARGETS = {
    "auraclaw_session": ("session_core_session_head", "tenant_id"),
    "auraclaw_projection": ("projection_task_view", "tenant_id"),
    "auraclaw_control": ("control_runtime_lease", "resource_id"),
    "auraclaw_delivery": ("delivery_delivery_job", "tenant_id"),
    "auraclaw_hands": ("hands_invocation", "tenant_id"),
    "auraclaw_policy": ("policy_decision", "tenant_id"),
    "auraclaw_credential": ("credential_reference", "tenant_id"),
    "auraclaw_artifact": ("artifact_metadata", "tenant_id"),
    "auraclaw_streaming": ("streaming_runtime_event", "tenant_id"),
    "auraclaw_model": ("model_gateway_model_call", "tenant_id"),
}
QUERY_ROLE = "auraclaw_task_query_ro"
ROLE_PASSWORD = os.environ.get("AURACLAW_MYSQL_ROLE_SMOKE_PWD", "auraclaw-role-smoke")


def _admin() -> tuple[str, str, str, int, str] | None:
    """Admin connection for GRANT smoke — prefer primary-storage Settings/DB_*."""
    settings = get_settings()
    if settings.mysql_enabled:
        parsed = urlparse(settings.resolved_database_url)
        if parsed.hostname and parsed.username is not None and parsed.password is not None:
            database = (parsed.path or "/").lstrip("/") or "auraclaw_dev"
            return (
                parsed.hostname,
                unquote(parsed.username),
                unquote(parsed.password),
                parsed.port or 3306,
                database,
            )

    host = (
        os.environ.get("AURACLAW_MYSQL_SMOKE_HOST")
        or os.environ.get("DB_HOST")
        or os.environ.get("MYSQL_DB_HOST")
    )
    user = os.environ.get("DB_USER") or os.environ.get("MYSQL_DB_USER")
    password = os.environ.get("DB_PWD")
    if password is None:
        password = os.environ.get("MYSQL_DB_PWD")
    port = int(os.environ.get("DB_PORT") or os.environ.get("MYSQL_DB_PORT") or "3306")
    database = (
        os.environ.get("AURACLAW_MYSQL_SMOKE_DB")
        or os.environ.get("DB_NAME")
        or "auraclaw_dev"
    )
    if not host or not user or password is None:
        return None
    return host, user, password, port, database

ADMIN = _admin()
pytestmark = pytest.mark.skipif(ADMIN is None, reason="MySQL admin smoke not configured")


def _role_url(host: str, port: int, role: str, database: str) -> str:
    return (
        f"mysql+aiomysql://{quote(role, safe='')}:{quote(ROLE_PASSWORD, safe='')}"
        f"@{host}:{port}/{database}"
    )


async def _connect(host: str, port: int, user: str, password: str, database: str):
    return await aiomysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        db=database,
        autocommit=True,
    )


async def _assert_owner_dml(connection, table: str, update_column: str) -> None:
    async with connection.cursor() as cursor:
        await cursor.execute(f"SELECT 1 FROM `{table}` WHERE FALSE")
        await cursor.execute(f"INSERT INTO `{table}` SELECT * FROM `{table}` WHERE FALSE")
        await cursor.execute(
            f"UPDATE `{table}` SET `{update_column}` = `{update_column}` WHERE FALSE"
        )
        await cursor.execute(f"DELETE FROM `{table}` WHERE FALSE")


async def _assert_denied(connection, statement: str) -> None:
    async with connection.cursor() as cursor:
        with pytest.raises(OperationalError) as raised:
            await cursor.execute(statement)
    assert raised.value.args[0] in {1142, 1143}


@pytest.mark.asyncio
async def test_mysql_roles_enforce_owner_and_query_boundaries() -> None:
    assert ADMIN is not None
    host, admin_user, admin_password, port, database = ADMIN
    await apply_mysql_roles(
        host=host,
        port=port,
        admin_user=admin_user,
        admin_password=admin_password,
        database=database,
        role_password=ROLE_PASSWORD,
    )

    tables = tuple(table for table, _ in ROLE_TARGETS.values())
    for role, (owner_table, update_column) in ROLE_TARGETS.items():
        connection = await _connect(host, port, role, ROLE_PASSWORD, database)
        try:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT CURRENT_USER()")
                current = (await cursor.fetchone())[0]
            assert current.startswith(f"{role}@")
            await _assert_owner_dml(connection, owner_table, update_column)
            for foreign_table in tables:
                if foreign_table == owner_table:
                    continue
                if role == "auraclaw_streaming" and foreign_table == "projection_task_view":
                    async with connection.cursor() as cursor:
                        await cursor.execute(
                            f"SELECT 1 FROM `{foreign_table}` WHERE FALSE"
                        )
                    continue
                await _assert_denied(
                    connection, f"SELECT 1 FROM `{foreign_table}` WHERE FALSE"
                )
        finally:
            connection.close()

    query = await _connect(host, port, QUERY_ROLE, ROLE_PASSWORD, database)
    try:
        async with query.cursor() as cursor:
            await cursor.execute("SELECT 1 FROM `projection_task_view` WHERE FALSE")
        await _assert_denied(
            query, "UPDATE `projection_task_view` SET `tenant_id` = `tenant_id` WHERE FALSE"
        )
        await _assert_denied(
            query, "DELETE FROM `projection_task_view` WHERE FALSE"
        )
        for table in tables:
            if table == "projection_task_view":
                continue
            await _assert_denied(query, f"SELECT 1 FROM `{table}` WHERE FALSE")
    finally:
        query.close()

    # Sanity: rendered DSN shape used by Compose secrets.
    assert _role_url(host, port, "auraclaw_session", database).startswith(
        "mysql+aiomysql://auraclaw_session:"
    )


if __name__ == "__main__":
    asyncio.run(test_mysql_roles_enforce_owner_and_query_boundaries())
