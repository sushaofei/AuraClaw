"""Render and apply MySQL role-scoped grants for AuraClaw primary storage."""

from __future__ import annotations

import re
import warnings
from pathlib import Path

import aiomysql  # type: ignore[import-untyped]

# Mirrors deploy/mysql/roles.sql. Concrete table grants are used because some
# managed MySQL builds reject GRANT wildcards (errno 1146 on `schema_%`).
_ROLE_USERS = (
    "auraclaw_session",
    "auraclaw_projection",
    "auraclaw_control",
    "auraclaw_delivery",
    "auraclaw_hands",
    "auraclaw_policy",
    "auraclaw_credential",
    "auraclaw_artifact",
    "auraclaw_streaming",
    "auraclaw_model",
    "auraclaw_task_query_ro",
)

# role -> list of (table_prefix_or_exact, privileges)
_ROLE_TABLE_PRIVILEGES: dict[str, list[tuple[str, str]]] = {
    "auraclaw_session": [
        ("session_core_", "SELECT, INSERT, UPDATE, DELETE"),
        ("auraclaw_meta_", "SELECT, INSERT, UPDATE"),
    ],
    "auraclaw_projection": [
        ("projection_", "SELECT, INSERT, UPDATE, DELETE"),
        ("observability_", "SELECT, INSERT, UPDATE, DELETE"),
    ],
    "auraclaw_task_query_ro": [
        ("projection_", "SELECT"),
    ],
    "auraclaw_streaming": [
        ("streaming_", "SELECT, INSERT, UPDATE, DELETE"),
        ("projection_task_view", "SELECT"),
    ],
    "auraclaw_control": [
        ("control_", "SELECT, INSERT, UPDATE, DELETE"),
    ],
    "auraclaw_delivery": [
        ("delivery_", "SELECT, INSERT, UPDATE, DELETE"),
    ],
    "auraclaw_hands": [
        ("hands_", "SELECT, INSERT, UPDATE, DELETE"),
        ("security_", "SELECT, INSERT, UPDATE, DELETE"),
    ],
    "auraclaw_policy": [
        ("policy_", "SELECT, INSERT, UPDATE, DELETE"),
        ("security_", "SELECT, INSERT, UPDATE, DELETE"),
    ],
    "auraclaw_credential": [
        ("credential_", "SELECT, INSERT, UPDATE, DELETE"),
        ("security_", "SELECT, INSERT, UPDATE, DELETE"),
    ],
    "auraclaw_artifact": [
        ("artifact_", "SELECT, INSERT, UPDATE, DELETE"),
    ],
    "auraclaw_model": [
        ("model_gateway_", "SELECT, INSERT, UPDATE, DELETE"),
    ],
}

_DEFAULT_ROLES_SQL = (
    Path(__file__).resolve().parents[4] / "deploy" / "mysql" / "roles.sql"
)


def render_roles_sql(
    database: str,
    password: str,
    *,
    source: Path | None = None,
) -> str:
    """Render the documented wildcard SQL (for review / compatible servers)."""
    if not re.fullmatch(r"[A-Za-z0-9_]+", database):
        raise ValueError(f"unsafe database name: {database}")
    escaped = password.replace("\\", "\\\\").replace("'", "''")
    text = (source or _DEFAULT_ROLES_SQL).read_text(encoding="utf-8")
    text = text.replace("`auraclaw`", f"`{database}`")
    text = text.replace("'change-me'", f"'{escaped}'")
    return text


async def _matching_tables(
    cursor: aiomysql.Cursor, database: str, pattern: str
) -> list[str]:
    if pattern.endswith("_"):
        like = pattern.replace("_", r"\_") + "%"
        await cursor.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema=%s AND table_name LIKE %s ESCAPE '\\\\'
            ORDER BY table_name
            """,
            (database, like),
        )
    else:
        await cursor.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema=%s AND table_name=%s
            ORDER BY table_name
            """,
            (database, pattern),
        )
    rows = await cursor.fetchall()
    return [str(row[0]) for row in rows]


async def apply_mysql_roles(
    *,
    host: str,
    port: int,
    admin_user: str,
    admin_password: str,
    database: str,
    role_password: str,
    source: Path | None = None,
) -> list[str]:
    del source  # documented SQL remains the source of truth for intent
    if not re.fullmatch(r"[A-Za-z0-9_]+", database):
        raise ValueError(f"unsafe database name: {database}")
    escaped = role_password.replace("\\", "\\\\").replace("'", "''")
    connection = await aiomysql.connect(
        host=host,
        port=port,
        user=admin_user,
        password=admin_password,
        autocommit=True,
    )
    try:
        async with connection.cursor() as cursor:
            for role in _ROLE_USERS:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", Warning)
                    await cursor.execute(
                        f"CREATE USER IF NOT EXISTS '{role}'@'%' "
                        f"IDENTIFIED BY '{escaped}'"
                    )
                await cursor.execute(
                    f"ALTER USER '{role}'@'%' IDENTIFIED BY '{escaped}'"
                )
            for role, grants in _ROLE_TABLE_PRIVILEGES.items():
                for pattern, privileges in grants:
                    tables = await _matching_tables(cursor, database, pattern)
                    for table in tables:
                        await cursor.execute(
                            f"GRANT {privileges} ON `{database}`.`{table}` "
                            f"TO '{role}'@'%'"
                        )
            await cursor.execute("FLUSH PRIVILEGES")
    finally:
        connection.close()
    return list(_ROLE_USERS)
