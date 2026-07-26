from __future__ import annotations

import re
from typing import Any, Literal
from urllib.parse import unquote, urlparse

Dialect = Literal["postgres", "mysql"]

_SCHEMAS = (
    "session_core",
    "projection",
    "control",
    "delivery",
    "artifact",
    "security",
    "observability",
    "streaming",
    "model_gateway",
    "hands",
    "policy",
    "credential",
    "auraclaw_meta",
)

_SCHEMA_TABLE = re.compile(
    r"\b(" + "|".join(_SCHEMAS) + r")\.([a-zA-Z_][a-zA-Z0-9_]*)\b"
)
_PG_PLACEHOLDER = re.compile(r"\$(\d+)")
_PG_CAST = re.compile(
    r"::(?:jsonb|json|text\[\]|timestamptz|interval|integer|bigint|boolean|int|text)\b"
)
_INTERVAL_LITERAL = re.compile(
    r"interval\s+'([^']+)'",
    re.IGNORECASE,
)
_INTERVAL_PARAM = re.compile(
    r"now\(\)\s*\+\s*%s::interval",
    re.IGNORECASE,
)


def detect_dialect(database_url: str) -> Dialect:
    lowered = database_url.lower()
    if lowered.startswith("mysql:") or lowered.startswith("mysql+"):
        return "mysql"
    if lowered.startswith("postgresql:") or lowered.startswith("postgres:"):
        return "postgres"
    if "+asyncpg" in lowered or "+psycopg" in lowered:
        return "postgres"
    if "+aiomysql" in lowered or "+asyncmy" in lowered or "+pymysql" in lowered:
        return "mysql"
    return "postgres"


def normalize_database_url(database_url: str, dialect: Dialect | None = None) -> str:
    selected = dialect or detect_dialect(database_url)
    if selected == "mysql":
        return (
            database_url.replace("mysql+aiomysql://", "mysql://", 1)
            .replace("mysql+asyncmy://", "mysql://", 1)
            .replace("mysql+pymysql://", "mysql://", 1)
        )
    return database_url.replace("postgresql+asyncpg://", "postgresql://", 1)


def parse_mysql_url(database_url: str) -> dict[str, Any]:
    normalized = normalize_database_url(database_url, "mysql")
    parsed = urlparse(normalized)
    if parsed.scheme not in {"mysql", "mysql+aiomysql", "mysql+asyncmy", "mysql+pymysql"}:
        raise ValueError(f"unsupported MySQL URL scheme: {parsed.scheme}")
    database = (parsed.path or "").lstrip("/")
    if not database:
        raise ValueError("MySQL URL requires a database name")
    return {
        "host": parsed.hostname or "127.0.0.1",
        "port": parsed.port or 3306,
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
        "db": database,
        "charset": "utf8mb4",
        "autocommit": True,
    }


def table(schema: str, name: str, dialect: Dialect) -> str:
    if dialect == "mysql":
        return f"`{schema}_{name}`"
    return f"{schema}.{name}"


def adapt_sql(query: str, dialect: Dialect) -> str:
    """Best-effort PG→MySQL SQL adaptation for store queries.

    Complex constructs (ON CONFLICT, ANY($n::text[]), advisory locks) still need
    dialect-specific SQL in the calling store.
    """
    if dialect == "postgres":
        return query
    adapted = _SCHEMA_TABLE.sub(r"`\1_\2`", query)
    adapted = _PG_CAST.sub("", adapted)
    adapted = _INTERVAL_LITERAL.sub(r"INTERVAL \1", adapted)
    adapted = adapted.replace("now()", "UTC_TIMESTAMP(6)")
    adapted = _INTERVAL_PARAM.sub(
        "DATE_ADD(UTC_TIMESTAMP(6), INTERVAL %s MICROSECOND)",
        adapted,
    )
    # Convert $1..$N to %s in ascending appearance order (asyncpg style).
    placeholders = _PG_PLACEHOLDER.findall(adapted)
    if placeholders:
        # Replace from highest index first so $10 is not corrupted by $1.
        for index in sorted({int(item) for item in placeholders}, reverse=True):
            adapted = adapted.replace(f"${index}", "%s")
    adapted = adapted.replace("ON CONFLICT DO NOTHING", "")
    return adapted


def mysql_insert_ignore(query: str) -> str:
    """Convert a simple INSERT ... ON CONFLICT DO NOTHING into INSERT IGNORE."""
    if "ON CONFLICT" not in query.upper():
        return query
    without_conflict = re.sub(
        r"\s+ON CONFLICT\b.*$",
        "",
        query,
        flags=re.IGNORECASE | re.DOTALL,
    )
    return re.sub(r"\bINSERT\b", "INSERT IGNORE", without_conflict, count=1, flags=re.IGNORECASE)
