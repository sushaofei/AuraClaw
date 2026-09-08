from __future__ import annotations

from typing import Literal

Dialect = Literal["postgres"]


def detect_dialect(database_url: str) -> Dialect:
    """Validate and identify a PostgreSQL-compatible database URL."""
    lowered = database_url.lower()
    supported = (
        lowered.startswith("postgresql:"),
        lowered.startswith("postgres:"),
        lowered.startswith("kingbase:"),
        lowered.startswith("kingbase+"),
        "+asyncpg" in lowered,
        "+psycopg" in lowered,
    )
    if not any(supported):
        raise ValueError("AuraClaw requires a PostgreSQL or Kingbase database URL")
    return "postgres"


def normalize_database_url(database_url: str, dialect: Dialect | None = None) -> str:
    if dialect is not None and dialect != "postgres":
        raise ValueError("AuraClaw only supports the PostgreSQL dialect")
    detect_dialect(database_url)
    return (
        database_url.replace("postgresql+asyncpg://", "postgresql://", 1)
        .replace("kingbase+asyncpg://", "postgresql://", 1)
        .replace("kingbase://", "postgresql://", 1)
    )


def table(schema: str, name: str, dialect: Dialect = "postgres") -> str:
    if dialect != "postgres":
        raise ValueError("AuraClaw only supports the PostgreSQL dialect")
    return f"{schema}.{name}"


def adapt_sql(query: str, dialect: Dialect = "postgres") -> str:
    if dialect != "postgres":
        raise ValueError("AuraClaw only supports the PostgreSQL dialect")
    return query
