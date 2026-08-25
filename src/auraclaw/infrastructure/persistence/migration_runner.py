from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import asyncpg  # type: ignore[import-untyped]

from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url

_MIGRATION_NAME = re.compile(r"^(?P<version>\d{4})_[a-z0-9_]+\.sql$")
_LOCK_NAME = "auraclaw-schema-migrations"


@dataclass(frozen=True)
class Migration:
    version: str
    name: str
    path: Path
    checksum: str


@dataclass(frozen=True)
class MigrationStatus:
    version: str
    name: str
    state: str
    checksum: str


class MigrationError(RuntimeError):
    pass


def discover_migrations(directory: Path) -> tuple[Migration, ...]:
    migrations: list[Migration] = []
    versions: set[str] = set()
    for path in sorted(directory.glob("[0-9][0-9][0-9][0-9]_*.sql")):
        if path.name.endswith(".down.sql"):
            continue
        matched = _MIGRATION_NAME.fullmatch(path.name)
        if matched is None:
            continue
        version = matched.group("version")
        if version in versions:
            raise MigrationError(f"duplicate migration version: {version}")
        versions.add(version)
        migrations.append(
            Migration(
                version=version,
                name=path.name,
                path=path,
                checksum=hashlib.sha256(path.read_bytes()).hexdigest(),
            )
        )
    if not migrations:
        raise MigrationError(f"no migrations found in {directory}")
    return tuple(migrations)


def _transaction_body(source: str) -> str:
    stripped = source.strip()
    if stripped.startswith("BEGIN;") and stripped.endswith("COMMIT;"):
        return stripped.removeprefix("BEGIN;").removesuffix("COMMIT;").strip()
    return stripped


class PostgresMigrationRunner:
    def __init__(self, database_url: str, directory: Path) -> None:
        self._database_url = asyncpg_url(database_url)
        self._migrations = discover_migrations(directory)

    async def status(self) -> tuple[MigrationStatus, ...]:
        connection = await asyncpg.connect(self._database_url)
        try:
            await self._lock(connection)
            try:
                await self._ensure_ledger(connection)
                installed = await self._installed(connection)
                return tuple(
                    MigrationStatus(
                        version=migration.version,
                        name=migration.name,
                        state=(
                            "pending"
                            if migration.version not in installed
                            else (
                                "applied"
                                if installed[migration.version] == migration.checksum
                                else "drifted"
                            )
                        ),
                        checksum=migration.checksum,
                    )
                    for migration in self._migrations
                )
            finally:
                await self._unlock(connection)
        finally:
            await connection.close()

    async def apply(self, target: str | None = None) -> tuple[str, ...]:
        selected = tuple(
            migration
            for migration in self._migrations
            if target is None or migration.version <= target
        )
        if target is not None and not any(item.version == target for item in self._migrations):
            raise MigrationError(f"unknown migration target: {target}")

        connection = await asyncpg.connect(self._database_url)
        applied: list[str] = []
        try:
            await self._lock(connection)
            try:
                await self._ensure_ledger(connection)
                installed = await self._installed(connection)
                for migration in selected:
                    existing_checksum = installed.get(migration.version)
                    if existing_checksum is not None:
                        if existing_checksum != migration.checksum:
                            raise MigrationError(
                                f"checksum mismatch for applied migration {migration.version}"
                            )
                        continue
                    async with connection.transaction():
                        await connection.execute(
                            _transaction_body(migration.path.read_text())
                        )
                        await connection.execute(
                            """INSERT INTO auraclaw_meta.schema_migration
                            (version,name,checksum) VALUES ($1,$2,$3)""",
                            migration.version,
                            migration.name,
                            migration.checksum,
                        )
                    applied.append(migration.name)
            finally:
                await self._unlock(connection)
        finally:
            await connection.close()
        return tuple(applied)

    async def baseline(self, target: str) -> tuple[str, ...]:
        selected = tuple(
            migration for migration in self._migrations if migration.version <= target
        )
        if not any(item.version == target for item in self._migrations):
            raise MigrationError(f"unknown migration target: {target}")
        connection = await asyncpg.connect(self._database_url)
        try:
            await self._lock(connection)
            try:
                await self._ensure_ledger(connection)
                if await self._installed(connection):
                    raise MigrationError("baseline requires an empty migration ledger")
                schema_count = await connection.fetchval(
                    """SELECT count(*) FROM pg_namespace WHERE nspname = ANY($1::text[])""",
                    [
                        "session_core",
                        "projection",
                        "control",
                        "delivery",
                        "observability",
                        "hands",
                        "policy",
                        "credential",
                        "artifact",
                        "streaming",
                        "model_gateway",
                    ],
                )
                if int(schema_count) == 0:
                    raise MigrationError("baseline requires an existing AuraClaw schema")
                async with connection.transaction():
                    await connection.executemany(
                        """INSERT INTO auraclaw_meta.schema_migration
                        (version,name,checksum) VALUES ($1,$2,$3)""",
                        [
                            (migration.version, migration.name, migration.checksum)
                            for migration in selected
                        ],
                    )
                return tuple(migration.name for migration in selected)
            finally:
                await self._unlock(connection)
        finally:
            await connection.close()

    @staticmethod
    async def _ensure_ledger(connection: asyncpg.Connection) -> None:
        await connection.execute(
            """CREATE SCHEMA IF NOT EXISTS auraclaw_meta;
            CREATE TABLE IF NOT EXISTS auraclaw_meta.schema_migration (
                version text PRIMARY KEY,
                name text NOT NULL UNIQUE,
                checksum text NOT NULL,
                applied_at timestamptz NOT NULL DEFAULT now()
            )"""
        )

    @staticmethod
    async def _installed(connection: asyncpg.Connection) -> dict[str, str]:
        rows = await connection.fetch(
            "SELECT version,checksum FROM auraclaw_meta.schema_migration"
        )
        return {str(row["version"]): str(row["checksum"]) for row in rows}

    @staticmethod
    async def _lock(connection: asyncpg.Connection) -> None:
        await connection.execute(
            "SELECT pg_advisory_lock(hashtextextended($1, 0))", _LOCK_NAME
        )

    @staticmethod
    async def _unlock(connection: asyncpg.Connection) -> None:
        await connection.execute(
            "SELECT pg_advisory_unlock(hashtextextended($1, 0))", _LOCK_NAME
        )


def _split_mysql_statements(source: str) -> tuple[str, ...]:
    body = _transaction_body(source)
    without_line_comments = "\n".join(
        line for line in body.splitlines() if not line.lstrip().startswith("--")
    )
    statements: list[str] = []
    for chunk in without_line_comments.split(";"):
        statement = chunk.strip()
        if not statement or statement.startswith("/*"):
            continue
        statements.append(statement)
    return tuple(statements)


class MysqlMigrationRunner:
    def __init__(self, database_url: str, directory: Path) -> None:
        from auraclaw.infrastructure.persistence.sql_dialect import parse_mysql_url

        self._params = parse_mysql_url(database_url)
        self._migrations = discover_migrations(directory)

    async def status(self) -> tuple[MigrationStatus, ...]:
        import aiomysql  # type: ignore[import-untyped]

        connection = await aiomysql.connect(**self._params)
        try:
            await self._lock(connection)
            try:
                await self._ensure_ledger(connection)
                installed = await self._installed(connection)
                return tuple(
                    MigrationStatus(
                        version=migration.version,
                        name=migration.name,
                        state=(
                            "pending"
                            if migration.version not in installed
                            else (
                                "applied"
                                if installed[migration.version] == migration.checksum
                                else "drifted"
                            )
                        ),
                        checksum=migration.checksum,
                    )
                    for migration in self._migrations
                )
            finally:
                await self._unlock(connection)
        finally:
            connection.close()

    async def apply(self, target: str | None = None) -> tuple[str, ...]:
        import aiomysql

        selected = tuple(
            migration
            for migration in self._migrations
            if target is None or migration.version <= target
        )
        if target is not None and not any(item.version == target for item in self._migrations):
            raise MigrationError(f"unknown migration target: {target}")

        connection = await aiomysql.connect(**self._params)
        applied: list[str] = []
        try:
            await self._lock(connection)
            try:
                await self._ensure_ledger(connection)
                installed = await self._installed(connection)
                for migration in selected:
                    existing_checksum = installed.get(migration.version)
                    if existing_checksum is not None:
                        if existing_checksum != migration.checksum:
                            raise MigrationError(
                                f"checksum mismatch for applied migration {migration.version}"
                            )
                        continue
                    async with connection.cursor() as cursor:
                        await connection.begin()
                        try:
                            for statement in _split_mysql_statements(migration.path.read_text()):
                                await cursor.execute(statement)
                            await cursor.execute(
                                """INSERT INTO `auraclaw_meta_schema_migration`
                                (version, name, checksum) VALUES (%s, %s, %s)""",
                                (migration.version, migration.name, migration.checksum),
                            )
                            await connection.commit()
                        except Exception:
                            await connection.rollback()
                            raise
                    applied.append(migration.name)
            finally:
                await self._unlock(connection)
        finally:
            connection.close()
        return tuple(applied)

    async def baseline(self, target: str) -> tuple[str, ...]:
        import aiomysql

        selected = tuple(
            migration for migration in self._migrations if migration.version <= target
        )
        if not any(item.version == target for item in self._migrations):
            raise MigrationError(f"unknown migration target: {target}")
        connection = await aiomysql.connect(**self._params)
        try:
            await self._lock(connection)
            try:
                await self._ensure_ledger(connection)
                if await self._installed(connection):
                    raise MigrationError("baseline requires an empty migration ledger")
                async with connection.cursor() as cursor:
                    await cursor.execute("SHOW TABLES LIKE 'session_core_canonical_event'")
                    row = await cursor.fetchone()
                    if row is None:
                        raise MigrationError("baseline requires an existing AuraClaw schema")
                    await connection.begin()
                    try:
                        for migration in selected:
                            await cursor.execute(
                                """INSERT INTO `auraclaw_meta_schema_migration`
                                (version, name, checksum) VALUES (%s, %s, %s)""",
                                (migration.version, migration.name, migration.checksum),
                            )
                        await connection.commit()
                    except Exception:
                        await connection.rollback()
                        raise
                return tuple(migration.name for migration in selected)
            finally:
                await self._unlock(connection)
        finally:
            connection.close()

    @staticmethod
    async def _ensure_ledger(connection: Any) -> None:
        async with connection.cursor() as cursor:
            await cursor.execute(
                """CREATE TABLE IF NOT EXISTS `auraclaw_meta_schema_migration` (
                    version VARCHAR(191) PRIMARY KEY,
                    name VARCHAR(191) NOT NULL UNIQUE,
                    checksum VARCHAR(191) NOT NULL,
                    applied_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                )"""
            )

    @staticmethod
    async def _installed(connection: Any) -> dict[str, str]:
        async with connection.cursor() as cursor:
            await cursor.execute(
                "SELECT version, checksum FROM `auraclaw_meta_schema_migration`"
            )
            rows = await cursor.fetchall()
        return {str(row[0]): str(row[1]) for row in rows}

    @staticmethod
    async def _lock(connection: Any) -> None:
        async with connection.cursor() as cursor:
            await cursor.execute("SELECT GET_LOCK(%s, 60)", (_LOCK_NAME,))
            row = await cursor.fetchone()
            if not row or not row[0]:
                raise MigrationError("failed to acquire MySQL migration lock")

    @staticmethod
    async def _unlock(connection: Any) -> None:
        async with connection.cursor() as cursor:
            await cursor.execute("SELECT RELEASE_LOCK(%s)", (_LOCK_NAME,))


def create_migration_runner(
    database_url: str, directory: Path
) -> PostgresMigrationRunner | MysqlMigrationRunner:
    from auraclaw.infrastructure.persistence.sql_dialect import detect_dialect

    if detect_dialect(database_url) == "mysql":
        return MysqlMigrationRunner(database_url, directory)
    return PostgresMigrationRunner(database_url, directory)


def default_migrations_directory(dialect: str) -> Path:
    if dialect == "mysql":
        return Path("migrations/mysql")
    return Path("migrations")
