from __future__ import annotations

import asyncio
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import asyncpg
import pytest

ROOT = Path(__file__).resolve().parents[2]
UP = "\n".join(
    (
        (ROOT / "migrations/0023_skill_lifecycle.sql").read_text(),
        (ROOT / "migrations/0024_skill_publication_actor.sql").read_text(),
        (ROOT / "migrations/0025_skill_package_retention.sql").read_text(),
        (ROOT / "migrations/0026_skill_publication_reliability.sql").read_text(),
        (ROOT / "migrations/0027_skill_publisher_registry.sql").read_text(),
        (ROOT / "migrations/0028_skill_source_reconcile_lease.sql").read_text(),
    )
)
DOWN = "\n".join(
    (
        (ROOT / "migrations/0028_skill_source_reconcile_lease.down.sql").read_text(),
        (ROOT / "migrations/0027_skill_publisher_registry.down.sql").read_text(),
        (ROOT / "migrations/0026_skill_publication_reliability.down.sql").read_text(),
        (ROOT / "migrations/0025_skill_package_retention.down.sql").read_text(),
        (ROOT / "migrations/0024_skill_publication_actor.down.sql").read_text(),
        (ROOT / "migrations/0023_skill_lifecycle.down.sql").read_text(),
    )
)


def _free_port() -> int:
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        return int(server.getsockname()[1])


def test_skill_lifecycle_migration_roundtrip_in_isolated_postgres() -> None:
    initdb = shutil.which("initdb")
    postgres = shutil.which("postgres")
    if initdb is None or postgres is None:
        pytest.skip("local PostgreSQL binaries are unavailable")

    async def scenario(database_url: str) -> None:
        connection: asyncpg.Connection | None = None
        deadline = time.monotonic() + 15
        while connection is None:
            try:
                connection = await asyncpg.connect(database_url)
            except (OSError, asyncpg.PostgresError):
                if time.monotonic() >= deadline:
                    raise
                await asyncio.sleep(0.1)
        try:
            await connection.execute("CREATE SCHEMA hands")
            await connection.execute(UP)
            for relation in (
                "skill_package",
                "skill_publication",
                "skill_installation",
                "skill_source",
                "skill_source_sync_state",
                "skill_source_lease",
                "skill_command",
                "skill_outbox",
                "skill_publisher",
                "skill_publisher_key",
                "skill_publisher_command",
            ):
                assert await connection.fetchval(
                    "SELECT to_regclass($1) IS NOT NULL", f"hands.{relation}"
                )
            assert await connection.fetchval(
                """SELECT is_nullable = 'NO' FROM information_schema.columns
                WHERE table_schema='hands' AND table_name='skill_publication'
                  AND column_name='updated_by'"""
            )
            for column in (
                "retention_until",
                "legal_hold",
                "retention_revision",
                "retention_updated_by",
                "retention_updated_at",
            ):
                assert await connection.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM information_schema.columns
                    WHERE table_schema='hands' AND table_name='skill_package'
                      AND column_name=$1)""",
                    column,
                )
            await connection.execute(DOWN)
            for relation in (
                "skill_package",
                "skill_publisher",
                "skill_publisher_key",
                "skill_publisher_command",
                "skill_publication",
                "skill_installation",
                "skill_source",
                "skill_source_sync_state",
                "skill_source_lease",
                "skill_command",
                "skill_outbox",
            ):
                assert not await connection.fetchval(
                    "SELECT to_regclass($1) IS NOT NULL", f"hands.{relation}"
                )
        finally:
            await connection.close()

    with tempfile.TemporaryDirectory(prefix="auraclaw-skill-lifecycle-pg-") as cluster:
        subprocess.run(
            [initdb, "-D", cluster, "-A", "trust", "-U", "postgres", "--no-locale"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        port = _free_port()
        process = subprocess.Popen(
            [
                postgres,
                "-D",
                cluster,
                "-h",
                "127.0.0.1",
                "-p",
                str(port),
                "-c",
                "fsync=off",
                "-c",
                "synchronous_commit=off",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            asyncio.run(scenario(f"postgresql://postgres@127.0.0.1:{port}/postgres"))
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
