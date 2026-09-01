import asyncio
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import asyncpg
import pytest

from auraclaw.infrastructure.persistence.migration_runner import (
    MigrationError,
    PostgresMigrationRunner,
    discover_migrations,
)

ROOT = Path(__file__).resolve().parents[2]


def _free_port() -> int:
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        return int(server.getsockname()[1])


def test_migration_runner_is_locked_idempotent_and_detects_drift(tmp_path: Path) -> None:
    initdb = shutil.which("initdb")
    postgres = shutil.which("postgres")
    if initdb is None or postgres is None:
        pytest.skip("local PostgreSQL binaries are unavailable")
    migration_dir = tmp_path / "migrations"
    shutil.copytree(ROOT / "migrations", migration_dir)

    async def scenario(database_url: str) -> None:
        deadline = time.monotonic() + 15
        while True:
            try:
                connection = await asyncpg.connect(database_url)
            except (OSError, asyncpg.PostgresError):
                if time.monotonic() >= deadline:
                    raise
                await asyncio.sleep(0.1)
            else:
                await connection.close()
                break

        first, second = await asyncio.gather(
            PostgresMigrationRunner(database_url, migration_dir).apply(),
            PostgresMigrationRunner(database_url, migration_dir).apply(),
        )
        expected = len(discover_migrations(migration_dir))
        assert sorted((len(first), len(second))) == [0, expected]
        runner = PostgresMigrationRunner(database_url, migration_dir)
        assert await runner.apply() == ()
        assert {item.state for item in await runner.status()} == {"applied"}

        connection = await asyncpg.connect(database_url)
        try:
            await connection.execute(
                """INSERT INTO hands.downstream_mcp_server
                   (server_id,title,endpoint,enabled,status,active_catalog_generation)
                   VALUES ('auraclaw-price-insight','retired provider',
                           'https://retired.invalid/mcp',true,'active',1),
                          ('stale-generation-test','stale generation',
                           'https://stale.invalid/mcp',true,'active',2)"""
            )
            await connection.execute(
                """INSERT INTO hands.capability_catalog
                   (capability_id,kind,server_id,canonical_name,version,
                    content_digest,title,trust_level,status,catalog_generation)
                   VALUES ('cap-stale-generation','resource','stale-generation-test',
                           'stale.resource','1.0.0','sha256:stale','stale resource',
                           'external_untrusted','active',1)"""
            )
            await connection.execute(
                (migration_dir / "0053_resource_catalog_backing_consistency.sql").read_text()
            )
            assert not await connection.fetchval(
                """SELECT EXISTS(SELECT 1 FROM hands.downstream_mcp_server
                   WHERE server_id='auraclaw-price-insight')"""
            )
            assert not await connection.fetchval(
                """SELECT EXISTS(SELECT 1 FROM hands.capability_catalog
                   WHERE capability_id='cap-stale-generation')"""
            )
            await connection.execute(
                """DELETE FROM hands.downstream_mcp_server
                   WHERE server_id='stale-generation-test'"""
            )
            await connection.execute(
                (
                    migration_dir
                    / "0041_capability_catalog_consistency.down.sql"
                ).read_text()
            )
            assert not await connection.fetchval(
                """SELECT EXISTS(SELECT 1 FROM information_schema.columns
                WHERE table_schema='hands' AND table_name='mcp_server_runtime'
                  AND column_name='instance_id')"""
            )
            await connection.execute(
                (
                    migration_dir / "0041_capability_catalog_consistency.sql"
                ).read_text()
            )
            assert await connection.fetchval(
                """SELECT EXISTS(SELECT 1 FROM information_schema.columns
                WHERE table_schema='hands' AND table_name='capability_catalog'
                  AND column_name='catalog_generation')"""
            )
            await connection.execute(
                (migration_dir / "0040_runtime_execution_claims.down.sql").read_text()
            )
            assert not await connection.fetchval(
                """SELECT EXISTS(SELECT 1 FROM information_schema.columns
                WHERE table_schema='control' AND table_name='assignment'
                  AND column_name='execution_claim_token')"""
            )
            await connection.execute(
                (migration_dir / "0040_runtime_execution_claims.sql").read_text()
            )
            assert await connection.fetchval(
                """SELECT EXISTS(SELECT 1 FROM information_schema.columns
                WHERE table_schema='control' AND table_name='runtime_instance'
                  AND column_name='registration_id')"""
            )
        finally:
            await connection.close()

        migration = migration_dir / "0014_s4_policy_version.sql"
        migration.write_text(migration.read_text() + "\n-- simulated drift\n")
        drifted = PostgresMigrationRunner(database_url, migration_dir)
        status = await drifted.status()
        assert next(item for item in status if item.version == "0014").state == "drifted"
        with pytest.raises(MigrationError, match="checksum mismatch"):
            await drifted.apply()

    with tempfile.TemporaryDirectory(prefix="auraclaw-s5-pg-") as cluster_dir:
        subprocess.run(
            [initdb, "-D", cluster_dir, "-A", "trust", "-U", "postgres", "--no-locale"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        port = _free_port()
        process = subprocess.Popen(
            [
                postgres,
                "-D",
                cluster_dir,
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
