import asyncio
import json
import shutil
import socket
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

import asyncpg
import pytest

from auraclaw.action.capability_catalog import CapabilityCatalog
from auraclaw.contracts.capabilities import CapabilityDescriptor, CapabilityKind
from auraclaw.infrastructure.persistence.migration_runner import (
    MigrationError,
    PostgresMigrationRunner,
    discover_migrations,
)
from auraclaw.infrastructure.persistence.postgres_capability_catalog import (
    PostgresCapabilityCatalogStore,
)
from auraclaw.infrastructure.persistence.postgres_mcp_registry import (
    PostgresMcpServerRegistryStore,
)

ROOT = Path(__file__).resolve().parents[2]


def _free_port() -> int:
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        return int(server.getsockname()[1])


async def _check_mcp_trust_migration(
    connection: asyncpg.Connection, database_url: str, migration_dir: Path
) -> None:
    legacy = {
        "server_id": "trust-migration",
        "title": "Managed MCP",
        "endpoint": "https://managed.example/mcp",
        "network_mode": "public",
        "auth_strategy": "workload_trusted_context",
        "credential_ref": "vault/test",
        "trust_level": "tenant_verified",
        "metadata": {
            "tool_policy_overrides": {
                "managed.query": {"permission": "write-with-approval", "risk_level": "high"}
            },
            "deployment": "internal",
        },
    }
    await connection.execute(
        """INSERT INTO hands.mcp_server
        (server_id,desired_state,latest_revision,active_revision,created_by)
        VALUES ('trust-migration','enabled',1,1,'admin')"""
    )
    await connection.execute(
        """INSERT INTO hands.mcp_server_revision
        (server_id,revision,config_json,config_digest,created_by)
        VALUES ('trust-migration',1,$1::jsonb,'original-digest','admin')""",
        json.dumps(legacy),
    )
    down = (migration_dir / "0057_remove_capability_trust.down.sql").read_text()
    up = (migration_dir / "0057_remove_capability_trust.sql").read_text()
    await connection.execute(down)
    await connection.execute(
        """INSERT INTO hands.downstream_mcp_server
        (server_id,title,endpoint,enabled,status,trust_level,metadata,config_revision)
        VALUES ('trust-migration','Managed MCP','https://managed.example/mcp',true,
                'active','tenant_verified',$1::jsonb,1)""",
        json.dumps(legacy["metadata"]),
    )
    await connection.execute(up)
    assert not await connection.fetchval(
        """SELECT EXISTS(SELECT 1 FROM information_schema.columns
        WHERE table_schema='hands' AND column_name='trust_level')"""
    )
    store = PostgresCapabilityCatalogStore(database_url)
    registry = PostgresMcpServerRegistryStore(database_url)
    try:
        revision = await registry.get_revision("trust-migration", 1)
        assert revision is not None
        assert revision.config_digest == "original-digest"
        assert revision.config.metadata == {"deployment": "internal"}
        server = await store.get_server("trust-migration")
        assert server is not None and "trust_level" not in server.model_dump()
        assert server.metadata["deployment"] == "internal"
        assert "tool_policy_overrides" not in server.metadata
        await store.upsert_server(server)
        catalog = CapabilityCatalog(store)
        await catalog.replace_server_capabilities(
            "trust-migration",
            (
                CapabilityDescriptor(
                    capability_id="trust-migration-query",
                    kind=CapabilityKind.TOOL,
                    server_id="trust-migration",
                    canonical_name="managed.query",
                    version="1.0.0",
                    content_digest="unchanged-schema",
                    title="Query",
                    permission="read-only",
                    risk_level="low",
                    updated_at=datetime.now(UTC),
                ),
            ),
        )
        items = await store.list_server_capabilities("platform", "trust-migration")
        assert len(items) == 1 and items[0].permission == "read-only"
        await connection.execute(down)
        assert (
            await connection.fetchval(
                "SELECT trust_level FROM hands.capability_catalog WHERE server_id='trust-migration'"
            )
            == "tenant_verified"
        )
        restored = await connection.fetchval(
            "SELECT metadata FROM hands.downstream_mcp_server WHERE server_id='trust-migration'"
        )
        assert (
            json.loads(restored)["tool_policy_overrides"]
            == legacy["metadata"]["tool_policy_overrides"]
        )
        await connection.execute(up)
        row = await connection.fetchrow(
            "SELECT config_json,config_digest FROM hands.mcp_server_revision "
            "WHERE server_id='trust-migration'"
        )
        assert json.loads(row["config_json"]) == legacy
        assert row["config_digest"] == "original-digest"
    finally:
        await store.close()
        await registry.close()


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

        with pytest.raises(MigrationError, match="ledger is missing"):
            await PostgresMigrationRunner(database_url, migration_dir).check()
        first, second = await asyncio.gather(
            PostgresMigrationRunner(database_url, migration_dir).apply(),
            PostgresMigrationRunner(database_url, migration_dir).apply(),
        )
        expected = len(discover_migrations(migration_dir))
        assert sorted((len(first), len(second))) == [0, expected]
        runner = PostgresMigrationRunner(database_url, migration_dir)
        assert await runner.apply() == ()
        assert {item.state for item in await runner.status()} == {"applied"}
        await runner.check()

        connection = await asyncpg.connect(database_url)
        try:
            await connection.execute((ROOT / "deploy/postgres/roles.sql").read_text())
            readonly_url = database_url.replace("postgres@", "auraclaw_task_query_ro@")
            await PostgresMigrationRunner(readonly_url, migration_dir).check()
            readonly = await asyncpg.connect(readonly_url)
            try:
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await readonly.execute("DELETE FROM auraclaw_meta.schema_migration")
            finally:
                await readonly.close()
            await _check_mcp_trust_migration(connection, database_url, migration_dir)
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
                    content_digest,title,status,catalog_generation)
                   VALUES ('cap-stale-generation','resource','stale-generation-test',
                           'stale.resource','1.0.0','sha256:stale','stale resource',
                           'active',1)"""
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
        with pytest.raises(MigrationError, match="checksum mismatch"):
            await drifted.check()

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
