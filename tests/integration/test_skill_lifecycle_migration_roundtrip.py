from __future__ import annotations

import asyncio
import shutil
import socket
import subprocess
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import asyncpg
import pytest

from auraclaw.action.skill_lifecycle import SkillPublishCommit
from auraclaw.contracts.skills import (
    SkillInstallationRecord,
    SkillInstallationStatus,
    SkillManifest,
    SkillPackageRecord,
    SkillPublicationRecord,
    SkillPublicationStatus,
)
from auraclaw.contracts.tools import ArtifactRef
from auraclaw.infrastructure.persistence.postgres_skill_lifecycle import (
    PostgresSkillLifecycleStore,
)

ROOT = Path(__file__).resolve().parents[2]
BASE_UP = "\n".join(
    (ROOT / "migrations" / name).read_text()
    for name in (
        "0023_skill_lifecycle.sql",
        "0024_skill_publication_actor.sql",
        "0025_skill_package_retention.sql",
        "0026_skill_publication_reliability.sql",
        "0027_skill_publisher_registry.sql",
        "0028_skill_source_reconcile_lease.sql",
        "0029_skill_source_inventory_retirement.sql",
        "0030_skill_publisher_suspension.sql",
        "0031_skill_publication_restore.sql",
        "0032_skill_admission_audit.sql",
        "0033_skill_content_quarantine.sql",
        "0034_skill_admission_operations.sql",
        "0035_skill_admission_retention.sql",
        "0036_skill_binding_revocation_policy.sql",
        "0037_skill_publication_sources.sql",
        "0038_skill_installation_draining.sql",
        "0039_skill_publisher_runtime_revocation.sql",
        "0050_batch_worker_lease_safety.sql",
        "0054_skill_lifecycle_broadcast_outbox.sql",
        "0055_skill_package_republish.sql",
    )
)
REMOVE_SOURCES = (ROOT / "migrations/0056_remove_skill_sources.sql").read_text()
RESTORE_SOURCES = (ROOT / "migrations/0056_remove_skill_sources.down.sql").read_text()


def _free_port() -> int:
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        return int(server.getsockname()[1])


def test_remove_skill_sources_migration_roundtrip_in_isolated_postgres() -> None:
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
            await connection.execute(BASE_UP)
            assert await connection.fetchval(
                "SELECT to_regclass('hands.skill_source') IS NOT NULL"
            )

            await connection.execute(REMOVE_SOURCES)
            await _assert_sources_removed(connection)
            await _publish_same_skill_for_two_tenants(database_url)

            await connection.execute(RESTORE_SOURCES)
            assert await connection.fetchval(
                "SELECT to_regclass('hands.skill_source') IS NOT NULL"
            )
            assert await _column_exists(connection, "skill_publication", "source_id")
            assert await _column_exists(connection, "skill_installation", "source_id")

            await connection.execute(REMOVE_SOURCES)
            await _assert_sources_removed(connection)
        finally:
            await connection.close()

    with tempfile.TemporaryDirectory(prefix="auraclaw-skill-source-removal-pg-") as cluster:
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


async def _assert_sources_removed(connection: asyncpg.Connection) -> None:
    for relation in (
        "skill_source",
        "skill_source_sync_state",
        "skill_source_lease",
        "skill_source_inventory",
        "skill_source_retirement_command",
        "skill_publication_source",
        "skill_source_command",
    ):
        assert not await connection.fetchval(
            "SELECT to_regclass($1) IS NOT NULL", f"hands.{relation}"
        )
    for table in (
        "skill_publication",
        "skill_installation",
        "skill_command",
        "skill_admission_audit",
    ):
        assert not await _column_exists(connection, table, "source_id")


async def _publish_same_skill_for_two_tenants(database_url: str) -> None:
    store = PostgresSkillLifecycleStore(database_url)
    now = datetime.now(UTC)
    manifest = SkillManifest(
        name="release.prepare",
        version="1.0.0",
        description="Cross-tenant migration smoke test",
        publisher="platform",
        signature="hmac-sha256:test",
    )
    digest = f"sha256:{'a' * 64}"
    try:
        for tenant_id in ("tenant-a", "tenant-b"):
            package = SkillPackageRecord(
                tenant_id=tenant_id,
                manifest=manifest,
                package_digest=digest,
                artifact_ref=ArtifactRef(
                    artifact_id=f"art_{tenant_id}",
                    version=1,
                    content_hash="a" * 64,
                    media_type="application/vnd.auraclaw.skill-package+json",
                    size=1,
                ),
                retention_until=now + timedelta(days=90),
                retention_updated_by="migration-test",
                retention_updated_at=now,
                created_at=now,
            )
            publication = SkillPublicationRecord(
                publication_id=f"skp_{tenant_id}",
                tenant_id=tenant_id,
                publisher="platform",
                name="release.prepare",
                version="1.0.0",
                package_digest=digest,
                status=SkillPublicationStatus.ACTIVE,
                created_by="migration-test",
                updated_by="migration-test",
                created_at=now,
                updated_at=now,
            )
            installation = SkillInstallationRecord(
                installation_id=f"ski_{tenant_id}",
                tenant_id=tenant_id,
                publisher="platform",
                name="release.prepare",
                status=SkillInstallationStatus.ACTIVE,
                created_by="migration-test",
                updated_by="migration-test",
                created_at=now,
                updated_at=now,
            )
            result = await store.commit_publish(
                SkillPublishCommit(
                    command_id=f"publish-{tenant_id}",
                    request_digest=f"sha256:{'b' * 64}",
                    actor_id="migration-test",
                    correlation_id=f"correlation-{tenant_id}",
                    causation_id=f"publish-{tenant_id}",
                    expected_publication_revision=0,
                    package=package,
                    publication=publication,
                    installation=installation,
                    occurred_at=now,
                )
            )
            assert result.installation is not None
    finally:
        await store.close()


async def _column_exists(
    connection: asyncpg.Connection, table: str, column: str
) -> bool:
    return bool(
        await connection.fetchval(
            """SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema='hands' AND table_name=$1 AND column_name=$2
            )""",
            table,
            column,
        )
    )
