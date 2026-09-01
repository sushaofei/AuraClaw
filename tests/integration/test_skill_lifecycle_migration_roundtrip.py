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

from auraclaw.action.skill_lifecycle import (
    SkillInstallationCommit,
    SkillPublishCommit,
)
from auraclaw.action.skill_sources import SkillSourceService
from auraclaw.contracts.errors import PolicyDeniedError
from auraclaw.contracts.skills import (
    ChangeSkillPublisherStatusCommand,
    ConfigureSkillSourceCommand,
    RegisterSkillPublisherCommand,
    RetireSkillSourceCommand,
    RevokeSkillPublisherKeyCommand,
    RotateSkillPublisherKeyCommand,
    SkillInstallationRecord,
    SkillInstallationStatus,
    SkillManifest,
    SkillPackageRecord,
    SkillPublicationRecord,
    SkillPublicationStatus,
    SkillPublisherKeyStatus,
    SkillPublisherStatus,
    SkillPublisherStatusOperation,
    SkillRevocationAction,
    SkillSourceDesiredState,
    SkillSourceKind,
)
from auraclaw.contracts.tools import ArtifactRef
from auraclaw.infrastructure.persistence.postgres_skill_lifecycle import (
    PostgresSkillLifecycleStore,
)
from auraclaw.infrastructure.persistence.postgres_skill_publishers import (
    PostgresSkillPublisherStore,
)

ROOT = Path(__file__).resolve().parents[2]
UP = "\n".join(
    (
        (ROOT / "migrations/0023_skill_lifecycle.sql").read_text(),
        (ROOT / "migrations/0024_skill_publication_actor.sql").read_text(),
        (ROOT / "migrations/0025_skill_package_retention.sql").read_text(),
        (ROOT / "migrations/0026_skill_publication_reliability.sql").read_text(),
        (ROOT / "migrations/0027_skill_publisher_registry.sql").read_text(),
        (ROOT / "migrations/0028_skill_source_reconcile_lease.sql").read_text(),
        (ROOT / "migrations/0029_skill_source_inventory_retirement.sql").read_text(),
        (ROOT / "migrations/0030_skill_publisher_suspension.sql").read_text(),
        (ROOT / "migrations/0031_skill_publication_restore.sql").read_text(),
        (ROOT / "migrations/0032_skill_admission_audit.sql").read_text(),
        (ROOT / "migrations/0033_skill_content_quarantine.sql").read_text(),
        (ROOT / "migrations/0034_skill_admission_operations.sql").read_text(),
        (ROOT / "migrations/0035_skill_admission_retention.sql").read_text(),
        (ROOT / "migrations/0036_skill_binding_revocation_policy.sql").read_text(),
        (ROOT / "migrations/0037_skill_publication_sources.sql").read_text(),
        (ROOT / "migrations/0038_skill_installation_draining.sql").read_text(),
        (ROOT / "migrations/0039_skill_publisher_runtime_revocation.sql").read_text(),
        (ROOT / "migrations/0050_batch_worker_lease_safety.sql").read_text(),
        (ROOT / "migrations/0054_skill_lifecycle_broadcast_outbox.sql").read_text(),
    )
)
DOWN = "\n".join(
    (
        (ROOT / "migrations/0054_skill_lifecycle_broadcast_outbox.down.sql").read_text(),
        (ROOT / "migrations/0039_skill_publisher_runtime_revocation.down.sql").read_text(),
        (ROOT / "migrations/0038_skill_installation_draining.down.sql").read_text(),
        (ROOT / "migrations/0037_skill_publication_sources.down.sql").read_text(),
        (ROOT / "migrations/0036_skill_binding_revocation_policy.down.sql").read_text(),
        (ROOT / "migrations/0035_skill_admission_retention.down.sql").read_text(),
        (ROOT / "migrations/0034_skill_admission_operations.down.sql").read_text(),
        (ROOT / "migrations/0033_skill_content_quarantine.down.sql").read_text(),
        (ROOT / "migrations/0032_skill_admission_audit.down.sql").read_text(),
        (ROOT / "migrations/0031_skill_publication_restore.down.sql").read_text(),
        (ROOT / "migrations/0030_skill_publisher_suspension.down.sql").read_text(),
        (ROOT / "migrations/0029_skill_source_inventory_retirement.down.sql").read_text(),
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
                "skill_source_inventory",
                "skill_source_retirement_command",
                "skill_publication_source",
                "skill_source_command",
                "skill_installation_command",
                "skill_publication_restore_command",
                "skill_admission_audit",
                "skill_command",
                "skill_outbox",
                "skill_publisher",
                "skill_publisher_key",
                "skill_publisher_command",
                "skill_lifecycle_revision",
                "skill_lifecycle_broadcast_outbox",
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
            for column in ("status_reason_code", "status_changed_at"):
                assert await connection.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM information_schema.columns
                    WHERE table_schema='hands' AND table_name='skill_publisher'
                      AND column_name=$1)""",
                    column,
                )
            for column in (
                "security_action",
                "security_policy_version",
                "security_policy_decision_id",
            ):
                assert await connection.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM information_schema.columns
                    WHERE table_schema='hands' AND table_name='skill_publisher'
                      AND column_name=$1)""",
                    column,
                )
            for column in (
                "revocation_action",
                "revocation_policy_version",
                "revocation_policy_decision_id",
            ):
                assert await connection.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM information_schema.columns
                    WHERE table_schema='hands' AND table_name='skill_publisher_key'
                      AND column_name=$1)""",
                    column,
                )
            assert await connection.fetchval(
                """SELECT is_nullable = 'NO' FROM information_schema.columns
                WHERE table_schema='hands' AND table_name='skill_admission_audit'
                  AND column_name='content_policy_version'"""
            )
            assert await connection.fetchval(
                """SELECT is_nullable = 'NO' FROM information_schema.columns
                WHERE table_schema='hands' AND table_name='skill_source'
                  AND column_name='priority'"""
            )
            lifecycle = PostgresSkillLifecycleStore(database_url)
            lifecycle_peer = PostgresSkillLifecycleStore(database_url)
            publishers = PostgresSkillPublisherStore(database_url)
            publisher_peer = PostgresSkillPublisherStore(database_url)
            sources = SkillSourceService(lifecycle)
            peer_sources = SkillSourceService(lifecycle_peer)
            now = datetime.now(UTC)

            def source_command(
                source_id: str,
                command_id: str,
                priority: int,
                expected_revision: int = 0,
            ) -> ConfigureSkillSourceCommand:
                return ConfigureSkillSourceCommand(
                    tenant_id="tenant-source-roundtrip",
                    actor_id="migration-test",
                    source_id=source_id,
                    kind=SkillSourceKind.MCP,
                    desired_state=SkillSourceDesiredState.ENABLED,
                    publisher_allowlist=("acme",),
                    credential_ref=f"vault/migration/{source_id}",
                    config_metadata={"server_id": source_id},
                    priority=priority,
                    command_id=command_id,
                    expected_revision=expected_revision,
                    correlation_id="migration-source-test",
                    causation_id=command_id,
                )

            try:
                await sources.configure(
                    source_command("sks_roundtrip_low", "source-low", 0)
                )
                await peer_sources.configure(
                    source_command("sks_roundtrip_high", "source-high", 10)
                )
                package = SkillPackageRecord(
                    tenant_id="tenant-source-roundtrip",
                    manifest=SkillManifest(
                        name="release.prepare",
                        version="1.0.0",
                        description="Prepare a release",
                        publisher="acme",
                        signature="hmac-sha256:test",
                    ),
                    package_digest=f"sha256:{'a' * 64}",
                    artifact_ref=ArtifactRef(
                        artifact_id="art-roundtrip-source",
                        version=1,
                        content_hash="a" * 64,
                        media_type="application/vnd.auraclaw.skill-package+json",
                        size=128,
                    ),
                    retention_until=now + timedelta(days=90),
                    retention_updated_by="migration-test",
                    retention_updated_at=now,
                    created_at=now,
                )

                async def publish_from(
                    store: PostgresSkillLifecycleStore,
                    source_id: str,
                    command_id: str,
                    expected_revision: int,
                ) -> SkillPublicationRecord:
                    publication = SkillPublicationRecord(
                        publication_id="skp_roundtrip_source",
                        tenant_id=package.tenant_id,
                        publisher="acme",
                        name="release.prepare",
                        version="1.0.0",
                        package_digest=package.package_digest,
                        source_id=source_id,
                        status=SkillPublicationStatus.ACTIVE,
                        revision=max(1, expected_revision),
                        created_by="migration-test",
                        updated_by="migration-test",
                        created_at=now,
                        updated_at=now,
                    )
                    result = await store.commit_publish(
                        SkillPublishCommit(
                            command_id=command_id,
                            request_digest=(
                                f"sha256:{command_id.encode().hex().ljust(64, '0')[:64]}"
                            ),
                            actor_id="migration-test",
                            source_id=source_id,
                            correlation_id="migration-source-test",
                            causation_id=command_id,
                            expected_publication_revision=expected_revision,
                            package=package,
                            publication=publication,
                            installation=(
                                SkillInstallationRecord(
                                    installation_id="ski_roundtrip_source",
                                    tenant_id=package.tenant_id,
                                    publisher="acme",
                                    name="release.prepare",
                                    status=SkillInstallationStatus.ACTIVE,
                                    source_id=source_id,
                                    revision=1,
                                    created_by="migration-test",
                                    updated_by="migration-test",
                                    created_at=now,
                                    updated_at=now,
                                )
                                if expected_revision == 0
                                else None
                            ),
                            occurred_at=now,
                        )
                    )
                    return result.publication

                assert (
                    await publish_from(
                        lifecycle,
                        "sks_roundtrip_low",
                        "publish-low",
                        0,
                    )
                ).source_id == "sks_roundtrip_low"
                selected = await publish_from(
                    lifecycle_peer,
                    "sks_roundtrip_high",
                    "publish-high",
                    1,
                )
                assert selected.source_id == "sks_roundtrip_high"
                assert selected.revision == 2

                await sources.configure(
                    source_command(
                        "sks_roundtrip_low", "source-low-priority", 20, 1
                    )
                )
                reprioritized = await lifecycle_peer.get_publication(
                    package.tenant_id, "acme", "release.prepare", "1.0.0"
                )
                assert reprioritized is not None
                assert reprioritized.source_id == "sks_roundtrip_low"

                await sources.retire(
                    RetireSkillSourceCommand(
                        tenant_id=package.tenant_id,
                        actor_id="migration-test",
                        source_id="sks_roundtrip_low",
                        reason_code="source_decommissioned",
                        command_id="source-low-retire",
                        expected_revision=2,
                        correlation_id="migration-source-test",
                        causation_id="source-low-retire",
                    )
                )
                fallback = await lifecycle_peer.get_publication(
                    package.tenant_id, "acme", "release.prepare", "1.0.0"
                )
                assert fallback is not None
                assert fallback.source_id == "sks_roundtrip_high"
                assert fallback.status is SkillPublicationStatus.ACTIVE

                installation = await lifecycle.get_installation(
                    package.tenant_id,
                    "acme",
                    "release.prepare",
                )
                assert installation is not None
                draining = installation.model_copy(
                    update={
                        "status": SkillInstallationStatus.DRAINING,
                        "revision": 2,
                        "updated_at": datetime.now(UTC),
                        "reason_code": "tenant_uninstalled",
                        "uninstall_action": SkillRevocationAction.CONTINUE,
                        "uninstall_policy_version": "skill-uninstall-v1",
                        "uninstall_policy_decision_id": "uninstall-drain",
                    }
                )
                drain_commit = SkillInstallationCommit(
                    command_id="uninstall-drain",
                    request_digest=f"sha256:{'d' * 64}",
                    operation="uninstall",
                    force_uninstall=False,
                    actor_id="migration-test",
                    correlation_id="migration-installation-test",
                    causation_id="uninstall-drain",
                    reason_code="tenant_uninstalled",
                    expected_revision=1,
                    installation=draining,
                    occurred_at=datetime.now(UTC),
                )
                drain_results = await asyncio.gather(
                    lifecycle.commit_installation_change(drain_commit),
                    lifecycle_peer.commit_installation_change(drain_commit),
                )
                assert all(
                    result.status is SkillInstallationStatus.DRAINING
                    for result in drain_results
                )
                assert drain_results[0] == drain_results[1]

                forced = draining.model_copy(
                    update={
                        "status": SkillInstallationStatus.UNINSTALLED,
                        "revision": 3,
                        "updated_at": datetime.now(UTC),
                        "reason_code": "security_force_uninstall",
                        "uninstall_action": SkillRevocationAction.CANCEL,
                        "uninstall_policy_decision_id": "uninstall-force",
                    }
                )
                assert (
                    await lifecycle_peer.commit_installation_change(
                        SkillInstallationCommit(
                            command_id="uninstall-force",
                            request_digest=f"sha256:{'e' * 64}",
                            operation="uninstall",
                            force_uninstall=True,
                            actor_id="migration-test",
                            correlation_id="migration-installation-test",
                            causation_id="uninstall-force",
                            reason_code="security_force_uninstall",
                            expected_revision=2,
                            installation=forced,
                            occurred_at=datetime.now(UTC),
                        )
                    )
                ).uninstall_action is SkillRevocationAction.CANCEL

                registered = await publishers.register_publisher(
                    RegisterSkillPublisherCommand(
                        tenant_id=package.tenant_id,
                        actor_id="migration-test",
                        publisher="acme",
                        display_name="Acme",
                        command_id="publisher-register",
                        correlation_id="migration-publisher-test",
                        causation_id="publisher-register",
                    )
                )
                publisher_record, key = await publisher_peer.rotate_key(
                    RotateSkillPublisherKeyCommand(
                        tenant_id=package.tenant_id,
                        actor_id="migration-test",
                        publisher="acme",
                        key_id="key-a",
                        public_key="A" * 43,
                        command_id="publisher-rotate",
                        expected_revision=registered.revision,
                        correlation_id="migration-publisher-test",
                        causation_id="publisher-rotate",
                    )
                )
                suspended = await publishers.change_status(
                    ChangeSkillPublisherStatusCommand(
                        tenant_id=package.tenant_id,
                        actor_id="migration-test",
                        publisher="acme",
                        operation=SkillPublisherStatusOperation.SUSPEND,
                        reason_code="publisher_under_review",
                        command_id="publisher-suspend",
                        expected_revision=publisher_record.revision,
                        correlation_id="migration-publisher-test",
                        causation_id="publisher-suspend",
                    )
                )
                assert (
                    await publisher_peer.get_publisher(package.tenant_id, "acme")
                ) == suspended
                assert suspended.security_action is SkillRevocationAction.PAUSE
                resumed = await publisher_peer.change_status(
                    ChangeSkillPublisherStatusCommand(
                        tenant_id=package.tenant_id,
                        actor_id="migration-test",
                        publisher="acme",
                        operation=SkillPublisherStatusOperation.RESUME,
                        reason_code="review_completed",
                        command_id="publisher-resume",
                        expected_revision=suspended.revision,
                        correlation_id="migration-publisher-test",
                        causation_id="publisher-resume",
                    )
                )
                revoked_key = await publishers.revoke_key(
                    RevokeSkillPublisherKeyCommand(
                        tenant_id=package.tenant_id,
                        actor_id="migration-test",
                        publisher="acme",
                        key_id=key.key_id,
                        reason_code="key_compromised",
                        revocation_action=SkillRevocationAction.PAUSE,
                        policy_decision_id="key-decision",
                        command_id="publisher-key-revoke",
                        expected_revision=key.revision,
                        correlation_id="migration-publisher-test",
                        causation_id="publisher-key-revoke",
                    )
                )
                assert revoked_key.status is SkillPublisherKeyStatus.REVOKED
                assert (
                    await publisher_peer.get_key(package.tenant_id, "acme", "key-a")
                ) == revoked_key
                revoked_publisher = await publisher_peer.change_status(
                    ChangeSkillPublisherStatusCommand(
                        tenant_id=package.tenant_id,
                        actor_id="migration-test",
                        publisher="acme",
                        operation=SkillPublisherStatusOperation.REVOKE,
                        reason_code="publisher_compromised",
                        revocation_action=SkillRevocationAction.CANCEL,
                        policy_version="skill-revocation-v2",
                        command_id="publisher-revoke",
                        expected_revision=resumed.revision,
                        correlation_id="migration-publisher-test",
                        causation_id="publisher-revoke",
                    )
                )
                assert revoked_publisher.status is SkillPublisherStatus.REVOKED
                assert (
                    await publishers.get_publisher(package.tenant_id, "acme")
                ) == revoked_publisher
                with pytest.raises(PolicyDeniedError, match="cannot change status"):
                    await publishers.change_status(
                        ChangeSkillPublisherStatusCommand(
                            tenant_id=package.tenant_id,
                            actor_id="migration-test",
                            publisher="acme",
                            operation=SkillPublisherStatusOperation.RESUME,
                            reason_code="unsafe_resume",
                            command_id="publisher-unsafe-resume",
                            expected_revision=revoked_publisher.revision,
                            correlation_id="migration-publisher-test",
                            causation_id="publisher-unsafe-resume",
                        )
                    )
            finally:
                await lifecycle.close()
                await lifecycle_peer.close()
                await publishers.close()
                await publisher_peer.close()
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
                "skill_source_inventory",
                "skill_source_retirement_command",
                "skill_publication_source",
                "skill_source_command",
                "skill_installation_command",
                "skill_publication_restore_command",
                "skill_admission_audit",
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
