from __future__ import annotations

import asyncio
import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
from tests.skill_lifecycle_contract import assert_skill_lifecycle_core_contract

from auraclaw.action.skill_lifecycle import (
    SkillPublishCommit,
)
from auraclaw.config import get_settings
from auraclaw.contracts.skills import (
    SkillInstallationRecord,
    SkillInstallationStatus,
    SkillManifest,
    SkillPackageRecord,
    SkillPackageRetentionStatus,
    SkillPublicationRecord,
    SkillPublicationStatus,
    SkillRevocationAction,
)
from auraclaw.contracts.tools import ArtifactRef
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url
from auraclaw.infrastructure.persistence.postgres_skill_lifecycle import (
    PostgresSkillLifecycleStore,
)
from auraclaw.infrastructure.persistence.postgres_skill_lifecycle_events import (
    PostgresSkillLifecycleSignalStore,
)

SETTINGS = get_settings()
DATABASE_URL = asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
ROOT = Path(__file__).resolve().parents[2]
MIGRATION = "\n".join(
    (
        (ROOT / "migrations/0023_skill_lifecycle.sql").read_text(),
        (ROOT / "migrations/0024_skill_publication_actor.sql").read_text(),
        (ROOT / "migrations/0025_skill_package_retention.sql").read_text(),
        (ROOT / "migrations/0026_skill_publication_reliability.sql").read_text(),
        (ROOT / "migrations/0028_skill_source_reconcile_lease.sql").read_text(),
        (ROOT / "migrations/0029_skill_source_inventory_retirement.sql").read_text(),
        (ROOT / "migrations/0031_skill_publication_restore.sql").read_text(),
        (ROOT / "migrations/0032_skill_admission_audit.sql").read_text(),
        (ROOT / "migrations/0033_skill_content_quarantine.sql").read_text(),
        (ROOT / "migrations/0034_skill_admission_operations.sql").read_text(),
        (ROOT / "migrations/0035_skill_admission_retention.sql").read_text(),
        (ROOT / "migrations/0036_skill_binding_revocation_policy.sql").read_text(),
        (ROOT / "migrations/0037_skill_publication_sources.sql").read_text(),
        (ROOT / "migrations/0038_skill_installation_draining.sql").read_text(),
        (ROOT / "migrations/0050_batch_worker_lease_safety.sql").read_text(),
        (ROOT / "migrations/0054_skill_lifecycle_broadcast_outbox.sql").read_text(),
        (ROOT / "migrations/0055_skill_package_republish.sql").read_text(),
        (ROOT / "migrations/0056_remove_skill_sources.sql").read_text(),
    )
)
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="PostgreSQL test URL not configured")


def test_postgres_store_satisfies_shared_lifecycle_contract() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        await _ensure_skill_lifecycle_schema(connection)
        suffix = uuid4().hex
        tenant_id = f"tenant-contract-{suffix}"
        store = PostgresSkillLifecycleStore(DATABASE_URL)
        try:
            await assert_skill_lifecycle_core_contract(
                store,
                tenant_id=tenant_id,
                identity_suffix=suffix,
            )
        finally:
            await store.close()
            for table in (
                "skill_installation",
                "skill_publication",
                "skill_package",
            ):
                await connection.execute(
                    f"DELETE FROM hands.{table} WHERE tenant_id=$1", tenant_id
                )
            await connection.close()

    asyncio.run(scenario())


class _SharedSkillArtifacts:
    def __init__(self) -> None:
        self.contents: dict[str, bytes] = {}

    async def put(self, **kwargs: object) -> ArtifactRef:
        content = kwargs["content"]
        assert isinstance(content, bytes)
        artifact_id = f"art-{uuid4().hex}"
        self.contents[artifact_id] = content
        return ArtifactRef(
            artifact_id=artifact_id,
            version=1,
            content_hash=hashlib.sha256(content).hexdigest(),
            media_type="application/vnd.auraclaw.skill-package+json",
            size=len(content),
        )

    async def read(
        self,
        *,
        tenant_id: str,
        artifact_ref: ArtifactRef,
        actor_id: str,
        correlation_id: str,
    ) -> bytes:
        del tenant_id, actor_id, correlation_id
        return self.contents[artifact_ref.artifact_id]


class _NoOpSkillProjector:
    async def rebuild_tenant(self, tenant_id: str) -> None:
        del tenant_id


async def _ensure_skill_lifecycle_schema(connection: asyncpg.Connection) -> None:
    current = await connection.fetchval(
        """SELECT EXISTS(
            SELECT 1 FROM pg_constraint constraint_record
            JOIN pg_class relation ON relation.oid=constraint_record.conrelid
            JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
            WHERE constraint_record.conname='skill_installation_uninstall_action_check'
              AND namespace.nspname='hands' AND relation.relname='skill_installation'
        )"""
    )
    if not current:
        await connection.execute(MIGRATION)
    elif not await connection.fetchval(
        "SELECT to_regclass('hands.skill_lifecycle_broadcast_outbox') IS NOT NULL"
    ):
        await connection.execute(
            (ROOT / "migrations/0054_skill_lifecycle_broadcast_outbox.sql").read_text()
        )
    republish_schema_current = await connection.fetchval(
        """SELECT to_regclass('hands.skill_package_tombstone') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM pg_constraint constraint_record
            JOIN pg_class relation ON relation.oid=constraint_record.conrelid
            JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
            WHERE namespace.nspname='hands'
              AND relation.relname='skill_publication'
              AND constraint_record.contype='f'
              AND constraint_record.conname IN (
                'skill_publication_package_digest_fk',
                'skill_publication_tenant_id_publisher_name_version_package_fkey'
              )
              AND NOT constraint_record.condeferrable
        )"""
    )
    if not republish_schema_current:
        await connection.execute(
            (ROOT / "migrations/0055_skill_package_republish.sql").read_text()
        )


def test_postgres_republish_replaces_purged_coordinate_and_archives_tombstone() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        await _ensure_skill_lifecycle_schema(connection)
        suffix = uuid4().hex
        tenant_id = f"tenant-republish-{suffix}"
        store = PostgresSkillLifecycleStore(DATABASE_URL)
        now = datetime.now(UTC)
        old_digest = f"sha256:{'a' * 64}"
        new_digest = f"sha256:{'b' * 64}"
        manifest = SkillManifest(
            name="release.prepare",
            version="1.0.0",
            description="Republish integration test",
            publisher="platform",
            signature="hmac-sha256:test",
        )
        old_package = SkillPackageRecord(
            tenant_id=tenant_id,
            manifest=manifest,
            package_digest=old_digest,
            artifact_ref=ArtifactRef(
                artifact_id=f"old-artifact-{suffix}",
                version=1,
                content_hash="a" * 64,
                media_type="application/octet-stream",
                size=1,
            ),
            retention_status=SkillPackageRetentionStatus.PURGED,
            retention_until=now,
            retention_revision=2,
            retention_updated_by="purger",
            retention_updated_at=now,
            created_at=now - timedelta(days=1),
            purged_at=now,
        )
        old_publication = SkillPublicationRecord(
            publication_id=f"skp_{suffix}",
            tenant_id=tenant_id,
            publisher="platform",
            name="release.prepare",
            version="1.0.0",
            package_digest=old_digest,
            status=SkillPublicationStatus.REVOKED,
            revision=2,
            created_by="publisher",
            updated_by="purger",
            created_at=now - timedelta(days=1),
            updated_at=now,
            reason_code="operator_purge",
            revocation_action=SkillRevocationAction.CANCEL,
            revocation_policy_version="skill-revocation-v1",
            revocation_policy_decision_id=f"purge-{suffix}",
        )
        old_installation = SkillInstallationRecord(
            installation_id=f"ski_{suffix}",
            tenant_id=tenant_id,
            publisher="platform",
            name="release.prepare",
            pinned_package_digest=old_digest,
            auto_upgrade=False,
            status=SkillInstallationStatus.UNINSTALLED,
            revision=3,
            created_by="publisher",
            updated_by="purger",
            created_at=now - timedelta(days=1),
            updated_at=now,
            reason_code="operator_purge",
            uninstall_action=SkillRevocationAction.CANCEL,
            uninstall_policy_version="skill-uninstall-v1",
            uninstall_policy_decision_id=f"purge-{suffix}",
        )
        try:
            await store.put_package(old_package)
            await store.put_publication(
                old_publication.model_copy(
                    update={
                        "status": SkillPublicationStatus.ACTIVE,
                        "revision": 1,
                        "reason_code": None,
                        "revocation_action": None,
                        "revocation_policy_version": None,
                        "revocation_policy_decision_id": None,
                    }
                ),
                expected_revision=0,
            )
            await store.put_publication(old_publication, expected_revision=1)
            active_installation = old_installation.model_copy(
                update={
                    "status": SkillInstallationStatus.ACTIVE,
                    "revision": 1,
                    "reason_code": None,
                    "uninstall_action": None,
                    "uninstall_policy_version": None,
                    "uninstall_policy_decision_id": None,
                }
            )
            await store.put_installation(active_installation, expected_revision=0)
            await store.put_installation(
                active_installation.model_copy(
                    update={
                        "status": SkillInstallationStatus.DISABLED,
                        "revision": 2,
                        "reason_code": "operator_purge",
                    }
                ),
                expected_revision=1,
            )
            await store.put_installation(old_installation, expected_revision=2)

            new_package = old_package.model_copy(
                update={
                    "package_digest": new_digest,
                    "artifact_ref": replace(
                        old_package.artifact_ref,
                        artifact_id=f"new-artifact-{suffix}",
                        content_hash="b" * 64,
                    ),
                    "retention_status": SkillPackageRetentionStatus.RETAINED,
                    "retention_until": now + timedelta(days=90),
                    "retention_revision": 1,
                    "retention_updated_by": "publisher",
                    "retention_updated_at": now + timedelta(seconds=1),
                    "created_at": now + timedelta(seconds=1),
                    "purged_at": None,
                }
            )
            new_publication = old_publication.model_copy(
                update={
                    "package_digest": new_digest,
                    "status": SkillPublicationStatus.ACTIVE,
                    "revision": 3,
                    "updated_by": "publisher",
                    "updated_at": now + timedelta(seconds=1),
                    "reason_code": None,
                    "revocation_action": None,
                    "revocation_policy_version": None,
                    "revocation_policy_decision_id": None,
                }
            )
            new_installation = old_installation.model_copy(
                update={
                    "version_constraint": "=1.0.0",
                    "pinned_package_digest": new_digest,
                    "status": SkillInstallationStatus.ACTIVE,
                    "revision": 4,
                    "updated_by": "publisher",
                    "updated_at": now + timedelta(seconds=1),
                    "reason_code": None,
                    "uninstall_action": None,
                    "uninstall_policy_version": None,
                    "uninstall_policy_decision_id": None,
                }
            )
            result = await store.commit_publish(
                SkillPublishCommit(
                    command_id=f"republish-{suffix}",
                    request_digest=new_digest,
                    actor_id="publisher",
                    correlation_id=f"corr-{suffix}",
                    causation_id=f"republish-{suffix}",
                    expected_publication_revision=2,
                    package=new_package,
                    publication=new_publication,
                    installation=new_installation,
                    occurred_at=now + timedelta(seconds=1),
                    replace_purged=True,
                    expected_installation_revision=3,
                )
            )
            assert result.package.package_digest == new_digest
            assert result.publication.status is SkillPublicationStatus.ACTIVE
            assert result.installation == new_installation
            assert await store.list_package_tombstones(
                tenant_id, "platform", "release.prepare"
            ) == (old_package,)
        finally:
            await store.close()
            for table in (
                "skill_outbox",
                "skill_command",
                "skill_installation",
                "skill_publication",
                "skill_package_tombstone",
                "skill_package",
            ):
                await connection.execute(
                    f"DELETE FROM hands.{table} WHERE tenant_id=$1", tenant_id
                )
            await connection.close()

    asyncio.run(scenario())


def test_postgres_skill_lifecycle_broadcast_outbox_is_monotonic_and_claimed_once() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        await _ensure_skill_lifecycle_schema(connection)
        tenant_id = f"tenant-signal-{uuid4().hex}"
        store_a = PostgresSkillLifecycleSignalStore(DATABASE_URL)
        store_b = PostgresSkillLifecycleSignalStore(DATABASE_URL)
        try:
            first = await store_a.enqueue(
                tenant_id=tenant_id,
                change_type="skill.lifecycle.snapshot_changed",
                snapshot_digest="sha256:a",
                origin_replica="hands-a",
            )
            second = await store_b.enqueue(
                tenant_id=tenant_id,
                change_type="skill.lifecycle.snapshot_changed",
                snapshot_digest="sha256:b",
                origin_replica="hands-b",
            )
            assert (first.revision, second.revision) == (1, 2)

            claimed_a = await store_a.claim(
                owner="relay-a", limit=10, claim_ttl=timedelta(seconds=30)
            )
            assert [record.signal.revision for record in claimed_a] == [1, 2]
            assert await store_b.claim(
                owner="relay-b", limit=10, claim_ttl=timedelta(seconds=30)
            ) == ()
            for record in claimed_a:
                assert await store_a.complete(
                    outbox_id=record.outbox_id, owner="relay-a"
                )
        finally:
            await store_a.close()
            await store_b.close()
            await connection.execute(
                "DELETE FROM hands.skill_lifecycle_broadcast_outbox WHERE tenant_id=$1",
                tenant_id,
            )
            await connection.execute(
                "DELETE FROM hands.skill_lifecycle_revision WHERE tenant_id=$1",
                tenant_id,
            )
            await connection.close()

    asyncio.run(scenario())
