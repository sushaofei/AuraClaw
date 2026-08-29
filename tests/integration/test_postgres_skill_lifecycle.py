from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.action.skill_lifecycle import (
    SkillAdmissionAuditRecord,
    SkillPublishCommit,
    SkillRestoreCommit,
    SkillSourcePackageIdentity,
    SkillSourceSnapshotCommit,
)
from auraclaw.config import get_settings
from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.skills import (
    SkillInstallationRecord,
    SkillInstallationStatus,
    SkillManifest,
    SkillPackageRecord,
    SkillPublicationRecord,
    SkillPublicationStatus,
    SkillSourceDesiredState,
    SkillSourceKind,
    SkillSourceRecord,
    SkillSourceSyncState,
)
from auraclaw.contracts.tools import ArtifactRef
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url
from auraclaw.infrastructure.persistence.postgres_skill_lifecycle import (
    PostgresSkillLifecycleStore,
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
        (ROOT / "migrations/0030_skill_publisher_suspension.sql").read_text(),
        (ROOT / "migrations/0031_skill_publication_restore.sql").read_text(),
        (ROOT / "migrations/0032_skill_admission_audit.sql").read_text(),
        (ROOT / "migrations/0033_skill_content_quarantine.sql").read_text(),
    )
)
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="PostgreSQL test URL not configured")


def test_postgres_skill_lifecycle_is_persistent_and_tenant_scoped() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        await connection.execute(MIGRATION)
        suffix = uuid4().hex
        tenant_id = f"tenant-skill-{suffix}"
        digest = f"sha256:{suffix.ljust(64, '0')}"
        store_a = PostgresSkillLifecycleStore(DATABASE_URL)
        store_b = PostgresSkillLifecycleStore(DATABASE_URL)
        now = datetime.now(UTC)
        try:
            admission = SkillAdmissionAuditRecord(
                admission_id=f"skad_{suffix}",
                tenant_id=tenant_id,
                command_id=f"admission-{suffix}",
                operation="publish_artifact",
                actor_id="integration-test",
                source_id="sks_admin_upload",
                correlation_id=f"corr-{suffix}",
                causation_id=f"cause-{suffix}",
                publisher=None,
                name=None,
                version=None,
                package_digest=digest,
                artifact_id=f"art-{suffix}",
                outcome="quarantined",
                stage="content_scan",
                safe_error_code="skill_content_prompt_injection",
                duration_ms=7,
                occurred_at=now,
            )
            await store_a.record_admission(admission)
            assert await store_b.list_admissions(tenant_id) == (admission,)
            assert await store_b.list_admissions(f"other-{tenant_id}") == ()
            package = SkillPackageRecord(
                tenant_id=tenant_id,
                manifest=SkillManifest(
                    name="release.prepare",
                    version="1.0.0",
                    description="Prepare release",
                    publisher="platform",
                    signature="hmac-sha256:abc",
                ),
                package_digest=digest,
                artifact_ref=ArtifactRef(
                    artifact_id=f"art-{suffix}",
                    version=1,
                    content_hash=suffix,
                    media_type="application/vnd.auraclaw.skill-package+json",
                    size=100,
                ),
                retention_until=now + timedelta(days=90),
                retention_updated_by="integration-test",
                retention_updated_at=now,
                created_at=now,
            )
            await store_a.put_package(package)
            publication = SkillPublicationRecord(
                publication_id=f"skp_{suffix}",
                tenant_id=tenant_id,
                publisher="platform",
                name="release.prepare",
                version="1.0.0",
                package_digest=digest,
                status=SkillPublicationStatus.ACTIVE,
                revision=1,
                created_by="integration-test",
                updated_by="integration-test",
                created_at=now,
                updated_at=now,
            )
            await store_a.put_publication(publication, expected_revision=0)
            installation = SkillInstallationRecord(
                installation_id=f"ski_{suffix}",
                tenant_id=tenant_id,
                publisher="platform",
                name="release.prepare",
                status=SkillInstallationStatus.ACTIVE,
                revision=1,
                created_by="integration-test",
                updated_by="integration-test",
                created_at=now,
                updated_at=now,
            )
            await store_a.put_installation(installation, expected_revision=0)
            source = SkillSourceRecord(
                source_id=f"sks_source-{suffix}",
                tenant_id=tenant_id,
                kind=SkillSourceKind.MCP,
                desired_state=SkillSourceDesiredState.ENABLED,
                publisher_allowlist=("platform",),
                credential_ref=f"vault/{suffix}#mcp",
                config_metadata={"server_id": f"server-{suffix}"},
                revision=1,
                created_by="integration-test",
                updated_by="integration-test",
                created_at=now,
                updated_at=now,
            )
            await store_a.put_source(source, expected_revision=0)
            first_lease = await store_a.claim_source_lease(
                tenant_id=tenant_id,
                source_id=source.source_id,
                owner="hands-a",
                ttl=timedelta(minutes=1),
            )
            assert first_lease is not None and first_lease.fencing_token == 1
            assert (
                await store_b.claim_source_lease(
                    tenant_id=tenant_id,
                    source_id=source.source_id,
                    owner="hands-b",
                    ttl=timedelta(minutes=1),
                )
                is None
            )
            await connection.execute(
                """UPDATE hands.skill_source_lease SET expires_at=now()
                WHERE tenant_id=$1 AND source_id=$2""",
                tenant_id,
                source.source_id,
            )
            second_lease = await store_b.claim_source_lease(
                tenant_id=tenant_id,
                source_id=source.source_id,
                owner="hands-b",
                ttl=timedelta(minutes=1),
            )
            assert second_lease is not None and second_lease.fencing_token == 2
            with pytest.raises(VersionConflictError, match="revision conflict"):
                await store_a.put_source(
                    source.model_copy(
                        update={"kind": SkillSourceKind.GIT, "revision": 2}
                    ),
                    expected_revision=1,
                )
            sync_state = SkillSourceSyncState(
                source_id=source.source_id,
                tenant_id=tenant_id,
                generation=1,
                cursor="cursor-1",
                complete_snapshot=True,
                last_success_at=now,
                last_attempt_at=now,
            )
            await store_a.put_sync_state(sync_state)

            assert await store_b.get_package(
                tenant_id, "platform", "release.prepare", "1.0.0"
            ) == package
            assert await store_b.get_installation(
                tenant_id, "platform", "release.prepare"
            ) == installation
            assert await store_b.list_installations(tenant_id) == (installation,)
            assert tenant_id in await store_b.list_tenants()
            assert await store_b.list_publications(f"other-{tenant_id}") == ()
            assert await store_b.get_source(tenant_id, source.source_id) == source
            assert await store_b.get_sync_state(tenant_id, source.source_id) == sync_state

            transactional_package = package.model_copy(
                update={
                    "manifest": package.manifest.model_copy(update={"version": "2.0.0"}),
                    "package_digest": f"sha256:{'b' * 64}",
                    "artifact_ref": ArtifactRef(
                        artifact_id=f"art-transaction-{suffix}",
                        version=1,
                        content_hash="b" * 64,
                        media_type="application/vnd.auraclaw.skill-package+json",
                        size=200,
                    ),
                }
            )
            transactional_publication = publication.model_copy(
                update={
                    "publication_id": f"skp_transaction_{suffix}",
                    "version": "2.0.0",
                    "package_digest": transactional_package.package_digest,
                    "status": SkillPublicationStatus.STAGED,
                    "source_id": source.source_id,
                }
            )
            publish_commit = SkillPublishCommit(
                command_id=f"publish-transaction-{suffix}",
                request_digest=f"sha256:{'c' * 64}",
                actor_id="integration-test",
                source_id=source.source_id,
                correlation_id=f"correlation-{suffix}",
                causation_id=f"publish-transaction-{suffix}",
                expected_publication_revision=0,
                package=transactional_package,
                publication=transactional_publication,
                installation=None,
                occurred_at=now,
                source_lease=second_lease,
            )
            with pytest.raises(VersionConflictError, match="lease is stale"):
                await store_a.commit_publish(
                    publish_commit.__class__(
                        **{
                            **publish_commit.__dict__,
                            "source_lease": first_lease,
                        }
                    )
                )
            committed = await store_a.commit_publish(publish_commit)
            replayed = await store_b.commit_publish(publish_commit)
            assert committed.publication == replayed.publication
            assert replayed.replayed
            with pytest.raises(VersionConflictError, match="command id was reused"):
                await store_b.commit_publish(
                    publish_commit.__class__(
                        **{
                            **publish_commit.__dict__,
                            "request_digest": f"sha256:{'d' * 64}",
                        }
                    )
                )
            outbox = await store_a.claim_outbox(owner="integration-a")
            assert len(outbox) == 1
            assert outbox[0].payload["package_digest"] == (
                transactional_package.package_digest
            )
            assert await store_b.claim_outbox(owner="integration-b") == ()
            await store_a.complete_outbox(
                outbox_id=outbox[0].outbox_id, owner="integration-a"
            )
            await store_a.commit_source_snapshot(
                SkillSourceSnapshotCommit(
                    state=sync_state.model_copy(
                        update={
                            "generation": second_lease.fencing_token,
                            "cursor": "snapshot-observed",
                        }
                    ),
                    lease=second_lease,
                    observed=(
                        SkillSourcePackageIdentity(
                            "platform", "release.prepare", "2.0.0"
                        ),
                    ),
                    missing_snapshot_threshold=2,
                    actor_id="action-hands-skill-reconciler",
                    command_prefix=f"source-retire:{source.source_id}",
                    correlation_id=f"reconcile-{suffix}",
                    causation_id=f"snapshot-observed-{suffix}",
                    occurred_at=datetime.now(UTC),
                )
            )
            await store_a.release_source_lease(second_lease)
            third_lease = await store_b.claim_source_lease(
                tenant_id=tenant_id,
                source_id=source.source_id,
                owner="hands-b",
                ttl=timedelta(minutes=1),
            )
            assert third_lease is not None
            first_missing_result = await store_b.commit_source_snapshot(
                SkillSourceSnapshotCommit(
                    state=sync_state.model_copy(
                        update={
                            "generation": third_lease.fencing_token,
                            "cursor": "snapshot-missing-1",
                            "last_success_at": datetime.now(UTC),
                            "last_attempt_at": datetime.now(UTC),
                        }
                    ),
                    lease=third_lease,
                    observed=(),
                    missing_snapshot_threshold=2,
                    actor_id="action-hands-skill-reconciler",
                    command_prefix=f"source-retire:{source.source_id}",
                    correlation_id=f"reconcile-{suffix}",
                    causation_id=f"snapshot-missing-1-{suffix}",
                    occurred_at=datetime.now(UTC),
                )
            )
            assert first_missing_result.retired == ()
            with pytest.raises(
                VersionConflictError, match="snapshot generation must advance"
            ):
                await store_a.commit_source_snapshot(
                    SkillSourceSnapshotCommit(
                        state=sync_state.model_copy(
                            update={
                                "generation": third_lease.fencing_token,
                                "cursor": "snapshot-missing-1-replay",
                                "last_success_at": datetime.now(UTC),
                                "last_attempt_at": datetime.now(UTC),
                            }
                        ),
                        lease=third_lease,
                        observed=(),
                        missing_snapshot_threshold=2,
                        actor_id="action-hands-skill-reconciler",
                        command_prefix=f"source-retire:{source.source_id}",
                        correlation_id=f"reconcile-{suffix}",
                        causation_id=f"snapshot-missing-1-replay-{suffix}",
                        occurred_at=datetime.now(UTC),
                    )
                )
            await store_b.release_source_lease(third_lease)
            fourth_lease = await store_a.claim_source_lease(
                tenant_id=tenant_id,
                source_id=source.source_id,
                owner="hands-a",
                ttl=timedelta(minutes=1),
            )
            assert fourth_lease is not None
            retired_result = await store_a.commit_source_snapshot(
                SkillSourceSnapshotCommit(
                    state=sync_state.model_copy(
                        update={
                            "generation": fourth_lease.fencing_token,
                            "cursor": "snapshot-missing-2",
                            "last_success_at": datetime.now(UTC),
                            "last_attempt_at": datetime.now(UTC),
                        }
                    ),
                    lease=fourth_lease,
                    observed=(),
                    missing_snapshot_threshold=2,
                    actor_id="action-hands-skill-reconciler",
                    command_prefix=f"source-retire:{source.source_id}",
                    correlation_id=f"reconcile-{suffix}",
                    causation_id=f"snapshot-missing-2-{suffix}",
                    occurred_at=datetime.now(UTC),
                )
            )
            assert len(retired_result.retired) == 1
            assert retired_result.retired[0].reason_code == (
                "source_missing_confirmed"
            )
            audit_count = await connection.fetchval(
                """SELECT count(*) FROM hands.skill_source_retirement_command
                WHERE tenant_id=$1 AND source_id=$2""",
                tenant_id,
                source.source_id,
            )
            assert audit_count == 1

            retired = retired_result.retired[0]
            restore = SkillRestoreCommit(
                command_id=f"restore-{suffix}",
                request_digest=f"sha256:{'f' * 64}",
                actor_id="reviewer-test",
                reason_code="source_inventory_reviewed",
                correlation_id=f"restore-correlation-{suffix}",
                causation_id=f"restore-causation-{suffix}",
                expected_revision=retired.revision,
                publication=retired.model_copy(
                    update={
                        "status": SkillPublicationStatus.RESTORING,
                        "revision": retired.revision + 1,
                        "updated_by": "reviewer-test",
                        "updated_at": datetime.now(UTC),
                        "reason_code": "source_inventory_reviewed",
                    }
                ),
                occurred_at=datetime.now(UTC),
            )
            restoring = await store_a.commit_restore(restore)
            assert restoring.status is SkillPublicationStatus.RESTORING
            assert restoring.revision == retired.revision + 1
            assert await store_b.commit_restore(restore) == restoring
            with pytest.raises(VersionConflictError, match="command id was reused"):
                await store_b.commit_restore(
                    restore.__class__(
                        **{
                            **restore.__dict__,
                            "request_digest": f"sha256:{'0' * 64}",
                        }
                    )
                )
            restore_audit = await connection.fetchrow(
                """SELECT actor_id, previous_revision, restoring_revision
                FROM hands.skill_publication_restore_command
                WHERE tenant_id=$1 AND command_id=$2""",
                tenant_id,
                restore.command_id,
            )
            assert restore_audit is not None
            assert restore_audit["actor_id"] == "reviewer-test"
            assert restore_audit["previous_revision"] == retired.revision
            assert restore_audit["restoring_revision"] == restoring.revision

            rollback_package = transactional_package.model_copy(
                update={
                    "manifest": transactional_package.manifest.model_copy(
                        update={"version": "3.0.0"}
                    ),
                    "package_digest": f"sha256:{'e' * 64}",
                    "artifact_ref": ArtifactRef(
                        artifact_id=f"art-rollback-{suffix}",
                        version=1,
                        content_hash="e" * 64,
                        media_type="application/vnd.auraclaw.skill-package+json",
                        size=300,
                    ),
                }
            )
            with pytest.raises(VersionConflictError, match="revision"):
                await store_a.commit_publish(
                    publish_commit.__class__(
                        **{
                            **publish_commit.__dict__,
                            "command_id": f"publish-rollback-{suffix}",
                            "request_digest": f"sha256:{'f' * 64}",
                            "expected_publication_revision": 1,
                            "package": rollback_package,
                                "publication": transactional_publication.model_copy(
                                update={
                                "publication_id": f"skp_rollback_{suffix}",
                                    "version": "3.0.0",
                                    "package_digest": rollback_package.package_digest,
                                    "revision": 2,
                                    }
                                ),
                                "source_lease": fourth_lease,
                            }
                    )
                )
            assert await store_b.get_package(
                tenant_id, "platform", "release.prepare", "3.0.0"
            ) is None

            revoked = await store_b.put_publication(
                publication.model_copy(
                    update={
                        "status": SkillPublicationStatus.REVOKED,
                        "revision": 2,
                        "updated_by": "security-test",
                        "updated_at": datetime.now(UTC),
                        "reason_code": "publisher_key_compromised",
                    }
                ),
                expected_revision=1,
            )
            assert revoked.updated_by == "security-test"
            assert revoked.status is SkillPublicationStatus.REVOKED

            retained = await store_b.update_package_retention(
                package.model_copy(
                    update={
                        "retention_until": now + timedelta(days=180),
                        "legal_hold": True,
                        "retention_revision": 2,
                        "retention_updated_by": "legal-test",
                        "retention_updated_at": datetime.now(UTC),
                    }
                ),
                expected_revision=1,
            )
            assert retained.legal_hold
            assert retained.retention_revision == 2
            with pytest.raises(VersionConflictError, match="retention revision"):
                await store_b.update_package_retention(
                    retained,
                    expected_revision=1,
                )

            disabled = await store_b.put_installation(
                installation.model_copy(
                    update={
                        "status": SkillInstallationStatus.DISABLED,
                        "revision": 2,
                        "updated_by": "admin-test",
                        "updated_at": datetime.now(UTC),
                        "reason_code": "tenant_disabled",
                    }
                ),
                expected_revision=1,
            )
            assert disabled.updated_by == "admin-test"

            with pytest.raises(VersionConflictError, match="revision conflict"):
                await store_b.put_installation(
                    installation,
                    expected_revision=0,
                )
        finally:
            await store_a.close()
            await store_b.close()
            await connection.execute(
                "DELETE FROM hands.skill_admission_audit WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM hands.skill_outbox WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM hands.skill_command WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM hands.skill_publication_restore_command WHERE tenant_id=$1",
                tenant_id,
            )
            await connection.execute(
                "DELETE FROM hands.skill_source_retirement_command WHERE tenant_id=$1",
                tenant_id,
            )
            await connection.execute(
                "DELETE FROM hands.skill_source_inventory WHERE tenant_id=$1",
                tenant_id,
            )
            await connection.execute(
                "DELETE FROM hands.skill_source_sync_state WHERE tenant_id=$1",
                tenant_id,
            )
            await connection.execute(
                "DELETE FROM hands.skill_source_lease WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM hands.skill_installation WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM hands.skill_publication WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM hands.skill_source WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM hands.skill_package WHERE tenant_id=$1", tenant_id
            )
            await connection.close()

    asyncio.run(scenario())
