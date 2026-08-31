import asyncio
import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import asyncpg
import httpx
import pytest

from auraclaw.artifact.internal_service import ArtifactInternalService, PendingUpload
from auraclaw.composition.object_storage import build_object_storage
from auraclaw.config import get_settings
from auraclaw.contracts.errors import ArtifactAccessError, NotFoundError
from auraclaw.contracts.internal import (
    ArtifactCreateUploadRequest,
    ArtifactDeleteRequest,
    ArtifactDownloadRequest,
    ArtifactFinalizeRequest,
    ArtifactSkillOrphanClaimRequest,
    ArtifactSkillOrphanResolveRequest,
    ArtifactSkillPublicationBindRequest,
    ArtifactSkillPublicationClaimRequest,
    InternalRequestContext,
    ServiceIdentity,
)
from auraclaw.infrastructure.persistence.postgres_artifact_repository import (
    PostgresArtifactRepository,
)
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url

SETTINGS = get_settings()
DATABASE_URL = asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (ROOT / "migrations/0013_s4_artifact_lifecycle.sql").read_text()
RELIABILITY_MIGRATION = (
    ROOT / "migrations/0026_skill_publication_reliability.sql"
).read_text()
RECONCILIATION_MIGRATION = (
    ROOT / "migrations/0049_artifact_operation_reconciliation.sql"
).read_text()
RECONCILIATION_DOWN = (
    ROOT / "migrations/0049_artifact_operation_reconciliation.down.sql"
).read_text()
pytestmark = pytest.mark.skipif(
    DATABASE_URL is None or not SETTINGS.object_storage_enabled,
    reason="PostgreSQL and object storage are required",
)


class _AllowPolicy:
    async def validate_decision(self, **_parameters: object) -> bool:
        return True


def test_artifact_multipart_scan_restart_and_gc() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        assert SETTINGS.object_storage_enabled
        storage = build_object_storage(SETTINGS)
        assert storage.verifier is not None
        assert storage.multipart is not None
        connection = await asyncpg.connect(DATABASE_URL)
        await connection.execute(MIGRATION)
        await connection.execute(RELIABILITY_MIGRATION)
        await connection.execute(RECONCILIATION_MIGRATION)
        suffix = uuid4().hex
        tenant_id = f"tenant-artifact-s4-{suffix}"
        context = InternalRequestContext(
            tenant_id=tenant_id,
            service_identity=ServiceIdentity.ACTION_HANDS,
            request_id=f"request-{suffix}",
            correlation_id=f"session-{suffix}",
            causation_id=f"run-{suffix}",
        )
        presigner = storage.presigner
        repository_a = PostgresArtifactRepository(DATABASE_URL)
        repository_b = PostgresArtifactRepository(DATABASE_URL)
        multipart = storage.multipart
        verifier = storage.verifier
        service = ArtifactInternalService(
            presigner,
            repository=repository_a,
            object_verifier=verifier,
            policy=_AllowPolicy(),
            multipart=multipart,
            multipart_threshold=5 * 1024 * 1024,
            multipart_part_size=5 * 1024 * 1024,
        )
        service_b = ArtifactInternalService(
            presigner,
            repository=repository_b,
            object_verifier=verifier,
            policy=_AllowPolicy(),
            multipart=multipart,
            multipart_threshold=5 * 1024 * 1024,
            multipart_part_size=5 * 1024 * 1024,
        )
        content = b"m" * (5 * 1024 * 1024) + b"checked-tail"
        checksum = hashlib.sha256(content).hexdigest()
        ready_key: str | None = None
        skill_ready_key: str | None = None
        try:
            upload = await service.create_upload(
                ArtifactCreateUploadRequest(
                    context=context,
                    root_session_id=f"root-{suffix}",
                    session_id=f"session-{suffix}",
                    name="large.bin",
                    media_type="application/octet-stream",
                    expected_size=len(content),
                    expected_checksum=checksum,
                    retention_until=datetime.now(UTC),
                )
            )
            assert upload.upload_mode == "multipart"
            assert upload.part_size is not None
            parts = []
            async with httpx.AsyncClient(timeout=30.0) as client:
                for number, url in enumerate(upload.part_urls, start=1):
                    offset = (number - 1) * upload.part_size
                    response = await client.put(
                        url, content=content[offset : offset + upload.part_size]
                    )
                    assert response.status_code in {200, 201, 204}
                    parts.append(
                        {"part_number": number, "etag": response.headers["ETag"]}
                    )
            finalized = await service.finalize(
                ArtifactFinalizeRequest(
                    context=context,
                    artifact_id=upload.artifact_id,
                    version=1,
                    upload_id=upload.upload_id,
                    size=len(content),
                    checksum=checksum,
                    parts=tuple(parts),
                )
            )
            assert finalized.status == "ready"
            idempotent = await service_b.finalize(
                ArtifactFinalizeRequest(
                    context=context,
                    artifact_id=upload.artifact_id,
                    version=1,
                    upload_id=upload.upload_id,
                    size=len(content),
                    checksum=checksum,
                    parts=tuple(parts),
                )
            )
            assert idempotent == finalized
            restarted = await repository_b.get_ready(tenant_id, upload.artifact_id, 1)
            assert restarted is not None
            assert restarted.multipart_completed
            ready_key = restarted.object_key

            download_request = ArtifactDownloadRequest(
                context=context,
                artifact_id=upload.artifact_id,
                version=1,
                actor_id="integration-runtime",
                policy_decision_id="integration-decision",
            )
            await service.download(download_request)

            deleted = await service_b.delete(
                ArtifactDeleteRequest(
                    context=context,
                    artifact_id=upload.artifact_id,
                    version=1,
                    actor_id="integration-admin",
                    reason_code="retention_elapsed",
                    policy_decision_id="integration-decision",
                )
            )
            assert deleted.status == "deleted"
            assert await repository_a.is_deleted(
                tenant_id, upload.artifact_id, 1
            )
            with pytest.raises(NotFoundError):
                await service.download(download_request)
            assert await service.delete(
                ArtifactDeleteRequest(
                    context=context,
                    artifact_id=upload.artifact_id,
                    version=1,
                    actor_id="integration-admin",
                    reason_code="idempotent_retry",
                    policy_decision_id="integration-decision",
                )
            ) == deleted
            ready_key = None

            skill_content = b'{"files":{"SKILL.md":"IyBTa2lsbA=="}}'
            skill_checksum = hashlib.sha256(skill_content).hexdigest()
            skill_scope = f"skill-upload:{suffix}"
            skill_context = context.model_copy(
                update={
                    "service_identity": ServiceIdentity.TASK_API,
                    "request_id": f"skill-upload-{suffix}",
                }
            )
            hands_skill_context = skill_context.model_copy(
                update={"service_identity": ServiceIdentity.ACTION_HANDS}
            )
            skill_upload = await service.create_upload(
                ArtifactCreateUploadRequest(
                    context=skill_context,
                    root_session_id=skill_scope,
                    session_id=skill_scope,
                    name="integration.skill.json",
                    media_type="application/vnd.auraclaw.skill-package+json",
                    expected_size=len(skill_content),
                    expected_checksum=skill_checksum,
                    classification="internal",
                    retention_until=datetime.now(UTC) + timedelta(days=90),
                )
            )
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.put(skill_upload.upload_url, content=skill_content)
                assert response.status_code in {200, 201, 204}
            skill_finalized = await service.finalize(
                ArtifactFinalizeRequest(
                    context=skill_context,
                    artifact_id=skill_upload.artifact_id,
                    version=1,
                    upload_id=skill_upload.upload_id,
                    size=len(skill_content),
                    checksum=skill_checksum,
                )
            )
            assert skill_finalized.status == "ready"
            await connection.execute(
                """UPDATE artifact.metadata SET legal_hold=true
                   WHERE tenant_id=$1 AND artifact_id=$2 AND version=1""",
                tenant_id,
                skill_upload.artifact_id,
            )
            skill_claim = await service.claim_skill_publication(
                ArtifactSkillPublicationClaimRequest(
                    context=hands_skill_context,
                    artifact_id=skill_upload.artifact_id,
                    version=1,
                    command_id=f"publish-{suffix}",
                )
            )
            assert not skill_claim.already_bound
            await service.bind_skill_publication(
                ArtifactSkillPublicationBindRequest(
                    context=hands_skill_context,
                    artifact_id=skill_upload.artifact_id,
                    version=1,
                    claim_token=f"publish-{suffix}",
                    package_digest=f"sha256:{skill_checksum}",
                )
            )
            assert await service_b.finalize(
                ArtifactFinalizeRequest(
                    context=skill_context,
                    artifact_id=skill_upload.artifact_id,
                    version=1,
                    upload_id=skill_upload.upload_id,
                    size=len(skill_content),
                    checksum=skill_checksum,
                )
            ) == skill_finalized
            skill_ready = await repository_b.get_ready(
                tenant_id, skill_upload.artifact_id, 1
            )
            assert skill_ready is not None
            skill_ready_key = skill_ready.object_key
            assert skill_ready.skill_bound_digest == f"sha256:{skill_checksum}"

            orphan_content = b"unpublished-skill"
            orphan_checksum = hashlib.sha256(orphan_content).hexdigest()
            orphan_upload = await service.create_upload(
                ArtifactCreateUploadRequest(
                    context=skill_context,
                    root_session_id=f"skill-upload:orphan-{suffix}",
                    session_id=f"skill-upload:orphan-{suffix}",
                    name="orphan.skill.json",
                    media_type="application/vnd.auraclaw.skill-package+json",
                    expected_size=len(orphan_content),
                    expected_checksum=orphan_checksum,
                    classification="internal",
                    retention_until=datetime.now(UTC),
                )
            )
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.put(
                    orphan_upload.upload_url, content=orphan_content
                )
                assert response.status_code in {200, 201, 204}
            await service.finalize(
                ArtifactFinalizeRequest(
                    context=skill_context,
                    artifact_id=orphan_upload.artifact_id,
                    version=1,
                    upload_id=orphan_upload.upload_id,
                    size=len(orphan_content),
                    checksum=orphan_checksum,
                )
            )
            await connection.execute(
                """UPDATE artifact.metadata
                   SET root_session_id='skill-registry', session_id='skill-registry'
                   WHERE tenant_id=$1 AND artifact_id=$2""",
                tenant_id,
                orphan_upload.artifact_id,
            )
            orphan_claims = await service.claim_skill_orphans(
                ArtifactSkillOrphanClaimRequest(
                    context=hands_skill_context,
                    owner=f"hands-{suffix}",
                    limit=10,
                )
            )
            claimed = next(
                item
                for item in orphan_claims.artifacts
                if item["artifact_id"] == orphan_upload.artifact_id
            )
            with pytest.raises(
                ArtifactAccessError,
                match="bound, expired, missing, or claimed",
            ):
                await service.claim_skill_publication(
                    ArtifactSkillPublicationClaimRequest(
                        context=hands_skill_context,
                        artifact_id=orphan_upload.artifact_id,
                        version=1,
                        command_id=f"late-publish-{suffix}",
                    )
                )
            resolved = await service.resolve_skill_orphan(
                ArtifactSkillOrphanResolveRequest(
                    context=hands_skill_context,
                    artifact_id=orphan_upload.artifact_id,
                    version=1,
                    claim_token=str(claimed["claim_token"]),
                    referenced=False,
                    policy_decision_id="integration-policy",
                )
            )
            assert resolved.status == "deleted"

            expired = await service.create_upload(
                ArtifactCreateUploadRequest(
                    context=context,
                    root_session_id=f"root-{suffix}",
                    session_id=f"session-{suffix}",
                    name="expired.bin",
                    media_type="application/octet-stream",
                    expected_size=1,
                    expected_checksum=hashlib.sha256(b"x").hexdigest(),
                )
            )
            await connection.execute(
                """UPDATE artifact.metadata SET upload_expires_at=$3
                   WHERE tenant_id=$1 AND artifact_id=$2""",
                tenant_id,
                expired.artifact_id,
                datetime.now(UTC) - timedelta(minutes=1),
            )
            reclaimed = await asyncio.gather(
                service.cleanup_expired(), service_b.cleanup_expired()
            )
            assert sum(reclaimed) == 1
        finally:
            if ready_key is not None:
                delete_url, _ = presigner.presign("DELETE", ready_key)
                async with httpx.AsyncClient(timeout=15.0) as client:
                    await client.delete(delete_url)
            if skill_ready_key is not None:
                delete_url, _ = presigner.presign("DELETE", skill_ready_key)
                async with httpx.AsyncClient(timeout=15.0) as client:
                    await client.delete(delete_url)
            await repository_a.close()
            await repository_b.close()
            await verifier.aclose()
            await multipart.aclose()
            await connection.execute(
                "DELETE FROM artifact.metadata WHERE tenant_id=$1", tenant_id
            )
            await connection.close()

    asyncio.run(scenario())


def test_artifact_claim_renewal_and_side_effect_fencing() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        await connection.execute(MIGRATION)
        await connection.execute(RECONCILIATION_MIGRATION)
        suffix = uuid4().hex
        tenant_id = f"tenant-artifact-lease-{suffix}"
        repository_a = PostgresArtifactRepository(DATABASE_URL)
        repository_b = PostgresArtifactRepository(DATABASE_URL)
        pending = PendingUpload(
            tenant_id=tenant_id,
            artifact_id=f"artifact-{suffix}",
            upload_id=f"upload-{suffix}",
            object_key=f"tenant/{suffix}/object",
            root_session_id=f"root-{suffix}",
            session_id=f"session-{suffix}",
            name="lease.bin",
            media_type="application/octet-stream",
            expected_size=1,
            expected_checksum=hashlib.sha256(b"x").hexdigest(),
            classification="internal",
            expires_at=datetime.now(UTC) + timedelta(minutes=5),
        )
        try:
            await repository_a.save_pending(pending)
            claim_a = await repository_a.claim_finalize(
                pending, claim_ttl=timedelta(milliseconds=300)
            )
            assert claim_a is not None
            await asyncio.sleep(0.1)
            assert await repository_a.renew_finalize(
                claim_a, claim_ttl=timedelta(milliseconds=300)
            )
            await asyncio.sleep(0.1)
            assert await repository_b.claim_finalize(
                pending, claim_ttl=timedelta(milliseconds=300)
            ) is None
            assert await repository_a.begin_object_side_effect(
                claim_a, operation="finalize"
            )
            await asyncio.sleep(0.31)
            assert await repository_b.claim_finalize(
                pending, claim_ttl=timedelta(milliseconds=300)
            ) is None
            row = await connection.fetchrow(
                """SELECT status,object_state,reconciliation_reason
                   FROM artifact.metadata WHERE tenant_id=$1 AND artifact_id=$2""",
                tenant_id,
                pending.artifact_id,
            )
            assert row is not None
            assert tuple(row) == (
                "reconciling",
                "unknown",
                "finalize_owner_lost_after_side_effect",
            )
            assert not await repository_a.mark_ready(claim_a, 1)
        finally:
            await repository_a.close()
            await repository_b.close()
            await connection.execute(
                "DELETE FROM artifact.metadata WHERE tenant_id=$1", tenant_id
            )
            await connection.close()

    asyncio.run(scenario())


def test_artifact_reconciliation_migration_roundtrip() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        try:
            await connection.execute(MIGRATION)
            await connection.execute(RECONCILIATION_MIGRATION)
            await connection.execute(RECONCILIATION_DOWN)
            assert not await connection.fetchval(
                """SELECT EXISTS(SELECT 1 FROM information_schema.columns
                WHERE table_schema='artifact' AND table_name='metadata'
                  AND column_name='object_state')"""
            )
            await connection.execute(RECONCILIATION_MIGRATION)
            assert await connection.fetchval(
                """SELECT count(*)=7 FROM information_schema.columns
                WHERE table_schema='artifact' AND table_name='metadata'
                  AND column_name IN (
                    'finalize_heartbeat_at','gc_heartbeat_at','object_state',
                    'reconciliation_reason','object_operation_ref',
                    'object_side_effect_started_at','reconciliation_updated_at')"""
            )
        finally:
            await connection.close()

    asyncio.run(scenario())
