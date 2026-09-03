from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.artifact.internal_service import ArtifactInternalService, PendingUpload
from auraclaw.config import get_settings
from auraclaw.contracts.errors import ArtifactAccessError
from auraclaw.contracts.internal import (
    ArtifactDeleteRequest,
    InternalRequestContext,
    ServiceIdentity,
)
from auraclaw.infrastructure.artifacts.s3 import S3CompatiblePresigner
from auraclaw.infrastructure.persistence.postgres_artifact_repository import (
    PostgresArtifactRepository,
)
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url

SETTINGS = get_settings()
DATABASE_URL = asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
MIGRATION = (
    Path(__file__).resolve().parents[2] / "migrations/0062_skill_package_physical_removal.sql"
).read_text()
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="Explicit PostgreSQL required")


@pytest.mark.parametrize("already_deleted,shared", [(False, False), (True, False), (False, True)])
def test_physical_skill_removal_recovers_partial_delete_and_removes_all_metadata(
    already_deleted: bool,
    shared: bool,
) -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        await connection.execute(MIGRATION)
        tenant = f"physical-removal-{uuid4().hex}"
        pending = PendingUpload(
            tenant_id=tenant,
            artifact_id=f"artifact-{uuid4().hex}",
            upload_id="upload",
            object_key=f"{tenant}/package",
            root_session_id="root",
            session_id="session",
            name="old-skill",
            media_type="application/vnd.auraclaw.skill-package+json",
            expected_size=1,
            expected_checksum="a" * 64,
            classification="internal",
            expires_at=datetime.now(UTC) + timedelta(minutes=5),
        )
        a, b = PostgresArtifactRepository(DATABASE_URL), PostgresArtifactRepository(DATABASE_URL)

        class Policy:
            async def validate_decision(self, **kwargs):
                return True

        class Verifier:
            calls = 0

            async def purge(self, candidate):
                assert candidate.object_key == pending.object_key
                self.calls += 1
                return self.calls > 1

        verifier = Verifier()

        def service(repository):
            return ArtifactInternalService(
                S3CompatiblePresigner(
                    "https://fixture.invalid",
                    access_key="fixture",
                    secret_key="fixture",
                    bucket="bucket",
                    region="test",
                ),
                repository=repository,
                object_verifier=verifier,
                policy=Policy(),
                claim_ttl=timedelta(milliseconds=100),
            )

        try:
            await a.save_pending(pending)
            # A finalized, published package fixture, owned exclusively by this synthetic tenant.
            await connection.execute(
                """UPDATE artifact.metadata SET status='ready',
                skill_bound_at=now(),skill_bound_digest=$3,content_hash=$4,scan_status='clean'
                WHERE tenant_id=$1 AND artifact_id=$2""",
                tenant,
                pending.artifact_id,
                "sha256:" + "a" * 64,
                "a" * 64,
            )
            if shared:
                other = replace(
                    pending,
                    artifact_id="shared-artifact",
                    upload_id="shared-upload",
                    media_type="application/octet-stream",
                )
                await a.save_pending(other)
                assert await a.mark_ready(other, 1)
            if already_deleted:
                await connection.execute(
                    "UPDATE artifact.metadata SET status='deleted',deleted_at=now() "
                    "WHERE tenant_id=$1",
                    tenant,
                )
            request = ArtifactDeleteRequest(
                context=InternalRequestContext(
                    tenant_id=tenant,
                    service_identity=ServiceIdentity.ACTION_HANDS,
                    request_id="delete",
                    correlation_id="upgrade",
                    causation_id="upgrade",
                ),
                artifact_id=pending.artifact_id,
                version=1,
                actor_id="admin",
                reason_code="skill_version_replaced",
                policy_decision_id="fixture",
                purpose="skill_package_purge",
                remove_history=True,
            )
            if not shared:
                with pytest.raises(ArtifactAccessError, match="incomplete"):
                    await service(a).delete(request)
                assert await a.get_ready(tenant, pending.artifact_id, 1) is None
                await asyncio.sleep(0.15)
                assert not await b.claim_reconciling(owner="generic-gc")
                assert (
                    await b.claim_ready_delete(
                        tenant, pending.artifact_id, 1, ignore_retention=True
                    )
                    is None
                )
            assert (await service(b).delete(request)).status == "deleted"
            assert (await service(a).delete(request)).status == "deleted"
            assert verifier.calls == (0 if shared else 2)
            assert not await connection.fetchval(
                "SELECT EXISTS(SELECT 1 FROM artifact.metadata "
                "WHERE tenant_id=$1 AND artifact_id=$2)",
                tenant,
                pending.artifact_id,
            )
            receipt = await connection.fetchrow(
                "SELECT * FROM artifact.skill_removal_receipt WHERE tenant_id=$1", tenant
            )
            assert receipt is not None and set(receipt.keys()) == {"tenant_id", "removal_digest"}
            assert pending.artifact_id not in str(dict(receipt))
        finally:
            await a.close()
            await b.close()
            await connection.execute("DELETE FROM artifact.metadata WHERE tenant_id=$1", tenant)
            await connection.execute(
                "DELETE FROM artifact.skill_removal_receipt WHERE tenant_id=$1", tenant
            )
            await connection.close()

    asyncio.run(scenario())
