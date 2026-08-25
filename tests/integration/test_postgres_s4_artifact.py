import asyncio
import hashlib
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import asyncpg
import httpx
import pytest

from auraclaw.artifact.internal_service import ArtifactInternalService
from auraclaw.composition.object_storage import build_object_storage
from auraclaw.config import get_settings
from auraclaw.contracts.internal import (
    ArtifactCreateUploadRequest,
    ArtifactFinalizeRequest,
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
pytestmark = pytest.mark.skipif(
    DATABASE_URL is None or not SETTINGS.object_storage_enabled,
    reason="PostgreSQL and object storage are required",
)


def test_artifact_multipart_scan_restart_and_gc() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        assert SETTINGS.object_storage_enabled
        storage = build_object_storage(SETTINGS)
        assert storage.verifier is not None
        assert storage.multipart is not None
        connection = await asyncpg.connect(DATABASE_URL)
        await connection.execute(MIGRATION)
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
            multipart=multipart,
            multipart_threshold=5 * 1024 * 1024,
            multipart_part_size=5 * 1024 * 1024,
        )
        service_b = ArtifactInternalService(
            presigner,
            repository=repository_b,
            object_verifier=verifier,
            multipart=multipart,
            multipart_threshold=5 * 1024 * 1024,
            multipart_part_size=5 * 1024 * 1024,
        )
        content = b"m" * (5 * 1024 * 1024) + b"checked-tail"
        checksum = hashlib.sha256(content).hexdigest()
        ready_key: str | None = None
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
                datetime.now(UTC),
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
            await repository_a.close()
            await repository_b.close()
            await verifier.aclose()
            await multipart.aclose()
            await connection.execute(
                "DELETE FROM artifact.metadata WHERE tenant_id=$1", tenant_id
            )
            await connection.close()

    asyncio.run(scenario())
