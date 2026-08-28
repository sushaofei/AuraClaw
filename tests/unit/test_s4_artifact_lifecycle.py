from dataclasses import replace
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from auraclaw.artifact.internal_service import ArtifactInternalService, PendingUpload
from auraclaw.contracts.errors import ArtifactAccessError
from auraclaw.contracts.internal import (
    ArtifactDeleteRequest,
    ArtifactFinalizeRequest,
    InternalRequestContext,
    ServiceIdentity,
)
from auraclaw.infrastructure.artifacts.seaweedfs import (
    SeaweedFSMultipartClient,
    SeaweedFSS3Presigner,
)
from auraclaw.infrastructure.clients.artifact import RemoteArtifactWriter
from auraclaw.internal.http import create_contract_app
from auraclaw.internal.routes import artifact_routes


class _RecoveryRepository:
    def __init__(self, pending: PendingUpload) -> None:
        self.pending = pending
        self.ready = False
        self.multipart_completed = False
        self.gc_released = False
        self.deleted = False
        self.delete_claimed = False

    async def get_upload(self, tenant_id: str, artifact_id: str, upload_id: str):
        del tenant_id, artifact_id, upload_id
        return self.pending

    async def get_ready(self, tenant_id: str, artifact_id: str, version: int):
        del tenant_id, artifact_id, version
        return self.pending if self.ready else None

    async def claim_finalize(self, pending: PendingUpload):
        return replace(pending, finalize_claim_token="claim")

    async def mark_multipart_completed(self, pending: PendingUpload) -> None:
        del pending
        self.multipart_completed = True

    async def mark_ready(self, pending: PendingUpload, version: int) -> bool:
        del pending, version
        self.ready = True
        return True

    async def expired_uploads(self, *, limit: int = 100):
        del limit
        return [replace(self.pending, gc_claim_token="gc")]

    async def release_gc(self, pending: PendingUpload, error: str) -> None:
        del pending, error
        self.gc_released = True

    async def claim_ready_delete(
        self, tenant_id: str, artifact_id: str, version: int
    ):
        del tenant_id, artifact_id, version
        if (
            self.deleted
            or self.pending.legal_hold
            or self.pending.retention_until is None
            or self.pending.retention_until > datetime.now(UTC)
        ):
            return None
        self.delete_claimed = True
        return replace(self.pending, gc_claim_token="delete")

    async def is_deleted(
        self, tenant_id: str, artifact_id: str, version: int
    ) -> bool:
        del tenant_id, artifact_id, version
        return self.deleted

    async def mark_ready_deleted(self, pending: PendingUpload) -> bool:
        del pending
        self.deleted = True
        return True

    async def release_ready_delete(
        self, pending: PendingUpload, error: str
    ) -> None:
        del pending, error
        self.delete_claimed = False


class _LostCompletionMultipart:
    async def complete(self, object_key: str, upload_id: str, parts) -> None:
        del object_key, upload_id, parts
        raise ArtifactAccessError("response was lost")

    async def abort(self, object_key: str, upload_id: str) -> bool:
        del object_key, upload_id
        return False


class _RecoveryVerifier:
    async def verify(self, pending: PendingUpload) -> bool:
        del pending
        return True

    async def inspect(self, pending: PendingUpload) -> str:
        del pending
        return "clean"

    async def delete(self, pending: PendingUpload) -> bool:
        del pending
        return False


class _DeleteVerifier(_RecoveryVerifier):
    def __init__(self) -> None:
        self.deleted: list[str] = []

    async def delete(self, pending: PendingUpload) -> bool:
        self.deleted.append(pending.object_key)
        return True


@pytest.mark.asyncio
async def test_remote_artifact_writer_completes_multipart_upload() -> None:
    completed_xml: list[bytes] = []

    async def seaweed_control(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and "uploads" in request.url.params:
            return httpx.Response(
                200,
                content=b"<InitiateMultipartUploadResult><UploadId>remote-1</UploadId>"
                b"</InitiateMultipartUploadResult>",
            )
        if request.method == "POST" and request.url.params.get("uploadId") == "remote-1":
            completed_xml.append(await request.aread())
            return httpx.Response(200, content=b"<CompleteMultipartUploadResult/>")
        raise AssertionError(f"unexpected SeaweedFS request: {request.method} {request.url}")

    async def upload_part(request: httpx.Request) -> httpx.Response:
        number = request.url.params["partNumber"]
        assert request.url.params["uploadId"] == "remote-1"
        return httpx.Response(200, headers={"ETag": f'"etag-{number}"'})

    presigner = SeaweedFSS3Presigner(
        "http://seaweed.test:8333",
        access_key="access",
        secret_key="secret",
        bucket="artifacts",
        region="us-east-1",
    )
    multipart_http = httpx.AsyncClient(transport=httpx.MockTransport(seaweed_control))
    multipart = SeaweedFSMultipartClient(presigner, client=multipart_http)
    service = ArtifactInternalService(
        presigner,
        multipart=multipart,
        multipart_threshold=8,
        multipart_part_size=5,
    )
    app = create_contract_app(
        "artifact-service",
        artifact_routes(service),
        workload_identities={"hands-token": ServiceIdentity.ACTION_HANDS},
    )
    writer = RemoteArtifactWriter(
        "http://artifact.test",
        bearer_token="hands-token",
        transport=httpx.ASGITransport(app=app),
        object_transport=httpx.MockTransport(upload_part),
    )
    try:
        result = await writer.put(
            tenant_id="tenant-s4",
            root_session_id="root-s4",
            session_id="session-s4",
            content=b"123456789",
            artifact_type="tool-output",
            media_type="application/octet-stream",
            name="large.bin",
            producer="hands",
        )
        assert result.size == 9
        assert len(completed_xml) == 1
        assert b"etag-1" in completed_xml[0]
        assert b"etag-2" in completed_xml[0]
    finally:
        await writer.aclose()
        await multipart.aclose()


@pytest.mark.asyncio
async def test_artifact_recovers_lost_complete_response_and_releases_failed_gc() -> None:
    pending = PendingUpload(
        tenant_id="tenant-s4",
        artifact_id="artifact-s4",
        upload_id="upload-s4",
        object_key="tenant/artifact/object",
        root_session_id="root-s4",
        session_id="session-s4",
        name="large.bin",
        media_type="application/octet-stream",
        expected_size=6,
        expected_checksum="checksum",
        classification="internal",
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
        upload_mode="multipart",
        multipart_upload_id="remote-s4",
        multipart_part_size=5,
    )
    repository = _RecoveryRepository(pending)
    service = ArtifactInternalService(
        SeaweedFSS3Presigner(
            "http://seaweed.test:8333",
            access_key="access",
            secret_key="secret",
            bucket="artifacts",
            region="us-east-1",
        ),
        repository=repository,  # type: ignore[arg-type]
        object_verifier=_RecoveryVerifier(),  # type: ignore[arg-type]
        multipart=_LostCompletionMultipart(),  # type: ignore[arg-type]
    )
    response = await service.finalize(
        ArtifactFinalizeRequest(
            context=InternalRequestContext(
                tenant_id="tenant-s4",
                service_identity=ServiceIdentity.ACTION_HANDS,
                request_id="request-s4",
                correlation_id="run-s4",
                causation_id="run-s4",
            ),
            artifact_id=pending.artifact_id,
            version=1,
            upload_id=pending.upload_id,
            size=pending.expected_size,
            checksum=pending.expected_checksum,
            parts=(
                {"part_number": 1, "etag": "etag-1"},
                {"part_number": 2, "etag": "etag-2"},
            ),
        )
    )
    assert response.status == "ready"
    assert repository.multipart_completed
    assert await service.cleanup_expired() == 0
    assert repository.gc_released


@pytest.mark.asyncio
async def test_ready_artifact_delete_enforces_retention_and_is_idempotent() -> None:
    pending = PendingUpload(
        tenant_id="tenant-s4",
        artifact_id="artifact-s4",
        upload_id="upload-s4",
        object_key="tenant/artifact/object",
        root_session_id="root-s4",
        session_id="session-s4",
        name="skill.pkg",
        media_type="application/vnd.auraclaw.skill-package+json",
        expected_size=6,
        expected_checksum="checksum",
        classification="internal",
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
        retention_until=datetime.now(UTC) + timedelta(days=1),
    )
    repository = _RecoveryRepository(pending)
    verifier = _DeleteVerifier()
    service = ArtifactInternalService(
        SeaweedFSS3Presigner(
            "http://seaweed.test:8333",
            access_key="access",
            secret_key="secret",
            bucket="artifacts",
            region="us-east-1",
        ),
        repository=repository,  # type: ignore[arg-type]
        object_verifier=verifier,  # type: ignore[arg-type]
    )
    request = ArtifactDeleteRequest(
        context=InternalRequestContext(
            tenant_id="tenant-s4",
            service_identity=ServiceIdentity.ACTION_HANDS,
            request_id="delete-s4",
            correlation_id="delete-s4",
            causation_id="delete-s4",
        ),
        artifact_id="artifact-s4",
        version=1,
        actor_id="admin-s4",
        reason_code="skill_package_purge",
        policy_decision_id="decision-s4",
    )
    with pytest.raises(ArtifactAccessError, match="retained"):
        await service.delete(request)
    repository.pending = replace(
        pending, retention_until=datetime.now(UTC) - timedelta(seconds=1)
    )
    first = await service.delete(request)
    second = await service.delete(request)
    assert first.status == second.status == "deleted"
    assert verifier.deleted == ["tenant/artifact/object"]
