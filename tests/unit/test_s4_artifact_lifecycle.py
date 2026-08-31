import asyncio
import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from auraclaw.artifact.internal_service import ArtifactInternalService, PendingUpload
from auraclaw.contracts.errors import ArtifactAccessError, NotFoundError
from auraclaw.contracts.internal import (
    ArtifactCreateUploadRequest,
    ArtifactDeleteRequest,
    ArtifactDownloadRequest,
    ArtifactFinalizeRequest,
    ArtifactSkillOrphanClaimRequest,
    ArtifactSkillPublicationBindRequest,
    ArtifactSkillPublicationClaimRequest,
    InternalRequestContext,
    ServiceIdentity,
)
from auraclaw.infrastructure.artifacts.seaweedfs import (
    SeaweedFSMultipartClient,
    SeaweedFSS3Presigner,
)
from auraclaw.infrastructure.clients.artifact import (
    RemoteArtifactWriter,
    RemoteSkillPackageUploadClient,
)
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
        self.skill_bound = False
        self.mark_ready_success = True
        self.finalize_renewals = 0
        self.gc_renewals = 0
        self.get_ready_error = False

    async def get_upload(self, tenant_id: str, artifact_id: str, upload_id: str):
        del tenant_id, artifact_id, upload_id
        return self.pending

    async def get_ready(self, tenant_id: str, artifact_id: str, version: int):
        del tenant_id, artifact_id, version
        if self.get_ready_error:
            raise RuntimeError("metadata unavailable")
        return self.pending if self.ready and not self.deleted else None

    async def claim_finalize(self, pending: PendingUpload, **_kwargs: object):
        return replace(pending, finalize_claim_token="claim")

    async def validate_finalize(self, pending: PendingUpload) -> bool:
        return pending.finalize_claim_token == "claim"

    async def renew_finalize(self, pending: PendingUpload, **_kwargs: object) -> bool:
        self.finalize_renewals += 1
        return await self.validate_finalize(pending)

    async def validate_gc(self, pending: PendingUpload) -> bool:
        return pending.gc_claim_token is not None

    async def renew_gc(self, pending: PendingUpload, **_kwargs: object) -> bool:
        self.gc_renewals += 1
        return await self.validate_gc(pending)

    async def mark_multipart_completed(self, pending: PendingUpload) -> bool:
        del pending
        self.multipart_completed = True
        return True

    async def mark_ready(self, pending: PendingUpload, version: int) -> bool:
        del pending, version
        self.ready = self.mark_ready_success
        return self.mark_ready_success

    async def expired_uploads(self, *, limit: int = 100, **_kwargs: object):
        del limit
        return [replace(self.pending, gc_claim_token="gc")]

    async def release_gc(self, pending: PendingUpload, error: str) -> bool:
        del pending, error
        self.gc_released = True
        return True

    async def claim_ready_delete(
        self, tenant_id: str, artifact_id: str, version: int, **_kwargs: object
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
    ) -> bool:
        del pending, error
        self.delete_claimed = False
        return True

    async def claim_skill_publication(
        self, tenant_id: str, artifact_id: str, version: int, command_id: str
    ):
        del tenant_id, artifact_id, version
        if self.delete_claimed:
            return None
        self.pending = replace(
            self.pending, skill_publish_claim_token=command_id
        )
        return self.pending

    async def bind_skill_publication(
        self, pending: PendingUpload, package_digest: str
    ) -> bool:
        del package_digest
        if pending.gc_claim_token is None and pending.skill_publish_claim_token is None:
            return False
        self.skill_bound = True
        self.delete_claimed = False
        self.pending = replace(
            self.pending,
            skill_bound_digest=f"sha256:{self.pending.expected_checksum}",
            skill_publish_claim_token=None,
            gc_claim_token=None,
        )
        return True

    async def claim_skill_orphans(
        self, *, owner: str, limit: int = 100, **_kwargs: object
    ):
        del owner, limit
        if self.skill_bound or self.pending.skill_publish_claim_token is not None:
            return []
        self.delete_claimed = True
        return [replace(self.pending, gc_claim_token="skill-orphan")]

    async def get_ready_delete_claim(
        self,
        tenant_id: str,
        artifact_id: str,
        version: int,
        claim_token: str,
    ):
        del tenant_id, artifact_id, version
        if not self.delete_claimed or claim_token != "skill-orphan":
            return None
        return replace(self.pending, gc_claim_token=claim_token)


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


class _SlowRecoveryVerifier(_RecoveryVerifier):
    async def inspect(self, pending: PendingUpload) -> str:
        del pending
        await asyncio.sleep(0.05)
        return "clean"


class _DeleteVerifier(_RecoveryVerifier):
    def __init__(self) -> None:
        self.deleted: list[str] = []

    async def delete(self, pending: PendingUpload) -> bool:
        self.deleted.append(pending.object_key)
        return True


class _AllowPolicy:
    async def validate_decision(self, **_parameters: object) -> bool:
        return True


class _UnavailablePolicy:
    async def validate_decision(self, **_parameters: object) -> bool:
        raise TimeoutError("policy timed out")


@pytest.mark.asyncio
@pytest.mark.parametrize("policy", [None, _UnavailablePolicy()])
@pytest.mark.parametrize("operation", ["download", "delete"])
async def test_artifact_access_fails_closed_before_storage_side_effect(
    policy: _UnavailablePolicy | None,
    operation: str,
) -> None:
    pending = PendingUpload(
        tenant_id="tenant-s4",
        artifact_id="artifact-s4",
        upload_id="upload-s4",
        object_key="tenant/artifact/object",
        root_session_id="root-s4",
        session_id="session-s4",
        name="result.txt",
        media_type="text/plain",
        expected_size=6,
        expected_checksum="checksum",
        classification="internal",
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
        retention_until=datetime.now(UTC) - timedelta(seconds=1),
    )
    repository = _RecoveryRepository(pending)
    repository.ready = True
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
        policy=policy,
    )
    context = InternalRequestContext(
        tenant_id="tenant-s4",
        service_identity=(
            ServiceIdentity.DELIVERY_WORKER
            if operation == "download"
            else ServiceIdentity.ACTION_HANDS
        ),
        request_id=f"{operation}-s4",
        correlation_id="run-s4",
        causation_id="decision-s4",
    )
    request = (
        ArtifactDownloadRequest(
            context=context,
            artifact_id=pending.artifact_id,
            version=1,
            actor_id="delivery-s4",
            policy_decision_id="decision-s4",
        )
        if operation == "download"
        else ArtifactDeleteRequest(
            context=context,
            artifact_id=pending.artifact_id,
            version=1,
            actor_id="admin-s4",
            reason_code="retention_elapsed",
            policy_decision_id="decision-s4",
        )
    )
    with pytest.raises(ArtifactAccessError, match="policy validation is unavailable"):
        if operation == "download":
            await service.download(request)  # type: ignore[arg-type]
        else:
            await service.delete(request)  # type: ignore[arg-type]
    assert repository.delete_claimed is False
    assert verifier.deleted == []


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
async def test_finalize_failure_never_publishes_ready_cache() -> None:
    pending = PendingUpload(
        tenant_id="tenant-s4",
        artifact_id="artifact-s4",
        upload_id="upload-s4",
        object_key="tenant/artifact/object",
        root_session_id="root-s4",
        session_id="session-s4",
        name="result.txt",
        media_type="text/plain",
        expected_size=6,
        expected_checksum="checksum",
        classification="internal",
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    repository = _RecoveryRepository(pending)
    repository.mark_ready_success = False
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
        policy=_AllowPolicy(),
    )
    context = InternalRequestContext(
        tenant_id="tenant-s4",
        service_identity=ServiceIdentity.ACTION_HANDS,
        request_id="request-s4",
        correlation_id="run-s4",
        causation_id="run-s4",
    )
    with pytest.raises(ArtifactAccessError, match="lease was lost"):
        await service.finalize(
            ArtifactFinalizeRequest(
                context=context,
                artifact_id=pending.artifact_id,
                version=1,
                upload_id=pending.upload_id,
                size=pending.expected_size,
                checksum=pending.expected_checksum,
            )
        )
    with pytest.raises(NotFoundError):
        await service.download(
            ArtifactDownloadRequest(
                context=context,
                artifact_id=pending.artifact_id,
                version=1,
                actor_id="runtime-s4",
                policy_decision_id="decision-s4",
            )
        )


@pytest.mark.asyncio
async def test_slow_finalize_renews_claim_until_ready_commit() -> None:
    pending = PendingUpload(
        tenant_id="tenant-s4",
        artifact_id="artifact-slow",
        upload_id="upload-slow",
        object_key="tenant/artifact/slow",
        root_session_id="root-s4",
        session_id="session-s4",
        name="slow.bin",
        media_type="application/octet-stream",
        expected_size=6,
        expected_checksum="checksum",
        classification="internal",
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
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
        object_verifier=_SlowRecoveryVerifier(),  # type: ignore[arg-type]
        claim_ttl=timedelta(milliseconds=30),
    )
    response = await service.finalize(
        ArtifactFinalizeRequest(
            context=InternalRequestContext(
                tenant_id="tenant-s4",
                service_identity=ServiceIdentity.ACTION_HANDS,
                request_id="slow-s4",
                correlation_id="slow-s4",
                causation_id="slow-s4",
            ),
            artifact_id=pending.artifact_id,
            version=1,
            upload_id=pending.upload_id,
            size=pending.expected_size,
            checksum=pending.expected_checksum,
        )
    )
    assert response.status == "ready"
    assert repository.finalize_renewals >= 2


@pytest.mark.asyncio
async def test_download_does_not_fallback_to_ready_cache_when_postgres_fails() -> None:
    pending = PendingUpload(
        tenant_id="tenant-s4",
        artifact_id="artifact-ready",
        upload_id="upload-ready",
        object_key="tenant/artifact/ready",
        root_session_id="root-s4",
        session_id="session-s4",
        name="ready.txt",
        media_type="text/plain",
        expected_size=1,
        expected_checksum="checksum",
        classification="internal",
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    repository = _RecoveryRepository(pending)
    repository.ready = True
    repository.get_ready_error = True
    service = ArtifactInternalService(
        SeaweedFSS3Presigner(
            "http://seaweed.test:8333",
            access_key="access",
            secret_key="secret",
            bucket="artifacts",
            region="us-east-1",
        ),
        repository=repository,  # type: ignore[arg-type]
        policy=_AllowPolicy(),
    )
    service._ready[(pending.tenant_id, pending.artifact_id, 1)] = pending
    with pytest.raises(RuntimeError, match="metadata unavailable"):
        await service.download(
            ArtifactDownloadRequest(
                context=InternalRequestContext(
                    tenant_id=pending.tenant_id,
                    service_identity=ServiceIdentity.ACTION_HANDS,
                    request_id="download-s4",
                    correlation_id="download-s4",
                    causation_id="download-s4",
                ),
                artifact_id=pending.artifact_id,
                version=1,
                actor_id="runtime-s4",
                policy_decision_id="decision-s4",
            )
        )


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
        policy=_AllowPolicy(),
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


@pytest.mark.asyncio
async def test_skill_publication_claim_fences_orphan_gc_and_repairs_reference() -> None:
    checksum = "a" * 64
    pending = PendingUpload(
        tenant_id="tenant-s4",
        artifact_id="artifact-skill",
        upload_id="upload-skill",
        object_key="tenant/artifact/skill",
        root_session_id="skill-upload:command-s4",
        session_id="skill-upload:command-s4",
        name="skill.pkg",
        media_type="application/vnd.auraclaw.skill-package+json",
        expected_size=6,
        expected_checksum=checksum,
        classification="internal",
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
        retention_until=datetime.now(UTC) - timedelta(seconds=1),
    )
    repository = _RecoveryRepository(pending)
    repository.ready = True
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
    context = InternalRequestContext(
        tenant_id="tenant-s4",
        service_identity=ServiceIdentity.ACTION_HANDS,
        request_id="reliability-s4",
        correlation_id="reliability-s4",
        causation_id="reliability-s4",
    )
    await service.claim_skill_publication(
        ArtifactSkillPublicationClaimRequest(
            context=context,
            artifact_id=pending.artifact_id,
            version=1,
            command_id="publish-s4",
        )
    )
    orphans = await service.claim_skill_orphans(
        ArtifactSkillOrphanClaimRequest(
            context=context, owner="hands-a", limit=10
        )
    )
    assert orphans.artifacts == ()
    await service.bind_skill_publication(
        ArtifactSkillPublicationBindRequest(
            context=context,
            artifact_id=pending.artifact_id,
            version=1,
            claim_token="publish-s4",
            package_digest=f"sha256:{checksum}",
        )
    )
    assert repository.skill_bound


@pytest.mark.asyncio
async def test_task_api_can_only_stage_governed_skill_packages() -> None:
    service = ArtifactInternalService(
        SeaweedFSS3Presigner(
            "http://seaweed.test:8333",
            access_key="access",
            secret_key="secret",
            bucket="artifacts",
            region="us-east-1",
        )
    )
    context = InternalRequestContext(
        tenant_id="tenant-s4",
        service_identity=ServiceIdentity.TASK_API,
        request_id="skill-upload-s4",
        correlation_id="skill-upload-s4",
        causation_id="skill-upload-s4",
    )
    valid = ArtifactCreateUploadRequest(
        context=context,
        root_session_id="skill-upload:skill-upload-s4",
        session_id="skill-upload:skill-upload-s4",
        name="package.skill.json",
        media_type="application/vnd.auraclaw.skill-package+json",
        expected_size=10,
        expected_checksum="a" * 64,
        retention_until=datetime.now(UTC) + timedelta(days=90),
    )
    assert (await service.create_upload(valid)).artifact_id.startswith("art_")
    with pytest.raises(ArtifactAccessError, match="only stage"):
        await service.create_upload(
            valid.model_copy(update={"media_type": "application/octet-stream"})
        )


@pytest.mark.asyncio
async def test_task_api_staged_upload_client_uses_restricted_artifact_contract() -> None:
    service = ArtifactInternalService(
        SeaweedFSS3Presigner(
            "http://seaweed.test:8333",
            access_key="access",
            secret_key="secret",
            bucket="artifacts",
            region="us-east-1",
        )
    )
    app = create_contract_app(
        "artifact-service",
        artifact_routes(service),
        workload_identities={"task-token": ServiceIdentity.TASK_API},
    )
    client = RemoteSkillPackageUploadClient(
        "http://artifact.test",
        bearer_token="task-token",
        transport=httpx.ASGITransport(app=app),
        object_transport=httpx.MockTransport(lambda _: httpx.Response(200)),
    )
    try:
        content = b"0123456789"
        finalized = await client.stage(
            tenant_id="tenant-s4",
            name="package.skill.json",
            content=content,
            checksum=hashlib.sha256(content).hexdigest(),
            correlation_id="skill-upload-s4",
            command_id="skill-upload-s4",
        )
        assert finalized.status == "ready"
        assert finalized.artifact_ref["media_type"] == (
            "application/vnd.auraclaw.skill-package+json"
        )
    finally:
        await client.aclose()
