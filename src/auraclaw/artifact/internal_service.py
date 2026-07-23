from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

import httpx

from auraclaw.contracts.errors import ArtifactAccessError, NotFoundError
from auraclaw.contracts.internal import (
    ArtifactCreateUploadRequest,
    ArtifactDownloadRequest,
    ArtifactDownloadResponse,
    ArtifactFinalizeRequest,
    ArtifactFinalizeResponse,
    ArtifactUploadResponse,
    ServiceIdentity,
)
from auraclaw.infrastructure.artifacts.seaweedfs import SeaweedFSS3Presigner


@dataclass(frozen=True)
class PendingUpload:
    tenant_id: str
    artifact_id: str
    upload_id: str
    object_key: str
    root_session_id: str
    session_id: str
    name: str
    media_type: str
    expected_size: int
    expected_checksum: str
    classification: str
    expires_at: datetime


class ArtifactMetadataRepository(Protocol):
    async def save_pending(
        self, pending: PendingUpload
    ) -> None: ...

    async def get_upload(
        self, tenant_id: str, artifact_id: str, upload_id: str
    ) -> PendingUpload | None: ...

    async def cleanup_expired(self) -> int: ...

    async def mark_ready(self, pending: PendingUpload, version: int) -> None: ...

    async def get_ready(
        self, tenant_id: str, artifact_id: str, version: int
    ) -> PendingUpload | None: ...


class ArtifactPolicyValidator(Protocol):
    async def validate_decision(
        self,
        *,
        tenant_id: str,
        decision_id: str,
        action: str,
        resource: str,
    ) -> bool: ...

class SeaweedFSObjectVerifier:
    def __init__(
        self,
        presigner: SeaweedFSS3Presigner,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._presigner = presigner
        self._client = client or httpx.AsyncClient(timeout=5.0)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def verify(self, pending: PendingUpload) -> bool:
        url, _ = self._presigner.presign("HEAD", pending.object_key)
        try:
            response = await self._client.head(url)
        except httpx.HTTPError:
            return False
        return (
            response.status_code == 200
            and int(response.headers.get("Content-Length", "-1"))
            == pending.expected_size
        )

    async def readiness(self) -> tuple[bool, str]:
        url, _ = self._presigner.presign("HEAD", "health/readiness-probe")
        try:
            response = await self._client.head(url)
        except httpx.HTTPError as exc:
            return False, type(exc).__name__
        reachable = response.status_code in {200, 404}
        return reachable, f"HTTP {response.status_code}"


class ArtifactInternalService:
    def __init__(
        self,
        presigner: SeaweedFSS3Presigner,
        *,
        repository: ArtifactMetadataRepository | None = None,
        object_verifier: SeaweedFSObjectVerifier | None = None,
        policy: ArtifactPolicyValidator | None = None,
    ) -> None:
        self._presigner = presigner
        self._repository = repository
        self._object_verifier = object_verifier
        self._policy = policy
        self._uploads: dict[str, PendingUpload] = {}
        self._ready: dict[tuple[str, str, int], PendingUpload] = {}

    async def create_upload(
        self, request: ArtifactCreateUploadRequest
    ) -> ArtifactUploadResponse:
        if request.context.service_identity not in {
            ServiceIdentity.ACTION_HANDS,
            ServiceIdentity.DELIVERY_WORKER,
        }:
            raise ArtifactAccessError("workload may not create Artifact uploads")
        artifact_id = f"art_{uuid.uuid4().hex}"
        upload_id = f"upl_{uuid.uuid4().hex}"
        object_key = (
            f"tenants/{request.context.tenant_id}/roots/{request.root_session_id}/"
            f"artifacts/{artifact_id}/v1/{uuid.uuid4().hex}"
        )
        upload_url, expires_at = self._presigner.presign("PUT", object_key)
        self._uploads[upload_id] = PendingUpload(
            tenant_id=request.context.tenant_id,
            artifact_id=artifact_id,
            upload_id=upload_id,
            object_key=object_key,
            root_session_id=request.root_session_id,
            session_id=request.session_id,
            name=request.name,
            media_type=request.media_type,
            expected_size=request.expected_size,
            expected_checksum=request.expected_checksum,
            classification=request.classification,
            expires_at=expires_at,
        )
        if self._repository is not None:
            await self._repository.save_pending(self._uploads[upload_id])
        return ArtifactUploadResponse(
            artifact_id=artifact_id,
            version=1,
            upload_id=upload_id,
            upload_url=upload_url,
            expires_at=expires_at,
        )

    async def finalize(
        self, request: ArtifactFinalizeRequest
    ) -> ArtifactFinalizeResponse:
        if request.context.service_identity not in {
            ServiceIdentity.ACTION_HANDS,
            ServiceIdentity.DELIVERY_WORKER,
        }:
            raise ArtifactAccessError("workload may not finalize Artifact uploads")
        pending = self._uploads.get(request.upload_id)
        if pending is None and self._repository is not None:
            pending = await self._repository.get_upload(
                request.context.tenant_id, request.artifact_id, request.upload_id
            )
        if pending is None or pending.artifact_id != request.artifact_id:
            raise NotFoundError("artifact upload was not found")
        if pending.tenant_id != request.context.tenant_id:
            raise ArtifactAccessError("artifact upload tenant mismatch")
        if datetime.now(UTC) >= pending.expires_at:
            raise ArtifactAccessError("artifact upload expired")
        if (
            pending.expected_size != request.size
            or pending.expected_checksum != request.checksum
        ):
            raise ArtifactAccessError("artifact upload integrity mismatch")
        if self._object_verifier is not None and not await self._object_verifier.verify(
            pending
        ):
            raise ArtifactAccessError("artifact object is missing or has wrong size")
        self._uploads.pop(request.upload_id, None)
        self._ready[(pending.tenant_id, pending.artifact_id, request.version)] = pending
        if self._repository is not None:
            await self._repository.mark_ready(pending, request.version)
        return ArtifactFinalizeResponse(
            artifact_ref={
                "artifact_id": pending.artifact_id,
                "version": request.version,
                "content_hash": pending.expected_checksum,
                "media_type": pending.media_type,
                "size": pending.expected_size,
            },
            status="ready",
        )

    async def download(self, request: ArtifactDownloadRequest) -> ArtifactDownloadResponse:
        if request.context.service_identity not in {
            ServiceIdentity.TASK_API,
            ServiceIdentity.DELIVERY_WORKER,
            ServiceIdentity.ACTION_HANDS,
        }:
            raise ArtifactAccessError("workload may not download Artifacts")
        record = self._ready.get(
            (request.context.tenant_id, request.artifact_id, request.version)
        )
        if record is None and self._repository is not None:
            record = await self._repository.get_ready(
                request.context.tenant_id, request.artifact_id, request.version
            )
        if record is None:
            raise NotFoundError("artifact was not found")
        if not request.policy_decision_id:
            raise ArtifactAccessError("artifact download requires policy decision")
        if self._policy is not None and not await self._policy.validate_decision(
            tenant_id=request.context.tenant_id,
            decision_id=request.policy_decision_id,
            action="artifact.download",
            resource=request.artifact_id,
        ):
            raise ArtifactAccessError("artifact policy decision is invalid or expired")
        url, expires_at = self._presigner.presign(
            "GET", record.object_key, ttl=timedelta(minutes=5)
        )
        return ArtifactDownloadResponse(download_url=url, expires_at=expires_at)
