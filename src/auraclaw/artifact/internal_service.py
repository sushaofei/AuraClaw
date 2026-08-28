from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from auraclaw.artifact.ports import (
    ObjectMultipartClient,
    ObjectPresigner,
    ObjectVerifier,
)
from auraclaw.contracts.errors import ArtifactAccessError, NotFoundError
from auraclaw.contracts.internal import (
    ArtifactCreateUploadRequest,
    ArtifactDeleteRequest,
    ArtifactDeleteResponse,
    ArtifactDownloadRequest,
    ArtifactDownloadResponse,
    ArtifactFinalizeRequest,
    ArtifactFinalizeResponse,
    ArtifactUploadResponse,
    ServiceIdentity,
)


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
    retention_until: datetime | None = None
    legal_hold: bool = False
    upload_mode: str = "single"
    multipart_upload_id: str | None = None
    multipart_part_size: int | None = None
    multipart_completed: bool = False
    gc_claim_token: str | None = None
    finalize_claim_token: str | None = None


class ArtifactMetadataRepository(Protocol):
    async def save_pending(
        self, pending: PendingUpload
    ) -> None: ...

    async def get_upload(
        self, tenant_id: str, artifact_id: str, upload_id: str
    ) -> PendingUpload | None: ...

    async def cleanup_expired(self) -> int: ...

    async def expired_uploads(self, *, limit: int = 100) -> list[PendingUpload]: ...

    async def mark_deleted(self, pending: PendingUpload) -> None: ...

    async def release_gc(self, pending: PendingUpload, error: str) -> None: ...

    async def mark_ready(self, pending: PendingUpload, version: int) -> bool: ...

    async def mark_multipart_completed(self, pending: PendingUpload) -> None: ...

    async def mark_quarantined(self, pending: PendingUpload, reason: str) -> None: ...

    async def claim_finalize(self, pending: PendingUpload) -> PendingUpload | None: ...

    async def get_ready(
        self, tenant_id: str, artifact_id: str, version: int
    ) -> PendingUpload | None: ...

    async def claim_ready_delete(
        self, tenant_id: str, artifact_id: str, version: int
    ) -> PendingUpload | None: ...

    async def is_deleted(
        self, tenant_id: str, artifact_id: str, version: int
    ) -> bool: ...

    async def mark_ready_deleted(self, pending: PendingUpload) -> bool: ...

    async def release_ready_delete(
        self, pending: PendingUpload, error: str
    ) -> None: ...


class ArtifactPolicyValidator(Protocol):
    async def validate_decision(
        self,
        *,
        tenant_id: str,
        decision_id: str,
        action: str,
        resource: str,
    ) -> bool: ...

class ArtifactInternalService:
    def __init__(
        self,
        presigner: ObjectPresigner,
        *,
        repository: ArtifactMetadataRepository | None = None,
        object_verifier: ObjectVerifier | None = None,
        policy: ArtifactPolicyValidator | None = None,
        multipart: ObjectMultipartClient | None = None,
        multipart_threshold: int = 16 * 1024 * 1024,
        multipart_part_size: int = 8 * 1024 * 1024,
    ) -> None:
        self._presigner = presigner
        self._repository = repository
        self._object_verifier = object_verifier
        self._policy = policy
        self._multipart = multipart
        self._multipart_threshold = multipart_threshold
        self._multipart_part_size = multipart_part_size
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
        upload_mode = "single"
        multipart_upload_id: str | None = None
        part_urls: tuple[str, ...] = ()
        if self._multipart is not None and request.expected_size >= self._multipart_threshold:
            upload_mode = "multipart"
            multipart_upload_id, part_urls = await self._multipart.create(
                object_key,
                expected_size=request.expected_size,
                part_size=self._multipart_part_size,
            )
            upload_url = part_urls[0]
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
            retention_until=request.retention_until,
            upload_mode=upload_mode,
            multipart_upload_id=multipart_upload_id,
            multipart_part_size=(
                self._multipart_part_size if upload_mode == "multipart" else None
            ),
        )
        if self._repository is not None:
            await self._repository.save_pending(self._uploads[upload_id])
        return ArtifactUploadResponse(
            artifact_id=artifact_id,
            version=1,
            upload_id=upload_id,
            upload_url=upload_url,
            expires_at=expires_at,
            upload_mode=upload_mode,
            part_size=(self._multipart_part_size if upload_mode == "multipart" else None),
            part_urls=part_urls,
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
            if pending is None:
                ready = await self._repository.get_ready(
                    request.context.tenant_id, request.artifact_id, request.version
                )
                if ready is not None:
                    return ArtifactFinalizeResponse(
                        artifact_ref={
                            "artifact_id": ready.artifact_id,
                            "version": request.version,
                            "content_hash": ready.expected_checksum,
                            "media_type": ready.media_type,
                            "size": ready.expected_size,
                        },
                        status="ready",
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
        if self._repository is not None:
            claimed = await self._repository.claim_finalize(pending)
            if claimed is None:
                ready = await self._repository.get_ready(
                    request.context.tenant_id, request.artifact_id, request.version
                )
                if ready is not None:
                    return ArtifactFinalizeResponse(
                        artifact_ref={
                            "artifact_id": ready.artifact_id,
                            "version": request.version,
                            "content_hash": ready.expected_checksum,
                            "media_type": ready.media_type,
                            "size": ready.expected_size,
                        },
                        status="ready",
                    )
                raise ArtifactAccessError("artifact finalization is already in progress")
            pending = claimed
        if pending.upload_mode == "multipart" and not pending.multipart_completed:
            if self._multipart is None or pending.multipart_upload_id is None:
                raise ArtifactAccessError("artifact multipart state is unavailable")
            expected_parts = max(
                1,
                (pending.expected_size + int(pending.multipart_part_size or 1) - 1)
                // int(pending.multipart_part_size or 1),
            )
            if len(request.parts) != expected_parts:
                raise ArtifactAccessError("artifact multipart completion is incomplete")
            try:
                await self._multipart.complete(
                    pending.object_key,
                    pending.multipart_upload_id,
                    request.parts,
                )
            except ArtifactAccessError:
                if self._object_verifier is None or not await self._object_verifier.verify(
                    pending
                ):
                    raise
            if self._repository is not None:
                await self._repository.mark_multipart_completed(pending)
        if self._object_verifier is not None:
            scan = await self._object_verifier.inspect(pending)
            if scan in {"size_mismatch", "checksum_mismatch"}:
                if self._repository is not None:
                    await self._repository.mark_quarantined(pending, scan)
                raise ArtifactAccessError(f"artifact object scan failed: {scan}")
            if scan != "clean":
                raise ArtifactAccessError(f"artifact object is not ready: {scan}")
        self._uploads.pop(request.upload_id, None)
        self._ready[(pending.tenant_id, pending.artifact_id, request.version)] = pending
        if self._repository is not None:
            if not await self._repository.mark_ready(pending, request.version):
                raise ArtifactAccessError("artifact finalization lease was lost")
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

    async def cleanup_expired(self, *, limit: int = 100) -> int:
        if self._repository is None:
            return 0
        deleted = 0
        for pending in await self._repository.expired_uploads(limit=limit):
            removed = False
            if (
                pending.upload_mode == "multipart"
                and not pending.multipart_completed
                and pending.multipart_upload_id is not None
                and self._multipart is not None
            ):
                removed = await self._multipart.abort(
                    pending.object_key, pending.multipart_upload_id
                )
            elif self._object_verifier is not None:
                removed = await self._object_verifier.delete(pending)
            else:
                removed = True
            if removed:
                await self._repository.mark_deleted(pending)
                deleted += 1
            else:
                await self._repository.release_gc(pending, "object deletion failed")
        return deleted

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

    async def delete(self, request: ArtifactDeleteRequest) -> ArtifactDeleteResponse:
        if request.context.service_identity is not ServiceIdentity.ACTION_HANDS:
            raise ArtifactAccessError("workload may not delete Artifacts")
        if self._repository is None or self._object_verifier is None:
            raise ArtifactAccessError("artifact deletion is unavailable")
        if self._policy is not None and not await self._policy.validate_decision(
            tenant_id=request.context.tenant_id,
            decision_id=request.policy_decision_id,
            action="artifact.delete",
            resource=request.artifact_id,
        ):
            raise ArtifactAccessError("artifact policy decision is invalid or expired")
        pending = await self._repository.claim_ready_delete(
            request.context.tenant_id,
            request.artifact_id,
            request.version,
        )
        if pending is None:
            if await self._repository.is_deleted(
                request.context.tenant_id,
                request.artifact_id,
                request.version,
            ):
                self._ready.pop(
                    (
                        request.context.tenant_id,
                        request.artifact_id,
                        request.version,
                    ),
                    None,
                )
                return ArtifactDeleteResponse(
                    artifact_id=request.artifact_id,
                    version=request.version,
                )
            raise ArtifactAccessError(
                "artifact is retained, held, missing, or already deleting"
            )
        if not await self._object_verifier.delete(pending):
            await self._repository.release_ready_delete(
                pending, "object deletion failed"
            )
            raise ArtifactAccessError("artifact object deletion failed")
        if not await self._repository.mark_ready_deleted(pending):
            raise ArtifactAccessError("artifact deletion lease was lost")
        self._ready.pop(
            (request.context.tenant_id, request.artifact_id, request.version),
            None,
        )
        return ArtifactDeleteResponse(
            artifact_id=request.artifact_id,
            version=request.version,
        )
