from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
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
    ArtifactSkillOrphanClaimRequest,
    ArtifactSkillOrphanClaimResponse,
    ArtifactSkillOrphanResolveRequest,
    ArtifactSkillOrphanResolveResponse,
    ArtifactSkillPublicationBindRequest,
    ArtifactSkillPublicationBindResponse,
    ArtifactSkillPublicationClaimRequest,
    ArtifactSkillPublicationClaimResponse,
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
    version: int = 1
    retention_until: datetime | None = None
    legal_hold: bool = False
    upload_mode: str = "single"
    multipart_upload_id: str | None = None
    multipart_part_size: int | None = None
    multipart_completed: bool = False
    gc_claim_token: str | None = None
    finalize_claim_token: str | None = None
    skill_bound_digest: str | None = None
    skill_publish_claim_token: str | None = None
    lifecycle_status: str = "pending"
    scan_status: str = "pending"
    object_operation_ref: str | None = None


class ArtifactMetadataRepository(Protocol):
    async def save_pending(
        self, pending: PendingUpload
    ) -> None: ...

    async def get_upload(
        self, tenant_id: str, artifact_id: str, upload_id: str
    ) -> PendingUpload | None: ...

    async def cleanup_expired(self) -> int: ...

    async def expired_uploads(
        self, *, limit: int = 100, claim_ttl: timedelta = timedelta(seconds=30)
    ) -> list[PendingUpload]: ...

    async def mark_deleted(self, pending: PendingUpload) -> bool: ...

    async def release_gc(self, pending: PendingUpload, error: str) -> bool: ...

    async def mark_ready(self, pending: PendingUpload, version: int) -> bool: ...

    async def mark_multipart_completed(self, pending: PendingUpload) -> bool: ...

    async def mark_quarantined(self, pending: PendingUpload, reason: str) -> bool: ...

    async def claim_finalize(
        self, pending: PendingUpload, *, claim_ttl: timedelta = timedelta(seconds=30)
    ) -> PendingUpload | None: ...

    async def renew_finalize(self, pending: PendingUpload, *, claim_ttl: timedelta) -> bool: ...

    async def validate_finalize(self, pending: PendingUpload) -> bool: ...

    async def renew_gc(self, pending: PendingUpload, *, claim_ttl: timedelta) -> bool: ...

    async def validate_gc(self, pending: PendingUpload) -> bool: ...

    async def mark_reconciling(
        self, pending: PendingUpload, *, operation: str, reason: str
    ) -> bool: ...

    async def begin_object_side_effect(
        self, pending: PendingUpload, *, operation: str
    ) -> bool: ...

    async def get_ready(
        self, tenant_id: str, artifact_id: str, version: int
    ) -> PendingUpload | None: ...

    async def claim_ready_delete(
        self,
        tenant_id: str,
        artifact_id: str,
        version: int,
        *,
        claim_ttl: timedelta = timedelta(seconds=30),
    ) -> PendingUpload | None: ...

    async def is_deleted(
        self, tenant_id: str, artifact_id: str, version: int
    ) -> bool: ...

    async def mark_ready_deleted(self, pending: PendingUpload) -> bool: ...

    async def release_ready_delete(
        self, pending: PendingUpload, error: str
    ) -> bool: ...

    async def get_ready_delete_claim(
        self,
        tenant_id: str,
        artifact_id: str,
        version: int,
        claim_token: str,
    ) -> PendingUpload | None: ...

    async def claim_skill_publication(
        self,
        tenant_id: str,
        artifact_id: str,
        version: int,
        command_id: str,
    ) -> PendingUpload | None: ...

    async def bind_skill_publication(
        self, pending: PendingUpload, package_digest: str
    ) -> bool: ...

    async def claim_skill_orphans(
        self,
        *,
        owner: str,
        limit: int = 100,
        claim_ttl: timedelta = timedelta(seconds=30),
    ) -> list[PendingUpload]: ...

    async def claim_reconciling(
        self,
        *,
        owner: str,
        limit: int = 1,
        claim_ttl: timedelta = timedelta(seconds=30),
    ) -> list[PendingUpload]: ...

    async def resolve_reconciling(
        self, pending: PendingUpload, *, status: str, reason: str
    ) -> bool: ...


class ArtifactPolicyValidator(Protocol):
    async def validate_decision(
        self,
        *,
        tenant_id: str,
        decision_id: str,
        action: str,
        resource: str,
    ) -> bool: ...


def _is_task_api_skill_upload(pending: PendingUpload) -> bool:
    return (
        pending.media_type == "application/vnd.auraclaw.skill-package+json"
        and pending.root_session_id == pending.session_id
        and pending.root_session_id.startswith("skill-upload:")
        and pending.retention_until is not None
        and pending.classification == "internal"
    )


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
        claim_ttl: timedelta = timedelta(seconds=30),
        orphan_claim_limit: int = 1,
    ) -> None:
        self._presigner = presigner
        self._repository = repository
        self._object_verifier = object_verifier
        self._policy = policy
        self._multipart = multipart
        self._multipart_threshold = multipart_threshold
        self._multipart_part_size = multipart_part_size
        self._claim_ttl = claim_ttl
        self._orphan_claim_limit = max(1, orphan_claim_limit)
        self._instance_id = f"artifact-service-{uuid.uuid4().hex}"
        self._uploads: dict[str, PendingUpload] = {}
        self._ready: dict[tuple[str, str, int], PendingUpload] = {}

    @asynccontextmanager
    async def _maintain_claim(
        self, pending: PendingUpload, kind: str
    ) -> AsyncIterator[asyncio.Event]:
        lost = asyncio.Event()
        if self._repository is None:
            yield lost
            return
        renew = getattr(self._repository, f"renew_{kind}", None)
        validate = getattr(self._repository, f"validate_{kind}", None)
        if not callable(renew) or not callable(validate):
            yield lost
            return
        if not await validate(pending):
            raise ArtifactAccessError("artifact operation claim was lost")
        stop = asyncio.Event()

        async def heartbeat() -> None:
            interval = max(0.01, self._claim_ttl.total_seconds() / 3)
            while not stop.is_set():
                try:
                    await asyncio.wait_for(stop.wait(), timeout=interval)
                    return
                except TimeoutError:
                    pass
                try:
                    if not await renew(pending, claim_ttl=self._claim_ttl):
                        lost.set()
                        return
                except Exception:
                    lost.set()
                    return

        task = asyncio.create_task(heartbeat())
        try:
            yield lost
        finally:
            stop.set()
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    async def _assert_claim(
        self,
        pending: PendingUpload,
        kind: str,
        lost: asyncio.Event,
        *,
        side_effect_may_have_happened: bool = False,
    ) -> None:
        if self._repository is None:
            return
        validate = getattr(self._repository, f"validate_{kind}", None)
        owned = not lost.is_set() and (
            not callable(validate) or await validate(pending)
        )
        if owned:
            return
        if side_effect_may_have_happened:
            mark = getattr(self._repository, "mark_reconciling", None)
            if callable(mark):
                with suppress(Exception):
                    await mark(
                        pending,
                        operation=kind,
                        reason="claim_lost_after_object_side_effect",
                    )
        raise ArtifactAccessError("artifact operation claim was lost")

    async def _record_reconciliation(
        self, pending: PendingUpload, *, operation: str, reason: str
    ) -> None:
        if self._repository is None:
            return
        mark = getattr(self._repository, "mark_reconciling", None)
        if callable(mark):
            with suppress(Exception):
                await mark(pending, operation=operation, reason=reason)

    async def _begin_side_effect(
        self, pending: PendingUpload, *, operation: str
    ) -> None:
        if self._repository is None:
            return
        begin = getattr(self._repository, "begin_object_side_effect", None)
        if callable(begin) and not await begin(pending, operation=operation):
            raise ArtifactAccessError("artifact operation claim was lost")

    async def _validate_policy_decision(
        self,
        *,
        tenant_id: str,
        decision_id: str | None,
        action: str,
        resource: str,
    ) -> None:
        if not decision_id:
            raise ArtifactAccessError(f"{action} requires policy decision")
        if self._policy is None:
            raise ArtifactAccessError("artifact policy validation is unavailable")
        try:
            valid = await self._policy.validate_decision(
                tenant_id=tenant_id,
                decision_id=decision_id,
                action=action,
                resource=resource,
            )
        except Exception as exc:
            raise ArtifactAccessError("artifact policy validation is unavailable") from exc
        if not valid:
            raise ArtifactAccessError("artifact policy decision is invalid or expired")

    async def create_upload(
        self, request: ArtifactCreateUploadRequest
    ) -> ArtifactUploadResponse:
        identity = request.context.service_identity
        if identity not in {
            ServiceIdentity.ACTION_HANDS,
            ServiceIdentity.DELIVERY_WORKER,
            ServiceIdentity.TASK_API,
        }:
            raise ArtifactAccessError("workload may not create Artifact uploads")
        if identity is ServiceIdentity.TASK_API and (
            request.media_type != "application/vnd.auraclaw.skill-package+json"
            or request.root_session_id != request.session_id
            or not request.root_session_id.startswith("skill-upload:")
            or request.retention_until is None
            or request.classification != "internal"
            or request.expected_size > 24 * 1024 * 1024
        ):
            raise ArtifactAccessError("Task API may only stage governed Skill packages")
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
            try:
                await self._repository.save_pending(self._uploads[upload_id])
            except Exception:
                self._uploads.pop(upload_id, None)
                raise
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
        identity = request.context.service_identity
        if identity not in {
            ServiceIdentity.ACTION_HANDS,
            ServiceIdentity.DELIVERY_WORKER,
            ServiceIdentity.TASK_API,
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
                    if identity is ServiceIdentity.TASK_API and not _is_task_api_skill_upload(
                        ready
                    ):
                        raise ArtifactAccessError(
                            "Task API may only finalize Skill packages"
                        )
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
        if identity is ServiceIdentity.TASK_API and not _is_task_api_skill_upload(
            pending
        ):
            raise ArtifactAccessError("Task API may only finalize Skill packages")
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
            claimed = await self._repository.claim_finalize(
                pending, claim_ttl=self._claim_ttl
            )
            if claimed is None:
                ready = await self._repository.get_ready(
                    request.context.tenant_id, request.artifact_id, request.version
                )
                if ready is not None:
                    if identity is ServiceIdentity.TASK_API and not _is_task_api_skill_upload(
                        ready
                    ):
                        raise ArtifactAccessError(
                            "Task API may only finalize Skill packages"
                        )
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
        async with self._maintain_claim(pending, "finalize") as claim_lost:
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
                await self._assert_claim(pending, "finalize", claim_lost)
                await self._begin_side_effect(pending, operation="finalize")
                try:
                    await self._multipart.complete(
                        pending.object_key,
                        pending.multipart_upload_id,
                        request.parts,
                    )
                except ArtifactAccessError:
                    if (
                        self._object_verifier is None
                        or not await self._object_verifier.verify(pending)
                    ):
                        raise
                await self._assert_claim(
                    pending,
                    "finalize",
                    claim_lost,
                    side_effect_may_have_happened=True,
                )
                multipart_saved = self._repository is None or await (
                    self._repository.mark_multipart_completed(pending)
                )
                if not multipart_saved:
                    assert self._repository is not None
                    await self._record_reconciliation(
                        pending,
                        operation="finalize",
                        reason="multipart_completed_metadata_commit_lost",
                    )
                    raise ArtifactAccessError("artifact finalization lease was lost")
            if self._object_verifier is not None:
                await self._assert_claim(pending, "finalize", claim_lost)
                scan = await self._object_verifier.inspect(pending)
                await self._assert_claim(
                    pending,
                    "finalize",
                    claim_lost,
                    side_effect_may_have_happened=False,
                )
                if scan in {"size_mismatch", "checksum_mismatch"}:
                    if self._repository is not None and not await self._repository.mark_quarantined(
                        pending, scan
                    ):
                        raise ArtifactAccessError("artifact finalization lease was lost")
                    raise ArtifactAccessError(f"artifact object scan failed: {scan}")
                if scan != "clean":
                    raise ArtifactAccessError(f"artifact object is not ready: {scan}")
            await self._assert_claim(pending, "finalize", claim_lost)
            if self._repository is not None:
                if not await self._repository.mark_ready(pending, request.version):
                    self._ready.pop(
                        (pending.tenant_id, pending.artifact_id, request.version), None
                    )
                    self._uploads.pop(request.upload_id, None)
                    await self._record_reconciliation(
                        pending,
                        operation="finalize",
                        reason="ready_metadata_commit_lost",
                    )
                    raise ArtifactAccessError("artifact finalization lease was lost")
        self._uploads.pop(request.upload_id, None)
        self._ready[(pending.tenant_id, pending.artifact_id, request.version)] = pending
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
        await self.reconcile_unknown(limit=min(limit, self._orphan_claim_limit))
        deleted = 0
        for _ in range(limit):
            claimed = await self._repository.expired_uploads(
                limit=1, claim_ttl=self._claim_ttl
            )
            if not claimed:
                break
            pending = claimed[0]
            async with self._maintain_claim(pending, "gc") as claim_lost:
                await self._assert_claim(pending, "gc", claim_lost)
                removed = False
                try:
                    await self._begin_side_effect(pending, operation="gc")
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
                except Exception:
                    await self._record_reconciliation(
                        pending,
                        operation="gc",
                        reason="expired_upload_delete_result_unknown",
                    )
                    raise
                await self._assert_claim(
                    pending,
                    "gc",
                    claim_lost,
                    side_effect_may_have_happened=removed,
                )
                if removed:
                    if not await self._repository.mark_deleted(pending):
                        await self._record_reconciliation(
                            pending,
                            operation="gc",
                            reason="expired_upload_deleted_metadata_commit_lost",
                        )
                        raise ArtifactAccessError("artifact GC claim was lost")
                    deleted += 1
                elif not await self._repository.release_gc(
                    pending, "object deletion failed"
                ):
                    raise ArtifactAccessError("artifact GC claim was lost")
        return deleted

    async def reconcile_unknown(self, *, limit: int = 10) -> int:
        if self._repository is None or self._object_verifier is None:
            return 0
        claim = getattr(self._repository, "claim_reconciling", None)
        resolve = getattr(self._repository, "resolve_reconciling", None)
        if not callable(claim) or not callable(resolve):
            return 0
        reconciled = 0
        for _ in range(limit):
            rows = await claim(
                owner=self._instance_id,
                limit=1,
                claim_ttl=self._claim_ttl,
            )
            if not rows:
                break
            pending = rows[0]
            async with self._maintain_claim(pending, "gc") as claim_lost:
                await self._assert_claim(pending, "gc", claim_lost)
                if (pending.object_operation_ref or "").startswith("finalize:"):
                    inspection = await self._object_verifier.inspect(pending)
                    if inspection == "clean":
                        status = "ready"
                    elif inspection in {"size_mismatch", "checksum_mismatch"}:
                        status = "quarantined"
                    else:
                        status = "pending"
                else:
                    exists = await self._object_verifier.verify(pending)
                    status = (
                        "ready"
                        if exists and pending.scan_status == "clean"
                        else "pending"
                        if exists
                        else "deleted"
                    )
                await self._assert_claim(pending, "gc", claim_lost)
                if not await resolve(
                    pending,
                    status=status,
                    reason=f"reconciled_object_state:{status}",
                ):
                    raise ArtifactAccessError("artifact reconciliation claim was lost")
                reconciled += 1
        return reconciled

    async def download(self, request: ArtifactDownloadRequest) -> ArtifactDownloadResponse:
        if request.context.service_identity not in {
            ServiceIdentity.TASK_API,
            ServiceIdentity.DELIVERY_WORKER,
            ServiceIdentity.ACTION_HANDS,
        }:
            raise ArtifactAccessError("workload may not download Artifacts")
        if self._repository is not None:
            record = await self._repository.get_ready(
                request.context.tenant_id, request.artifact_id, request.version
            )
        else:
            record = self._ready.get(
                (request.context.tenant_id, request.artifact_id, request.version)
            )
        if record is None:
            raise NotFoundError("artifact was not found")
        await self._validate_policy_decision(
            tenant_id=request.context.tenant_id,
            decision_id=request.policy_decision_id,
            action="artifact.download",
            resource=request.artifact_id,
        )
        url, expires_at = self._presigner.presign(
            "GET", record.object_key, ttl=timedelta(minutes=5)
        )
        return ArtifactDownloadResponse(download_url=url, expires_at=expires_at)

    async def delete(self, request: ArtifactDeleteRequest) -> ArtifactDeleteResponse:
        if request.context.service_identity is not ServiceIdentity.ACTION_HANDS:
            raise ArtifactAccessError("workload may not delete Artifacts")
        if self._repository is None or self._object_verifier is None:
            raise ArtifactAccessError("artifact deletion is unavailable")
        await self._validate_policy_decision(
            tenant_id=request.context.tenant_id,
            decision_id=request.policy_decision_id,
            action="artifact.delete",
            resource=request.artifact_id,
        )
        pending = await self._repository.claim_ready_delete(
            request.context.tenant_id,
            request.artifact_id,
            request.version,
            claim_ttl=self._claim_ttl,
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
        async with self._maintain_claim(pending, "gc") as claim_lost:
            await self._assert_claim(pending, "gc", claim_lost)
            try:
                await self._begin_side_effect(pending, operation="gc")
                removed = await self._object_verifier.delete(pending)
            except Exception:
                await self._record_reconciliation(
                    pending, operation="gc", reason="object_delete_result_unknown"
                )
                raise
            await self._assert_claim(
                pending,
                "gc",
                claim_lost,
                side_effect_may_have_happened=removed,
            )
            if not removed:
                if not await self._repository.release_ready_delete(
                    pending, "object deletion failed"
                ):
                    raise ArtifactAccessError("artifact deletion lease was lost")
                raise ArtifactAccessError("artifact object deletion failed")
            if not await self._repository.mark_ready_deleted(pending):
                await self._record_reconciliation(
                    pending,
                    operation="gc",
                    reason="object_deleted_metadata_commit_lost",
                )
                raise ArtifactAccessError("artifact deletion lease was lost")
        self._ready.pop(
            (request.context.tenant_id, request.artifact_id, request.version),
            None,
        )
        return ArtifactDeleteResponse(
            artifact_id=request.artifact_id,
            version=request.version,
        )

    async def claim_skill_publication(
        self, request: ArtifactSkillPublicationClaimRequest
    ) -> ArtifactSkillPublicationClaimResponse:
        if request.context.service_identity is not ServiceIdentity.ACTION_HANDS:
            raise ArtifactAccessError("workload may not claim Skill Artifacts")
        if self._repository is None:
            raise ArtifactAccessError("Skill Artifact claims are unavailable")
        pending = await self._repository.claim_skill_publication(
            request.context.tenant_id,
            request.artifact_id,
            request.version,
            request.command_id,
        )
        if pending is None:
            raise ArtifactAccessError(
                "Skill Artifact is bound, expired, missing, or claimed"
            )
        return ArtifactSkillPublicationClaimResponse(
            artifact_ref=_artifact_ref(pending, request.version),
            claim_token=request.command_id,
            already_bound=pending.skill_bound_digest is not None,
        )

    async def bind_skill_publication(
        self, request: ArtifactSkillPublicationBindRequest
    ) -> ArtifactSkillPublicationBindResponse:
        if request.context.service_identity is not ServiceIdentity.ACTION_HANDS:
            raise ArtifactAccessError("workload may not bind Skill Artifacts")
        if self._repository is None:
            raise ArtifactAccessError("Skill Artifact binding is unavailable")
        pending = await self._repository.get_ready(
            request.context.tenant_id, request.artifact_id, request.version
        )
        if pending is None or pending.skill_publish_claim_token != request.claim_token:
            if pending is not None and pending.skill_bound_digest == request.package_digest:
                return ArtifactSkillPublicationBindResponse()
            raise ArtifactAccessError("Skill Artifact publication claim was lost")
        if not await self._repository.bind_skill_publication(
            pending, request.package_digest
        ):
            raise ArtifactAccessError("Skill Artifact publication claim was lost")
        return ArtifactSkillPublicationBindResponse()

    async def claim_skill_orphans(
        self, request: ArtifactSkillOrphanClaimRequest
    ) -> ArtifactSkillOrphanClaimResponse:
        if request.context.service_identity is not ServiceIdentity.ACTION_HANDS:
            raise ArtifactAccessError("workload may not claim Skill orphans")
        if self._repository is None:
            raise ArtifactAccessError("Skill orphan collection is unavailable")
        pending = await self._repository.claim_skill_orphans(
            owner=request.owner,
            limit=min(request.limit, self._orphan_claim_limit),
            claim_ttl=self._claim_ttl,
        )
        return ArtifactSkillOrphanClaimResponse(
            artifacts=tuple(
                {
                    **_artifact_ref(item, item.version),
                    "tenant_id": item.tenant_id,
                    "claim_token": item.gc_claim_token,
                }
                for item in pending
            )
        )

    async def resolve_skill_orphan(
        self, request: ArtifactSkillOrphanResolveRequest
    ) -> ArtifactSkillOrphanResolveResponse:
        if request.context.service_identity is not ServiceIdentity.ACTION_HANDS:
            raise ArtifactAccessError("workload may not resolve Skill orphans")
        if self._repository is None or self._object_verifier is None:
            raise ArtifactAccessError("Skill orphan collection is unavailable")
        pending = await self._repository.get_ready_delete_claim(
            request.context.tenant_id,
            request.artifact_id,
            request.version,
            request.claim_token,
        )
        if pending is None:
            raise ArtifactAccessError("Skill orphan claim was lost")
        async with self._maintain_claim(pending, "gc") as claim_lost:
            await self._assert_claim(pending, "gc", claim_lost)
            if request.referenced:
                if request.package_digest is None:
                    raise ArtifactAccessError("referenced Skill requires package digest")
                if not await self._repository.bind_skill_publication(
                    pending, request.package_digest
                ):
                    raise ArtifactAccessError("Skill orphan claim was lost")
                return ArtifactSkillOrphanResolveResponse(status="retained")
            await self._validate_policy_decision(
                tenant_id=request.context.tenant_id,
                decision_id=request.policy_decision_id,
                action="artifact.delete",
                resource=request.artifact_id,
            )
            await self._assert_claim(pending, "gc", claim_lost)
            try:
                await self._begin_side_effect(pending, operation="gc")
                removed = await self._object_verifier.delete(pending)
            except Exception:
                await self._record_reconciliation(
                    pending,
                    operation="gc",
                    reason="skill_orphan_delete_result_unknown",
                )
                raise
            await self._assert_claim(
                pending,
                "gc",
                claim_lost,
                side_effect_may_have_happened=removed,
            )
            if not removed:
                if not await self._repository.release_ready_delete(
                    pending, "Skill orphan object deletion failed"
                ):
                    raise ArtifactAccessError("Skill orphan claim was lost")
                raise ArtifactAccessError("Skill orphan object deletion failed")
            if not await self._repository.mark_ready_deleted(pending):
                await self._record_reconciliation(
                    pending,
                    operation="gc",
                    reason="skill_orphan_deleted_metadata_commit_lost",
                )
                raise ArtifactAccessError("Skill orphan deletion claim was lost")
            return ArtifactSkillOrphanResolveResponse(status="deleted")


def _artifact_ref(pending: PendingUpload, version: int) -> dict[str, object]:
    return {
        "artifact_id": pending.artifact_id,
        "version": version,
        "content_hash": pending.expected_checksum,
        "media_type": pending.media_type,
        "size": pending.expected_size,
    }
