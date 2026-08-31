from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime, timedelta

import httpx

from auraclaw.contracts.errors import ArtifactAccessError
from auraclaw.contracts.internal import (
    ArtifactCreateUploadRequest,
    ArtifactFinalizeRequest,
    ArtifactFinalizeResponse,
    ArtifactUploadResponse,
    InternalRequestContext,
    ServiceIdentity,
)
from auraclaw.contracts.tools import ArtifactRef
from auraclaw.internal.http import HttpContractClient


class RemoteArtifactWriter:
    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str,
        transport: httpx.AsyncBaseTransport | None = None,
        object_transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(base_url=base_url, transport=transport)
        self._objects = httpx.AsyncClient(transport=object_transport)
        self._contract = HttpContractClient(self._client, bearer_token=bearer_token)

    async def aclose(self) -> None:
        await self._client.aclose()
        await self._objects.aclose()

    async def put(
        self,
        *,
        tenant_id: str,
        root_session_id: str,
        session_id: str,
        content: bytes,
        artifact_type: str,
        media_type: str,
        name: str,
        producer: str,
        lineage_refs: tuple[str, ...] = (),
        classification: str = "internal",
        acl: tuple[str, ...] = (),
        retention_until: datetime | None = None,
    ) -> ArtifactRef:
        del artifact_type, producer, lineage_refs, acl
        checksum = hashlib.sha256(content).hexdigest()
        request_id = str(uuid.uuid4())
        context = InternalRequestContext(
            tenant_id=tenant_id,
            service_identity=ServiceIdentity.ACTION_HANDS,
            request_id=request_id,
            correlation_id=session_id,
            causation_id=request_id,
        )
        upload = await self._contract.call(
            "/internal/v1/artifacts/uploads/create",
            ArtifactCreateUploadRequest(
                context=context,
                root_session_id=root_session_id,
                session_id=session_id,
                name=name,
                media_type=media_type,
                expected_size=len(content),
                expected_checksum=checksum,
                classification=classification,
                retention_until=retention_until,
            ),
            ArtifactUploadResponse,
        )
        completed_parts: tuple[dict[str, object], ...] = ()
        if upload.upload_mode == "multipart":
            if upload.part_size is None or not upload.part_urls:
                raise ArtifactAccessError("artifact multipart plan is invalid")
            parts: list[dict[str, object]] = []
            for index, part_url in enumerate(upload.part_urls, start=1):
                offset = (index - 1) * upload.part_size
                response = await self._objects.put(
                    part_url,
                    content=content[offset : offset + upload.part_size],
                    headers={"Content-Type": media_type},
                )
                if response.is_error or not response.headers.get("ETag"):
                    raise ArtifactAccessError("artifact multipart part upload failed")
                parts.append(
                    {"part_number": index, "etag": response.headers["ETag"]}
                )
            completed_parts = tuple(parts)
        else:
            response = await self._objects.put(
                upload.upload_url,
                content=content,
                headers={"Content-Type": media_type},
            )
            if response.is_error:
                raise ArtifactAccessError("artifact object upload failed")
        finalized = await self._contract.call(
            "/internal/v1/artifacts/uploads/finalize",
            ArtifactFinalizeRequest(
                context=context,
                artifact_id=upload.artifact_id,
                version=upload.version,
                upload_id=upload.upload_id,
                size=len(content),
                checksum=checksum,
                parts=completed_parts,
            ),
            ArtifactFinalizeResponse,
        )
        return ArtifactRef(**finalized.artifact_ref)


class RemoteSkillPackageUploadClient:
    """Task API adapter that proxies Skill package bytes to object storage."""

    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str,
        transport: httpx.AsyncBaseTransport | None = None,
        object_transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(base_url=base_url, transport=transport)
        self._objects = httpx.AsyncClient(transport=object_transport, timeout=60.0)
        self._contract = HttpContractClient(self._client, bearer_token=bearer_token)

    async def aclose(self) -> None:
        await self._client.aclose()
        await self._objects.aclose()

    async def stage(
        self,
        *,
        tenant_id: str,
        name: str,
        content: bytes,
        checksum: str,
        correlation_id: str,
        command_id: str,
    ) -> ArtifactFinalizeResponse:
        if hashlib.sha256(content).hexdigest() != checksum:
            raise ArtifactAccessError("Skill package proxy checksum mismatch")
        request_id = command_id
        scope = f"skill-upload:{request_id}"
        upload = await self._contract.call(
            "/internal/v1/artifacts/uploads/create",
            ArtifactCreateUploadRequest(
                context=InternalRequestContext(
                    tenant_id=tenant_id,
                    service_identity=ServiceIdentity.TASK_API,
                    request_id=request_id,
                    correlation_id=correlation_id,
                    causation_id=request_id,
                ),
                root_session_id=scope,
                session_id=scope,
                name=name,
                media_type="application/vnd.auraclaw.skill-package+json",
                expected_size=len(content),
                expected_checksum=checksum,
                classification="internal",
                retention_until=datetime.now(UTC) + timedelta(days=90),
            ),
            ArtifactUploadResponse,
        )
        completed_parts: tuple[dict[str, object], ...] = ()
        media_type = "application/vnd.auraclaw.skill-package+json"
        if upload.upload_mode == "multipart":
            if upload.part_size is None or not upload.part_urls:
                raise ArtifactAccessError("Skill package multipart plan is invalid")
            parts: list[dict[str, object]] = []
            for index, part_url in enumerate(upload.part_urls, start=1):
                offset = (index - 1) * upload.part_size
                response = await self._objects.put(
                    part_url,
                    content=content[offset : offset + upload.part_size],
                    headers={"Content-Type": media_type},
                )
                etag = response.headers.get("ETag")
                if response.is_error or not etag:
                    raise ArtifactAccessError("Skill package multipart upload failed")
                parts.append({"part_number": index, "etag": etag})
            completed_parts = tuple(parts)
        else:
            response = await self._objects.put(
                upload.upload_url,
                content=content,
                headers={"Content-Type": media_type},
            )
            if response.is_error:
                raise ArtifactAccessError("Skill package object upload failed")
        return await self._contract.call(
            "/internal/v1/artifacts/uploads/finalize",
            ArtifactFinalizeRequest(
                context=InternalRequestContext(
                    tenant_id=tenant_id,
                    service_identity=ServiceIdentity.TASK_API,
                    request_id=request_id,
                    correlation_id=correlation_id,
                    causation_id=request_id,
                ),
                artifact_id=upload.artifact_id,
                version=upload.version,
                upload_id=upload.upload_id,
                size=len(content),
                checksum=checksum,
                parts=completed_parts,
            ),
            ArtifactFinalizeResponse,
        )
