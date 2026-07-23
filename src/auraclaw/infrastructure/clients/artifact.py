from __future__ import annotations

import hashlib
import uuid
from datetime import datetime

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
        del artifact_type, producer, lineage_refs, acl, retention_until
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
