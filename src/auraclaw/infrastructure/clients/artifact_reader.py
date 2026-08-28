from __future__ import annotations

import hashlib
import uuid
from typing import Protocol

import httpx

from auraclaw.action.ports import PolicyEvaluation
from auraclaw.contracts.errors import ArtifactAccessError
from auraclaw.contracts.internal import (
    ArtifactDeleteRequest,
    ArtifactDeleteResponse,
    ArtifactDownloadRequest,
    ArtifactDownloadResponse,
    InternalRequestContext,
    ServiceIdentity,
)
from auraclaw.contracts.tools import ArtifactRef, PolicyDecision
from auraclaw.internal.http import HttpContractClient


class ArtifactDownloadPolicy(Protocol):
    async def evaluate_action(
        self,
        *,
        tenant_id: str,
        subject: str,
        action: str,
        resource: str,
        input_digest: str,
        correlation_id: str,
        attributes: dict[str, object],
    ) -> PolicyEvaluation: ...


class RemoteArtifactReader:
    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str,
        policy: ArtifactDownloadPolicy,
        max_content_bytes: int = 24 * 1024 * 1024,
        transport: httpx.AsyncBaseTransport | None = None,
        object_transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(base_url=base_url, transport=transport)
        self._objects = httpx.AsyncClient(transport=object_transport)
        self._contract = HttpContractClient(self._client, bearer_token=bearer_token)
        self._policy = policy
        self._max_content_bytes = max_content_bytes

    async def aclose(self) -> None:
        await self._client.aclose()
        await self._objects.aclose()

    async def read(
        self,
        *,
        tenant_id: str,
        artifact_ref: ArtifactRef,
        actor_id: str,
        correlation_id: str,
    ) -> bytes:
        if artifact_ref.size > self._max_content_bytes:
            raise ArtifactAccessError("Skill package Artifact is too large")
        evaluation = await self._policy.evaluate_action(
            tenant_id=tenant_id,
            subject=actor_id,
            action="artifact.download",
            resource=artifact_ref.artifact_id,
            input_digest=artifact_ref.content_hash,
            correlation_id=correlation_id,
            attributes={
                "artifact_version": artifact_ref.version,
                "media_type": artifact_ref.media_type,
                "purpose": "skill-registry-rebuild",
            },
        )
        if evaluation.decision not in {
            PolicyDecision.ALLOW,
            PolicyDecision.ALLOW_WITH_CONSTRAINTS,
        }:
            raise ArtifactAccessError("Skill package Artifact policy denied access")
        request_id = str(uuid.uuid4())
        download = await self._contract.call(
            "/internal/v1/artifacts/download",
            ArtifactDownloadRequest(
                context=InternalRequestContext(
                    tenant_id=tenant_id,
                    service_identity=ServiceIdentity.ACTION_HANDS,
                    request_id=request_id,
                    correlation_id=correlation_id,
                    causation_id=request_id,
                ),
                artifact_id=artifact_ref.artifact_id,
                version=artifact_ref.version,
                actor_id=actor_id,
                policy_decision_id=evaluation.decision_id,
            ),
            ArtifactDownloadResponse,
        )
        chunks: list[bytes] = []
        content_size = 0
        async with self._objects.stream("GET", download.download_url) as response:
            if response.is_error:
                raise ArtifactAccessError("Skill package Artifact download failed")
            for header in response.headers.get_list("content-length"):
                if int(header) > self._max_content_bytes:
                    raise ArtifactAccessError("Skill package Artifact is too large")
            async for chunk in response.aiter_bytes():
                content_size += len(chunk)
                if content_size > self._max_content_bytes:
                    raise ArtifactAccessError("Skill package Artifact is too large")
                chunks.append(chunk)
        content = b"".join(chunks)
        if len(content) != artifact_ref.size:
            raise ArtifactAccessError("Skill package Artifact size does not match")
        if hashlib.sha256(content).hexdigest() != artifact_ref.content_hash:
            raise ArtifactAccessError("Skill package Artifact digest does not match")
        return content

    async def delete(
        self,
        *,
        tenant_id: str,
        artifact_ref: ArtifactRef,
        actor_id: str,
        reason_code: str,
        correlation_id: str,
    ) -> None:
        evaluation = await self._policy.evaluate_action(
            tenant_id=tenant_id,
            subject=actor_id,
            action="artifact.delete",
            resource=artifact_ref.artifact_id,
            input_digest=artifact_ref.content_hash,
            correlation_id=correlation_id,
            attributes={
                "artifact_version": artifact_ref.version,
                "media_type": artifact_ref.media_type,
                "purpose": "skill-package-purge",
                "reason_code": reason_code,
                "permission": "write-autonomous",
                "risk_level": "high",
                "runtime_location": "hands",
            },
        )
        if evaluation.decision not in {
            PolicyDecision.ALLOW,
            PolicyDecision.ALLOW_WITH_CONSTRAINTS,
        }:
            raise ArtifactAccessError("Skill package Artifact policy denied deletion")
        request_id = str(uuid.uuid4())
        await self._contract.call(
            "/internal/v1/artifacts/delete",
            ArtifactDeleteRequest(
                context=InternalRequestContext(
                    tenant_id=tenant_id,
                    service_identity=ServiceIdentity.ACTION_HANDS,
                    request_id=request_id,
                    correlation_id=correlation_id,
                    causation_id=request_id,
                ),
                artifact_id=artifact_ref.artifact_id,
                version=artifact_ref.version,
                actor_id=actor_id,
                reason_code=reason_code,
                policy_decision_id=evaluation.decision_id,
            ),
            ArtifactDeleteResponse,
        )
