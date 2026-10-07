from __future__ import annotations

import uuid

import httpx

from auraclaw.contracts.internal import (
    ArtifactShareRequest,
    ArtifactShareResponse,
    InternalRequestContext,
    ServiceIdentity,
)
from auraclaw.internal.http import HttpContractClient


class RemoteArtifactShareClient:
    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(base_url=base_url, transport=transport, timeout=30.0)
        self._contract = HttpContractClient(self._client, bearer_token=bearer_token)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def share(
        self,
        *,
        tenant_id: str,
        artifact_id: str,
        version: int,
        actor_id: str,
        audience: str,
        ttl_seconds: int,
        correlation_id: str,
    ) -> ArtifactShareResponse:
        request_id = str(uuid.uuid4())
        return await self._contract.call(
            "/internal/v1/artifacts/share",
            ArtifactShareRequest(
                context=InternalRequestContext(
                    tenant_id=tenant_id,
                    service_identity=ServiceIdentity.TASK_API,
                    request_id=request_id,
                    correlation_id=correlation_id,
                    causation_id=request_id,
                ),
                artifact_id=artifact_id,
                version=version,
                actor_id=actor_id,
                audience=audience,
                ttl_seconds=ttl_seconds,
            ),
            ArtifactShareResponse,
        )
