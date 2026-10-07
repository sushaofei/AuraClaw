from __future__ import annotations

from typing import Annotated, Protocol

from fastapi import APIRouter, Depends

from auraclaw.api.dependencies import RequestIdentity, get_artifact_share_gateway, request_identity
from auraclaw.api.models import ArtifactSharePublicRequest, ArtifactSharePublicResponse
from auraclaw.contracts.internal import ArtifactShareResponse


class ArtifactShareGateway(Protocol):
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
    ) -> ArtifactShareResponse: ...


router = APIRouter(prefix="/v1/artifacts", tags=["artifacts"])
Identity = Annotated[RequestIdentity, Depends(request_identity)]
ShareGateway = Annotated[ArtifactShareGateway, Depends(get_artifact_share_gateway)]


@router.post("/{artifact_id}/shares", response_model=ArtifactSharePublicResponse)
async def create_artifact_share(
    artifact_id: str,
    request: ArtifactSharePublicRequest,
    identity: Identity,
    service: ShareGateway,
) -> ArtifactShareResponse:
    return await service.share(
        tenant_id=identity.tenant_id,
        artifact_id=artifact_id,
        version=request.version,
        actor_id=identity.actor.id,
        audience=request.audience,
        ttl_seconds=request.ttl_seconds,
        correlation_id=identity.correlation_id,
    )
