from typing import Annotated

from fastapi import APIRouter, Depends, Header
from fastapi.responses import StreamingResponse

from auraclaw.api.dependencies import (
    RequestIdentity,
    get_streaming_gateway,
    request_identity,
)
from auraclaw.gateways.streaming.gateway import StreamingGateway

router = APIRouter(prefix="/v1", tags=["streams"])
Identity = Annotated[RequestIdentity, Depends(request_identity)]
Gateway = Annotated[StreamingGateway, Depends(get_streaming_gateway)]


@router.get("/streams/{session_id}")
async def stream_session(
    session_id: str,
    identity: Identity,
    gateway: Gateway,
    last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
) -> StreamingResponse:
    # Authorize before returning a streaming response so unknown/foreign Sessions are 404.
    await gateway.authorize(tenant_id=identity.tenant_id, session_id=session_id)
    return StreamingResponse(
        gateway.sse(
            tenant_id=identity.tenant_id,
            session_id=session_id,
            last_event_id=last_event_id,
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
