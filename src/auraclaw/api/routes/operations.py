from dataclasses import asdict
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from auraclaw.api.dependencies import (
    RequestIdentity,
    get_observability_service,
    get_task_projection,
    request_identity,
)
from auraclaw.contracts.errors import NotFoundError
from auraclaw.observability.service import ObservabilityService
from auraclaw.projection.ports import TaskReader

router = APIRouter(prefix="/v1/operations", tags=["operations"])
Identity = Annotated[RequestIdentity, Depends(request_identity)]
Service = Annotated[ObservabilityService, Depends(get_observability_service)]
Reader = Annotated[TaskReader, Depends(get_task_projection)]


@router.get("/sessions/{session_id}/timeline")
async def session_timeline(
    session_id: str,
    identity: Identity,
    service: Service,
    reader: Reader,
) -> dict[str, object]:
    if await reader.get_task(identity.tenant_id, session_id) is None:
        raise NotFoundError(f"Session not found: {session_id}")
    return await service.timeline(identity.tenant_id, session_id)


@router.get("/metrics")
async def metric_snapshot(
    identity: Identity,
    service: Service,
) -> dict[str, object]:
    points = await service.metrics()
    return {
        "tenant_id": identity.tenant_id,
        "metrics": [
            {
                "name": point.name,
                "value": point.value,
                "observed_at": point.observed_at.isoformat(),
                "labels": point.labels,
            }
            for point in points
            if point.tenant_id in {None, identity.tenant_id}
        ],
    }


@router.get("/metrics/summary")
async def metric_summary(
    identity: Identity,
    service: Service,
    window_hours: int = Query(default=24, ge=1, le=720),
) -> dict[str, object]:
    summaries = await service.metric_summary(
        identity.tenant_id, window_hours=window_hours
    )
    return {
        "tenant_id": identity.tenant_id,
        "window_hours": window_hours,
        "metrics": [asdict(summary) for summary in summaries],
    }
