from dataclasses import asdict
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from pydantic import AwareDatetime

from auraclaw.api.dependencies import (
    RequestIdentity,
    get_observability_service,
    get_task_projection,
    request_identity,
)
from auraclaw.api.projection_contract import apply_projection_contract
from auraclaw.contracts.errors import NotFoundError
from auraclaw.observability.service import ObservabilityService
from auraclaw.projection.ports import TaskReader

router = APIRouter(prefix="/v1/operations", tags=["operations"])
Identity = Annotated[RequestIdentity, Depends(request_identity)]
Service = Annotated[ObservabilityService, Depends(get_observability_service)]
Reader = Annotated[TaskReader, Depends(get_task_projection)]


@router.get("/audits")
async def audit_search(
    identity: Identity,
    service: Service,
    action: str | None = Query(default=None, min_length=1, max_length=128),
    outcome: str | None = Query(default=None, min_length=1, max_length=64),
    actor_id: str | None = Query(default=None, min_length=1, max_length=256),
    session_id: str | None = Query(default=None, min_length=1, max_length=256),
    before: AwareDatetime | None = None,
    before_id: str | None = Query(default=None, min_length=1, max_length=256),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, object]:
    if (before is None) != (before_id is None):
        raise HTTPException(
            status_code=422,
            detail="audit cursor requires both before and before_id",
        )
    return await service.search_audits(
        identity.tenant_id,
        action=action,
        outcome=outcome,
        actor_id=actor_id,
        session_id=session_id,
        before=before,
        before_id=before_id,
        limit=limit,
    )


@router.get("/sessions/{session_id}/timeline")
async def session_timeline(
    session_id: str,
    response: Response,
    identity: Identity,
    service: Service,
    reader: Reader,
    min_version: int | None = Query(default=None, ge=0),
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
) -> dict[str, object]:
    task = await reader.get_task(identity.tenant_id, session_id)
    if task is None:
        raise NotFoundError(f"Session not found: {session_id}")
    projection_version = int(str(task["projection_version"]))
    apply_projection_contract(
        response,
        projection_version=projection_version,
        min_version=min_version,
        if_none_match=if_none_match,
    )
    return {
        **await service.timeline(identity.tenant_id, session_id),
        "projection_version": projection_version,
    }


@router.get("/metrics")
async def metric_snapshot(
    identity: Identity,
    service: Service,
) -> dict[str, object]:
    points = await service.metrics(identity.tenant_id)
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
