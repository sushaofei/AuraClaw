from __future__ import annotations

from dataclasses import asdict
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Query
from pydantic import AwareDatetime

from auraclaw.action.skill_admission_query import SkillAdmissionMetricsService
from auraclaw.api.routes.skill_admin.contracts import (
    Identity,
    SkillAdmissionReader,
)
from auraclaw.api.routes.skill_admin.presentation import (
    require_admission_reader,
)


def create_admissions_router(
    *,
    admission_reader: SkillAdmissionReader | None,
    metrics_service: SkillAdmissionMetricsService | None,
) -> APIRouter:
    router = APIRouter()

    @router.get("/skill-admissions")
    async def list_skill_admissions(
        identity: Identity,
        outcome: Annotated[
            Literal["accepted", "rejected", "quarantined"] | None,
            Query(),
        ] = None,
        stage: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
        content_policy_version: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
        since: AwareDatetime | None = None,
        cursor: Annotated[str | None, Query(min_length=1, max_length=2048)] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
    ) -> dict[str, Any]:
        reader = require_admission_reader(admission_reader)
        page = await reader.page_admissions(
            identity.tenant_id,
            outcome=outcome,
            stage=stage,
            content_policy_version=content_policy_version,
            since=since,
            cursor=cursor,
            limit=limit,
        )
        return {
            "admissions": [
                {
                    **asdict(record),
                    "occurred_at": record.occurred_at.isoformat(),
                }
                for record in page.admissions
            ],
            "next_cursor": page.next_cursor,
        }

    @router.get("/skill-admissions/metrics")
    async def skill_admission_metrics(
        identity: Identity,
        window_hours: Annotated[int | None, Query(ge=1, le=2160)] = None,
    ) -> dict[str, Any]:
        require_admission_reader(admission_reader)
        assert metrics_service is not None
        return await metrics_service.metrics(identity.tenant_id, window_hours=window_hours)

    return router
