from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Header, HTTPException, Query, status

from auraclaw.action.skill_admin_queries import SkillAdminManagementQueryService
from auraclaw.api.routes.skill_admin.contracts import (
    Identity,
    SkillManager,
)
from auraclaw.api.routes.skill_admin.presentation import (
    change_installation,
    installation_summary,
    paginate,
    require_management_service,
)
from auraclaw.contracts.skills import (
    SkillInstallationOperation,
)


def create_installations_router(
    *,
    management_query: SkillAdminManagementQueryService | None,
    management_service: SkillManager | None,
) -> APIRouter:
    router = APIRouter()

    @router.get("/skill-installations")
    async def list_skill_installations(
        identity: Identity,
        status_filter: Annotated[str | None, Query(alias="status", max_length=64)] = None,
        publisher: Annotated[str | None, Query(max_length=128)] = None,
        cursor: Annotated[str | None, Query(max_length=2048)] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
    ) -> dict[str, Any]:
        require_management_service(management_service)
        assert management_query is not None
        records = [
            installation_summary(item)
            for item in await management_query.list_installations(
                identity.tenant_id, status=status_filter, publisher=publisher
            )
        ]
        page, next_cursor = paginate(
            records,
            cursor=cursor,
            limit=limit,
            key=lambda item: (str(item["publisher"]), str(item["name"])),
        )
        return {"installations": page, "next_cursor": next_cursor}

    @router.get("/skills/{publisher}/{name}/installation")
    async def get_skill_installation(
        publisher: str,
        name: str,
        identity: Identity,
    ) -> dict[str, Any]:
        if management_service is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Skill management service is not configured",
            )
        installation = await management_service.get_installation(
            identity.tenant_id,
            publisher,
            name,
        )
        return {"installation": installation_summary(installation)}

    @router.post(
        "/skills/{publisher}/{name}:install",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def install_skill(
        publisher: str,
        name: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision", ge=1),
    ) -> dict[str, Any]:
        return await change_installation(
            management_service,
            identity,
            publisher,
            name,
            SkillInstallationOperation.INSTALL,
            command_id,
            expected_revision,
        )

    @router.post(
        "/skills/{publisher}/{name}:enable",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def enable_skill(
        publisher: str,
        name: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision", ge=1),
    ) -> dict[str, Any]:
        return await change_installation(
            management_service,
            identity,
            publisher,
            name,
            SkillInstallationOperation.ENABLE,
            command_id,
            expected_revision,
        )

    @router.post(
        "/skills/{publisher}/{name}:disable",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def disable_skill(
        publisher: str,
        name: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision", ge=1),
        reason_code: str = Header(alias="X-Reason-Code", min_length=1, max_length=128),
    ) -> dict[str, Any]:
        return await change_installation(
            management_service,
            identity,
            publisher,
            name,
            SkillInstallationOperation.DISABLE,
            command_id,
            expected_revision,
            reason_code,
        )

    @router.post(
        "/skills/{publisher}/{name}:uninstall",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def uninstall_skill(
        publisher: str,
        name: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision", ge=1),
        reason_code: str = Header(alias="X-Reason-Code", min_length=1, max_length=128),
        force: bool = Query(default=False),
    ) -> dict[str, Any]:
        return await change_installation(
            management_service,
            identity,
            publisher,
            name,
            SkillInstallationOperation.UNINSTALL,
            command_id,
            expected_revision,
            reason_code,
            force=force,
        )

    return router
