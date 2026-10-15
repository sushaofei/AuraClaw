from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Query

from auraclaw.action.skill_admin_catalog import (
    SkillAdminCatalogQueryService,
    SkillCatalogQuery,
)
from auraclaw.action.skill_admin_catalog import (
    published_skill as _published_skill,
)
from auraclaw.action.skill_admin_queries import SkillAdminManagementQueryService
from auraclaw.action.skill_packages import SkillPackageRegistry
from auraclaw.api.routes.skill_admin.contracts import (
    Identity,
    SkillContentReader,
    SkillManager,
)
from auraclaw.api.routes.skill_admin.presentation import (
    installation_summary,
    package_state_summary,
    paginate,
    publication_state_summary,
    require_management_service,
    skill_item_key,
    skill_markdown,
    summary,
)


def create_catalog_router(
    registry: SkillPackageRegistry,
    *,
    management_query: SkillAdminManagementQueryService | None,
    management_service: SkillManager | None,
    content_reader: SkillContentReader | None,
    catalog_query_service: SkillAdminCatalogQueryService | None,
) -> APIRouter:
    router = APIRouter()

    @router.get("/skills")
    async def list_skills(
        identity: Identity,
        q: Annotated[str | None, Query(max_length=256)] = None,
        publisher: Annotated[str | None, Query(max_length=128)] = None,
        risk_level: Annotated[str | None, Query(max_length=64)] = None,
        publication_status: Annotated[str | None, Query(max_length=64)] = None,
        installation_status: Annotated[str | None, Query(max_length=64)] = None,
        cursor: Annotated[str | None, Query(max_length=2048)] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
    ) -> dict[str, Any]:
        require_management_service(management_service)
        assert catalog_query_service is not None
        catalog_items = await catalog_query_service.list_latest(
            identity.tenant_id,
            SkillCatalogQuery(
                text=q,
                publisher=publisher,
                risk_level=risk_level,
                publication_status=publication_status,
                installation_status=installation_status,
            ),
        )
        items: list[dict[str, Any]] = []
        for catalog_item in catalog_items:
            publication = catalog_item.publication
            manifest = publication.manifest
            installation = catalog_item.installation
            current_state = catalog_item.publication_state
            publication_payload = {
                "status": publication.status.value,
                "revision": current_state.revision,
            }
            installation_payload = (
                None if installation is None else installation_summary(installation)
            )
            items.append(
                {
                    **summary(publication),
                    "latest_version": manifest.version,
                    "publication": publication_payload,
                    "installation": installation_payload,
                    "availability": catalog_item.availability,
                }
            )
        page, next_cursor = paginate(items, cursor=cursor, limit=limit, key=skill_item_key)
        return {"skills": page, "items": page, "next_cursor": next_cursor}

    @router.get("/skills/{publisher}/{name}")
    async def get_skill(publisher: str, name: str, identity: Identity) -> dict[str, Any]:
        require_management_service(management_service)
        assert catalog_query_service is not None and management_query is not None
        detail = await management_query.detail(
            identity.tenant_id,
            publisher,
            name,
            catalog_query_service,
        )
        publication = detail.publication
        markdown = await skill_markdown(
            content_reader,
            registry,
            identity.tenant_id,
            publisher,
            name,
            publication.manifest.version,
        )
        payload = summary(publication, skill_markdown=markdown)
        payload["versions"] = list(detail.versions)
        return payload

    @router.get("/skills/{publisher}/{name}/management")
    async def get_skill_management_view(
        publisher: str, name: str, identity: Identity
    ) -> dict[str, Any]:
        require_management_service(management_service)
        assert management_query is not None
        view = await management_query.management_view(identity.tenant_id, publisher, name)
        return {
            "publisher": publisher,
            "name": name,
            "installation": (
                None if view.installation is None else installation_summary(view.installation)
            ),
            "versions": [
                {
                    "publication": publication_state_summary(publication),
                    "package": None if package is None else package_state_summary(package),
                }
                for publication, package in view.versions
            ],
            "upgrade": None
            if view.upgrade is None
            else view.upgrade.model_dump(
                mode="json",
                include={"operation_id", "current_version", "generation", "phase", "reason_code"},
            ),
        }

    @router.get("/skills/{publisher}/{name}/versions/{version}")
    async def get_skill_version(
        publisher: str, name: str, version: str, identity: Identity
    ) -> dict[str, Any]:
        service = require_management_service(management_service)
        state = await service.get_publication(identity.tenant_id, publisher, name, version)
        package = await service.get_package(identity.tenant_id, publisher, name, version)
        publication = _published_skill(package, state)
        markdown = await skill_markdown(
            content_reader,
            registry,
            identity.tenant_id,
            publisher,
            name,
            version,
        )
        return summary(publication, skill_markdown=markdown)

    return router
