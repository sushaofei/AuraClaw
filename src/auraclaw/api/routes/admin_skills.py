from __future__ import annotations

import base64
import binascii
from typing import Annotated, Any, Protocol

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import Field

from auraclaw.action.skill_packages import SkillPackage, SkillPackageRegistry
from auraclaw.api.dependencies import RequestIdentity, request_identity
from auraclaw.contracts.internal import ContractModel
from auraclaw.contracts.skills import (
    ChangeSkillInstallationCommand,
    PublishedSkill,
    PublishSkillCommand,
    RevokeSkillPublicationCommand,
    SkillInstallationOperation,
    SkillInstallationRecord,
    SkillPublicationRecord,
)

Identity = Annotated[RequestIdentity, Depends(request_identity)]

_MAX_UPLOAD_FILES = 512
_MAX_ENCODED_UPLOAD_BYTES = 24 * 1024 * 1024


class PublishSkillRequest(ContractModel):
    source_id: str = Field(min_length=1, max_length=128)
    activate: bool = True
    files: dict[str, str] = Field(min_length=1, max_length=_MAX_UPLOAD_FILES)


class SkillPublisher(Protocol):
    async def publish(
        self, command: PublishSkillCommand, package: SkillPackage
    ) -> PublishedSkill: ...


class SkillManager(Protocol):
    async def get_installation(
        self, tenant_id: str, publisher: str, name: str
    ) -> SkillInstallationRecord: ...

    async def get_publication(
        self,
        tenant_id: str,
        publisher: str,
        name: str,
        version: str,
    ) -> SkillPublicationRecord: ...

    async def change_installation(
        self, command: ChangeSkillInstallationCommand
    ) -> SkillInstallationRecord: ...

    async def revoke_publication(
        self, command: RevokeSkillPublicationCommand
    ) -> SkillPublicationRecord: ...


def _summary(publication: PublishedSkill, *, skill_markdown: str | None = None) -> dict[str, Any]:
    manifest = publication.manifest
    payload = {
        "publisher": manifest.publisher,
        "name": manifest.name,
        "version": manifest.version,
        "status": publication.status.value,
        "description": manifest.description,
        "risk_level": manifest.risk_level,
        "package_digest": publication.package_digest,
        "required_tools": [item.model_dump(mode="json") for item in manifest.required_tools],
        "required_resources": [
            item.model_dump(mode="json") for item in manifest.required_resources
        ],
        "required_skills": [item.model_dump(mode="json") for item in manifest.required_skills],
    }
    if skill_markdown is not None:
        payload["skill_markdown"] = skill_markdown
    return payload


def create_skill_admin_router(
    registry: SkillPackageRegistry,
    *,
    publication_service: SkillPublisher | None = None,
    management_service: SkillManager | None = None,
) -> APIRouter:
    router = APIRouter(prefix="/v1/admin", tags=["skill-admin"])

    @router.get("/skills")
    async def list_skills(identity: Identity) -> dict[str, Any]:
        latest: dict[tuple[str, str], PublishedSkill] = {}
        for publication in registry.list_publications(identity.tenant_id):
            key = (publication.manifest.publisher, publication.manifest.name)
            current = latest.get(key)
            if current is None or publication.manifest.version > current.manifest.version:
                latest[key] = publication
        return {"skills": [_summary(item) for item in latest.values()]}

    @router.get("/skills/{publisher}/{name}")
    async def get_skill(
        publisher: str, name: str, identity: Identity
    ) -> dict[str, Any]:
        publication = registry.get_publication(identity.tenant_id, publisher, name)
        markdown = registry.skill_markdown(
            identity.tenant_id,
            publisher,
            name,
            publication.manifest.version,
        )
        versions = [
            item.manifest.version
            for item in registry.list_publications(identity.tenant_id)
            if item.manifest.publisher == publisher and item.manifest.name == name
        ]
        payload = _summary(publication, skill_markdown=markdown)
        payload["versions"] = versions
        return payload

    @router.get("/skills/{publisher}/{name}/versions/{version}")
    async def get_skill_version(
        publisher: str, name: str, version: str, identity: Identity
    ) -> dict[str, Any]:
        publication = registry.get_publication(
            identity.tenant_id, publisher, name, version
        )
        markdown = registry.skill_markdown(
            identity.tenant_id, publisher, name, version
        )
        return _summary(publication, skill_markdown=markdown)

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
        return {"installation": _installation_summary(installation)}

    @router.get(
        "/skill-publications/{publisher}/{name}/versions/{version}"
    )
    async def get_skill_publication_state(
        publisher: str,
        name: str,
        version: str,
        identity: Identity,
    ) -> dict[str, Any]:
        if management_service is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Skill management service is not configured",
            )
        publication = await management_service.get_publication(
            identity.tenant_id,
            publisher,
            name,
            version,
        )
        return {"publication": _publication_state_summary(publication)}

    @router.post(
        "/skill-publications",
        status_code=status.HTTP_201_CREATED,
    )
    async def publish_skill(
        payload: PublishSkillRequest,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(
            default=0, alias="X-Expected-Revision", ge=0
        ),
    ) -> dict[str, Any]:
        if publication_service is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Skill publication service is not configured",
            )
        encoded_size = sum(len(value) for value in payload.files.values())
        if encoded_size > _MAX_ENCODED_UPLOAD_BYTES:
            raise HTTPException(status_code=413, detail="Skill package is too large")
        try:
            files = {
                path: base64.b64decode(content, validate=True)
                for path, content in payload.files.items()
            }
        except (binascii.Error, ValueError) as exc:
            raise HTTPException(
                status_code=422,
                detail="Skill package files must use valid base64",
            ) from exc
        package = SkillPackage.from_files(files)
        publication = await publication_service.publish(
            PublishSkillCommand(
                tenant_id=identity.tenant_id,
                actor_id=identity.actor.id,
                source_id=payload.source_id,
                activate=payload.activate,
                command_id=command_id,
                expected_revision=expected_revision,
                correlation_id=identity.correlation_id,
                causation_id=command_id,
            ),
            package,
        )
        return _summary(publication)

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
        return await _change_installation(
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
        return await _change_installation(
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
        return await _change_installation(
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
    ) -> dict[str, Any]:
        return await _change_installation(
            management_service,
            identity,
            publisher,
            name,
            SkillInstallationOperation.UNINSTALL,
            command_id,
            expected_revision,
            reason_code,
        )

    @router.post(
        "/skill-publications/{publisher}/{name}/versions/{version}:revoke",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def revoke_skill_publication(
        publisher: str,
        name: str,
        version: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision", ge=1),
        reason_code: str = Header(alias="X-Reason-Code", min_length=1, max_length=128),
    ) -> dict[str, Any]:
        if management_service is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Skill management service is not configured",
            )
        publication = await management_service.revoke_publication(
            RevokeSkillPublicationCommand(
                tenant_id=identity.tenant_id,
                actor_id=identity.actor.id,
                publisher=publisher,
                name=name,
                version=version,
                reason_code=reason_code,
                command_id=command_id,
                expected_revision=expected_revision,
                correlation_id=identity.correlation_id,
                causation_id=command_id,
            )
        )
        return {
            "publication": _publication_state_summary(publication)
        }

    return router


async def _change_installation(
    service: SkillManager | None,
    identity: RequestIdentity,
    publisher: str,
    name: str,
    operation: SkillInstallationOperation,
    command_id: str,
    expected_revision: int,
    reason_code: str | None = None,
) -> dict[str, Any]:
    if service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Skill management service is not configured",
        )
    installation = await service.change_installation(
        ChangeSkillInstallationCommand(
            tenant_id=identity.tenant_id,
            actor_id=identity.actor.id,
            publisher=publisher,
            name=name,
            operation=operation,
            reason_code=reason_code,
            command_id=command_id,
            expected_revision=expected_revision,
            correlation_id=identity.correlation_id,
            causation_id=command_id,
        )
    )
    return {
        "installation": _installation_summary(installation)
    }


def _installation_summary(
    installation: SkillInstallationRecord,
) -> dict[str, Any]:
    return {
        "publisher": installation.publisher,
        "name": installation.name,
        "version_constraint": installation.version_constraint,
        "pinned_package_digest": installation.pinned_package_digest,
        "status": installation.status.value,
        "revision": installation.revision,
        "reason_code": installation.reason_code,
        "updated_by": installation.updated_by,
        "updated_at": installation.updated_at.isoformat(),
    }


def _publication_state_summary(
    publication: SkillPublicationRecord,
) -> dict[str, Any]:
    return {
        "publisher": publication.publisher,
        "name": publication.name,
        "version": publication.version,
        "package_digest": publication.package_digest,
        "status": publication.status.value,
        "revision": publication.revision,
        "reason_code": publication.reason_code,
        "updated_by": publication.updated_by,
        "updated_at": publication.updated_at.isoformat(),
    }
