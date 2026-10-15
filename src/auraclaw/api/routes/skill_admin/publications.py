from __future__ import annotations

import base64
import binascii
import hashlib
from typing import Annotated, Any

from fastapi import APIRouter, Header, HTTPException, Query, Request, status

from auraclaw.action.skill_admin_queries import SkillAdminManagementQueryService
from auraclaw.action.skill_packages import SkillPackage
from auraclaw.api.routes.skill_admin.contracts import (
    _MAX_ENCODED_UPLOAD_BYTES,
    Identity,
    PublishSkillRequest,
    SkillManager,
    SkillPackageUploadManager,
    SkillPublisher,
)
from auraclaw.api.routes.skill_admin.presentation import (
    package_state_summary,
    paginate,
    publication_state_summary,
    require_management_service,
    summary,
)
from auraclaw.contracts.skills import (
    PublishSkillCommand,
    PurgeSkillPackageCommand,
    RestoreSkillPublicationCommand,
    RevokeSkillPublicationCommand,
    SkillRevocationAction,
)
from auraclaw.contracts.tools import ArtifactRef


def create_publications_router(
    *,
    publication_service: SkillPublisher | None,
    management_query: SkillAdminManagementQueryService | None,
    management_service: SkillManager | None,
    upload_service: SkillPackageUploadManager | None,
) -> APIRouter:
    router = APIRouter()

    @router.get("/skill-publications")
    async def list_skill_publications(
        identity: Identity,
        status_filter: Annotated[str | None, Query(alias="status", max_length=64)] = None,
        publisher: Annotated[str | None, Query(max_length=128)] = None,
        name: Annotated[str | None, Query(max_length=256)] = None,
        cursor: Annotated[str | None, Query(max_length=2048)] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
    ) -> dict[str, Any]:
        require_management_service(management_service)
        assert management_query is not None
        records = [
            publication_state_summary(item)
            for item in await management_query.list_publications(
                identity.tenant_id, status=status_filter, publisher=publisher, name=name
            )
        ]
        page, next_cursor = paginate(
            records,
            cursor=cursor,
            limit=limit,
            key=lambda item: (
                str(item["publisher"]),
                str(item["name"]),
                str(item["version"]),
            ),
        )
        return {"publications": page, "next_cursor": next_cursor}

    @router.get("/skill-packages")
    async def list_skill_packages(
        identity: Identity,
        retention_status: Annotated[str | None, Query(max_length=64)] = None,
        publisher: Annotated[str | None, Query(max_length=128)] = None,
        name: Annotated[str | None, Query(max_length=256)] = None,
        legal_hold: bool | None = None,
        cursor: Annotated[str | None, Query(max_length=2048)] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
    ) -> dict[str, Any]:
        require_management_service(management_service)
        assert management_query is not None
        records = [
            package_state_summary(item)
            for item in await management_query.list_packages(
                identity.tenant_id,
                retention_status=retention_status,
                publisher=publisher,
                name=name,
                legal_hold=legal_hold,
            )
        ]
        page, next_cursor = paginate(
            records,
            cursor=cursor,
            limit=limit,
            key=lambda item: (
                str(item["publisher"]),
                str(item["name"]),
                str(item["version"]),
            ),
        )
        return {"packages": page, "next_cursor": next_cursor}

    @router.get("/skill-publications/{publisher}/{name}/versions/{version}")
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
        return {"publication": publication_state_summary(publication)}

    @router.get("/skill-packages/{publisher}/{name}/versions/{version}")
    async def get_skill_package_state(
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
        package = await management_service.get_package(identity.tenant_id, publisher, name, version)
        return {"package": package_state_summary(package)}

    @router.post(
        "/skill-package-uploads",
        status_code=status.HTTP_201_CREATED,
    )
    async def proxy_skill_package_upload(
        request: Request,
        identity: Identity,
        command_id: Annotated[
            str,
            Header(alias="Idempotency-Key", min_length=1, max_length=256),
        ],
        upload_name: Annotated[
            str,
            Header(alias="X-Upload-Name", min_length=1, max_length=512),
        ],
        expected_checksum: Annotated[
            str,
            Header(alias="X-Content-SHA256", pattern=r"^[0-9a-f]{64}$"),
        ],
    ) -> dict[str, Any]:
        if upload_service is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Skill package upload service is not configured",
            )
        if request.headers.get("content-type", "").split(";", 1)[0].strip() != (
            "application/vnd.auraclaw.skill-package+json"
        ):
            raise HTTPException(
                status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                detail="Skill package upload content type is invalid",
            )
        content = bytearray()
        async for chunk in request.stream():
            if len(content) + len(chunk) > _MAX_ENCODED_UPLOAD_BYTES:
                raise HTTPException(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    detail="Skill package upload exceeds 24 MiB",
                )
            content.extend(chunk)
        if not content:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="Skill package upload is empty",
            )
        checksum = hashlib.sha256(content).hexdigest()
        if checksum != expected_checksum:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="Skill package upload checksum mismatch",
            )
        result = await upload_service.stage(
            tenant_id=identity.tenant_id,
            name=upload_name,
            content=bytes(content),
            checksum=checksum,
            correlation_id=identity.correlation_id,
            command_id=command_id,
        )
        return result.model_dump(mode="json")

    @router.post(
        "/skill-publications",
        status_code=status.HTTP_201_CREATED,
    )
    async def publish_skill(
        payload: PublishSkillRequest,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(default=0, alias="X-Expected-Revision", ge=0),
    ) -> dict[str, Any]:
        if publication_service is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Skill publication service is not configured",
            )
        command = PublishSkillCommand(
            tenant_id=identity.tenant_id,
            actor_id=identity.actor.id,
            activate=payload.activate,
            expected_installation_revision=payload.expected_installation_revision,
            command_id=command_id,
            expected_revision=expected_revision,
            correlation_id=identity.correlation_id,
            causation_id=command_id,
        )
        if payload.files is not None:
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
            publication = await publication_service.publish(command, SkillPackage.from_files(files))
        else:
            assert payload.artifact_ref is not None
            assert payload.expected_digest is not None
            publication = await publication_service.publish_artifact(
                command,
                ArtifactRef(**payload.artifact_ref),
                payload.expected_digest,
            )
        return summary(publication)

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
        revocation_action: Annotated[
            SkillRevocationAction,
            Header(alias="X-Skill-Revocation-Action"),
        ] = SkillRevocationAction.CANCEL,
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
                revocation_action=revocation_action,
                command_id=command_id,
                expected_revision=expected_revision,
                correlation_id=identity.correlation_id,
                causation_id=command_id,
            )
        )
        return {"publication": publication_state_summary(publication)}

    @router.post(
        "/skill-publications/{publisher}/{name}/versions/{version}:restore",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def restore_skill_publication(
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
        publication = await management_service.restore_publication(
            RestoreSkillPublicationCommand(
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
        return {"publication": publication_state_summary(publication)}

    @router.post(
        "/skill-packages/{publisher}/{name}/versions/{version}:purge",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def purge_skill_package(
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
        package = await management_service.purge_package(
            PurgeSkillPackageCommand(
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
        return {"package": package_state_summary(package)}

    return router
