from __future__ import annotations

import base64
import binascii
from typing import Annotated, Any, Protocol

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import Field, model_validator

from auraclaw.action.skill_packages import SkillPackage, SkillPackageRegistry
from auraclaw.api.dependencies import RequestIdentity, request_identity
from auraclaw.contracts.internal import (
    ArtifactFinalizeResponse,
    ArtifactUploadResponse,
    ContractModel,
)
from auraclaw.contracts.skills import (
    ChangeSkillInstallationCommand,
    PublishedSkill,
    PublishSkillCommand,
    PurgeSkillPackageCommand,
    RegisterSkillPublisherCommand,
    RevokeSkillPublicationCommand,
    RevokeSkillPublisherKeyCommand,
    RotateSkillPublisherKeyCommand,
    SkillInstallationOperation,
    SkillInstallationRecord,
    SkillPackageRecord,
    SkillPublicationRecord,
    SkillPublisherKeyRecord,
    SkillPublisherRecord,
)
from auraclaw.contracts.tools import ArtifactRef

Identity = Annotated[RequestIdentity, Depends(request_identity)]

_MAX_UPLOAD_FILES = 512
_MAX_ENCODED_UPLOAD_BYTES = 24 * 1024 * 1024


class PublishSkillRequest(ContractModel):
    source_id: str = Field(min_length=1, max_length=128)
    activate: bool = True
    files: dict[str, str] | None = Field(
        default=None, min_length=1, max_length=_MAX_UPLOAD_FILES
    )
    artifact_ref: dict[str, Any] | None = None
    expected_digest: str | None = Field(
        default=None, pattern=r"^sha256:[0-9a-f]{64}$"
    )

    @model_validator(mode="after")
    def validate_source(self) -> PublishSkillRequest:
        direct = self.files is not None
        staged = self.artifact_ref is not None or self.expected_digest is not None
        if direct == staged:
            raise ValueError("Supply either files or artifact_ref with expected_digest")
        if staged and (self.artifact_ref is None or self.expected_digest is None):
            raise ValueError("Staged publication requires artifact_ref and expected_digest")
        return self


class CreateSkillPackageUploadRequest(ContractModel):
    name: str = Field(min_length=1, max_length=512)
    expected_size: int = Field(ge=1, le=_MAX_ENCODED_UPLOAD_BYTES)
    expected_checksum: str = Field(pattern=r"^[0-9a-f]{64}$")


class FinalizeSkillPackageUploadRequest(ContractModel):
    upload_id: str = Field(min_length=1, max_length=256)
    version: int = Field(default=1, ge=1)
    size: int = Field(ge=1, le=_MAX_ENCODED_UPLOAD_BYTES)
    checksum: str = Field(pattern=r"^[0-9a-f]{64}$")
    parts: tuple[dict[str, object], ...] = Field(default=(), max_length=10_000)


class RegisterSkillPublisherRequest(ContractModel):
    display_name: str = Field(min_length=1, max_length=256)


class RotateSkillPublisherKeyRequest(ContractModel):
    key_id: str = Field(min_length=1, max_length=128)
    public_key: str = Field(min_length=43, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")


class SkillPublisher(Protocol):
    async def publish(
        self, command: PublishSkillCommand, package: SkillPackage
    ) -> PublishedSkill: ...

    async def publish_artifact(
        self,
        command: PublishSkillCommand,
        artifact_ref: ArtifactRef,
        expected_digest: str,
    ) -> PublishedSkill: ...


class SkillPackageUploadManager(Protocol):
    async def create(
        self,
        *,
        tenant_id: str,
        name: str,
        expected_size: int,
        expected_checksum: str,
        correlation_id: str,
        command_id: str,
    ) -> ArtifactUploadResponse: ...

    async def finalize(
        self,
        *,
        tenant_id: str,
        artifact_id: str,
        version: int,
        upload_id: str,
        size: int,
        checksum: str,
        parts: tuple[dict[str, object], ...],
        correlation_id: str,
        command_id: str,
    ) -> ArtifactFinalizeResponse: ...


class SkillManager(Protocol):
    async def get_package(
        self,
        tenant_id: str,
        publisher: str,
        name: str,
        version: str,
    ) -> SkillPackageRecord: ...

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

    async def purge_package(
        self, command: PurgeSkillPackageCommand
    ) -> SkillPackageRecord: ...


class SkillPublisherManager(Protocol):
    async def register_publisher(
        self, command: RegisterSkillPublisherCommand
    ) -> tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]]: ...

    async def rotate_publisher_key(
        self, command: RotateSkillPublisherKeyCommand
    ) -> tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]]: ...

    async def revoke_publisher_key(
        self, command: RevokeSkillPublisherKeyCommand
    ) -> tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]]: ...

    async def get_publisher(
        self, tenant_id: str, publisher: str
    ) -> tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]]: ...

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
    upload_service: SkillPackageUploadManager | None = None,
    publisher_service: SkillPublisherManager | None = None,
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

    @router.get("/skill-publishers/{publisher}")
    async def get_skill_publisher(
        publisher: str, identity: Identity
    ) -> dict[str, Any]:
        service = _require_publisher_service(publisher_service)
        record, keys = await service.get_publisher(identity.tenant_id, publisher)
        return _publisher_summary(record, keys)

    @router.post("/skill-publishers/{publisher}", status_code=status.HTTP_201_CREATED)
    async def register_skill_publisher(
        publisher: str,
        payload: RegisterSkillPublisherRequest,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(
            default=0, alias="X-Expected-Revision", ge=0
        ),
    ) -> dict[str, Any]:
        service = _require_publisher_service(publisher_service)
        record, keys = await service.register_publisher(
            RegisterSkillPublisherCommand(
                tenant_id=identity.tenant_id,
                actor_id=identity.actor.id,
                publisher=publisher,
                display_name=payload.display_name,
                command_id=command_id,
                expected_revision=expected_revision,
                correlation_id=identity.correlation_id,
                causation_id=command_id,
            )
        )
        return _publisher_summary(record, keys)

    @router.post(
        "/skill-publishers/{publisher}/keys:rotate",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def rotate_skill_publisher_key(
        publisher: str,
        payload: RotateSkillPublisherKeyRequest,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision", ge=1),
    ) -> dict[str, Any]:
        service = _require_publisher_service(publisher_service)
        record, keys = await service.rotate_publisher_key(
            RotateSkillPublisherKeyCommand(
                tenant_id=identity.tenant_id,
                actor_id=identity.actor.id,
                publisher=publisher,
                key_id=payload.key_id,
                public_key=payload.public_key,
                command_id=command_id,
                expected_revision=expected_revision,
                correlation_id=identity.correlation_id,
                causation_id=command_id,
            )
        )
        return _publisher_summary(record, keys)

    @router.post(
        "/skill-publishers/{publisher}/keys/{key_id}:revoke",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def revoke_skill_publisher_key(
        publisher: str,
        key_id: str,
        identity: Identity,
        command_id: str = Header(alias="Idempotency-Key"),
        expected_revision: int = Header(alias="X-Expected-Revision", ge=1),
        reason_code: str = Header(alias="X-Reason-Code", min_length=1, max_length=128),
    ) -> dict[str, Any]:
        service = _require_publisher_service(publisher_service)
        record, keys = await service.revoke_publisher_key(
            RevokeSkillPublisherKeyCommand(
                tenant_id=identity.tenant_id,
                actor_id=identity.actor.id,
                publisher=publisher,
                key_id=key_id,
                reason_code=reason_code,
                command_id=command_id,
                expected_revision=expected_revision,
                correlation_id=identity.correlation_id,
                causation_id=command_id,
            )
        )
        return _publisher_summary(record, keys)

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
        package = await management_service.get_package(
            identity.tenant_id, publisher, name, version
        )
        return {"package": _package_state_summary(package)}

    @router.post(
        "/skill-package-uploads",
        status_code=status.HTTP_201_CREATED,
    )
    async def create_skill_package_upload(
        payload: CreateSkillPackageUploadRequest,
        identity: Identity,
        command_id: Annotated[
            str,
            Header(alias="Idempotency-Key", min_length=1, max_length=256),
        ],
    ) -> dict[str, Any]:
        if upload_service is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Skill package upload service is not configured",
            )
        result = await upload_service.create(
            tenant_id=identity.tenant_id,
            name=payload.name,
            expected_size=payload.expected_size,
            expected_checksum=payload.expected_checksum,
            correlation_id=identity.correlation_id,
            command_id=command_id,
        )
        return result.model_dump(mode="json")

    @router.post(
        "/skill-package-uploads/{artifact_id}:finalize",
    )
    async def finalize_skill_package_upload(
        artifact_id: str,
        payload: FinalizeSkillPackageUploadRequest,
        identity: Identity,
        command_id: Annotated[
            str,
            Header(alias="Idempotency-Key", min_length=1, max_length=256),
        ],
    ) -> dict[str, Any]:
        if upload_service is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Skill package upload service is not configured",
            )
        result = await upload_service.finalize(
            tenant_id=identity.tenant_id,
            artifact_id=artifact_id,
            version=payload.version,
            upload_id=payload.upload_id,
            size=payload.size,
            checksum=payload.checksum,
            parts=payload.parts,
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
        expected_revision: int = Header(
            default=0, alias="X-Expected-Revision", ge=0
        ),
    ) -> dict[str, Any]:
        if publication_service is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Skill publication service is not configured",
            )
        command = PublishSkillCommand(
            tenant_id=identity.tenant_id,
            actor_id=identity.actor.id,
            source_id=payload.source_id,
            activate=payload.activate,
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
            publication = await publication_service.publish(
                command, SkillPackage.from_files(files)
            )
        else:
            assert payload.artifact_ref is not None
            assert payload.expected_digest is not None
            publication = await publication_service.publish_artifact(
                command,
                ArtifactRef(**payload.artifact_ref),
                payload.expected_digest,
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
        return {"package": _package_state_summary(package)}

    return router


def _require_publisher_service(
    service: SkillPublisherManager | None,
) -> SkillPublisherManager:
    if service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Skill Publisher Registry is not configured",
        )
    return service


def _publisher_summary(
    publisher: SkillPublisherRecord,
    keys: tuple[SkillPublisherKeyRecord, ...],
) -> dict[str, Any]:
    return {
        "publisher": publisher.model_dump(mode="json"),
        "keys": [key.model_dump(mode="json") for key in keys],
    }


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


def _package_state_summary(package: SkillPackageRecord) -> dict[str, Any]:
    return {
        "publisher": package.manifest.publisher,
        "name": package.manifest.name,
        "version": package.manifest.version,
        "package_digest": package.package_digest,
        "retention_status": package.retention_status.value,
        "retention_until": package.retention_until.isoformat(),
        "legal_hold": package.legal_hold,
        "retention_revision": package.retention_revision,
        "retention_updated_by": package.retention_updated_by,
        "retention_updated_at": package.retention_updated_at.isoformat(),
        "purged_at": (
            None if package.purged_at is None else package.purged_at.isoformat()
        ),
    }
