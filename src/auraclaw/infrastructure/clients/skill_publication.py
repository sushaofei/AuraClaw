from __future__ import annotations

import base64
from contextlib import suppress
from uuid import uuid4

import httpx

from auraclaw.action.skill_packages import SkillPackage, SkillPackageRegistry
from auraclaw.contracts.errors import NotFoundError
from auraclaw.contracts.internal import (
    InternalRequestContext,
    ServiceIdentity,
    SkillInstallationInternalRequest,
    SkillInstallationInternalResponse,
    SkillPackageStateInternalRequest,
    SkillPackageStateInternalResponse,
    SkillPublishInternalRequest,
    SkillPublishInternalResponse,
    SkillPurgeInternalRequest,
    SkillPurgeInternalResponse,
    SkillRevokeInternalRequest,
    SkillRevokeInternalResponse,
    SkillStateInternalRequest,
    SkillStateInternalResponse,
)
from auraclaw.contracts.skills import (
    ChangeSkillInstallationCommand,
    PublishedSkill,
    PublishSkillCommand,
    PurgeSkillPackageCommand,
    RevokeSkillPublicationCommand,
    SkillInstallationRecord,
    SkillInstallationStatus,
    SkillPackageRecord,
    SkillPublicationRecord,
)
from auraclaw.internal.http import HttpContractClient


class RemoteSkillPublicationClient:
    """Task API port; Action Hands owns package admission and Artifact writes."""

    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str,
        timeout: float = 60.0,
        transport: httpx.AsyncBaseTransport | None = None,
        compatibility_cache: SkillPackageRegistry | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url, timeout=timeout, transport=transport
        )
        self._contract = HttpContractClient(self._client, bearer_token=bearer_token)
        self._compatibility_cache = compatibility_cache

    async def aclose(self) -> None:
        await self._client.aclose()

    async def publish(
        self, command: PublishSkillCommand, package: SkillPackage
    ) -> PublishedSkill:
        response = await self._contract.call(
            "/internal/v1/skill-publications/publish",
            SkillPublishInternalRequest(
                context=InternalRequestContext(
                    tenant_id=command.tenant_id,
                    service_identity=ServiceIdentity.TASK_API,
                    request_id=command.command_id,
                    correlation_id=command.correlation_id,
                    causation_id=command.causation_id,
                ),
                actor_id=command.actor_id,
                source_id=command.source_id,
                activate=command.activate,
                command_id=command.command_id,
                expected_revision=command.expected_revision,
                files={
                    path: base64.b64encode(content).decode()
                    for path, content in package.files.items()
                },
            ),
            SkillPublishInternalResponse,
        )
        publication = PublishedSkill.model_validate(response.publication)
        if self._compatibility_cache is not None:
            publication = self._compatibility_cache.restore(
                command.tenant_id, package, publication
            )
        return publication

    async def change_installation(
        self,
        command: ChangeSkillInstallationCommand,
    ) -> SkillInstallationRecord:
        response = await self._contract.call(
            "/internal/v1/skill-publications/installation",
            SkillInstallationInternalRequest(
                context=_context(command),
                actor_id=command.actor_id,
                publisher=command.publisher,
                name=command.name,
                operation=command.operation.value,
                reason_code=command.reason_code,
                command_id=command.command_id,
                expected_revision=command.expected_revision,
            ),
            SkillInstallationInternalResponse,
        )
        installation = SkillInstallationRecord.model_validate(response.installation)
        if self._compatibility_cache is not None:
            with suppress(NotFoundError):
                self._compatibility_cache.set_skill_discoverable(
                    command.tenant_id,
                    command.publisher,
                    command.name,
                    discoverable=(
                        installation.status is SkillInstallationStatus.ACTIVE
                    ),
                )
        return installation

    async def revoke_publication(
        self,
        command: RevokeSkillPublicationCommand,
    ) -> SkillPublicationRecord:
        response = await self._contract.call(
            "/internal/v1/skill-publications/revoke",
            SkillRevokeInternalRequest(
                context=_context(command),
                actor_id=command.actor_id,
                publisher=command.publisher,
                name=command.name,
                version=command.version,
                reason_code=command.reason_code,
                command_id=command.command_id,
                expected_revision=command.expected_revision,
            ),
            SkillRevokeInternalResponse,
        )
        publication = SkillPublicationRecord.model_validate(response.publication)
        if self._compatibility_cache is not None:
            with suppress(NotFoundError):
                self._compatibility_cache.revoke(
                    command.tenant_id,
                    command.publisher,
                    command.name,
                    command.version,
                )
        return publication

    async def get_package(
        self,
        tenant_id: str,
        publisher: str,
        name: str,
        version: str,
    ) -> SkillPackageRecord:
        request_id = f"skill-package-state-{uuid4().hex}"
        response = await self._contract.call(
            "/internal/v1/skill-publications/package",
            SkillPackageStateInternalRequest(
                context=InternalRequestContext(
                    tenant_id=tenant_id,
                    service_identity=ServiceIdentity.TASK_API,
                    request_id=request_id,
                    correlation_id=request_id,
                    causation_id=request_id,
                ),
                publisher=publisher,
                name=name,
                version=version,
            ),
            SkillPackageStateInternalResponse,
        )
        return SkillPackageRecord.model_validate(response.package)

    async def purge_package(
        self, command: PurgeSkillPackageCommand
    ) -> SkillPackageRecord:
        response = await self._contract.call(
            "/internal/v1/skill-publications/purge",
            SkillPurgeInternalRequest(
                context=_context(command),
                actor_id=command.actor_id,
                publisher=command.publisher,
                name=command.name,
                version=command.version,
                reason_code=command.reason_code,
                command_id=command.command_id,
                expected_revision=command.expected_revision,
            ),
            SkillPurgeInternalResponse,
        )
        return SkillPackageRecord.model_validate(response.package)

    async def get_installation(
        self,
        tenant_id: str,
        publisher: str,
        name: str,
    ) -> SkillInstallationRecord:
        response = await self._state(
            tenant_id=tenant_id,
            publisher=publisher,
            name=name,
        )
        if response.installation is None:
            raise NotFoundError("Skill installation not found")
        return SkillInstallationRecord.model_validate(response.installation)

    async def get_publication(
        self,
        tenant_id: str,
        publisher: str,
        name: str,
        version: str,
    ) -> SkillPublicationRecord:
        response = await self._state(
            tenant_id=tenant_id,
            publisher=publisher,
            name=name,
            version=version,
        )
        if response.publication is None:
            raise NotFoundError("Skill publication not found")
        return SkillPublicationRecord.model_validate(response.publication)

    async def _state(
        self,
        *,
        tenant_id: str,
        publisher: str,
        name: str,
        version: str | None = None,
    ) -> SkillStateInternalResponse:
        request_id = f"skill-state-{uuid4().hex}"
        return await self._contract.call(
            "/internal/v1/skill-publications/state",
            SkillStateInternalRequest(
                context=InternalRequestContext(
                    tenant_id=tenant_id,
                    service_identity=ServiceIdentity.TASK_API,
                    request_id=request_id,
                    correlation_id=request_id,
                    causation_id=request_id,
                ),
                publisher=publisher,
                name=name,
                version=version,
            ),
            SkillStateInternalResponse,
        )


def _context(
    command: (
        ChangeSkillInstallationCommand
        | RevokeSkillPublicationCommand
        | PurgeSkillPackageCommand
    ),
) -> InternalRequestContext:
    return InternalRequestContext(
        tenant_id=command.tenant_id,
        service_identity=ServiceIdentity.TASK_API,
        request_id=command.command_id,
        correlation_id=command.correlation_id,
        causation_id=command.causation_id,
    )
