from __future__ import annotations

import base64
import binascii

from auraclaw.action.skill_management import SkillManagementService
from auraclaw.action.skill_packages import SkillPackage
from auraclaw.action.skill_publication import SkillPublicationService
from auraclaw.action.skill_rebuild import SkillStateRebuilder
from auraclaw.contracts.errors import AuthorizationError, SchemaValidationError
from auraclaw.contracts.internal import (
    ServiceIdentity,
    SkillInstallationInternalRequest,
    SkillInstallationInternalResponse,
    SkillPublishInternalRequest,
    SkillPublishInternalResponse,
    SkillRevokeInternalRequest,
    SkillRevokeInternalResponse,
    SkillStateInternalRequest,
    SkillStateInternalResponse,
)
from auraclaw.contracts.skills import (
    ChangeSkillInstallationCommand,
    PublishSkillCommand,
    RevokeSkillPublicationCommand,
    SkillInstallationOperation,
)

_MAX_ENCODED_PACKAGE_BYTES = 24 * 1024 * 1024


class SkillPublicationInternalService:
    def __init__(
        self,
        publication: SkillPublicationService,
        *,
        management: SkillManagementService | None = None,
        rebuilder: SkillStateRebuilder | None = None,
    ) -> None:
        self._publication = publication
        self._management = management
        self._rebuilder = rebuilder

    async def publish(
        self, request: SkillPublishInternalRequest
    ) -> SkillPublishInternalResponse:
        if request.context.service_identity is not ServiceIdentity.TASK_API:
            raise AuthorizationError("workload may not publish Skill packages")
        if request.context.request_id != request.command_id:
            raise SchemaValidationError("Skill command request id does not match")
        if sum(len(content) for content in request.files.values()) > (
            _MAX_ENCODED_PACKAGE_BYTES
        ):
            raise SchemaValidationError("Skill package is too large")
        try:
            files = {
                path: base64.b64decode(content, validate=True)
                for path, content in request.files.items()
            }
        except (binascii.Error, ValueError) as exc:
            raise SchemaValidationError(
                "Skill package files must use valid base64"
            ) from exc
        result = await self._publication.publish(
            PublishSkillCommand(
                tenant_id=request.context.tenant_id,
                actor_id=request.actor_id,
                source_id=request.source_id,
                activate=request.activate,
                command_id=request.command_id,
                expected_revision=request.expected_revision,
                correlation_id=request.context.correlation_id,
                causation_id=request.context.causation_id,
            ),
            SkillPackage.from_files(files),
        )
        if self._rebuilder is not None:
            await self._rebuilder.rebuild_tenant(request.context.tenant_id)
        return SkillPublishInternalResponse(
            publication=result.model_dump(mode="json")
        )

    async def change_installation(
        self,
        request: SkillInstallationInternalRequest,
    ) -> SkillInstallationInternalResponse:
        self._validate_management_request(
            request.context.service_identity,
            request.context.request_id,
            request.command_id,
        )
        if self._management is None:
            raise SchemaValidationError("Skill management service is not configured")
        result = await self._management.change_installation(
            ChangeSkillInstallationCommand(
                tenant_id=request.context.tenant_id,
                actor_id=request.actor_id,
                publisher=request.publisher,
                name=request.name,
                operation=SkillInstallationOperation(request.operation),
                reason_code=request.reason_code,
                command_id=request.command_id,
                expected_revision=request.expected_revision,
                correlation_id=request.context.correlation_id,
                causation_id=request.context.causation_id,
            )
        )
        return SkillInstallationInternalResponse(
            installation=result.model_dump(mode="json")
        )

    async def revoke(
        self,
        request: SkillRevokeInternalRequest,
    ) -> SkillRevokeInternalResponse:
        self._validate_management_request(
            request.context.service_identity,
            request.context.request_id,
            request.command_id,
        )
        if self._management is None:
            raise SchemaValidationError("Skill management service is not configured")
        result = await self._management.revoke_publication(
            RevokeSkillPublicationCommand(
                tenant_id=request.context.tenant_id,
                actor_id=request.actor_id,
                publisher=request.publisher,
                name=request.name,
                version=request.version,
                reason_code=request.reason_code,
                command_id=request.command_id,
                expected_revision=request.expected_revision,
                correlation_id=request.context.correlation_id,
                causation_id=request.context.causation_id,
            )
        )
        return SkillRevokeInternalResponse(
            publication=result.model_dump(mode="json")
        )

    @staticmethod
    def _validate_management_request(
        identity: ServiceIdentity,
        request_id: str,
        command_id: str,
    ) -> None:
        if identity is not ServiceIdentity.TASK_API:
            raise AuthorizationError("workload may not manage Skills")
        if request_id != command_id:
            raise SchemaValidationError("Skill command request id does not match")

    async def state(
        self,
        request: SkillStateInternalRequest,
    ) -> SkillStateInternalResponse:
        if request.context.service_identity is not ServiceIdentity.TASK_API:
            raise AuthorizationError("workload may not query Skill state")
        if self._management is None:
            raise SchemaValidationError("Skill management service is not configured")
        installation = None
        publication = None
        if request.version is None:
            installation = await self._management.get_installation(
                request.context.tenant_id,
                request.publisher,
                request.name,
            )
        else:
            publication = await self._management.get_publication(
                request.context.tenant_id,
                request.publisher,
                request.name,
                request.version,
            )
        return SkillStateInternalResponse(
            installation=(
                None
                if installation is None
                else installation.model_dump(mode="json")
            ),
            publication=(
                None
                if publication is None
                else publication.model_dump(mode="json")
            ),
        )
