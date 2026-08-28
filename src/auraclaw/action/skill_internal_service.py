from __future__ import annotations

import base64
import binascii

from auraclaw.action.skill_packages import SkillPackage
from auraclaw.action.skill_publication import SkillPublicationService
from auraclaw.action.skill_rebuild import SkillStateRebuilder
from auraclaw.contracts.errors import AuthorizationError, SchemaValidationError
from auraclaw.contracts.internal import (
    ServiceIdentity,
    SkillPublishInternalRequest,
    SkillPublishInternalResponse,
)
from auraclaw.contracts.skills import PublishSkillCommand

_MAX_ENCODED_PACKAGE_BYTES = 24 * 1024 * 1024


class SkillPublicationInternalService:
    def __init__(
        self,
        publication: SkillPublicationService,
        *,
        rebuilder: SkillStateRebuilder | None = None,
    ) -> None:
        self._publication = publication
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
