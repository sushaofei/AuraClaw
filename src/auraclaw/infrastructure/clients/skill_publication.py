from __future__ import annotations

import base64

import httpx

from auraclaw.action.skill_packages import SkillPackage, SkillPackageRegistry
from auraclaw.contracts.internal import (
    InternalRequestContext,
    ServiceIdentity,
    SkillPublishInternalRequest,
    SkillPublishInternalResponse,
)
from auraclaw.contracts.skills import PublishedSkill, PublishSkillCommand
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
