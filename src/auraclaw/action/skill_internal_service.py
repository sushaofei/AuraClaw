from __future__ import annotations

import base64
import binascii
from dataclasses import asdict
from typing import Any

from auraclaw.action.ports import ArtifactContentReader
from auraclaw.action.skill_content_cache import SkillPackageContentCache
from auraclaw.action.skill_lifecycle import SkillLifecycleStore
from auraclaw.action.skill_lifecycle_events import SkillTenantRebuilder
from auraclaw.action.skill_management import SkillManagementService
from auraclaw.action.skill_packages import (
    SkillPackage,
    skill_package_digest,
    skill_package_from_archive,
)
from auraclaw.action.skill_publication import SkillPublicationService
from auraclaw.action.skill_publishers import SkillPublisherService
from auraclaw.contracts.errors import (
    AuthorizationError,
    SchemaValidationError,
    VersionConflictError,
)
from auraclaw.contracts.internal import (
    ServiceIdentity,
    SkillAdminSnapshotInternalRequest,
    SkillAdminSnapshotInternalResponse,
    SkillAdmissionListInternalRequest,
    SkillAdmissionListInternalResponse,
    SkillAdmissionMetricsInternalRequest,
    SkillAdmissionMetricsInternalResponse,
    SkillInstallationInternalRequest,
    SkillInstallationInternalResponse,
    SkillPackageStateInternalRequest,
    SkillPackageStateInternalResponse,
    SkillPublishArtifactInternalRequest,
    SkillPublisherInternalResponse,
    SkillPublisherRegisterInternalRequest,
    SkillPublisherRevokeKeyInternalRequest,
    SkillPublisherRotateKeyInternalRequest,
    SkillPublisherStateInternalRequest,
    SkillPublisherStatusInternalRequest,
    SkillPublishInternalRequest,
    SkillPublishInternalResponse,
    SkillPurgeInternalRequest,
    SkillPurgeInternalResponse,
    SkillRestoreInternalRequest,
    SkillRestoreInternalResponse,
    SkillRevokeInternalRequest,
    SkillRevokeInternalResponse,
    SkillStateInternalRequest,
    SkillStateInternalResponse,
)
from auraclaw.contracts.skills import (
    ChangeSkillInstallationCommand,
    ChangeSkillPublisherStatusCommand,
    PublishSkillCommand,
    PurgeSkillPackageCommand,
    RegisterSkillPublisherCommand,
    RestoreSkillPublicationCommand,
    RevokeSkillPublicationCommand,
    RevokeSkillPublisherKeyCommand,
    RotateSkillPublisherKeyCommand,
    SkillInstallationOperation,
    SkillPublisherStatusOperation,
    SkillRevocationAction,
)
from auraclaw.contracts.tools import ArtifactRef

_MAX_ENCODED_PACKAGE_BYTES = 24 * 1024 * 1024


class SkillPublicationInternalService:
    def __init__(
        self,
        publication: SkillPublicationService,
        *,
        management: SkillManagementService | None = None,
        rebuilder: SkillTenantRebuilder | None = None,
        publishers: SkillPublisherService | None = None,
        admissions: SkillLifecycleStore | None = None,
        artifacts: ArtifactContentReader | None = None,
        package_cache: SkillPackageContentCache | None = None,
    ) -> None:
        self._publication = publication
        self._management = management
        self._rebuilder = rebuilder
        self._publishers = publishers
        self._admissions = admissions
        self._artifacts = artifacts
        self._package_cache = package_cache


    async def admin_snapshot(
        self, request: SkillAdminSnapshotInternalRequest
    ) -> SkillAdminSnapshotInternalResponse:
        if request.context.service_identity is not ServiceIdentity.TASK_API:
            raise AuthorizationError("workload may not query Skill management state")
        if self._management is None or self._admissions is None:
            raise SchemaValidationError("Skill management query is not configured")
        tenant_id = request.context.tenant_id
        publisher_rows: tuple[dict[str, Any], ...] = ()
        if self._publishers is not None:
            publisher_rows = tuple(
                {
                    "publisher": record.model_dump(mode="json"),
                    "keys": tuple(key.model_dump(mode="json") for key in keys),
                }
                for record, keys in await self._publishers.list_publishers(tenant_id)
            )
        return SkillAdminSnapshotInternalResponse(
            packages=tuple(
                item.model_dump(mode="json")
                for item in await self._management.list_packages(tenant_id)
            ),
            publications=tuple(
                item.model_dump(mode="json")
                for item in await self._management.list_publications(tenant_id)
            ),
            installations=tuple(
                item.model_dump(mode="json")
                for item in await self._management.list_installations(tenant_id)
            ),
            publishers=publisher_rows,
        )

    async def list_admissions(
        self, request: SkillAdmissionListInternalRequest
    ) -> SkillAdmissionListInternalResponse:
        self._validate_admission_reader(request.context.service_identity)
        if self._admissions is None:
            raise SchemaValidationError("Skill admission reader is not configured")
        page = await self._admissions.page_admissions(
            request.context.tenant_id,
            outcome=request.outcome,
            stage=request.stage,
            content_policy_version=request.content_policy_version,
            since=request.since,
            cursor=request.cursor,
            limit=request.limit,
        )
        return SkillAdmissionListInternalResponse(
            admissions=tuple(asdict(record) for record in page.admissions),
            next_cursor=page.next_cursor,
        )

    async def admission_metrics(
        self, request: SkillAdmissionMetricsInternalRequest
    ) -> SkillAdmissionMetricsInternalResponse:
        self._validate_admission_reader(request.context.service_identity)
        if self._admissions is None:
            raise SchemaValidationError("Skill admission reader is not configured")
        metrics = await self._admissions.admission_metrics(
            request.context.tenant_id, since=request.since
        )
        return SkillAdmissionMetricsInternalResponse(
            metrics=tuple(asdict(metric) for metric in metrics)
        )

    @staticmethod
    def _validate_admission_reader(identity: ServiceIdentity) -> None:
        if identity is not ServiceIdentity.TASK_API:
            raise AuthorizationError("workload may not query Skill admissions")

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
        return SkillPublishInternalResponse(publication=result.model_dump(mode="json"))

    async def publish_artifact(
        self, request: SkillPublishArtifactInternalRequest
    ) -> SkillPublishInternalResponse:
        if request.context.service_identity is not ServiceIdentity.TASK_API:
            raise AuthorizationError("workload may not publish Skill packages")
        if request.context.request_id != request.command_id:
            raise SchemaValidationError("Skill command request id does not match")
        result = await self._publication.publish_artifact(
            PublishSkillCommand(
                tenant_id=request.context.tenant_id,
                actor_id=request.actor_id,
                activate=request.activate,
                command_id=request.command_id,
                expected_revision=request.expected_revision,
                correlation_id=request.context.correlation_id,
                causation_id=request.context.causation_id,
            ),
            ArtifactRef(**request.artifact_ref),
            request.expected_digest,
        )
        if self._rebuilder is not None:
            await self._rebuilder.rebuild_tenant(request.context.tenant_id)
        return SkillPublishInternalResponse(publication=result.model_dump(mode="json"))

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
                force=request.force,
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
                revocation_action=request.revocation_action,
                policy_version=request.policy_version,
                policy_decision_id=request.policy_decision_id,
                command_id=request.command_id,
                expected_revision=request.expected_revision,
                correlation_id=request.context.correlation_id,
                causation_id=request.context.causation_id,
            )
        )
        return SkillRevokeInternalResponse(publication=result.model_dump(mode="json"))

    async def restore(
        self,
        request: SkillRestoreInternalRequest,
    ) -> SkillRestoreInternalResponse:
        self._validate_management_request(
            request.context.service_identity,
            request.context.request_id,
            request.command_id,
        )
        if self._management is None:
            raise SchemaValidationError("Skill management service is not configured")
        result = await self._management.restore_publication(
            RestoreSkillPublicationCommand(
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
        return SkillRestoreInternalResponse(publication=result.model_dump(mode="json"))

    async def package_state(
        self, request: SkillPackageStateInternalRequest
    ) -> SkillPackageStateInternalResponse:
        if request.context.service_identity is not ServiceIdentity.TASK_API:
            raise AuthorizationError("workload may not query Skill package state")
        if self._management is None:
            raise SchemaValidationError("Skill management service is not configured")
        package = await self._management.get_package(
            request.context.tenant_id,
            request.publisher,
            request.name,
            request.version,
        )
        skill_markdown: str | None = None
        if self._package_cache is not None:
            archive = await self._package_cache.load(
                tenant_id=request.context.tenant_id,
                package_digest=package.package_digest,
                artifact_ref=package.artifact_ref,
                actor_id="task-api-skill-admin",
                correlation_id=request.context.correlation_id,
            )
            markdown = archive.files.get("SKILL.md")
            skill_markdown = None if markdown is None else markdown.decode()
        elif self._artifacts is not None:
            content = await self._artifacts.read(
                tenant_id=request.context.tenant_id,
                artifact_ref=package.artifact_ref,
                actor_id="task-api-skill-admin",
                correlation_id=request.context.correlation_id,
            )
            archive = skill_package_from_archive(content)
            if skill_package_digest(archive) != package.package_digest:
                raise VersionConflictError("Persisted Skill package digest does not match")
            markdown = archive.files.get("SKILL.md")
            skill_markdown = None if markdown is None else markdown.decode()
        return SkillPackageStateInternalResponse(
            package=package.model_dump(mode="json"),
            skill_markdown=skill_markdown,
        )

    async def purge(
        self, request: SkillPurgeInternalRequest
    ) -> SkillPurgeInternalResponse:
        self._validate_management_request(
            request.context.service_identity,
            request.context.request_id,
            request.command_id,
        )
        if self._management is None:
            raise SchemaValidationError("Skill management service is not configured")
        package = await self._management.purge_package(
            PurgeSkillPackageCommand(
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
        return SkillPurgeInternalResponse(package=package.model_dump(mode="json"))

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
                None if installation is None else installation.model_dump(mode="json")
            ),
            publication=(
                None if publication is None else publication.model_dump(mode="json")
            ),
        )

    async def register_publisher(
        self, request: SkillPublisherRegisterInternalRequest
    ) -> SkillPublisherInternalResponse:
        self._validate_management_request(
            request.context.service_identity,
            request.context.request_id,
            request.command_id,
        )
        service = self._require_publishers()
        await service.register(
            RegisterSkillPublisherCommand(
                tenant_id=request.context.tenant_id,
                actor_id=request.actor_id,
                publisher=request.publisher,
                display_name=request.display_name,
                command_id=request.command_id,
                expected_revision=request.expected_revision,
                correlation_id=request.context.correlation_id,
                causation_id=request.context.causation_id,
            )
        )
        return await self._publisher_state(
            service, request.context.tenant_id, request.publisher
        )

    async def rotate_publisher_key(
        self, request: SkillPublisherRotateKeyInternalRequest
    ) -> SkillPublisherInternalResponse:
        self._validate_management_request(
            request.context.service_identity,
            request.context.request_id,
            request.command_id,
        )
        service = self._require_publishers()
        await service.rotate_key(
            RotateSkillPublisherKeyCommand(
                tenant_id=request.context.tenant_id,
                actor_id=request.actor_id,
                publisher=request.publisher,
                key_id=request.key_id,
                public_key=request.public_key,
                command_id=request.command_id,
                expected_revision=request.expected_revision,
                correlation_id=request.context.correlation_id,
                causation_id=request.context.causation_id,
            )
        )
        return await self._publisher_state(
            service, request.context.tenant_id, request.publisher
        )

    async def revoke_publisher_key(
        self, request: SkillPublisherRevokeKeyInternalRequest
    ) -> SkillPublisherInternalResponse:
        self._validate_management_request(
            request.context.service_identity,
            request.context.request_id,
            request.command_id,
        )
        service = self._require_publishers()
        await service.revoke_key(
            RevokeSkillPublisherKeyCommand(
                tenant_id=request.context.tenant_id,
                actor_id=request.actor_id,
                publisher=request.publisher,
                key_id=request.key_id,
                reason_code=request.reason_code,
                revocation_action=SkillRevocationAction(request.revocation_action),
                policy_version=request.policy_version,
                policy_decision_id=request.policy_decision_id,
                command_id=request.command_id,
                expected_revision=request.expected_revision,
                correlation_id=request.context.correlation_id,
                causation_id=request.context.causation_id,
            )
        )
        if self._rebuilder is not None:
            await self._rebuilder.rebuild_tenant(request.context.tenant_id)
        return await self._publisher_state(
            service, request.context.tenant_id, request.publisher
        )

    async def change_publisher_status(
        self, request: SkillPublisherStatusInternalRequest
    ) -> SkillPublisherInternalResponse:
        self._validate_management_request(
            request.context.service_identity,
            request.context.request_id,
            request.command_id,
        )
        service = self._require_publishers()
        await service.change_status(
            ChangeSkillPublisherStatusCommand(
                tenant_id=request.context.tenant_id,
                actor_id=request.actor_id,
                publisher=request.publisher,
                operation=SkillPublisherStatusOperation(request.operation),
                reason_code=request.reason_code,
                revocation_action=(
                    SkillRevocationAction(request.revocation_action)
                    if request.revocation_action is not None
                    else None
                ),
                policy_version=request.policy_version,
                policy_decision_id=request.policy_decision_id,
                command_id=request.command_id,
                expected_revision=request.expected_revision,
                correlation_id=request.context.correlation_id,
                causation_id=request.context.causation_id,
            )
        )
        if self._rebuilder is not None:
            await self._rebuilder.rebuild_tenant(request.context.tenant_id)
        return await self._publisher_state(
            service, request.context.tenant_id, request.publisher
        )

    async def publisher_state(
        self, request: SkillPublisherStateInternalRequest
    ) -> SkillPublisherInternalResponse:
        if request.context.service_identity is not ServiceIdentity.TASK_API:
            raise AuthorizationError("workload may not query Skill Publishers")
        service = self._require_publishers()
        return await self._publisher_state(
            service, request.context.tenant_id, request.publisher
        )

    def _require_publishers(self) -> SkillPublisherService:
        if self._publishers is None:
            raise SchemaValidationError("Skill Publisher Registry is not configured")
        return self._publishers

    @staticmethod
    async def _publisher_state(
        service: SkillPublisherService, tenant_id: str, publisher: str
    ) -> SkillPublisherInternalResponse:
        record, keys = await service.get(tenant_id, publisher)
        return SkillPublisherInternalResponse(
            publisher=record.model_dump(mode="json"),
            keys=tuple(key.model_dump(mode="json") for key in keys),
        )
