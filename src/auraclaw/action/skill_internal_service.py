from __future__ import annotations

import base64
import binascii
from dataclasses import asdict

from auraclaw.action.skill_lifecycle import SkillLifecycleStore
from auraclaw.action.skill_management import SkillManagementService
from auraclaw.action.skill_packages import SkillPackage
from auraclaw.action.skill_publication import SkillPublicationService
from auraclaw.action.skill_publishers import SkillPublisherService
from auraclaw.action.skill_rebuild import SkillStateRebuilder
from auraclaw.contracts.errors import AuthorizationError, SchemaValidationError
from auraclaw.contracts.internal import (
    ServiceIdentity,
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
)
from auraclaw.contracts.tools import ArtifactRef

_MAX_ENCODED_PACKAGE_BYTES = 24 * 1024 * 1024


class SkillPublicationInternalService:
    def __init__(
        self,
        publication: SkillPublicationService,
        *,
        management: SkillManagementService | None = None,
        rebuilder: SkillStateRebuilder | None = None,
        publishers: SkillPublisherService | None = None,
        admissions: SkillLifecycleStore | None = None,
    ) -> None:
        self._publication = publication
        self._management = management
        self._rebuilder = rebuilder
        self._publishers = publishers
        self._admissions = admissions

    async def list_admissions(
        self, request: SkillAdmissionListInternalRequest
    ) -> SkillAdmissionListInternalResponse:
        self._validate_admission_reader(request.context.service_identity)
        if self._admissions is None:
            raise SchemaValidationError("Skill admission reader is not configured")
        records = await self._admissions.list_admissions(
            request.context.tenant_id,
            outcome=request.outcome,
            stage=request.stage,
            content_policy_version=request.content_policy_version,
            limit=request.limit,
        )
        return SkillAdmissionListInternalResponse(
            admissions=tuple(asdict(record) for record in records)
        )

    async def admission_metrics(
        self, request: SkillAdmissionMetricsInternalRequest
    ) -> SkillAdmissionMetricsInternalResponse:
        self._validate_admission_reader(request.context.service_identity)
        if self._admissions is None:
            raise SchemaValidationError("Skill admission reader is not configured")
        metrics = await self._admissions.admission_metrics(request.context.tenant_id)
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
                source_id=request.source_id,
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
        return SkillRestoreInternalResponse(
            publication=result.model_dump(mode="json")
        )

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
        return SkillPackageStateInternalResponse(
            package=package.model_dump(mode="json")
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
        return await self._publisher_state(service, request.context.tenant_id, request.publisher)

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
        return await self._publisher_state(service, request.context.tenant_id, request.publisher)

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
                command_id=request.command_id,
                expected_revision=request.expected_revision,
                correlation_id=request.context.correlation_id,
                causation_id=request.context.causation_id,
            )
        )
        if self._rebuilder is not None:
            await self._rebuilder.rebuild_tenant(request.context.tenant_id)
        return await self._publisher_state(service, request.context.tenant_id, request.publisher)

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
        return await self._publisher_state(service, request.context.tenant_id, request.publisher)

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
