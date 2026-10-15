from __future__ import annotations

from typing import Annotated, Any, Protocol

from fastapi import Depends
from pydantic import Field, model_validator

from auraclaw.action.skill_admission_query import SkillAdmissionReader as SkillAdmissionReader
from auraclaw.action.skill_packages import SkillPackage
from auraclaw.api.dependencies import RequestIdentity, request_identity
from auraclaw.contracts.capabilities import CapabilityDescriptor
from auraclaw.contracts.internal import (
    ArtifactFinalizeResponse,
    ContractModel,
)
from auraclaw.contracts.skills import (
    ChangeSkillInstallationCommand,
    ChangeSkillPublisherStatusCommand,
    PublishedSkill,
    PublishSkillCommand,
    PurgeSkillPackageCommand,
    RegisterSkillPublisherCommand,
    RestoreSkillPublicationCommand,
    RevokeSkillPublicationCommand,
    RevokeSkillPublisherKeyCommand,
    RotateSkillPublisherKeyCommand,
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
    activate: bool = True
    expected_installation_revision: int | None = Field(default=None, ge=0)
    files: dict[str, str] | None = Field(default=None, min_length=1, max_length=_MAX_UPLOAD_FILES)
    artifact_ref: dict[str, Any] | None = None
    expected_digest: str | None = Field(default=None, pattern=r"^sha256:[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_payload(self) -> PublishSkillRequest:
        direct = self.files is not None
        staged = self.artifact_ref is not None or self.expected_digest is not None
        if direct == staged:
            raise ValueError("Supply either files or artifact_ref with expected_digest")
        if staged and (self.artifact_ref is None or self.expected_digest is None):
            raise ValueError("Staged publication requires artifact_ref and expected_digest")
        return self


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
    async def stage(
        self,
        *,
        tenant_id: str,
        name: str,
        content: bytes,
        checksum: str,
        correlation_id: str,
        command_id: str,
    ) -> ArtifactFinalizeResponse: ...


class SkillCapabilityAvailability(Protocol):
    async def is_available(self, tenant_id: str, capability: CapabilityDescriptor) -> bool: ...


class SkillManager(Protocol):
    async def get_admin_snapshot(
        self, tenant_id: str
    ) -> tuple[
        tuple[SkillPackageRecord, ...],
        tuple[SkillPublicationRecord, ...],
        tuple[SkillInstallationRecord, ...],
    ]: ...

    async def get_package(
        self,
        tenant_id: str,
        publisher: str,
        name: str,
        version: str,
    ) -> SkillPackageRecord: ...

    async def list_packages(self, tenant_id: str) -> tuple[SkillPackageRecord, ...]: ...

    async def get_installation(
        self, tenant_id: str, publisher: str, name: str
    ) -> SkillInstallationRecord: ...

    async def list_installations(self, tenant_id: str) -> tuple[SkillInstallationRecord, ...]: ...

    async def get_publication(
        self,
        tenant_id: str,
        publisher: str,
        name: str,
        version: str,
    ) -> SkillPublicationRecord: ...

    async def list_publications(self, tenant_id: str) -> tuple[SkillPublicationRecord, ...]: ...

    async def change_installation(
        self, command: ChangeSkillInstallationCommand
    ) -> SkillInstallationRecord: ...

    async def revoke_publication(
        self, command: RevokeSkillPublicationCommand
    ) -> SkillPublicationRecord: ...

    async def restore_publication(
        self, command: RestoreSkillPublicationCommand
    ) -> SkillPublicationRecord: ...

    async def purge_package(self, command: PurgeSkillPackageCommand) -> SkillPackageRecord: ...


class SkillContentReader(Protocol):
    async def get_skill_markdown(
        self,
        tenant_id: str,
        publisher: str,
        name: str,
        version: str,
    ) -> str | None: ...


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

    async def change_publisher_status(
        self, command: ChangeSkillPublisherStatusCommand
    ) -> tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]]: ...

    async def get_publisher(
        self, tenant_id: str, publisher: str
    ) -> tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]]: ...

    async def list_publishers(
        self, tenant_id: str
    ) -> tuple[tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]], ...]: ...
