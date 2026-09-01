from __future__ import annotations

import re
from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from auraclaw.contracts.internal import ContractModel
from auraclaw.contracts.tools import ArtifactRef

_SKILL_NAME = r"^[a-z0-9]+(?:[._-][a-z0-9]+)*$"
_SEMVER = r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z.-]+)?$"
_DIGEST = r"^sha256:[0-9a-f]{64}$"


class SkillPublicationStatus(StrEnum):
    STAGED = "staged"
    VALIDATING = "validating"
    ACTIVE = "active"
    RESTORING = "restoring"
    QUARANTINED = "quarantined"
    RETIRED = "retired"
    REVOKED = "revoked"


class SkillRevocationAction(StrEnum):
    CONTINUE = "continue"
    PAUSE = "pause"
    CANCEL = "cancel"


class SkillInstallationStatus(StrEnum):
    ACTIVE = "active"
    DISABLED = "disabled"
    DRAINING = "draining"
    UNINSTALLED = "uninstalled"


class SkillPackageRetentionStatus(StrEnum):
    RETAINED = "retained"
    PURGED = "purged"


class SkillSourceKind(StrEnum):
    BUILTIN = "builtin"
    ADMIN_UPLOAD = "admin_upload"
    MCP = "mcp"
    MODEL_COMPILER = "model_compiler"
    GIT = "git"
    OCI = "oci"


class SkillSourceDesiredState(StrEnum):
    ENABLED = "enabled"
    DISABLED = "disabled"
    RETIRED = "retired"


class SkillPublisherStatus(StrEnum):
    ACTIVE = "active"
    SUSPENDED = "suspended"
    REVOKED = "revoked"


class SkillPublisherStatusOperation(StrEnum):
    SUSPEND = "suspend"
    RESUME = "resume"
    REVOKE = "revoke"


class SkillPublisherKeyStatus(StrEnum):
    ACTIVE = "active"
    RETIRING = "retiring"
    REVOKED = "revoked"


class SkillInstallationOperation(StrEnum):
    INSTALL = "install"
    ENABLE = "enable"
    DISABLE = "disable"
    UNINSTALL = "uninstall"


class PublishSkillCommand(ContractModel):
    tenant_id: str = Field(min_length=1, max_length=128)
    actor_id: str = Field(min_length=1, max_length=256)
    source_id: str = Field(min_length=1, max_length=128)
    activate: bool = True
    command_id: str = Field(min_length=1, max_length=256)
    expected_revision: int = Field(default=0, ge=0)
    correlation_id: str = Field(min_length=1, max_length=256)
    causation_id: str = Field(min_length=1, max_length=256)


class ChangeSkillInstallationCommand(ContractModel):
    tenant_id: str = Field(min_length=1, max_length=128)
    actor_id: str = Field(min_length=1, max_length=256)
    publisher: str = Field(min_length=1, max_length=128, pattern=_SKILL_NAME)
    name: str = Field(min_length=1, max_length=256, pattern=_SKILL_NAME)
    operation: SkillInstallationOperation
    force: bool = False
    reason_code: str | None = Field(default=None, min_length=1, max_length=128)
    command_id: str = Field(min_length=1, max_length=256)
    expected_revision: int = Field(ge=1)
    correlation_id: str = Field(min_length=1, max_length=256)
    causation_id: str = Field(min_length=1, max_length=256)

    @model_validator(mode="after")
    def validate_reason(self) -> ChangeSkillInstallationCommand:
        if self.force and self.operation is not SkillInstallationOperation.UNINSTALL:
            raise ValueError("Force is only valid for uninstall")
        if (
            self.operation
            in {
                SkillInstallationOperation.DISABLE,
                SkillInstallationOperation.UNINSTALL,
            }
            and self.reason_code is None
        ):
            raise ValueError("Disable or uninstall requires a reason")
        if (
            self.operation
            in {
                SkillInstallationOperation.INSTALL,
                SkillInstallationOperation.ENABLE,
            }
            and self.reason_code is not None
        ):
            raise ValueError("Install or enable cannot carry a reason")
        return self


class RevokeSkillPublicationCommand(ContractModel):
    tenant_id: str = Field(min_length=1, max_length=128)
    actor_id: str = Field(min_length=1, max_length=256)
    publisher: str = Field(min_length=1, max_length=128, pattern=_SKILL_NAME)
    name: str = Field(min_length=1, max_length=256, pattern=_SKILL_NAME)
    version: str = Field(pattern=_SEMVER)
    reason_code: str = Field(min_length=1, max_length=128)
    revocation_action: SkillRevocationAction = SkillRevocationAction.CANCEL
    policy_version: str = Field(default="skill-revocation-v1", min_length=1, max_length=128)
    policy_decision_id: str | None = Field(default=None, max_length=256)
    command_id: str = Field(min_length=1, max_length=256)
    expected_revision: int = Field(ge=1)
    correlation_id: str = Field(min_length=1, max_length=256)
    causation_id: str = Field(min_length=1, max_length=256)


class RestoreSkillPublicationCommand(ContractModel):
    tenant_id: str = Field(min_length=1, max_length=128)
    actor_id: str = Field(min_length=1, max_length=256)
    publisher: str = Field(min_length=1, max_length=128, pattern=_SKILL_NAME)
    name: str = Field(min_length=1, max_length=256, pattern=_SKILL_NAME)
    version: str = Field(pattern=_SEMVER)
    reason_code: str = Field(min_length=1, max_length=128)
    command_id: str = Field(min_length=1, max_length=256)
    expected_revision: int = Field(ge=1)
    correlation_id: str = Field(min_length=1, max_length=256)
    causation_id: str = Field(min_length=1, max_length=256)


class PurgeSkillPackageCommand(ContractModel):
    tenant_id: str = Field(min_length=1, max_length=128)
    actor_id: str = Field(min_length=1, max_length=256)
    publisher: str = Field(min_length=1, max_length=128, pattern=_SKILL_NAME)
    name: str = Field(min_length=1, max_length=256, pattern=_SKILL_NAME)
    version: str = Field(pattern=_SEMVER)
    reason_code: str = Field(min_length=1, max_length=128)
    command_id: str = Field(min_length=1, max_length=256)
    expected_revision: int = Field(ge=1)
    correlation_id: str = Field(min_length=1, max_length=256)
    causation_id: str = Field(min_length=1, max_length=256)


class RegisterSkillPublisherCommand(ContractModel):
    tenant_id: str = Field(min_length=1, max_length=128)
    actor_id: str = Field(min_length=1, max_length=256)
    publisher: str = Field(min_length=1, max_length=128, pattern=_SKILL_NAME)
    display_name: str = Field(min_length=1, max_length=256)
    command_id: str = Field(min_length=1, max_length=256)
    expected_revision: int = Field(default=0, ge=0)
    correlation_id: str = Field(min_length=1, max_length=256)
    causation_id: str = Field(min_length=1, max_length=256)


class RotateSkillPublisherKeyCommand(ContractModel):
    tenant_id: str = Field(min_length=1, max_length=128)
    actor_id: str = Field(min_length=1, max_length=256)
    publisher: str = Field(min_length=1, max_length=128, pattern=_SKILL_NAME)
    key_id: str = Field(min_length=1, max_length=128, pattern=_SKILL_NAME)
    public_key: str = Field(min_length=43, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")
    command_id: str = Field(min_length=1, max_length=256)
    expected_revision: int = Field(ge=1)
    correlation_id: str = Field(min_length=1, max_length=256)
    causation_id: str = Field(min_length=1, max_length=256)


class RevokeSkillPublisherKeyCommand(ContractModel):
    tenant_id: str = Field(min_length=1, max_length=128)
    actor_id: str = Field(min_length=1, max_length=256)
    publisher: str = Field(min_length=1, max_length=128, pattern=_SKILL_NAME)
    key_id: str = Field(min_length=1, max_length=128, pattern=_SKILL_NAME)
    reason_code: str = Field(min_length=1, max_length=128)
    revocation_action: SkillRevocationAction = SkillRevocationAction.CANCEL
    policy_version: str = Field(default="skill-revocation-v1", min_length=1, max_length=128)
    policy_decision_id: str | None = Field(default=None, max_length=256)
    command_id: str = Field(min_length=1, max_length=256)
    expected_revision: int = Field(ge=1)
    correlation_id: str = Field(min_length=1, max_length=256)
    causation_id: str = Field(min_length=1, max_length=256)

    @model_validator(mode="after")
    def validate_runtime_action(self) -> RevokeSkillPublisherKeyCommand:
        if self.revocation_action is SkillRevocationAction.CONTINUE:
            raise ValueError("Publisher key revocation action cannot continue")
        return self


class ChangeSkillPublisherStatusCommand(ContractModel):
    tenant_id: str = Field(min_length=1, max_length=128)
    actor_id: str = Field(min_length=1, max_length=256)
    publisher: str = Field(min_length=1, max_length=128, pattern=_SKILL_NAME)
    operation: SkillPublisherStatusOperation
    reason_code: str = Field(min_length=1, max_length=128)
    revocation_action: SkillRevocationAction | None = None
    policy_version: str | None = Field(default=None, max_length=128)
    policy_decision_id: str | None = Field(default=None, max_length=256)
    command_id: str = Field(min_length=1, max_length=256)
    expected_revision: int = Field(ge=1)
    correlation_id: str = Field(min_length=1, max_length=256)
    causation_id: str = Field(min_length=1, max_length=256)

    @model_validator(mode="after")
    def validate_security_action(self) -> ChangeSkillPublisherStatusCommand:
        if self.operation is SkillPublisherStatusOperation.RESUME:
            if any(
                value is not None
                for value in (
                    self.revocation_action,
                    self.policy_version,
                    self.policy_decision_id,
                )
            ):
                raise ValueError("Publisher resume cannot carry security policy")
            return self
        if self.operation is SkillPublisherStatusOperation.REVOKE and (
            self.revocation_action is None or not self.policy_version
        ):
            raise ValueError("Publisher revocation requires security policy")
        if self.operation is SkillPublisherStatusOperation.SUSPEND and (
            (self.revocation_action is None) != (self.policy_version is None)
        ):
            raise ValueError("Publisher suspension policy must be complete")
        if self.revocation_action is SkillRevocationAction.CONTINUE:
            raise ValueError("Publisher security action cannot continue")
        return self


class ConfigureSkillSourceCommand(ContractModel):
    tenant_id: str = Field(min_length=1, max_length=128)
    actor_id: str = Field(min_length=1, max_length=256)
    source_id: str = Field(pattern=r"^sks_[A-Za-z0-9_.-]+$")
    kind: SkillSourceKind
    desired_state: SkillSourceDesiredState
    publisher_allowlist: tuple[str, ...]
    credential_ref: str | None = Field(default=None, max_length=512)
    config_metadata: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=0, ge=-1000, le=1000)
    command_id: str = Field(min_length=1, max_length=256)
    expected_revision: int = Field(ge=0)
    correlation_id: str = Field(min_length=1, max_length=256)
    causation_id: str = Field(min_length=1, max_length=256)


class RetireSkillSourceCommand(ContractModel):
    tenant_id: str = Field(min_length=1, max_length=128)
    actor_id: str = Field(min_length=1, max_length=256)
    source_id: str = Field(pattern=r"^sks_[A-Za-z0-9_.-]+$")
    reason_code: str = Field(min_length=1, max_length=128)
    command_id: str = Field(min_length=1, max_length=256)
    expected_revision: int = Field(ge=1)
    correlation_id: str = Field(min_length=1, max_length=256)
    causation_id: str = Field(min_length=1, max_length=256)


class SkillToolRequirement(ContractModel):
    name: str = Field(min_length=1, max_length=256)
    version: str = Field(default="*", min_length=1, max_length=128)


class SkillResourceRequirement(ContractModel):
    uri_template: str = Field(min_length=1, max_length=2048)


class SkillRequirement(ContractModel):
    name: str = Field(min_length=1, max_length=256, pattern=_SKILL_NAME)
    version: str = Field(default="*", min_length=1, max_length=128)
    publisher: str | None = Field(
        default=None,
        min_length=1,
        max_length=128,
        pattern=_SKILL_NAME,
    )


class SkillWorkflowEntrypoint(ContractModel):
    api_version: Literal["skills.auraclaw.io/v1alpha1"] = (
        "skills.auraclaw.io/v1alpha1"
    )
    entrypoint: str = Field(
        min_length=1,
        max_length=512,
        pattern=r"^scripts/(?:[A-Za-z0-9_.-]+/)*[A-Za-z0-9_.-]+\.workflow\.json$",
    )


class SkillReferenceRequirement(ContractModel):
    path: str = Field(
        min_length=1,
        max_length=512,
        pattern=r"^references/(?:[A-Za-z0-9_.-]+/)*[A-Za-z0-9_.-]+$",
    )
    media_type: Literal["application/json", "text/markdown"]
    max_bytes: int = Field(default=64 * 1024, ge=1, le=1024 * 1024)
    preload: bool = False


class SkillManifest(ContractModel):
    name: str = Field(min_length=1, max_length=256, pattern=_SKILL_NAME)
    version: str = Field(pattern=_SEMVER)
    description: str = Field(min_length=1, max_length=4096)
    applies_when: tuple[str, ...] = ()
    not_when: tuple[str, ...] = ()
    input_schema: dict[str, Any] = Field(default_factory=lambda: {"type": "object"})
    output_schema: dict[str, Any] = Field(default_factory=lambda: {"type": "object"})
    required_tools: tuple[SkillToolRequirement, ...] = ()
    required_resources: tuple[SkillResourceRequirement, ...] = ()
    required_skills: tuple[SkillRequirement, ...] = ()
    workflow: SkillWorkflowEntrypoint | None = None
    required_references: tuple[SkillReferenceRequirement, ...] = ()
    allowed_roles: tuple[str, ...] = ("coordinator", "worker")
    data_classification: str = "internal"
    risk_level: str = "medium"
    max_steps: int = Field(default=20, ge=1, le=1000)
    timeout_seconds: int = Field(default=900, ge=1, le=86400)
    publisher: str = Field(min_length=1, max_length=128, pattern=_SKILL_NAME)
    signature_key_id: str | None = Field(
        default=None, min_length=1, max_length=128, pattern=_SKILL_NAME
    )
    signature: str = Field(pattern=r"^[a-z0-9-]+:[A-Za-z0-9_-]+$")

    @field_validator("required_tools")
    @classmethod
    def validate_tool_version_ranges(
        cls,
        requirements: tuple[SkillToolRequirement, ...],
    ) -> tuple[SkillToolRequirement, ...]:
        clause = re.compile(
            r"^(>=|<=|>|<|==|=)?(0|[1-9]\d*)"
            r"(?:\.(0|[1-9]\d*))?(?:\.(0|[1-9]\d*))?$"
        )
        for requirement in requirements:
            if requirement.version != "*" and any(
                clause.fullmatch(item.strip()) is None for item in requirement.version.split(",")
            ):
                raise ValueError(f"Unsupported Tool version constraint: {requirement.version}")
        return requirements

    @field_validator("required_skills")
    @classmethod
    def validate_skill_version_ranges(
        cls,
        requirements: tuple[SkillRequirement, ...],
    ) -> tuple[SkillRequirement, ...]:
        clause = re.compile(
            r"^(>=|<=|>|<|==|=)?(0|[1-9]\d*)"
            r"(?:\.(0|[1-9]\d*))?(?:\.(0|[1-9]\d*))?$"
        )
        for requirement in requirements:
            if requirement.version != "*" and any(
                clause.fullmatch(item.strip()) is None for item in requirement.version.split(",")
            ):
                raise ValueError(f"Unsupported Skill version constraint: {requirement.version}")
        return requirements

    @model_validator(mode="after")
    def validate_dependencies(self) -> SkillManifest:
        tool_names = [item.name for item in self.required_tools]
        resource_uris = [item.uri_template for item in self.required_resources]
        skill_names = [(item.publisher, item.name) for item in self.required_skills]
        reference_paths = [item.path for item in self.required_references]
        if len(tool_names) != len(set(tool_names)):
            raise ValueError("Skill Tool dependencies must be unique")
        if len(resource_uris) != len(set(resource_uris)):
            raise ValueError("Skill Resource dependencies must be unique")
        if len(skill_names) != len(set(skill_names)):
            raise ValueError("Skill dependencies must be unique")
        if len(reference_paths) != len(set(reference_paths)):
            raise ValueError("Skill reference dependencies must be unique")
        if any(item.name == self.name for item in self.required_skills):
            raise ValueError("Skill cannot depend directly on itself")
        if not self.allowed_roles or any(not role for role in self.allowed_roles):
            raise ValueError("Skill must allow at least one non-empty role")
        for schema in (self.input_schema, self.output_schema):
            if schema.get("type") != "object":
                raise ValueError("Skill input and output schemas must describe objects")
        return self


class SkillPublisherRecord(ContractModel):
    tenant_id: str = Field(min_length=1, max_length=128)
    publisher: str = Field(min_length=1, max_length=128, pattern=_SKILL_NAME)
    display_name: str = Field(min_length=1, max_length=256)
    status: SkillPublisherStatus = SkillPublisherStatus.ACTIVE
    status_reason_code: str | None = Field(default=None, max_length=128)
    status_changed_at: datetime | None = None
    security_action: SkillRevocationAction | None = None
    security_policy_version: str | None = Field(default=None, max_length=128)
    security_policy_decision_id: str | None = Field(default=None, max_length=256)
    revision: int = Field(default=1, ge=1)
    created_by: str = Field(min_length=1, max_length=256)
    updated_by: str = Field(min_length=1, max_length=256)
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def validate_status_evidence(self) -> SkillPublisherRecord:
        if self.status in {
            SkillPublisherStatus.SUSPENDED,
            SkillPublisherStatus.REVOKED,
        } and (
            not self.status_reason_code
            or self.status_changed_at is None
            or self.security_action is None
            or not self.security_policy_version
        ):
            raise ValueError("Non-active Skill Publisher requires security evidence")
        if self.status is SkillPublisherStatus.ACTIVE and any(
            value is not None
            for value in (
                self.status_reason_code,
                self.security_action,
                self.security_policy_version,
                self.security_policy_decision_id,
            )
        ):
            raise ValueError("Active Skill Publisher cannot carry security evidence")
        return self


class SkillPublisherKeyRecord(ContractModel):
    tenant_id: str = Field(min_length=1, max_length=128)
    publisher: str = Field(min_length=1, max_length=128, pattern=_SKILL_NAME)
    key_id: str = Field(min_length=1, max_length=128, pattern=_SKILL_NAME)
    algorithm: str = Field(default="ed25519", pattern=r"^ed25519$")
    public_key: str = Field(min_length=43, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")
    status: SkillPublisherKeyStatus = SkillPublisherKeyStatus.ACTIVE
    revision: int = Field(default=1, ge=1)
    activated_at: datetime
    retired_at: datetime | None = None
    revoked_at: datetime | None = None
    reason_code: str | None = Field(default=None, max_length=128)
    revocation_action: SkillRevocationAction | None = None
    revocation_policy_version: str | None = Field(default=None, max_length=128)
    revocation_policy_decision_id: str | None = Field(default=None, max_length=256)
    created_by: str = Field(min_length=1, max_length=256)
    updated_by: str = Field(min_length=1, max_length=256)
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def validate_state(self) -> SkillPublisherKeyRecord:
        if self.status is SkillPublisherKeyStatus.ACTIVE:
            if self.retired_at is not None or self.revoked_at is not None:
                raise ValueError("Active Publisher key cannot be retired or revoked")
        elif self.status is SkillPublisherKeyStatus.RETIRING:
            if self.retired_at is None or self.revoked_at is not None:
                raise ValueError("Retiring Publisher key requires retired_at")
        else:
            if self.revoked_at is None or not self.reason_code:
                raise ValueError("Revoked Publisher key requires time and reason")
            if self.revocation_action is None or not self.revocation_policy_version:
                raise ValueError("Revoked Publisher key requires runtime policy")
        if self.status is not SkillPublisherKeyStatus.REVOKED and any(
            value is not None
            for value in (
                self.revocation_action,
                self.revocation_policy_version,
                self.revocation_policy_decision_id,
            )
        ):
            raise ValueError("Trusted Publisher key cannot carry revocation policy")
        return self


class PublishedSkill(ContractModel):
    tenant_id: str
    manifest: SkillManifest
    package_digest: str = Field(pattern=_DIGEST)
    artifact_ref: ArtifactRef
    status: SkillPublicationStatus = SkillPublicationStatus.ACTIVE
    revocation_action: SkillRevocationAction | None = None


class SkillPackageRecord(ContractModel):
    tenant_id: str = Field(min_length=1, max_length=128)
    manifest: SkillManifest
    package_digest: str = Field(pattern=_DIGEST)
    artifact_ref: ArtifactRef
    signature_key_id: str | None = Field(default=None, max_length=256)
    retention_status: SkillPackageRetentionStatus = SkillPackageRetentionStatus.RETAINED
    retention_until: datetime
    legal_hold: bool = False
    retention_revision: int = Field(default=1, ge=1)
    retention_updated_by: str = Field(min_length=1, max_length=256)
    retention_updated_at: datetime
    created_at: datetime
    purged_at: datetime | None = None

    @model_validator(mode="after")
    def validate_retention(self) -> SkillPackageRecord:
        if self.retention_status is SkillPackageRetentionStatus.PURGED and self.purged_at is None:
            raise ValueError("Purged Skill package requires purged_at")
        if (
            self.retention_status is SkillPackageRetentionStatus.RETAINED
            and self.purged_at is not None
        ):
            raise ValueError("Retained Skill package cannot have purged_at")
        return self


class SkillPublicationRecord(ContractModel):
    publication_id: str = Field(pattern=r"^skp_[A-Za-z0-9_-]+$")
    tenant_id: str = Field(min_length=1, max_length=128)
    publisher: str = Field(min_length=1, max_length=128, pattern=_SKILL_NAME)
    name: str = Field(min_length=1, max_length=256, pattern=_SKILL_NAME)
    version: str = Field(pattern=_SEMVER)
    package_digest: str = Field(pattern=_DIGEST)
    status: SkillPublicationStatus = SkillPublicationStatus.STAGED
    source_id: str | None = Field(default=None, max_length=128)
    revision: int = Field(default=1, ge=1)
    created_by: str = Field(min_length=1, max_length=256)
    updated_by: str = Field(min_length=1, max_length=256)
    created_at: datetime
    updated_at: datetime
    reason_code: str | None = Field(default=None, max_length=128)
    revocation_action: SkillRevocationAction | None = None
    revocation_policy_version: str | None = Field(default=None, max_length=128)
    revocation_policy_decision_id: str | None = Field(default=None, max_length=256)

    @model_validator(mode="after")
    def validate_status_reason(self) -> SkillPublicationRecord:
        if (
            self.status
            in {
                SkillPublicationStatus.RESTORING,
                SkillPublicationStatus.QUARANTINED,
                SkillPublicationStatus.RETIRED,
                SkillPublicationStatus.REVOKED,
            }
            and not self.reason_code
        ):
            raise ValueError(
                "Restoring, quarantined, retired, or revoked Skill publication requires a reason"
            )
        if self.status is SkillPublicationStatus.REVOKED and (
            self.revocation_action is None or not self.revocation_policy_version
        ):
            raise ValueError(
                "Revoked Skill publication requires an explicit runtime action and policy version"
            )
        if self.status is not SkillPublicationStatus.REVOKED and any(
            value is not None
            for value in (
                self.revocation_action,
                self.revocation_policy_version,
                self.revocation_policy_decision_id,
            )
        ):
            raise ValueError("Only revoked Skill publication may carry revocation policy evidence")
        return self


class SkillInstallationRecord(ContractModel):
    installation_id: str = Field(pattern=r"^ski_[A-Za-z0-9_-]+$")
    tenant_id: str = Field(min_length=1, max_length=128)
    publisher: str = Field(min_length=1, max_length=128, pattern=_SKILL_NAME)
    name: str = Field(min_length=1, max_length=256, pattern=_SKILL_NAME)
    version_constraint: str = Field(default="*", min_length=1, max_length=128)
    pinned_package_digest: str | None = Field(default=None, pattern=_DIGEST)
    status: SkillInstallationStatus = SkillInstallationStatus.ACTIVE
    source_id: str | None = Field(default=None, max_length=128)
    auto_upgrade: bool = True
    revision: int = Field(default=1, ge=1)
    created_by: str = Field(min_length=1, max_length=256)
    updated_by: str = Field(min_length=1, max_length=256)
    created_at: datetime
    updated_at: datetime
    reason_code: str | None = Field(default=None, max_length=128)
    uninstall_action: SkillRevocationAction | None = None
    uninstall_policy_version: str | None = Field(default=None, max_length=128)
    uninstall_policy_decision_id: str | None = Field(default=None, max_length=256)

    @field_validator("version_constraint")
    @classmethod
    def validate_version_constraint(cls, constraint: str) -> str:
        clause = re.compile(
            r"^(>=|<=|>|<|==|=)?(0|[1-9]\d*)"
            r"(?:\.(0|[1-9]\d*))?(?:\.(0|[1-9]\d*))?$"
        )
        if constraint != "*" and any(
            clause.fullmatch(item.strip()) is None for item in constraint.split(",")
        ):
            raise ValueError(f"Unsupported Skill version constraint: {constraint}")
        return constraint

    @model_validator(mode="after")
    def validate_pin(self) -> SkillInstallationRecord:
        if self.pinned_package_digest is not None and self.auto_upgrade:
            raise ValueError("Pinned Skill installation cannot auto-upgrade")
        if (
            self.status
            in {
                SkillInstallationStatus.DISABLED,
                SkillInstallationStatus.DRAINING,
                SkillInstallationStatus.UNINSTALLED,
            }
            and not self.reason_code
        ):
            raise ValueError("Disabled, draining or uninstalled Skill requires a reason")
        if self.status is SkillInstallationStatus.DRAINING and (
            self.uninstall_action is None or not self.uninstall_policy_version
        ):
            raise ValueError("Draining Skill requires uninstall policy evidence")
        if self.uninstall_action is SkillRevocationAction.PAUSE:
            raise ValueError("Skill uninstall action cannot pause")
        if self.uninstall_action is None and any(
            value is not None
            for value in (
                self.uninstall_policy_version,
                self.uninstall_policy_decision_id,
            )
        ):
            raise ValueError("Skill uninstall policy requires an action")
        if self.uninstall_action is not None and not self.uninstall_policy_version:
            raise ValueError("Skill uninstall action requires a policy version")
        if self.status not in {
            SkillInstallationStatus.DRAINING,
            SkillInstallationStatus.UNINSTALLED,
        } and any(
            value is not None
            for value in (
                self.uninstall_action,
                self.uninstall_policy_version,
                self.uninstall_policy_decision_id,
            )
        ):
            raise ValueError("Only draining or uninstalled Skill may carry uninstall evidence")
        return self


class SkillSourceRecord(ContractModel):
    source_id: str = Field(pattern=r"^sks_[A-Za-z0-9_.-]+$")
    tenant_id: str = Field(min_length=1, max_length=128)
    kind: SkillSourceKind
    desired_state: SkillSourceDesiredState = SkillSourceDesiredState.DISABLED
    publisher_allowlist: tuple[str, ...] = ()
    credential_ref: str | None = Field(default=None, max_length=512)
    config_metadata: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=0, ge=-1000, le=1000)
    revision: int = Field(default=1, ge=1)
    created_by: str = Field(min_length=1, max_length=256)
    updated_by: str = Field(min_length=1, max_length=256)
    created_at: datetime
    updated_at: datetime

    @field_validator("publisher_allowlist")
    @classmethod
    def validate_publishers(cls, publishers: tuple[str, ...]) -> tuple[str, ...]:
        if len(publishers) != len(set(publishers)):
            raise ValueError("Skill Source publisher allowlist must be unique")
        if any(re.fullmatch(_SKILL_NAME, publisher) is None for publisher in publishers):
            raise ValueError("Skill Source publisher is invalid")
        return publishers

    @model_validator(mode="after")
    def validate_source_security(self) -> SkillSourceRecord:
        if self.desired_state is SkillSourceDesiredState.ENABLED and not self.publisher_allowlist:
            raise ValueError("Enabled Skill Source requires a publisher allowlist")
        if _contains_sensitive_metadata_key(self.config_metadata):
            raise ValueError(
                "Skill Source metadata cannot contain credentials or secrets; use credential_ref"
            )
        return self


class SkillSourceSyncState(ContractModel):
    source_id: str = Field(pattern=r"^sks_[A-Za-z0-9_.-]+$")
    tenant_id: str = Field(min_length=1, max_length=128)
    generation: int = Field(default=0, ge=0)
    cursor: str | None = Field(default=None, max_length=2048)
    complete_snapshot: bool = False
    last_success_at: datetime | None = None
    last_attempt_at: datetime | None = None
    consecutive_failures: int = Field(default=0, ge=0)
    safe_error_code: str | None = Field(default=None, max_length=128)

    @model_validator(mode="after")
    def validate_sync_evidence(self) -> SkillSourceSyncState:
        if self.complete_snapshot and self.last_success_at is None:
            raise ValueError("Complete Skill Source snapshot requires last_success_at")
        if (
            self.last_success_at is not None
            and self.last_attempt_at is not None
            and self.last_success_at > self.last_attempt_at
        ):
            raise ValueError("Skill Source success cannot be after its last attempt")
        return self


class ResolvedSkillTool(ContractModel):
    capability_id: str
    canonical_name: str
    version: str
    schema_digest: str
    expected_side_effect: Literal["read", "write"] = "write"


class ResolvedSkillResource(ContractModel):
    capability_id: str
    server_id: str
    uri_template: str
    content_digest: str


class ResolvedSkillDependency(ContractModel):
    capability_id: str
    skill_name: str
    skill_version: str
    publisher: str
    package_digest: str = Field(pattern=_DIGEST)
    artifact_ref: ArtifactRef


class ResolvedSkillWorkflow(ContractModel):
    api_version: Literal["skills.auraclaw.io/v1alpha1"]
    entrypoint: str
    workflow_digest: str = Field(pattern=_DIGEST)
    reference_paths: tuple[str, ...] = ()


class SkillBinding(ContractModel):
    skill_name: str
    skill_version: str
    publisher: str
    package_digest: str = Field(pattern=_DIGEST)
    artifact_ref: ArtifactRef
    resolved_tools: tuple[ResolvedSkillTool, ...] = ()
    resolved_resources: tuple[ResolvedSkillResource, ...] = ()
    resolved_skills: tuple[ResolvedSkillDependency, ...] = ()
    resolved_workflow: ResolvedSkillWorkflow | None = None
    policy_version: str
    policy_decision_id: str | None = None
    max_steps: int = Field(ge=1, le=1000)
    timeout_seconds: int = Field(ge=1, le=86400)


class SkillActivation(ContractModel):
    skill_activation_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^ska_[A-Za-z0-9_-]+$",
    )
    activation_key: str = Field(min_length=1, max_length=128)
    binding: SkillBinding
    input_digest: str = Field(pattern=_DIGEST)


def is_semver(value: str) -> bool:
    return re.fullmatch(_SEMVER, value) is not None


def _contains_sensitive_metadata_key(value: Any) -> bool:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = str(key).lower().replace("-", "_")
            if any(
                marker in normalized
                for marker in (
                    "secret",
                    "password",
                    "token",
                    "private_key",
                    "api_key",
                    "access_key",
                    "credential",
                )
            ):
                return True
            if _contains_sensitive_metadata_key(child):
                return True
    elif isinstance(value, (list, tuple)):
        return any(_contains_sensitive_metadata_key(item) for item in value)
    return False
