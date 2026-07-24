from __future__ import annotations

import re
from enum import StrEnum
from typing import Any

from pydantic import Field, field_validator, model_validator

from auraclaw.contracts.internal import ContractModel
from auraclaw.contracts.tools import ArtifactRef

_SKILL_NAME = r"^[a-z0-9]+(?:[._-][a-z0-9]+)*$"
_SEMVER = r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z.-]+)?$"
_DIGEST = r"^sha256:[0-9a-f]{64}$"


class SkillPublicationStatus(StrEnum):
    ACTIVE = "active"
    REVOKED = "revoked"


class SkillToolRequirement(ContractModel):
    name: str = Field(min_length=1, max_length=256)
    version: str = Field(default="*", min_length=1, max_length=128)


class SkillResourceRequirement(ContractModel):
    uri_template: str = Field(min_length=1, max_length=2048)


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
    allowed_roles: tuple[str, ...] = ("coordinator", "worker")
    data_classification: str = "internal"
    risk_level: str = "medium"
    max_steps: int = Field(default=20, ge=1, le=1000)
    timeout_seconds: int = Field(default=900, ge=1, le=86400)
    publisher: str = Field(min_length=1, max_length=128, pattern=_SKILL_NAME)
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
                clause.fullmatch(item.strip()) is None
                for item in requirement.version.split(",")
            ):
                raise ValueError(
                    f"Unsupported Tool version constraint: {requirement.version}"
                )
        return requirements

    @model_validator(mode="after")
    def validate_dependencies(self) -> SkillManifest:
        tool_names = [item.name for item in self.required_tools]
        resource_uris = [item.uri_template for item in self.required_resources]
        if len(tool_names) != len(set(tool_names)):
            raise ValueError("Skill Tool dependencies must be unique")
        if len(resource_uris) != len(set(resource_uris)):
            raise ValueError("Skill Resource dependencies must be unique")
        if not self.allowed_roles or any(not role for role in self.allowed_roles):
            raise ValueError("Skill must allow at least one non-empty role")
        for schema in (self.input_schema, self.output_schema):
            if schema.get("type") != "object":
                raise ValueError("Skill input and output schemas must describe objects")
        return self


class PublishedSkill(ContractModel):
    tenant_id: str
    manifest: SkillManifest
    package_digest: str = Field(pattern=_DIGEST)
    artifact_ref: ArtifactRef
    status: SkillPublicationStatus = SkillPublicationStatus.ACTIVE


class ResolvedSkillTool(ContractModel):
    capability_id: str
    canonical_name: str
    version: str
    schema_digest: str


class ResolvedSkillResource(ContractModel):
    capability_id: str
    server_id: str
    uri_template: str
    content_digest: str


class SkillBinding(ContractModel):
    skill_name: str
    skill_version: str
    publisher: str
    package_digest: str = Field(pattern=_DIGEST)
    artifact_ref: ArtifactRef
    resolved_tools: tuple[ResolvedSkillTool, ...] = ()
    resolved_resources: tuple[ResolvedSkillResource, ...] = ()
    policy_version: str


def is_semver(value: str) -> bool:
    return re.fullmatch(_SEMVER, value) is not None
