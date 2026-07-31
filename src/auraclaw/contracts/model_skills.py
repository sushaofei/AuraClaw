from __future__ import annotations

from typing import Any, Literal

from pydantic import Field, model_validator

from auraclaw.contracts.internal import ContractModel
from auraclaw.contracts.skills import SkillRequirement, SkillToolRequirement


class ModelSkillSnapshot(ContractModel):
    """Normalized source material used to build one immutable Skill package."""

    tenant_id: str = Field(min_length=1, max_length=128)
    model: dict[str, Any]
    version: dict[str, Any]
    sections: dict[str, list[dict[str, Any]]]
    source_revision: str = Field(min_length=1, max_length=256)
    source_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")


class ExecutableModelSkillConfig(ContractModel):
    """Validated extension embedded in ct_model_version.config_snapshot_json."""

    schema_version: Literal[
        "auraclaw.model-skill/v1",
        "auraclaw.model-skill/v2",
    ]
    execution_profile: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=256)
    description: str = Field(min_length=1, max_length=4096)
    applies_when: tuple[str, ...] = ()
    not_when: tuple[str, ...] = ()
    input_schema: dict[str, Any] = Field(default_factory=lambda: {"type": "object"})
    output_schema: dict[str, Any] = Field(default_factory=lambda: {"type": "object"})
    required_tools: tuple[SkillToolRequirement, ...] = ()
    required_skills: tuple[SkillRequirement, ...] = ()
    data_tables: tuple[str, ...]
    metric_keys: tuple[str, ...]
    allowed_roles: tuple[str, ...] = ("coordinator", "worker")
    data_classification: str = "internal"
    risk_level: str = "medium"
    max_steps: int = Field(default=20, ge=1, le=1000)
    timeout_seconds: int = Field(default=300, ge=1, le=86400)

    @model_validator(mode="after")
    def validate_contract(self) -> ExecutableModelSkillConfig:
        if self.input_schema.get("type") != "object":
            raise ValueError("Executable Model Skill input_schema must be an object")
        if self.output_schema.get("type") != "object":
            raise ValueError("Executable Model Skill output_schema must be an object")
        if len(self.data_tables) != len(set(self.data_tables)):
            raise ValueError("Executable Model Skill data tables must be unique")
        if len(self.metric_keys) != len(set(self.metric_keys)):
            raise ValueError("Executable Model Skill metric keys must be unique")
        if not self.required_tools and not self.required_skills:
            raise ValueError(
                "Executable Model Skill must require at least one Tool or child Skill"
            )
        if not self.allowed_roles or any(not role for role in self.allowed_roles):
            raise ValueError("Executable Model Skill must allow at least one role")
        return self
