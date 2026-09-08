from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from auraclaw.contracts.internal import ContractModel

WORKFLOW_API_VERSION = "skills.auraclaw.io/v1alpha1"
_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,127}$")


class WorkflowReference(ContractModel):
    id: str = Field(min_length=1, max_length=128)
    path: str = Field(min_length=1, max_length=512)
    required: bool = True

    @field_validator("id")
    @classmethod
    def validate_identifier(cls, value: str) -> str:
        if _IDENTIFIER.fullmatch(value) is None:
            raise ValueError("Workflow reference id is invalid")
        return value


class WorkflowRetry(ContractModel):
    max_attempts: int = Field(default=1, ge=1, le=5)
    strategy: Literal["none", "exponential"] = "none"
    retry_on: tuple[Literal["timeout", "unavailable", "rate_limited"], ...] = ()


class WorkflowStep(ContractModel):
    id: str = Field(min_length=1, max_length=128)
    operation: Literal["tool.call", "resource.read"]
    capability: str = Field(min_length=1, max_length=256)
    arguments: dict[str, dict[str, Any]] = Field(default_factory=dict)
    result: str = Field(min_length=1, max_length=128)
    timeout_seconds: int = Field(default=30, ge=1, le=3600)
    retry: WorkflowRetry = Field(default_factory=WorkflowRetry)

    @field_validator("id", "result")
    @classmethod
    def validate_identifier(cls, value: str) -> str:
        if _IDENTIFIER.fullmatch(value) is None:
            raise ValueError("Workflow step/result identifier is invalid")
        return value


class SkillWorkflow(ContractModel):
    api_version: Literal["skills.auraclaw.io/v1alpha1"] = Field(alias="apiVersion")
    kind: Literal["Workflow"]
    references: tuple[WorkflowReference, ...] = ()
    steps: tuple[WorkflowStep, ...] = Field(min_length=1, max_length=1000)
    outputs: dict[str, dict[str, Any]] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_uniqueness(self) -> SkillWorkflow:
        step_ids = [step.id for step in self.steps]
        results = [step.result for step in self.steps]
        reference_ids = [reference.id for reference in self.references]
        if len(step_ids) != len(set(step_ids)):
            raise ValueError("Workflow step ids must be unique")
        if len(results) != len(set(results)):
            raise ValueError("Workflow result ids must be unique")
        if len(reference_ids) != len(set(reference_ids)):
            raise ValueError("Workflow reference ids must be unique")
        return self
