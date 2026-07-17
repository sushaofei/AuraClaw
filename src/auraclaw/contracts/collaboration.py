from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class CollaborationRole(StrEnum):
    ROOT = "root"
    COORDINATOR = "coordinator"
    WORKER = "worker"
    REVIEWER = "reviewer"
    REPAIR = "repair"


class ReviewDecision(StrEnum):
    ACCEPTED = "accepted"
    CHANGES_REQUESTED = "changes_requested"
    REJECTED = "rejected"


@dataclass(frozen=True)
class CollaborationLimits:
    max_depth: int = 4
    max_children_per_parent: int = 8
    max_children: int = 32
    max_budget: float = 100.0

    def __post_init__(self) -> None:
        if min(self.max_depth, self.max_children_per_parent, self.max_children) < 1:
            raise ValueError("collaboration limits must be positive")
        if self.max_budget <= 0:
            raise ValueError("collaboration budget must be positive")


@dataclass(frozen=True)
class OutputContract:
    version: str = "1"
    required_fields: tuple[str, ...] = ("summary", "result_ref")
    require_artifacts: bool = False
    require_evidence: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "required_fields": list(self.required_fields),
            "require_artifacts": self.require_artifacts,
            "require_evidence": self.require_evidence,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> OutputContract:
        return cls(
            version=str(value.get("version", "1")),
            required_fields=tuple(str(item) for item in value.get("required_fields", ())),
            require_artifacts=bool(value.get("require_artifacts", False)),
            require_evidence=bool(value.get("require_evidence", False)),
        )

    def validate(self, result: dict[str, Any]) -> None:
        missing = [field_name for field_name in self.required_fields if not result.get(field_name)]
        if self.require_artifacts and not result.get("artifact_refs"):
            missing.append("artifact_refs")
        if self.require_evidence and not result.get("evidence_refs"):
            missing.append("evidence_refs")
        if missing:
            raise ValueError(f"result does not satisfy output contract: {', '.join(missing)}")


@dataclass(frozen=True)
class ChildSpec:
    task_key: str
    role: CollaborationRole
    goal: str
    output_contract: OutputContract
    dependency_ids: tuple[str, ...] = ()
    input_refs: tuple[str, ...] = ()
    tool_permissions: tuple[str, ...] = ()
    budget: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.task_key.strip() or not self.goal.strip():
            raise ValueError("child task_key and goal are required")
        if self.role is CollaborationRole.ROOT:
            raise ValueError("a Child cannot use the root role")
        if self.budget <= 0:
            raise ValueError("child budget must be positive")


@dataclass(frozen=True)
class ChildResult:
    summary: str
    result_ref: str
    artifact_refs: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()
    limitations: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "summary": self.summary,
            "result_ref": self.result_ref,
            "artifact_refs": list(self.artifact_refs),
            "evidence_refs": list(self.evidence_refs),
            "limitations": list(self.limitations),
        }


@dataclass(frozen=True)
class ReviewResult:
    decision: ReviewDecision
    evidence_refs: tuple[str, ...]
    findings: tuple[str, ...] = ()
    repair_suggestions: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.evidence_refs:
            raise ValueError("review decisions require evidence")
